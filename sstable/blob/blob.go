// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package blob

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/crc"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/block/blockkind"
	"github.com/cockroachdb/pebble/sstable/colblk"
)

var (
	errClosed = errors.New("blob: writer closed")
)

// FileFormat identifies the format of a blob file.
type FileFormat uint8

// String implements the fmt.Stringer interface.
func (f FileFormat) String() string {
	switch f {
	case FileFormatV2:
		return "blobV2"
	case FileFormatV1:
		return "blobV1"
	default:
		return "unknown"
	}
}

const (
	// FileFormatV1 is the first version of the blob file format.
	FileFormatV1 FileFormat = 1
	// FileFormatV2 adds a property block. The property block offset and length
	// are encoded in a separate v2 footer that comes before the v1 file footer.
	FileFormatV2 FileFormat = 2

	latestFileFormat FileFormat = iota
)

const (
	fileFooterLength   = 38
	fileMagic          = "\xf0\x9f\xaa\xb3\xf0\x9f\xa6\x80" // 🪳🦀
	v2FileFooterLength = 4 + 8 + 8
)

// FileWriterOptions are used to configure the FileWriter.
type FileWriterOptions struct {
	Format        FileFormat
	Compression   *block.CompressionProfile
	ChecksumType  block.ChecksumType
	FlushGovernor block.FlushGovernor
	// Only CPUMeasurer.MeasureCPUBlobFileSecondary is used.
	CpuMeasurer base.CPUMeasurer
}

func (o *FileWriterOptions) ensureDefaults() {
	if o.Format == 0 {
		o.Format = FileFormatV1
	}
	if o.Compression == nil {
		o.Compression = block.SnappyCompression
	}
	if o.ChecksumType == block.ChecksumTypeNone {
		o.ChecksumType = block.ChecksumTypeCRC32c
	}
	if o.FlushGovernor == (block.FlushGovernor{}) {
		o.FlushGovernor = block.MakeFlushGovernor(
			base.DefaultBlockSize,
			base.DefaultBlockSizeThreshold,
			base.SizeClassAwareBlockSizeThreshold,
			nil)
	}
	if o.CpuMeasurer == nil {
		o.CpuMeasurer = base.NoopCPUMeasurer{}
	}
}

// FileWriterStats aggregates statistics about a blob file written by a
// FileWriter.
type FileWriterStats struct {
	BlockCount             uint32
	ValueCount             uint32
	UncompressedValueBytes uint64
	FileLen                uint64
}

// String implements the fmt.Stringer interface.
func (s FileWriterStats) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "{BlockCount: %d, ValueCount: %d, UncompressedValueBytes: %d, FileLen: %d}",
		s.BlockCount, s.ValueCount, s.UncompressedValueBytes, s.FileLen)
	return buf.String()
}

// A FileWriter writes a blob file.
type FileWriter struct {
	fileNum       base.DiskFileNum
	w             objstorage.Writable
	err           error
	format        FileFormat
	valuesEncoder blobValueBlockEncoder
	// indexEncoder is an encoder for the index block. Every blob file has an
	// index block encoding the offsets at which each block is written.
	// Additionally, when rewriting a blob file, the index block's virtualBlocks
	// column is also populated to remap blockIDs to the physical block indexes.
	indexEncoder   indexBlockEncoder
	stats          FileWriterStats
	flushGov       block.FlushGovernor
	physBlockMaker block.PhysicalBlockMaker
	cpuMeasurer    base.CPUMeasurer
	writeQueue     struct {
		wg  sync.WaitGroup
		ch  chan compressedBlock
		err error
	}
}

type compressedBlock struct {
	pb  block.PhysicalBlock
	off uint64
}

// NewFileWriter creates a new FileWriter.
func NewFileWriter(fn base.DiskFileNum, w objstorage.Writable, opts FileWriterOptions) *FileWriter {
	opts.ensureDefaults()
	fw := writerPool.Get().(*FileWriter)
	fw.fileNum = fn
	fw.w = w
	fw.format = opts.Format
	fw.valuesEncoder.Init()
	fw.flushGov = opts.FlushGovernor
	fw.indexEncoder.Init()
	fw.physBlockMaker.Init(opts.Compression, opts.ChecksumType)
	fw.cpuMeasurer = opts.CpuMeasurer
	fw.writeQueue.ch = make(chan compressedBlock)
	fw.writeQueue.wg.Add(1)
	go fw.drainWriteQueue()
	return fw
}

var writerPool = sync.Pool{
	New: func() interface{} { return &FileWriter{} },
}

// AddValue adds the provided value to the blob file, returning a Handle
// identifying the location of the value.
func (w *FileWriter) AddValue(v []byte) Handle {
	// Determine if we should first flush the block.
	if sz := w.valuesEncoder.size(); w.flushGov.ShouldFlush(sz, sz+len(v)) {
		w.flush()
	}
	valuesInBlock := w.valuesEncoder.Count()
	w.stats.ValueCount++
	w.stats.UncompressedValueBytes += uint64(len(v))
	w.valuesEncoder.AddValue(v)
	return Handle{
		BlobFileID: base.BlobFileID(w.fileNum),
		ValueLen:   uint32(len(v)),
		BlockID:    BlockID(w.stats.BlockCount),
		ValueID:    BlockValueID(valuesInBlock),
	}
}

// beginNewVirtualBlock adds a virtual block mapping to the current physical
// block and valueID offset within the block.
//
// When a blob file is rewritten, beginNewVirtualBlock is called for each block
// in the original blob file before adding any of the block's extant values.
// beginNewVirtualBlock records a mapping from the original block ID (referred
// to as a virtual block) to a tuple of the physical block index and the offset
// of the BlockValueIDs within the new physical block.
//
// This mapping is used by readers to determine which physical block contains a
// given virtual block, and how to map BlockValueIDs from the given virtual
// block to BlockValueIDs in the physical block.
func (w *FileWriter) beginNewVirtualBlock(vblockID BlockID) {
	// TODO(jackson): Update tests to use the blob.FileRewriter type and move this
	// into the FileRewriter.
	w.indexEncoder.AddVirtualBlockMapping(vblockID, int(w.stats.BlockCount),
		BlockValueID(w.valuesEncoder.Count()))
}

// EstimatedSize returns an estimate of the disk space consumed by the blob file
// if it were closed now.
func (w *FileWriter) EstimatedSize() uint64 {
	sz := w.stats.FileLen                                   // Completed blocks
	sz += uint64(w.valuesEncoder.size()) + block.TrailerLen // Pending uncompressed block
	// We estimate the size of the index block as 4 bytes per offset, and n+1
	// offsets for n block handles. We don't use an exact accounting because the
	// index block is constructed from the write queue goroutine, so using the
	// exact size would introduce nondeterminism. The index block is small
	// relatively speaking. In practice, offsets should use at most 4 bytes per
	// offset.
	sz += uint64(w.stats.BlockCount+1)*4 + block.TrailerLen // Index block
	sz += fileFooterLength                                  // Footer
	return sz
}

// FlushForTesting flushes the current block to the write queue. Writers should
// generally not call FlushForTesting, and instead let the heuristics configured
// through FileWriterOptions handle flushing.
//
// It's exposed so that tests can force flushes to construct blob files with
// arbitrary structures.
func (w *FileWriter) FlushForTesting() {
	if w.valuesEncoder.Count() == 0 {
		return
	}
	w.flush()
}

// flush flushes the current block to the write queue.
func (w *FileWriter) flush() {
	if w.valuesEncoder.Count() == 0 {
		panic(errors.AssertionFailedf("no values to flush"))
	}
	pb := w.physBlockMaker.Make(w.valuesEncoder.Finish(), blockkind.BlobValue, block.NoFlags)
	compressedLen := uint64(pb.LengthWithoutTrailer())
	w.stats.BlockCount++
	off := w.stats.FileLen
	w.stats.FileLen += compressedLen + block.TrailerLen
	w.writeQueue.ch <- compressedBlock{pb: pb, off: off}
	w.valuesEncoder.Reset()
}

// drainWriteQueue runs in its own goroutine and is responsible for writing
// finished, compressed data blocks to the writable. It reads from w.writeQueue
// until the channel is closed. All value blocks are written by this goroutine.
func (w *FileWriter) drainWriteQueue() {
	defer w.writeQueue.wg.Done()
	// Call once to initialize the CPU measurer.
	w.cpuMeasurer.MeasureCPU(base.CompactionGoroutineBlobFileSecondary)
	for cb := range w.writeQueue.ch {
		length, err := block.WriteAndReleasePhysicalBlock(cb.pb.Take(), w.w)
		// Report to the CPU measurer immediately after writing (note that there
		// may be a time lag until the next block is available to write).
		w.cpuMeasurer.MeasureCPU(base.CompactionGoroutineBlobFileSecondary)
		if err != nil {
			w.writeQueue.err = err
			continue
		}
		w.indexEncoder.AddBlockHandle(block.Handle{
			Offset: cb.off,
			Length: uint64(length.WithoutTrailer()),
		})
	}
}

// Close finishes writing the blob file.
func (w *FileWriter) Close() (FileWriterStats, error) {
	if w.w == nil {
		return FileWriterStats{}, w.err
	}
	// Flush the last block to the write queue if it's non-empty.
	if w.valuesEncoder.Count() > 0 {
		w.flush()
	}
	// Inform the write queue we're finished by closing the channel and wait
	// for it to complete.
	close(w.writeQueue.ch)
	w.writeQueue.wg.Wait()

	err := func() error {
		if w.writeQueue.err != nil {
			return w.writeQueue.err
		}

		if w.stats.BlockCount != uint32(w.indexEncoder.countBlocks) {
			panic(errors.AssertionFailedf("block count mismatch: %d vs %d",
				w.stats.BlockCount, w.indexEncoder.countBlocks))
		}
		if w.stats.BlockCount == 0 {
			panic(errors.AssertionFailedf("no blocks written"))
		}

		// Write the index block.
		indexBlockHandle, err := w.writeMetadataBlock(w.indexEncoder.Finish())
		if err != nil {
			return err
		}

		// Write the properties and the v2 footer if the file format is v2.
		var propBlockHandle block.Handle
		if w.format >= FileFormatV2 {
			var cw colblk.KeyValueBlockWriter
			cw.Init()
			p := Properties{
				CompressionStats: w.physBlockMaker.Compressor.Stats().String(),
			}
			p.writeTo(&cw)
			propBlockHandle, err = w.writeMetadataBlock(cw.Finish(cw.Rows()))
			if err != nil {
				return err
			}
		}
		// Write the footer.
		footer := fileFooter{
			format:          w.format,
			checksumType:    w.physBlockMaker.Checksummer.Type,
			indexHandle:     indexBlockHandle,
			originalFileNum: w.fileNum,
		}
		var footerBuf [v2FileFooterLength + fileFooterLength]byte
		footer.encode((*[fileFooterLength]byte)(footerBuf[v2FileFooterLength:]))
		footerLength := fileFooterLength
		if w.format >= FileFormatV2 {
			footerLength += v2FileFooterLength
			v2footer := v2FileFooter{
				propertiesHandle: propBlockHandle,
			}
			v2footer.encode((*[v2FileFooterLength]byte)(footerBuf[:]))
		}
		if err := w.w.Write(footerBuf[len(footerBuf)-footerLength:]); err != nil {
			return err
		}
		w.stats.FileLen += uint64(footerLength)
		return w.w.Finish()
	}()
	if err != nil {
		w.err = err
		w.w.Abort()
		w.w = nil
		return FileWriterStats{}, err
	}

	// Clean up w and return it to the pool.
	w.indexEncoder.Reset()
	w.valuesEncoder.Reset()
	w.w = nil
	stats := w.stats
	w.stats = FileWriterStats{}
	w.err = errClosed
	w.writeQueue.ch = nil
	w.writeQueue.err = nil
	writerPool.Put(w)
	return stats, nil
}

func (w *FileWriter) writeMetadataBlock(data []byte) (block.Handle, error) {
	pb := w.physBlockMaker.Make(data, blockkind.Metadata, block.DontCompress)
	length, err := block.WriteAndReleasePhysicalBlock(pb.Take(), w.w)
	if err != nil {
		return block.Handle{}, err
	}
	h := block.Handle{
		Offset: w.stats.FileLen,
		Length: uint64(length.WithoutTrailer()),
	}
	w.stats.FileLen += uint64(length.WithTrailer())
	return h, nil
}

// fileFooter contains the information contained within the footer of a blob
// file.
//
// Blob file footer format:
//   - checksum CRC over footer data (4 bytes)
//   - index block offset (8 bytes)
//   - index block length (8 bytes)
//   - checksum type (1 byte)
//   - format (1 byte)
//   - original file number (8 bytes)
//   - blob file magic string (8 bytes)
type fileFooter struct {
	format          FileFormat
	checksumType    block.ChecksumType
	indexHandle     block.Handle
	originalFileNum base.DiskFileNum
}

func (f *fileFooter) decode(b []byte) error {
	if len(b) != fileFooterLength {
		return errors.AssertionFailedf("invalid blob file footer length")
	}
	encodedChecksum := binary.LittleEndian.Uint32(b[0:])
	computedChecksum := crc.New(b[4:]).Value()
	if encodedChecksum != computedChecksum {
		return base.CorruptionErrorf("invalid blob file footer checksum 0x%04x, expected: 0x%04x", encodedChecksum, computedChecksum)
	}
	f.indexHandle.Offset = binary.LittleEndian.Uint64(b[4:])
	f.indexHandle.Length = binary.LittleEndian.Uint64(b[12:])
	f.checksumType = block.ChecksumType(b[20])
	f.format = FileFormat(b[21])
	if f.format < FileFormatV1 || f.format > latestFileFormat {
		return base.CorruptionErrorf("invalid blob file format %x", f.format)
	}
	f.originalFileNum = base.DiskFileNum(binary.LittleEndian.Uint64(b[22:]))
	if string(b[30:]) != fileMagic {
		return base.CorruptionErrorf("invalid blob file magic string %x", b[30:])
	}
	return nil
}

func (f *fileFooter) encode(b *[fileFooterLength]byte) {
	binary.LittleEndian.PutUint64(b[4:], f.indexHandle.Offset)
	binary.LittleEndian.PutUint64(b[12:], f.indexHandle.Length)
	b[20] = byte(f.checksumType)
	b[21] = byte(f.format)
	binary.LittleEndian.PutUint64(b[22:], uint64(f.originalFileNum))
	copy(b[30:], fileMagic)
	footerChecksum := crc.New(b[4:]).Value()
	binary.LittleEndian.PutUint32(b[:4], footerChecksum)
}

// v2FileFooter is an extra footer for blob files in FileFormatV2, which is
// always right before the fileFooter.
//
// Blob v2 extra footer format:
//   - checksum CRC over v2 footer data (4 bytes)
//   - properties block offset (8 bytes)
//   - properties block length (8 bytes)
type v2FileFooter struct {
	propertiesHandle block.Handle
}

func (f *v2FileFooter) decode(b []byte) error {
	if len(b) != v2FileFooterLength {
		return errors.AssertionFailedf("invalid blob file footer length")
	}
	encodedChecksum := binary.LittleEndian.Uint32(b[0:])
	computedChecksum := crc.New(b[4:]).Value()
	if encodedChecksum != computedChecksum {
		return base.CorruptionErrorf("invalid blob file v2 footer checksum 0x%04x, expected: 0x%04x", encodedChecksum, computedChecksum)
	}
	f.propertiesHandle.Offset = binary.LittleEndian.Uint64(b[4:])
	f.propertiesHandle.Length = binary.LittleEndian.Uint64(b[12:])
	return nil
}

func (f *v2FileFooter) encode(b *[v2FileFooterLength]byte) {
	binary.LittleEndian.PutUint64(b[4:], f.propertiesHandle.Offset)
	binary.LittleEndian.PutUint64(b[12:], f.propertiesHandle.Length)
	footerChecksum := crc.New(b[4:]).Value()
	binary.LittleEndian.PutUint32(b[:4], footerChecksum)
}

// FileReader reads a blob file.
// If you update this struct, make sure you also update the magic number in
// StringForTests() in metrics.go.
type FileReader struct {
	r        block.Reader
	footer   fileFooter
	v2Footer v2FileFooter
}

// Assert that FileReader implements the ValueReader interface.
var _ ValueReader = (*FileReader)(nil)

// FileReaderOptions configures a reader of a blob file.
type FileReaderOptions struct {
	block.ReaderOptions
}

func (o FileReaderOptions) ensureDefaults() FileReaderOptions {
	if o.LoggerAndTracer == nil {
		o.LoggerAndTracer = base.NoopLoggerAndTracer{}
	}
	return o
}

// NewFileReader opens a blob file for reading.
//
// In error cases, the objstorage.Readable is still open. The caller remains
// responsible for closing it if necessary.
func NewFileReader(
	ctx context.Context, r objstorage.Readable, ro FileReaderOptions,
) (*FileReader, error) {
	ro = ro.ensureDefaults()

	fileNum := ro.CacheOpts.FileNum

	footerBuf := make([]byte, v2FileFooterLength+fileFooterLength)
	size := r.Size()
	if size < fileFooterLength {
		return nil, base.CorruptionErrorf("pebble: invalid blob file %s (file size is too small)",
			errors.Safe(fileNum))
	}
	if size < fileFooterLength+v2FileFooterLength {
		footerBuf = footerBuf[v2FileFooterLength:]
	}

	rh := r.NewReadHandle(objstorage.ReadBeforeForNewReader)

	offset := size - int64(len(footerBuf))
	encodedFooter, err := block.ReadRaw(ctx, r, rh, ro.LoggerAndTracer, fileNum, footerBuf, offset)
	_ = rh.Close()
	if err != nil {
		return nil, err
	}

	fr := &FileReader{}
	if err := fr.footer.decode(encodedFooter[len(encodedFooter)-fileFooterLength:]); err != nil {
		return nil, err
	}
	if fr.footer.format >= FileFormatV2 {
		if size < fileFooterLength+v2FileFooterLength {
			return nil, base.CorruptionErrorf("pebble: invalid blob file %s (v2 file size is too small)",
				errors.Safe(fileNum))
		}
		if err := fr.v2Footer.decode(encodedFooter[:v2FileFooterLength]); err != nil {
			return nil, err
		}
	}
	fr.r.Init(r, ro.ReaderOptions, fr.footer.checksumType)
	return fr, nil
}

// Close implements io.Closer, closing the underlying Readable.
func (r *FileReader) Close() error {
	return r.r.Close()
}

// InitReadHandle initializes a read handle for the file reader, using the
// provided preallocated read handle.
func (r *FileReader) InitReadHandle(
	rh *objstorageprovider.PreallocatedReadHandle,
) objstorage.ReadHandle {
	return objstorageprovider.UsePreallocatedReadHandle(r.r.Readable(), objstorage.NoReadBefore, rh)
}

// ReadValueBlock reads a value block from the file.
func (r *FileReader) ReadValueBlock(
	ctx context.Context, env block.ReadEnv, rh objstorage.ReadHandle, h block.Handle,
) (block.BufferHandle, error) {
	return r.r.Read(ctx, env, rh, h, blockkind.BlobValue, initBlobValueBlockMetadata)
}

// ReadIndexBlock reads the index block from the file.
func (r *FileReader) ReadIndexBlock(
	ctx context.Context, env block.ReadEnv, rh objstorage.ReadHandle,
) (block.BufferHandle, error) {
	return r.r.Read(ctx, env, rh, r.footer.indexHandle, blockkind.Metadata, initIndexBlockMetadata)
}

// IndexHandle returns the block handle for the file's index block.
func (r *FileReader) IndexHandle() block.Handle {
	return r.footer.indexHandle
}

// Layout returns the layout (block organization) as a string for a blob file.
func (r *FileReader) Layout() (string, error) {
	ctx := context.TODO()
	var buf bytes.Buffer

	if r.footer.format == FileFormatV2 {
		h := r.v2Footer.propertiesHandle
		fmt.Fprintf(&buf, "properties block: offset=%d length=%d\n", h.Offset, h.Length)
	}

	indexH, err := r.ReadIndexBlock(ctx, block.NoReadEnv, nil /* rh */)
	if err != nil {
		return "", err
	}
	defer indexH.Release()

	indexDecoder := indexBlockDecoder{}
	indexDecoder.Init(indexH.BlockData())

	if indexDecoder.virtualBlockCount > 0 {
		fmt.Fprintf(&buf, "virtual blocks mapping:\n")
		for i := range indexDecoder.virtualBlockCount {
			blockIndex, valueIDOffset := indexDecoder.RemapVirtualBlockID(BlockID(i))
			fmt.Fprintf(&buf, "virtual block %d -> physical block %d (valueID offset: %d)\n",
				i, blockIndex, valueIDOffset)
		}
		fmt.Fprintf(&buf, "\n")
	}

	fmt.Fprintf(&buf, "physical blocks:\n")
	for i := range indexDecoder.BlockCount() {
		handle := indexDecoder.BlockHandle(i)
		fmt.Fprintf(&buf, "block %d: offset=%d length=%d\n", i, handle.Offset, handle.Length)

		valueBlockH, err := r.ReadValueBlock(ctx, block.NoReadEnv, nil /* rh */, handle)
		if err != nil {
			return "", err
		}

		valueDecoder := blobValueBlockDecoder{}
		valueDecoder.Init(valueBlockH.BlockData())

		fmt.Fprintf(&buf, "values: %d\n", valueDecoder.bd.Rows())
		fmt.Fprintf(&buf, "%s", valueDecoder.bd.FormattedString())

		valueBlockH.Release()
	}

	return buf.String(), nil
}

type Properties struct {
	CompressionStats string
}

// String returns any set properties, one per line.
func (p *Properties) String() string {
	var buf bytes.Buffer
	if p.CompressionStats != "" {
		fmt.Fprintf(&buf, "%s: %s\n", propertyKeyCompressionStats, p.CompressionStats)
	}
	return buf.String()
}

func (p *Properties) set(key []byte, value []byte) {
	switch string(key) {
	case propertyKeyCompressionStats:
		p.CompressionStats = string(value)
	default:
		// Ignore unknown properties (for forward compatibility).
	}
}

func (p *Properties) writeTo(w *colblk.KeyValueBlockWriter) {
	if p.CompressionStats != "" {
		w.AddKV([]byte(propertyKeyCompressionStats), []byte(p.CompressionStats))
	}
}

const propertyKeyCompressionStats = "compression_stats"

// ReadProperties reads the properties block from the file, if it exists.
func (r *FileReader) ReadProperties(ctx context.Context) (Properties, error) {
	if r.footer.format != FileFormatV2 {
		return Properties{}, nil
	}
	// We don't want the property block to go into the block cache, so we use a
	// buffer pool.
	var bufferPool block.BufferPool
	bufferPool.Init(1)
	defer bufferPool.Release()
	b, err := r.r.Read(
		ctx, block.NoReadEnv, nil /* readHandle */, r.v2Footer.propertiesHandle, blockkind.Metadata,
		func(*block.Metadata, []byte) error { return nil },
	)
	if err != nil {
		return Properties{}, err
	}
	defer b.Release()
	var decoder colblk.KeyValueBlockDecoder
	decoder.Init(b.BlockData())
	var p Properties
	for k, v := range decoder.All() {
		p.set(k, v)
	}
	return p, nil
}
