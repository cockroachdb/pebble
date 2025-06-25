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
)

var (
	errClosed = errors.New("blob: writer closed")
)

// FileFormat identifies the format of a blob file.
type FileFormat uint8

// String implements the fmt.Stringer interface.
func (f FileFormat) String() string {
	switch f {
	case FileFormatV1:
		return "blobV1"
	default:
		return "unknown"
	}
}

const (
	// FileFormatV1 is the first version of the blob file format.
	FileFormatV1 FileFormat = 1
)

const (
	fileFooterLength = 38
	fileMagic        = "\xf0\x9f\xaa\xb3\xf0\x9f\xa6\x80" // 🪳🦀
)

// FileWriterOptions are used to configure the FileWriter.
type FileWriterOptions struct {
	Compression   *block.CompressionProfile
	ChecksumType  block.ChecksumType
	FlushGovernor block.FlushGovernor
	// Only CPUMeasurer.MeasureCPUBlobFileSecondary is used.
	CpuMeasurer base.CPUMeasurer
}

func (o *FileWriterOptions) ensureDefaults() {
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
	valuesEncoder blobValueBlockEncoder
	// indexEncoder is an encoder for the index block. Every blob file has an
	// index block encoding the offsets at which each block is written.
	// Additionally, when rewriting a blob file, the index block's virtualBlocks
	// column is also populated to remap blockIDs to the physical block indexes.
	indexEncoder indexBlockEncoder
	stats        FileWriterStats
	flushGov     block.FlushGovernor
	checksummer  block.Checksummer
	compressor   block.Compressor
	cpuMeasurer  base.CPUMeasurer
	writeQueue   struct {
		wg  sync.WaitGroup
		ch  chan compressedBlock
		err error
	}
}

type compressedBlock struct {
	pb  block.PhysicalBlock
	bh  *block.TempBuffer
	off uint64
}

// NewFileWriter creates a new FileWriter.
func NewFileWriter(fn base.DiskFileNum, w objstorage.Writable, opts FileWriterOptions) *FileWriter {
	opts.ensureDefaults()
	fw := writerPool.Get().(*FileWriter)
	fw.fileNum = fn
	fw.w = w
	fw.valuesEncoder.Init()
	fw.flushGov = opts.FlushGovernor
	fw.indexEncoder.Init()
	fw.checksummer = block.Checksummer{Type: opts.ChecksumType}
	fw.compressor = block.MakeCompressor(opts.Compression)
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
	pb, bh := block.CompressAndChecksumToTempBuffer(w.valuesEncoder.Finish(), blockkind.BlobValue, &w.compressor, &w.checksummer)
	compressedLen := uint64(pb.LengthWithoutTrailer())
	w.stats.BlockCount++
	off := w.stats.FileLen
	w.stats.FileLen += compressedLen + block.TrailerLen
	w.writeQueue.ch <- compressedBlock{pb: pb, bh: bh, off: off}
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
		_, err := cb.pb.WriteTo(w.w)
		// Report to the CPU measurer immediately after writing (note that there
		// may be a time lag until the next block is available to write).
		w.cpuMeasurer.MeasureCPU(base.CompactionGoroutineBlobFileSecondary)
		if err != nil {
			w.writeQueue.err = err
			continue
		}
		w.indexEncoder.AddBlockHandle(block.Handle{
			Offset: cb.off,
			Length: uint64(cb.pb.LengthWithoutTrailer()),
		})
		// We're done with the buffer associated with this physical block.
		// Release it back to its pool.
		cb.bh.Release()
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
	var err error
	if w.writeQueue.err != nil {
		err = w.writeQueue.err
		if w.w != nil {
			w.w.Abort()
		}
		return FileWriterStats{}, err
	}
	stats := w.stats
	if stats.BlockCount != uint32(w.indexEncoder.countBlocks) {
		panic(errors.AssertionFailedf("block count mismatch: %d vs %d",
			stats.BlockCount, w.indexEncoder.countBlocks))
	}
	if stats.BlockCount == 0 {
		panic(errors.AssertionFailedf("no blocks written"))
	}

	// Write the index block.
	var indexBlockHandle block.Handle
	{
		indexBlock := w.indexEncoder.Finish()
		var compressedBuf []byte
		pb := block.CopyAndChecksum(&compressedBuf, indexBlock, blockkind.Metadata, &w.compressor, &w.checksummer)
		if _, w.err = pb.WriteTo(w.w); w.err != nil {
			err = w.err
			if w.w != nil {
				w.w.Abort()
			}
			return FileWriterStats{}, err
		}
		indexBlockHandle.Offset = stats.FileLen
		indexBlockHandle.Length = uint64(pb.LengthWithoutTrailer())
		stats.FileLen += uint64(pb.LengthWithTrailer())
	}

	// Write the footer.
	footer := fileFooter{
		format:          FileFormatV1,
		checksum:        w.checksummer.Type,
		indexHandle:     indexBlockHandle,
		originalFileNum: w.fileNum,
	}
	footerBuf := make([]byte, fileFooterLength)
	footer.encode(footerBuf)
	if w.err = w.w.Write(footerBuf); w.err != nil {
		err = w.err
		if w.w != nil {
			w.w.Abort()
		}
		return FileWriterStats{}, err
	}
	stats.FileLen += fileFooterLength
	if w.err = w.w.Finish(); w.err != nil {
		err = w.err
		if w.w != nil {
			w.w.Abort()
		}
		return FileWriterStats{}, err
	}

	// Clean up w and return it to the pool.
	w.indexEncoder.Reset()
	w.valuesEncoder.Reset()
	w.w = nil
	w.stats = FileWriterStats{}
	w.err = errClosed
	w.writeQueue.ch = nil
	w.writeQueue.err = nil
	writerPool.Put(w)
	return stats, nil
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
	checksum        block.ChecksumType
	indexHandle     block.Handle
	originalFileNum base.DiskFileNum
}

func (f *fileFooter) decode(b []byte) error {
	if uint64(len(b)) != fileFooterLength {
		return errors.AssertionFailedf("invalid blob file footer length")
	}
	encodedChecksum := binary.LittleEndian.Uint32(b[0:])
	computedChecksum := crc.New(b[4:]).Value()
	if encodedChecksum != computedChecksum {
		return base.CorruptionErrorf("invalid blob file checksum 0x%04x, expected: 0x%04x", encodedChecksum, computedChecksum)
	}
	f.indexHandle.Offset = binary.LittleEndian.Uint64(b[4:])
	f.indexHandle.Length = binary.LittleEndian.Uint64(b[12:])
	f.checksum = block.ChecksumType(b[20])
	f.format = FileFormat(b[21])
	if f.format != FileFormatV1 {
		return base.CorruptionErrorf("invalid blob file format %x", f.format)
	}
	f.originalFileNum = base.DiskFileNum(binary.LittleEndian.Uint64(b[22:]))
	if string(b[30:]) != fileMagic {
		return base.CorruptionErrorf("invalid blob file magic string %x", b[30:])
	}
	return nil
}

func (f *fileFooter) encode(b []byte) {
	binary.LittleEndian.PutUint64(b[4:], f.indexHandle.Offset)
	binary.LittleEndian.PutUint64(b[12:], f.indexHandle.Length)
	b[20] = byte(f.checksum)
	b[21] = byte(f.format)
	binary.LittleEndian.PutUint64(b[22:], uint64(f.originalFileNum))
	copy(b[30:], fileMagic)
	footerChecksum := crc.New(b[4 : 30+len(fileMagic)]).Value()
	binary.LittleEndian.PutUint32(b[:4], footerChecksum)
}

// FileReader reads a blob file.
// If you update this struct, make sure you also update the magic number in
// StringForTests() in metrics.go.
type FileReader struct {
	r      block.Reader
	footer fileFooter
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

	var footerBuf [fileFooterLength]byte
	size := r.Size()
	off := size - fileFooterLength
	if size < fileFooterLength {
		return nil, base.CorruptionErrorf("pebble: invalid blob file %s (file size is too small)",
			errors.Safe(fileNum))
	}
	var preallocRH objstorageprovider.PreallocatedReadHandle
	rh := objstorageprovider.UsePreallocatedReadHandle(
		r, objstorage.ReadBeforeForNewReader, &preallocRH)

	encodedFooter, err := block.ReadRaw(ctx, r, rh, ro.LoggerAndTracer, fileNum, footerBuf[:], off)
	_ = rh.Close()
	if err != nil {
		return nil, err
	}

	fr := &FileReader{}
	if err := fr.footer.decode(encodedFooter); err != nil {
		return nil, err
	}
	fr.r.Init(r, ro.ReaderOptions, fr.footer.checksum)
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
