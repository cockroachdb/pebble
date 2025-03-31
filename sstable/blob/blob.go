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
	"github.com/cockroachdb/pebble/sstable/valblk"
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
	fileFooterLength = 33
	fileMagic        = "\xf0\x9f\xaa\xb3\xf0\x9f\xa6\x80" // 🪳🦀
)

// FileWriterOptions are used to configure the FileWriter.
type FileWriterOptions struct {
	Compression   block.Compression
	ChecksumType  block.ChecksumType
	FlushGovernor block.FlushGovernor
}

func (o *FileWriterOptions) ensureDefaults() {
	if o.Compression <= block.DefaultCompression || o.Compression >= block.NCompression {
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
}

// FileWriterStats aggregates statistics about a blob file written by a
// FileWriter.
type FileWriterStats struct {
	BlockCount             uint32
	ValueCount             uint32
	BlockLenLongest        uint64
	UncompressedValueBytes uint64
	FileLen                uint64
}

// String implements the fmt.Stringer interface.
func (s FileWriterStats) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "{BlockCount: %d, ValueCount: %d, BlockLenLongest: %d, UncompressedValueBytes: %d, FileLen: %d}",
		s.BlockCount, s.ValueCount, s.BlockLenLongest, s.UncompressedValueBytes, s.FileLen)
	return buf.String()
}

// A FileWriter writes a blob file.
type FileWriter struct {
	fileNum      base.DiskFileNum
	w            objstorage.Writable
	b            block.Buffer
	stats        FileWriterStats
	flushGov     block.FlushGovernor
	blockOffsets []uint64
	err          error
	checksumType block.ChecksumType
	writeQueue   struct {
		wg  sync.WaitGroup
		ch  chan compressedBlock
		err error
	}
}

type compressedBlock struct {
	pb  block.PhysicalBlock
	bh  *block.BufHandle
	off uint64
}

// NewFileWriter creates a new FileWriter.
func NewFileWriter(fn base.DiskFileNum, w objstorage.Writable, opts FileWriterOptions) *FileWriter {
	opts.ensureDefaults()
	fw := writerPool.Get().(*FileWriter)
	fw.fileNum = fn
	fw.w = w
	fw.b.Init(opts.Compression, opts.ChecksumType)
	fw.flushGov = opts.FlushGovernor
	fw.checksumType = opts.ChecksumType
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
	if sz := w.b.Size(); sz != 0 && w.flushGov.ShouldFlush(sz, sz+len(v)) {
		w.Flush()
	}
	w.stats.ValueCount++
	w.stats.UncompressedValueBytes += uint64(len(v))
	off := uint32(w.b.Append(v))
	return Handle{
		FileNum:       w.fileNum,
		BlockNum:      uint32(w.stats.BlockCount),
		OffsetInBlock: off,
		ValueLen:      uint32(len(v)),
	}
}

// EstimatedSize returns an estimate of the disk space consumed by the blob file
// if it were closed now.
func (w *FileWriter) EstimatedSize() uint64 {
	sz := w.stats.FileLen                                    // Completed blocks
	sz += uint64(w.b.Size()) + block.TrailerLen              // Pending uncompressed block
	sz += uint64(w.stats.BlockCount+1)*12 + block.TrailerLen // Index block (worst case of 12 bytes per block [4 per integer])
	sz += fileFooterLength                                   // Footer
	return sz
}

// Flush flushes the current block to the write queue. Writers should generally
// not call Flush, and instead let the heuristics configured through
// FileWriterOptions handle flushing.
//
// It's exposed so that tests can force flushes to construct blob files with
// arbitrary structures.
func (w *FileWriter) Flush() {
	if w.b.Size() == 0 {
		return
	}
	pb, bh := w.b.CompressAndChecksum()
	compressedLen := uint64(pb.LengthWithoutTrailer())
	w.stats.BlockCount++
	w.stats.BlockLenLongest = max(w.stats.BlockLenLongest, compressedLen)
	off := w.stats.FileLen
	w.stats.FileLen += compressedLen + block.TrailerLen
	w.writeQueue.ch <- compressedBlock{pb: pb, bh: bh, off: off}
}

// drainWriteQueue runs in its own goroutine and is responsible for writing
// finished, compressed data blocks to the writable. It reads from w.writeQueue
// until the channel is closed. All value blocks are written by this goroutine.
func (w *FileWriter) drainWriteQueue() {
	defer w.writeQueue.wg.Done()
	for cb := range w.writeQueue.ch {
		_, err := cb.pb.WriteTo(w.w)
		if err != nil {
			w.writeQueue.err = err
			continue
		}
		w.blockOffsets = append(w.blockOffsets, cb.off)
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
	defer func() {
		if w.w != nil {
			w.w.Abort()
			w.w = nil
		}
		if w.err == nil {
			w.err = errClosed
		}
	}()
	// Flush the last block to the write queue if it's non-empty.
	if w.b.Size() > 0 {
		w.Flush()
	}
	// Inform the write queue we're finished by closing the channel and wait
	// for it to complete.
	close(w.writeQueue.ch)
	w.writeQueue.wg.Wait()
	if w.writeQueue.err != nil {
		w.err = w.writeQueue.err
		return FileWriterStats{}, w.err
	}
	stats := w.stats
	if stats.BlockCount != uint32(len(w.blockOffsets)) {
		panic(errors.AssertionFailedf("block count mismatch: %d vs %d",
			stats.BlockCount, len(w.blockOffsets)))
	}
	if stats.BlockCount == 0 {
		panic(errors.AssertionFailedf("no blocks written"))
	}

	// Write the index block.
	vbih := valblk.IndexHandle{
		Handle:                block.Handle{Offset: stats.FileLen},
		BlockNumByteLength:    uint8(lenLittleEndian(uint64(stats.BlockCount - 1))),
		BlockOffsetByteLength: uint8(lenLittleEndian(w.blockOffsets[len(w.blockOffsets)-1])),
		BlockLengthByteLength: uint8(lenLittleEndian(stats.BlockLenLongest)),
	}
	indexBlockLen := vbih.RowWidth() * int(stats.BlockCount)
	vbih.Handle.Length = uint64(indexBlockLen)
	{
		w.b.Resize(indexBlockLen)
		b := w.b.Get()
		for i := 0; i < len(w.blockOffsets); i++ {
			littleEndianPut(uint64(i), b, int(vbih.BlockNumByteLength))
			b = b[int(vbih.BlockNumByteLength):]
			littleEndianPut(w.blockOffsets[i], b, int(vbih.BlockOffsetByteLength))
			b = b[int(vbih.BlockOffsetByteLength):]
			var l uint64
			if i == len(w.blockOffsets)-1 {
				l = stats.FileLen - w.blockOffsets[i] - block.TrailerLen
			} else {
				l = w.blockOffsets[i+1] - w.blockOffsets[i] - block.TrailerLen
			}
			littleEndianPut(l, b, int(vbih.BlockLengthByteLength))
			b = b[int(vbih.BlockLengthByteLength):]
		}
		if len(b) != 0 {
			panic(errors.AssertionFailedf("incorrect length calculation: buffer has %d bytes remaining of expected %d byte block",
				len(b), indexBlockLen))
		}
		w.b.SetCompression(block.NoCompression)
		pb, bh := w.b.CompressAndChecksum()
		if _, w.err = pb.WriteTo(w.w); w.err != nil {
			return FileWriterStats{}, w.err
		}
		bh.Release()
		stats.FileLen += vbih.Handle.Length + block.TrailerLen
	}

	// Write the footer.
	footer := fileFooter{
		format:      FileFormatV1,
		checksum:    w.checksumType,
		indexHandle: vbih,
	}
	w.b.Resize(fileFooterLength)
	footerBuf := w.b.Get()
	footer.encode(footerBuf)
	if w.err = w.w.Write(footerBuf); w.err != nil {
		return FileWriterStats{}, w.err
	}
	stats.FileLen += fileFooterLength
	if w.err = w.w.Finish(); w.err != nil {
		return FileWriterStats{}, w.err
	}

	// Clean up w and return it to the pool.
	w.b.Release()
	blockOffsets := w.blockOffsets[:0]
	*w = FileWriter{}
	w.blockOffsets = blockOffsets
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
//   - index block block-number byte length (1 byte)
//   - index block block-offset byte length (1 byte)
//   - index block block-length byte length (1 byte)
//   - checksum type (1 byte)
//   - format (1 byte)
//   - blob file magic string (8 bytes)
type fileFooter struct {
	format      FileFormat
	checksum    block.ChecksumType
	indexHandle valblk.IndexHandle
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
	f.indexHandle.Handle.Offset = binary.LittleEndian.Uint64(b[4:])
	f.indexHandle.Handle.Length = binary.LittleEndian.Uint64(b[12:])
	f.indexHandle.BlockNumByteLength = b[20]
	f.indexHandle.BlockOffsetByteLength = b[21]
	f.indexHandle.BlockLengthByteLength = b[22]
	if f.indexHandle.BlockNumByteLength > 4 {
		return base.CorruptionErrorf("invalid block num byte length %d", f.indexHandle.BlockNumByteLength)
	}
	if f.indexHandle.BlockOffsetByteLength > 8 {
		return base.CorruptionErrorf("invalid block offset byte length %d", f.indexHandle.BlockOffsetByteLength)
	}
	if f.indexHandle.BlockLengthByteLength > 8 {
		return base.CorruptionErrorf("invalid block length byte length %d", f.indexHandle.BlockLengthByteLength)
	}

	f.checksum = block.ChecksumType(b[23])
	f.format = FileFormat(b[24])
	if f.format != FileFormatV1 {
		return base.CorruptionErrorf("invalid blob file format %x", f.format)
	}
	if string(b[25:]) != fileMagic {
		return base.CorruptionErrorf("invalid blob file magic string %x", b[25:])
	}
	return nil
}

func (f *fileFooter) encode(b []byte) {
	binary.LittleEndian.PutUint64(b[4:], f.indexHandle.Handle.Offset)
	binary.LittleEndian.PutUint64(b[12:], f.indexHandle.Handle.Length)
	b[20] = f.indexHandle.BlockNumByteLength
	b[21] = f.indexHandle.BlockOffsetByteLength
	b[22] = f.indexHandle.BlockLengthByteLength
	b[23] = byte(f.checksum)
	b[24] = byte(f.format)
	copy(b[25:], fileMagic)
	footerChecksum := crc.New(b[4:]).Value()
	binary.LittleEndian.PutUint32(b[:4], footerChecksum)
}

// FileReader reads a blob file.
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
	defer rh.Close()

	encodedFooter, err := block.ReadRaw(ctx, r, rh, ro.LoggerAndTracer, fileNum, footerBuf[:], off)
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
	return r.r.Read(ctx, env, rh, h, noInitBlockMetadata)
}

// ReadValueIndexBlock reads the index block from the file.
func (r *FileReader) ReadValueIndexBlock(
	ctx context.Context, env block.ReadEnv, rh objstorage.ReadHandle,
) (block.BufferHandle, error) {
	return r.r.Read(ctx, env, rh, r.footer.indexHandle.Handle, noInitBlockMetadata)
}

// ValueIndexHandle returns the index handle for the file's value block.
func (r *FileReader) ValueIndexHandle() valblk.IndexHandle {
	return r.footer.indexHandle
}

func noInitBlockMetadata(_ *block.Metadata, _ []byte) error { return nil }

// lenLittleEndian returns the minimum number of bytes needed to encode v
// using little endian encoding.
func lenLittleEndian(v uint64) int {
	n := 0
	for i := 0; i < 8; i++ {
		n++
		v = v >> 8
		if v == 0 {
			break
		}
	}
	return n
}

// littleEndianPut writes v to b using little endian encoding, under the
// assumption that v can be represented using n bytes.
func littleEndianPut(v uint64, b []byte, n int) {
	_ = b[n-1] // bounds check
	for i := 0; i < n; i++ {
		b[i] = byte(v)
		v = v >> 8
	}
}
