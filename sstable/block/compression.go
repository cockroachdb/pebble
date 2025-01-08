// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package block

import (
	"encoding/binary"
	"math/rand/v2"
	"slices"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/bytealloc"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/golang/snappy"
)

// Compression is the per-block compression algorithm to use.
type Compression int

// The available compression types.
const (
	DefaultCompression Compression = iota
	NoCompression
	SnappyCompression
	ZstdCompression
	NCompression
)

// String implements fmt.Stringer, returning a human-readable name for the
// compression algorithm.
func (c Compression) String() string {
	switch c {
	case DefaultCompression:
		return "Default"
	case NoCompression:
		return "NoCompression"
	case SnappyCompression:
		return "Snappy"
	case ZstdCompression:
		return "ZSTD"
	default:
		return "Unknown"
	}
}

// CompressionFromString returns an sstable.Compression from its
// string representation. Inverse of c.String() above.
func CompressionFromString(s string) Compression {
	switch s {
	case "Default":
		return DefaultCompression
	case "NoCompression":
		return NoCompression
	case "Snappy":
		return SnappyCompression
	case "ZSTD":
		return ZstdCompression
	default:
		return DefaultCompression
	}
}

// CompressionIndicator is the byte stored physically within the block.Trailer
// to indicate the compression type.
//
// TODO(jackson): Avoid exporting once all compression and decompression is
// delegated to the block package.
type CompressionIndicator byte

// The block type gives the per-block compression format.
// These constants are part of the file format and should not be changed.
// They are different from the Compression constants because the latter
// are designed so that the zero value of the Compression type means to
// use the default compression (which is snappy).
// Not all compression types listed here are supported.
const (
	NoCompressionIndicator     CompressionIndicator = 0
	SnappyCompressionIndicator CompressionIndicator = 1
	ZlibCompressionIndicator   CompressionIndicator = 2
	Bzip2CompressionIndicator  CompressionIndicator = 3
	Lz4CompressionIndicator    CompressionIndicator = 4
	Lz4hcCompressionIndicator  CompressionIndicator = 5
	XpressCompressionIndicator CompressionIndicator = 6
	ZstdCompressionIndicator   CompressionIndicator = 7
)

// String implements fmt.Stringer.
func (i CompressionIndicator) String() string {
	switch i {
	case 0:
		return "none"
	case 1:
		return "snappy"
	case 2:
		return "zlib"
	case 3:
		return "bzip2"
	case 4:
		return "lz4"
	case 5:
		return "lz4hc"
	case 6:
		return "xpress"
	case 7:
		return "zstd"
	default:
		panic(errors.Newf("sstable: unknown block type: %d", i))
	}
}

// DecompressedLen returns the length of the provided block once decompressed,
// allowing the caller to allocate a buffer exactly sized to the decompressed
// payload. For some compression algorithms, the payload is prefixed with a
// varint encoding the length of the decompressed block. In such cases, a
// non-zero prefixLength is returned indicating the length of this prefix.
func DecompressedLen(
	algo CompressionIndicator, b []byte,
) (decompressedLen int, prefixLength int, err error) {
	switch algo {
	case NoCompressionIndicator:
		return len(b), 0, nil
	case SnappyCompressionIndicator:
		l, err := snappy.DecodedLen(b)
		return l, 0, err
	case ZstdCompressionIndicator:
		// This will also be used by zlib, bzip2 and lz4 to retrieve the decodedLen
		// if we implement these algorithms in the future.
		decodedLenU64, varIntLen := binary.Uvarint(b)
		if varIntLen <= 0 {
			return 0, 0, base.CorruptionErrorf("pebble/table: compression block has invalid length")
		}
		return int(decodedLenU64), varIntLen, nil
	default:
		return 0, 0, base.CorruptionErrorf("pebble/table: unknown block compression: %d", errors.Safe(algo))
	}
}

// DecompressInto decompresses compressed into buf. The buf slice must have the
// exact size as the decompressed value. Callers may use DecompressedLen to
// determine the correct size.
func DecompressInto(algo CompressionIndicator, compressed []byte, buf []byte) error {
	var result []byte
	var err error
	switch algo {
	case NoCompressionIndicator:
		result = buf[:len(compressed)]
		copy(result, compressed)
	case SnappyCompressionIndicator:
		result, err = snappy.Decode(buf, compressed)
	case ZstdCompressionIndicator:
		result, err = decodeZstd(buf, compressed)
	default:
		return base.CorruptionErrorf("pebble/table: unknown block compression: %d", errors.Safe(algo))
	}
	if err != nil {
		return base.MarkCorruptionError(err)
	}
	if len(result) != len(buf) || (len(result) > 0 && &result[0] != &buf[0]) {
		return base.CorruptionErrorf("pebble/table: decompressed into unexpected buffer: %p != %p",
			errors.Safe(result), errors.Safe(buf))
	}
	return nil
}

// PhysicalBlock represents a block (possibly compressed) as it is stored
// physically on disk, including its trailer.
type PhysicalBlock struct {
	// data contains the possibly compressed block data.
	data    []byte
	trailer Trailer
}

// NewPhysicalBlock returns a new PhysicalBlock with the provided block
// data. The trailer is set from the last TrailerLen bytes of the
// block. The data could be compressed.
func NewPhysicalBlock(data []byte) PhysicalBlock {
	trailer := Trailer(data[len(data)-TrailerLen:])
	data = data[:len(data)-TrailerLen]
	return PhysicalBlock{data: data, trailer: trailer}
}

// LengthWithTrailer returns the length of the data block, including the trailer.
func (b *PhysicalBlock) LengthWithTrailer() int {
	return len(b.data) + TrailerLen
}

// LengthWithoutTrailer returns the length of the data block, excluding the trailer.
func (b *PhysicalBlock) LengthWithoutTrailer() int {
	return len(b.data)
}

// CloneWithByteAlloc returns a deep copy of the block, using the provided
// bytealloc.A to allocate memory for the new copy.
func (b *PhysicalBlock) CloneWithByteAlloc(a *bytealloc.A) PhysicalBlock {
	var data []byte
	*a, data = (*a).Alloc(len(b.data))
	copy(data, b.data)
	return PhysicalBlock{
		data:    data,
		trailer: b.trailer,
	}
}

// Clone returns a deep copy of the block.
func (b PhysicalBlock) Clone() PhysicalBlock {
	data := make([]byte, len(b.data))
	copy(data, b.data)
	return PhysicalBlock{data: data, trailer: b.trailer}
}

// CloneUsingBuf makes a copy of the block data, using the given slice if it has
// enough capacity.
func (b PhysicalBlock) CloneUsingBuf(buf []byte) (_ PhysicalBlock, newBuf []byte) {
	newBuf = append(buf[:0], b.data...)
	return PhysicalBlock{data: newBuf, trailer: b.trailer}, newBuf
}

// IsCompressed returns true if the block is compressed.
func (b *PhysicalBlock) IsCompressed() bool {
	return CompressionIndicator(b.trailer[0]) != NoCompressionIndicator
}

// WriteTo writes the block (including its trailer) to the provided Writable. If
// err == nil, n is the number of bytes successfully written to the Writable.
//
// WriteTo might mangle the block data.
func (b *PhysicalBlock) WriteTo(w objstorage.Writable) (n int, err error) {
	if err := w.Write(b.data); err != nil {
		return 0, err
	}
	if err := w.Write(b.trailer[:]); err != nil {
		return 0, err
	}
	return len(b.data) + len(b.trailer), nil
}

// CompressAndChecksum compresses and checksums the provided block, returning
// the compressed block and its trailer. The dst argument is used for the
// compressed payload if it's sufficiently large. If it's not, a new buffer is
// allocated and *dst is updated to point to it.
//
// If the compressed block is not sufficiently smaller than the original block,
// the compressed payload is discarded and the original, uncompressed block is
// used to avoid unnecessary decompression overhead at read time.
func CompressAndChecksum(
	dst *[]byte, block []byte, compression Compression, checksummer *Checksummer,
) PhysicalBlock {
	// Compress the buffer, discarding the result if the improvement isn't at
	// least 12.5%.
	algo := NoCompressionIndicator
	if compression != NoCompression {
		var compressed []byte
		algo, compressed = compress(compression, block, *dst)
		if algo != NoCompressionIndicator && cap(compressed) > cap(*dst) {
			*dst = compressed[:cap(compressed)]
		}
		if len(compressed) < len(block)-len(block)/8 {
			block = compressed
		} else {
			algo = NoCompressionIndicator
		}
	}

	// Calculate the checksum.
	pb := PhysicalBlock{data: block}
	checksum := checksummer.Checksum(block, byte(algo))
	pb.trailer = MakeTrailer(byte(algo), checksum)
	return pb
}

// compress compresses a sstable block, using dstBuf as the desired destination.
//
// The result is aliased to dstBuf if that buffer had enough capacity, otherwise
// it is a newly-allocated buffer.
func compress(
	compression Compression, b []byte, dstBuf []byte,
) (indicator CompressionIndicator, compressed []byte) {
	switch compression {
	case SnappyCompression:
		// snappy relies on the length of the buffer, and not the capacity to
		// determine if it needs to make an allocation.
		dstBuf = dstBuf[:cap(dstBuf):cap(dstBuf)]
		return SnappyCompressionIndicator, snappy.Encode(dstBuf, b)
	case ZstdCompression:
		if len(dstBuf) < binary.MaxVarintLen64 {
			dstBuf = append(dstBuf, make([]byte, binary.MaxVarintLen64-len(dstBuf))...)
		}
		varIntLen := binary.PutUvarint(dstBuf, uint64(len(b)))
		return ZstdCompressionIndicator, encodeZstd(dstBuf, varIntLen, b)
	default:
		panic("unreachable")
	}
}

// A Buffer is a buffer for encoding a block. The caller mutates the buffer to
// construct the uncompressed block, and calls CompressAndChecksum to produce
// the physical, possibly-compressed PhysicalBlock. A Buffer recycles byte
// slices used in construction of the uncompressed block and the compressed
// physical block.
type Buffer struct {
	h           *BufHandle
	compression Compression
	checksummer Checksummer
}

// Init configures the BlockBuffer with the specified compression and checksum
// type.
func (b *Buffer) Init(compression Compression, checksumType ChecksumType) {
	b.h = uncompressedBuffers.Get()
	b.h.b = b.h.b[:0]
	b.compression = compression
	b.checksummer.Type = checksumType
}

// Get returns the byte slice currently backing the Buffer.
func (b *Buffer) Get() []byte {
	return b.h.b
}

// Size returns the current size of the buffer.
func (b *Buffer) Size() int {
	return len(b.h.b)
}

// Append appends the contents of v to the buffer, returning the offset at which
// it was appended, growing the buffer if necessary.
func (b *Buffer) Append(v []byte) int {
	// We may need to grow b.Buffer to accommodate the new value. If necessary,
	// double the size of the buffer until it's sufficiently large.
	off := len(b.h.b)
	newLen := off + len(v)
	if cap(b.h.b) < newLen {
		size := max(2*cap(b.h.b), 1024)
		for size < newLen {
			size *= 2
		}
		b.h.b = slices.Grow(b.h.b, size-cap(b.h.b))
	}
	b.h.b = b.h.b[:newLen]
	if n := copy(b.h.b[off:], v); n != len(v) {
		panic("incorrect length computation")
	}
	return off
}

// Resize resizes the buffer to the specified length, allocating if necessary.
func (b *Buffer) Resize(length int) {
	if length > cap(b.h.b) {
		b.h.b = slices.Grow(b.h.b, length-len(b.h.b))
	}
	b.h.b = b.h.b[:length]
}

// CompressAndChecksum compresses and checksums the block data, returning a
// PhysicalBlock that is owned by the caller. The returned PhysicalBlock's
// memory is backed by the returned BufHandle. If non-nil, the returned
// BufHandle may be Released once the caller is done with the physical block to
// recycle the block's underlying memory.
//
// When CompressAndChecksum returns, the callee has been reset and is ready to
// be reused.
func (b *Buffer) CompressAndChecksum() (PhysicalBlock, *BufHandle) {
	// Grab a buffer to use as the destination for compression.
	compressedBuf := compressedBuffers.Get()
	pb := CompressAndChecksum(&compressedBuf.b, b.h.b, b.compression, &b.checksummer)
	if pb.IsCompressed() {
		// Compression was fruitful, and pb's data points into compressedBuf. We
		// can reuse b.Buffer because we've copied the compressed data.
		b.h.b = b.h.b[:0]
		return pb, compressedBuf
	}
	// Compression was not fruitful, and pb's data points into b.h. The
	// compressedBuf we retrieved from the pool isn't needed, but our b.h is.
	// Use the compressedBuf as the new b.h.
	pbHandle := b.h
	b.h = compressedBuf
	b.h.b = b.h.b[:0]
	return pb, pbHandle
}

// SetCompression changes the compression algorithm used by CompressAndChecksum.
func (b *Buffer) SetCompression(compression Compression) {
	b.compression = compression
}

// Release may be called when a buffer will no longer be used. It releases to
// pools any memory held by the Buffer so that it may be reused.
func (b *Buffer) Release() {
	if b.h != nil {
		b.h.Release()
		b.h = nil
	}
}

// BufHandle is a handle to a buffer that can be released to a pool for reuse.
type BufHandle struct {
	pool *bufferSyncPool
	b    []byte
}

// Release releases the buffer back to the pool for reuse.
func (h *BufHandle) Release() {
	if invariants.Enabled && (h.pool == nil) != (h.b == nil) {
		panic(errors.AssertionFailedf("pool (%t) and buffer (%t) nilness disagree", h.pool == nil, h.b == nil))
	}
	if invariants.Enabled && (h.pool != nil && h.pool.Max == 0) {
		panic(errors.AssertionFailedf("pool has no maximum size"))
	}
	// Note we avoid releasing buffers that are larger than the configured
	// maximum to the pool. This avoids holding on to occassional large buffers
	// necesary for, for example, single large values.
	if h.b != nil && len(h.b) < h.pool.Max {
		if invariants.Enabled {
			// Set the bytes to a random value. Cap the number of bytes being
			// randomized to prevent test timeouts.
			l := min(cap(h.b), 1000)
			h.b = h.b[:l:l]
			for j := range h.b {
				h.b[j] = byte(rand.Uint32())
			}
		}
		h.pool.pool.Put(h)
	}
}

var (
	// uncompressedBuffers is a pool of buffers that were used to store
	// uncompressed block data. These buffers should be sized right around the
	// configured block size. If multiple Pebble engines are running in the same
	// process, they all share a pool and the size of the buffers may vary.
	uncompressedBuffers = bufferSyncPool{Max: 256 << 10, Default: 4 << 10}
	// compressedBuffers is a pool of buffers that were used to store compressed
	// block data. These buffers will vary significantly in size depending on
	// the compressibility of the data.
	compressedBuffers = bufferSyncPool{Max: 128 << 10, Default: 4 << 10}
)

// bufferSyncPool is a pool of block buffers for memory re-use.
type bufferSyncPool struct {
	Max     int
	Default int
	pool    sync.Pool
}

// Get retrieves a new buf from the pool, or allocates one of the configured
// default size if the pool is empty.
func (p *bufferSyncPool) Get() *BufHandle {
	v := p.pool.Get()
	if v != nil {
		return v.(*BufHandle)
	}
	if invariants.Enabled && p.Default == 0 {
		// Guard against accidentally forgetting to initialize a buffer sync pool.
		panic(errors.AssertionFailedf("buffer pool has no default size"))
	}
	return &BufHandle{b: make([]byte, 0, p.Default), pool: p}
}
