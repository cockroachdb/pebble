// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package block

import (
	"slices"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/bytealloc"
	"github.com/cockroachdb/pebble/internal/compression"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/objstorage"
)

// Compression is the per-block compression algorithm to use.
type Compression int

// The available compression types.
const (
	DefaultCompression Compression = iota
	NoCompression
	SnappyCompression
	ZstdCompression
	// MinLZCompression is only supported with table formats v6+. Older formats
	// fall back to snappy.
	MinLZCompression
	NCompression
)

var profiles = [...]CompressionProfile{
	DefaultCompression: SimpleCompressionProfile(DefaultCompression.String(), compression.Snappy),
	NoCompression:      SimpleCompressionProfile(NoCompression.String(), compression.None),
	SnappyCompression:  SimpleCompressionProfile(SnappyCompression.String(), compression.Snappy),
	ZstdCompression:    SimpleCompressionProfile(ZstdCompression.String(), compression.ZstdLevel3),
	MinLZCompression:   SimpleCompressionProfile(MinLZCompression.String(), compression.MinLZFastest),
}

func (c Compression) ToProfile() *CompressionProfile {
	return &profiles[c]
}

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
	case MinLZCompression:
		return "MinLZ"
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
	case "MinLZ":
		return MinLZCompression
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
	MinLZCompressionIndicator  CompressionIndicator = 8
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
	case 8:
		return "minlz"
	default:
		panic(errors.Newf("sstable: unknown block type: %d", i))
	}
}

func (i CompressionIndicator) algorithm() compression.Algorithm {
	switch i {
	case NoCompressionIndicator:
		return compression.NoCompression
	case SnappyCompressionIndicator:
		return compression.SnappyAlgorithm
	case ZstdCompressionIndicator:
		return compression.Zstd
	case MinLZCompressionIndicator:
		return compression.MinLZ
	default:
		panic("Invalid compression type.")
	}
}

func compressionIndicatorFromAlgorithm(algo compression.Algorithm) CompressionIndicator {
	switch algo {
	case compression.NoCompression:
		return NoCompressionIndicator
	case compression.SnappyAlgorithm:
		return SnappyCompressionIndicator
	case compression.Zstd:
		return ZstdCompressionIndicator
	case compression.MinLZ:
		return MinLZCompressionIndicator
	default:
		panic("invalid algorithm")
	}
}

// DecompressedLen returns the length of the provided block once decompressed,
// allowing the caller to allocate a buffer exactly sized to the decompressed
// payload.
func DecompressedLen(ci CompressionIndicator, b []byte) (decompressedLen int, err error) {
	decompressor := GetDecompressor(ci)
	defer decompressor.Close()
	return decompressor.DecompressedLen(b)
}

// DecompressInto decompresses compressed into buf. The buf slice must have the
// exact size as the decompressed value. Callers may use DecompressedLen to
// determine the correct size.
func DecompressInto(ci CompressionIndicator, compressed []byte, buf []byte) error {
	decompressor := GetDecompressor(ci)
	defer decompressor.Close()
	err := decompressor.DecompressInto(buf, compressed)
	if err != nil {
		return base.MarkCorruptionError(err)
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

	// WriteTo is allowed to mangle the data. Mangle it ourselves some of the time
	// in invariant builds to catch callers that don't handle this.
	if invariants.Enabled && invariants.Sometimes(1) {
		for i := range b.data {
			b.data[i] = 0xFF
		}
	}
	return len(b.data) + len(b.trailer), nil
}

// CompressAndChecksum compresses and checksums the provided block, returning
// the compressed block and its trailer. The result is appended to the dst
// argument.
//
// If the compressed block is not sufficiently smaller than the original block,
// the compressed payload is discarded and the original, uncompressed block data
// is used to avoid unnecessary decompression overhead at read time.
func CompressAndChecksum(
	dst *[]byte, blockData []byte, blockKind Kind, compressor *Compressor, checksummer *Checksummer,
) PhysicalBlock {
	buf := (*dst)[:0]
	ci, buf := compressor.Compress(buf, blockData, blockKind)
	*dst = buf

	// Calculate the checksum.
	pb := PhysicalBlock{data: buf}
	checksum := checksummer.Checksum(buf, byte(ci))
	pb.trailer = MakeTrailer(byte(ci), checksum)
	return pb
}

// CompressAndChecksumToTempBuffer compresses and checksums the provided block
// into a TempBuffer. The caller should Release() the TempBuffer once it is no
// longer necessary.
func CompressAndChecksumToTempBuffer(
	blockData []byte, blockKind Kind, compressor *Compressor, checksummer *Checksummer,
) (PhysicalBlock, *TempBuffer) {
	// Grab a buffer to use as the destination for compression.
	compressedBuf := NewTempBuffer()
	pb := CompressAndChecksum(&compressedBuf.b, blockData, blockKind, compressor, checksummer)
	return pb, compressedBuf
}

// TempBuffer is a buffer that is used temporarily and is released back to a
// pool for reuse.
type TempBuffer struct {
	b []byte
}

// NewTempBuffer returns a TempBuffer from the pool. The buffer will have zero
// size and length and arbitrary capacity.
func NewTempBuffer() *TempBuffer {
	tb := tempBufferPool.Get().(*TempBuffer)
	if invariants.Enabled && len(tb.b) > 0 {
		panic("NewTempBuffer length not 0")
	}
	return tb
}

// Data returns the byte slice currently backing the Buffer.
func (tb *TempBuffer) Data() []byte {
	return tb.b
}

// Size returns the current size of the buffer.
func (tb *TempBuffer) Size() int {
	return len(tb.b)
}

// Append appends the contents of v to the buffer, growing the buffer if
// necessary. Returns the offset at which it was appended.
func (tb *TempBuffer) Append(v []byte) (startOffset int) {
	startOffset = len(tb.b)
	tb.b = append(tb.b, v...)
	return startOffset
}

// Resize resizes the buffer to the specified length, allocating if necessary.
// If the length is longer than the current length, the values of the new bytes
// are arbitrary.
func (tb *TempBuffer) Resize(length int) {
	if length > cap(tb.b) {
		tb.b = slices.Grow(tb.b, length-len(tb.b))
	}
	tb.b = tb.b[:length]
}

// Reset is equivalent to Resize(0).
func (tb *TempBuffer) Reset() {
	tb.b = tb.b[:0]
}

// Release releases the buffer back to the pool for reuse.
func (tb *TempBuffer) Release() {
	// Note we avoid releasing buffers that are larger than the configured
	// maximum to the pool. This avoids holding on to occasional large buffers
	// necessary for e.g. singular large values.
	if tb.b != nil && len(tb.b) < tempBufferMaxReusedSize {
		if invariants.Sometimes(20) {
			// Mangle the buffer data.
			for i := range tb.b {
				tb.b[i] = 0xCC
			}
		}
		tb.b = tb.b[:0]
		tempBufferPool.Put(tb)
	}
}

// tempBufferPool is a pool of buffers that are used to temporarily hold either
// compressed or uncompressed block data.
var tempBufferPool = sync.Pool{
	New: func() any {
		return &TempBuffer{b: make([]byte, 0, tempBufferInitialSize)}
	},
}

const tempBufferInitialSize = 32 * 1024
const tempBufferMaxReusedSize = 256 * 1024
