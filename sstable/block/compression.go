// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package block

import (
	"encoding/binary"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/bytealloc"
	"github.com/cockroachdb/pebble/internal/cache"
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
		return 0, 0, nil
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

// Decompress decompresses an sstable block into memory manually allocated with
// `cache.Alloc`.  NB: If Decompress returns (nil, nil), no decompression was
// necessary and the caller may use `b` directly.
func Decompress(algo CompressionIndicator, b []byte) (*cache.Value, error) {
	if algo == NoCompressionIndicator {
		return nil, nil
	}
	// first obtain the decoded length.
	decodedLen, prefixLen, err := DecompressedLen(algo, b)
	if err != nil {
		return nil, err
	}
	b = b[prefixLen:]
	// Allocate sufficient space from the cache.
	decoded := cache.Alloc(decodedLen)
	decodedBuf := decoded.Buf()
	if err := DecompressInto(algo, b, decodedBuf); err != nil {
		cache.Free(decoded)
		return nil, err
	}
	return decoded, nil
}

// PhysicalBlock represents a block (possibly compressed) as it is stored
// physically on disk, including its trailer.
type PhysicalBlock struct {
	// data contains the possibly compressed block data.
	data    []byte
	trailer Trailer
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

// IsCompressed returns true if the block is compressed.
func (b *PhysicalBlock) IsCompressed() bool {
	return CompressionIndicator(b.trailer[0]) != NoCompressionIndicator
}

// WriteTo writes the block (including its trailer) to the provided Writable. If
// err == nil, n is the number of bytes successfully written to the Writable.
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
	pb.trailer[0] = byte(algo)
	checksum := checksummer.Checksum(block, pb.trailer[:1])
	pb.trailer = MakeTrailer(byte(algo), checksum)
	return pb
}

// compress compresses a sstable block, using dstBuf as the desired destination.
func compress(
	compression Compression, b []byte, dstBuf []byte,
) (indicator CompressionIndicator, compressed []byte) {
	switch compression {
	case SnappyCompression:
		return SnappyCompressionIndicator, snappy.Encode(dstBuf, b)
	case NoCompression:
		return NoCompressionIndicator, b
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
