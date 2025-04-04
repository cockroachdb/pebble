package block

import (
	"encoding/binary"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/golang/snappy"
)

type Compressor interface {
	Compress(dst, src []byte) (CompressionIndicator, []byte)
}

type noopCompressor struct{}
type snappyCompressor struct{}
type zstdCompressor struct{}

var _ Compressor = noopCompressor{}
var _ Compressor = snappyCompressor{}
var _ Compressor = zstdCompressor{}

func (noopCompressor) Compress(dst, src []byte) (CompressionIndicator, []byte) {
	panic("NoCompressionCompressor.Compress() should not be called.")
}

func (snappyCompressor) Compress(dst, src []byte) (CompressionIndicator, []byte) {
	dst = dst[:cap(dst):cap(dst)]
	return SnappyCompressionIndicator, snappy.Encode(dst, src)
}

func GetCompressor(c Compression) Compressor {
	switch c {
	case NoCompression:
		return noopCompressor{}
	case SnappyCompression:
		return snappyCompressor{}
	case ZstdCompression:
		return zstdCompressor{}
	default:
		panic("Invalid compression type.")
	}
}

type Decompressor interface {
	// DecompressInto decompresses compressed into buf. The buf slice must have the
	// exact size as the decompressed value. Callers may use DecompressedLen to
	// determine the correct size.
	DecompressInto(buf, compressed []byte) error

	// DecompressedLen returns the length of the provided block once decompressed,
	// allowing the caller to allocate a buffer exactly sized to the decompressed
	// payload.
	DecompressedLen(b []byte) (decompressedLen int, err error)
}

type noopDecompressor struct{}
type snappyDecompressor struct{}
type zstdDecompressor struct{}

var _ Decompressor = noopDecompressor{}
var _ Decompressor = snappyDecompressor{}
var _ Decompressor = zstdDecompressor{}

func (noopDecompressor) DecompressInto(dst, src []byte) error {
	dst = dst[:len(src)]
	copy(dst, src)
	return nil
}

func (noopDecompressor) DecompressedLen(b []byte) (decompressedLen int, err error) {
	return len(b), nil
}

func (snappyDecompressor) DecompressInto(buf, compressed []byte) error {
	result, err := snappy.Decode(buf, compressed)
	if err != nil {
		return err
	}
	if len(result) != len(buf) || (len(result) > 0 && &result[0] != &buf[0]) {
		return base.CorruptionErrorf("pebble: decompressed into unexpected buffer: %p != %p",
			errors.Safe(result), errors.Safe(buf))
	}
	return nil
}

func (snappyDecompressor) DecompressedLen(b []byte) (decompressedLen int, err error) {
	l, err := snappy.DecodedLen(b)
	return l, err
}

func (zstdDecompressor) DecompressedLen(b []byte) (decompressedLen int, err error) {
	// This will also be used by zlib, bzip2 and lz4 to retrieve the decodedLen
	// if we implement these algorithms in the future.
	decodedLenU64, varIntLen := binary.Uvarint(b)
	if varIntLen <= 0 {
		return 0, base.CorruptionErrorf("pebble: compression block has invalid length")
	}
	return int(decodedLenU64), nil
}

func GetDecompressor(c CompressionIndicator) Decompressor {
	switch c {
	case NoCompressionIndicator:
		return noopDecompressor{}
	case SnappyCompressionIndicator:
		return snappyDecompressor{}
	case ZstdCompressionIndicator:
		return zstdDecompressor{}
	default:
		panic("Invalid compression type.")
	}
}
