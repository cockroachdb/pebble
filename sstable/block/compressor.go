package block

import (
	"encoding/binary"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/golang/snappy"
	"github.com/minio/minlz"
)

type Compressor interface {
	Compress(dst, src []byte) (CompressionIndicator, []byte)

	// Close must be called when the Compressor is no longer needed.
	// After Close is called, the Compressor must not be used again.
	Close()
}

type noopCompressor struct{}
type snappyCompressor struct{}
type minlzCompressor struct{}

var _ Compressor = noopCompressor{}
var _ Compressor = snappyCompressor{}
var _ Compressor = minlzCompressor{}

func (noopCompressor) Compress(dst, src []byte) (CompressionIndicator, []byte) {
	dst = append(dst[:0], src...)
	return NoCompressionIndicator, dst
}
func (noopCompressor) Close() {}

func (snappyCompressor) Compress(dst, src []byte) (CompressionIndicator, []byte) {
	dst = dst[:cap(dst):cap(dst)]
	return SnappyCompressionIndicator, snappy.Encode(dst, src)
}

func (snappyCompressor) Close() {}

func (minlzCompressor) Compress(dst, src []byte) (CompressionIndicator, []byte) {
	// MinLZ cannot encode blocks greater than 8MB. Fall back to Snappy in those
	// cases. Note that MinLZ can decode the Snappy compressed block.
	if len(src) > minlz.MaxBlockSize {
		_, result := (snappyCompressor{}).Compress(dst, src)
		return MinLZCompressionIndicator, result
	}

	compressed, err := minlz.Encode(dst, src, minlz.LevelFastest)
	if err != nil {
		panic(errors.Wrap(err, "minlz compression"))
	}
	return MinLZCompressionIndicator, compressed
}

func (minlzCompressor) Close() {}

func GetCompressor(c Compression) Compressor {
	switch c {
	case NoCompression:
		return noopCompressor{}
	case SnappyCompression:
		return snappyCompressor{}
	case ZstdCompression:
		return getZstdCompressor()
	case MinLZCompression:
		return minlzCompressor{}
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

	// Close must be called when the Decompressor is no longer needed.
	// After Close is called, the Decompressor must not be used again.
	Close()
}

type noopDecompressor struct{}
type snappyDecompressor struct{}
type minlzDecompressor struct{}

var _ Decompressor = noopDecompressor{}
var _ Decompressor = snappyDecompressor{}
var _ Decompressor = minlzDecompressor{}

func (noopDecompressor) DecompressInto(dst, src []byte) error {
	dst = dst[:len(src)]
	copy(dst, src)
	return nil
}

func (noopDecompressor) DecompressedLen(b []byte) (decompressedLen int, err error) {
	return len(b), nil
}

func (noopDecompressor) Close() {}

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
	return snappy.DecodedLen(b)
}

func (snappyDecompressor) Close() {}

func (zstdDecompressor) DecompressedLen(b []byte) (decompressedLen int, err error) {
	// This will also be used by zlib, bzip2 and lz4 to retrieve the decodedLen
	// if we implement these algorithms in the future.
	decodedLenU64, varIntLen := binary.Uvarint(b)
	if varIntLen <= 0 {
		return 0, base.CorruptionErrorf("pebble: compression block has invalid length")
	}
	return int(decodedLenU64), nil
}

func (minlzDecompressor) DecompressInto(buf, compressed []byte) error {
	result, err := minlz.Decode(buf, compressed)
	if len(result) != len(buf) || (len(result) > 0 && &result[0] != &buf[0]) {
		return base.CorruptionErrorf("pebble/table: decompressed into unexpected buffer: %p != %p",
			errors.Safe(result), errors.Safe(buf))
	}
	return err
}

func (minlzDecompressor) DecompressedLen(b []byte) (decompressedLen int, err error) {
	l, err := minlz.DecodedLen(b)
	return l, err
}

func (minlzDecompressor) Close() {}

func GetDecompressor(c CompressionIndicator) Decompressor {
	switch c {
	case NoCompressionIndicator:
		return noopDecompressor{}
	case SnappyCompressionIndicator:
		return snappyDecompressor{}
	case ZstdCompressionIndicator:
		return getZstdDecompressor()
	case MinLZCompressionIndicator:
		return minlzDecompressor{}
	default:
		panic("Invalid compression type.")
	}
}
