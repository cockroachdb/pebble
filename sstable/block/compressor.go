package block

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/golang/snappy"
)

type Compressor interface {
	Compress(dst, src []byte) (CompressionIndicator, []byte)
}

type NoopCompressor struct{}
type SnappyCompressor struct{}
type ZstdCompressor struct{}

var _ Compressor = NoopCompressor{}
var _ Compressor = SnappyCompressor{}
var _ Compressor = ZstdCompressor{}

func (NoopCompressor) Compress(dst, src []byte) (CompressionIndicator, []byte) {
	panic("NoCompressionCompressor.Compress() should not be called.")
}

func (SnappyCompressor) Compress(dst, src []byte) (CompressionIndicator, []byte) {
	dst = dst[:cap(dst):cap(dst)]
	return SnappyCompressionIndicator, snappy.Encode(dst, src)
}

func GetCompressor(c Compression) Compressor {
	switch c {
	case NoCompression:
		return NoopCompressor{}
	case SnappyCompression:
		return SnappyCompressor{}
	case ZstdCompression:
		return ZstdCompressor{}
	default:
		panic("Invalid compression type.")
	}
}

type Decompressor interface {
	// DecompressInto decompresses compressed into buf. The buf slice must have the
	// exact size as the decompressed value. Callers may use DecompressedLen to
	// determine the correct size.
	DecompressInto(buf, compressed []byte) error
}

type NoopDecompressor struct{}
type SnappyDecompressor struct{}
type ZstdDecompressor struct{}

var _ Decompressor = NoopDecompressor{}
var _ Decompressor = SnappyDecompressor{}
var _ Decompressor = ZstdDecompressor{}

func (NoopDecompressor) DecompressInto(dst, src []byte) error {
	dst = dst[:len(src)]
	copy(dst, src)
	return nil
}

func (SnappyDecompressor) DecompressInto(buf, compressed []byte) error {
	result, err := snappy.Decode(buf, compressed)
	if err != nil {
		return err
	}
	if len(result) != len(buf) || (len(result) > 0 && &result[0] != &buf[0]) {
		return base.CorruptionErrorf("pebble/table: decompressed into unexpected buffer: %p != %p",
			errors.Safe(result), errors.Safe(buf))
	}
	return nil
}

func GetDecompressor(c CompressionIndicator) Decompressor {
	switch c {
	case NoCompressionIndicator:
		return NoopDecompressor{}
	case SnappyCompressionIndicator:
		return SnappyDecompressor{}
	case ZstdCompressionIndicator:
		return ZstdDecompressor{}
	default:
		panic("Invalid compression type.")
	}
}
