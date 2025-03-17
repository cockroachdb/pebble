package block

import "github.com/golang/snappy"

type Compressor interface {
	Compress(dst, src []byte) (CompressionIndicator, []byte)
}

type NoCompressionCompressor struct{}
type SnappyCompressor struct{}
type ZstdCompressor struct{}

var _ Compressor = NoCompressionCompressor{}
var _ Compressor = SnappyCompressor{}
var _ Compressor = ZstdCompressor{}

func (NoCompressionCompressor) Compress(dst, src []byte) (CompressionIndicator, []byte) {
	panic("NoCompressionCompressor.Compress() should not be called.")
}

func (SnappyCompressor) Compress(dst, src []byte) (CompressionIndicator, []byte) {
	dst = dst[:cap(dst):cap(dst)]
	return SnappyCompressionIndicator, snappy.Encode(dst, src)
}

func GetCompressor(c Compression) Compressor {
	switch c {
	case NoCompression:
		return NoCompressionCompressor{}
	case SnappyCompression:
		return SnappyCompressor{}
	case ZstdCompression:
		return ZstdCompressor{}
	default:
		panic("Invalid compression type.")
	}
}

type Decoder interface {
	Decode(buf, compressed []byte) ([]byte, error)
}

type NoCompressionDecoder struct{}
type SnappyDecoder struct{}
type ZstdDecoder struct{}

var _ Decoder = NoCompressionDecoder{}
var _ Decoder = SnappyDecoder{}
var _ Decoder = ZstdDecoder{}

func (NoCompressionDecoder) Decode(dst, src []byte) ([]byte, error) {
	dst = dst[:len(src)]
	copy(dst, src)
	return dst, nil
}

func (SnappyDecoder) Decode(buf, compressed []byte) ([]byte, error) {
	return snappy.Decode(buf, compressed)
}

func GetDecoder(c CompressionIndicator) Decoder {
	switch c {
	case NoCompressionIndicator:
		return NoCompressionDecoder{}
	case SnappyCompressionIndicator:
		return SnappyDecoder{}
	case ZstdCompressionIndicator:
		return ZstdDecoder{}
	default:
		panic("Invalid compression type.")
	}
}
