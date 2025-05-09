package block

import "github.com/cockroachdb/pebble/internal/compression"

// Compressor is used to compress blocks. Typical usage:
//
//	c := GetCompressor(compression)
//	.. = c.Compress(..)
//	.. = c.Compress(..)
//	c.Close()
type Compressor struct {
	algorithm  compression.Algorithm
	compressor compression.Compressor
}

// GetCompressor returns a Compressor that applies the given compression. Close
// must be called when it is no longer needed.
func GetCompressor(c Compression) Compressor {
	algorithm := c.algorithm()
	return Compressor{
		algorithm:  algorithm,
		compressor: compression.GetCompressor(algorithm),
	}
}

// Compress a block, appending the compressed data to dst[:0].
//
// In addition to the buffer, returns the algorithm that was used.
func (c *Compressor) Compress(dst, src []byte) (CompressionIndicator, []byte) {
	ci := compressionIndicatorFromAlgorithm(c.algorithm)
	return ci, c.compressor.Compress(dst, src)
}

// Close must be called when the Compressor is no longer needed.
// After Close is called, the Compressor must not be used again.
func (c *Compressor) Close() {
	c.compressor.Close()
	*c = Compressor{}
}

type Decompressor = compression.Decompressor

func GetDecompressor(c CompressionIndicator) Decompressor {
	return compression.GetDecompressor(c.algorithm())
}
