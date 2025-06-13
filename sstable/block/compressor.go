package block

import (
	"github.com/cockroachdb/pebble/internal/compression"
	"github.com/cockroachdb/pebble/sstable/block/blockkind"
)

// Compressor is used to compress blocks. Typical usage:
//
//	c := MakeCompressor(profile)
//	.. = c.Compress(..)
//	.. = c.Compress(..)
//	c.Close()
type Compressor struct {
	profile              CompressionProfile
	dataBlocksCompressor compression.Compressor
	// otherBlocksCompressor is used for blocks that are not data blocks, such as
	// index blocks or metadata blocks. It can be the same object as
	// dataBlocksCompressor.
	otherBlocksCompressor compression.Compressor
}

// MakeCompressor returns a Compressor that applies the given compression
// profile. Close must be called when the compressor is no longer needed.
func MakeCompressor(profile *CompressionProfile) Compressor {
	c := Compressor{
		profile: *profile,
	}
	c.dataBlocksCompressor = compression.GetCompressor(profile.DataBlocks)
	c.otherBlocksCompressor = c.dataBlocksCompressor
	if profile.OtherBlocks != profile.DataBlocks {
		c.otherBlocksCompressor = compression.GetCompressor(profile.OtherBlocks)
	}
	return c
}

// Close must be called when the Compressor is no longer needed.
// After Close is called, the Compressor must not be used again.
func (c *Compressor) Close() {
	if c.otherBlocksCompressor != c.dataBlocksCompressor {
		c.otherBlocksCompressor.Close()
	}
	c.dataBlocksCompressor.Close()
	*c = Compressor{}
}

// Compress a block, appending the compressed data to dst[:0].
//
// In addition to the buffer, returns the algorithm that was used.
func (c *Compressor) Compress(dst, src []byte, kind Kind) (CompressionIndicator, []byte) {
	compressor := c.dataBlocksCompressor
	if kind != blockkind.SSTableData && kind != blockkind.SSTableValue && kind != blockkind.BlobValue {
		compressor = c.otherBlocksCompressor
	}
	out := compressor.Compress(dst, src)

	// Return the original data uncompressed if the reduction is less than the
	// minimum, i.e.:
	//
	//   after * 100
	//   -----------  >  100 - MinReductionPercent
	//      before
	algorithm := compressor.Algorithm()
	if algorithm != compression.NoCompression &&
		int64(len(out))*100 > int64(len(src))*int64(100-c.profile.MinReductionPercent) {
		return NoCompressionIndicator, append(out[:0], src...)
	}
	return compressionIndicatorFromAlgorithm(algorithm), out
}

// NoopCompressor is a Compressor that does not compress data. It does not have
// any state and can be used in parallel.
var NoopCompressor = &noopCompressor

var noopCompressor = MakeCompressor(NoCompression)

type Decompressor = compression.Decompressor

func GetDecompressor(c CompressionIndicator) Decompressor {
	return compression.GetDecompressor(c.algorithm())
}
