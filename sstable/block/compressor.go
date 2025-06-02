package block

import (
	"github.com/cockroachdb/pebble/internal/compression"
	"github.com/cockroachdb/pebble/sstable/block/blockkind"
)

// CompressionProfile contains the parameters for compressing blocks in an
// sstable or blob file.
//
// CompressionProfile is a more advanced successor to Compression.
type CompressionProfile struct {
	Name string

	// DataBlocks is the setting that applies to sstable data and value blocks and
	// blob file value blocks.
	DataBlocks  compression.Setting
	OtherBlocks compression.Setting

	// Blocks that are reduced by less than this percentage are stored
	// uncompressed.
	MinReductionPercent uint8

	// TODO(radu): knobs for adaptive compression go here.
}

// SimpleCompressionProfile returns a CompressionProfile that uses the same
// compression setting for all blocks and which uses the uncompressed block if
// compression reduces it by less than 12%. This is similar to older Pebble
// versions which used Compression.
func SimpleCompressionProfile(name string, setting compression.Setting) CompressionProfile {
	return CompressionProfile{
		Name:                name,
		DataBlocks:          setting,
		OtherBlocks:         setting,
		MinReductionPercent: 12,
	}
}

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
	setting := c.profile.DataBlocks
	compressor := c.dataBlocksCompressor
	if kind != blockkind.SSTableData && kind != blockkind.SSTableValue && kind != blockkind.BlobValue {
		setting = c.profile.OtherBlocks
		compressor = c.otherBlocksCompressor
	}
	out := compressor.Compress(dst, src)

	// Return the original data uncompressed if the reduction is less than the
	// minimum, i.e.:
	//
	//   after * 100
	//   -----------  >  100 - MinReductionPercent
	//      before
	if setting.Algorithm != compression.NoCompression &&
		int64(len(out))*100 > int64(len(src))*int64(100-c.profile.MinReductionPercent) {
		return NoCompressionIndicator, append(out[:0], src...)
	}
	return compressionIndicatorFromAlgorithm(setting.Algorithm), out
}

// NoopCompressor is a Compressor that does not compress data. It does not have
// any state and can be used in parallel.
var NoopCompressor = &noopCompressor

var noopCompressor = MakeCompressor(NoCompression.ToProfile())

type Decompressor = compression.Decompressor

func GetDecompressor(c CompressionIndicator) Decompressor {
	return compression.GetDecompressor(c.algorithm())
}
