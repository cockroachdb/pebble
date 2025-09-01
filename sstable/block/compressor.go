package block

import (
	"fmt"
	"iter"
	"math/rand"
	"strings"

	"github.com/cockroachdb/pebble/v2/internal/compression"
	"github.com/cockroachdb/pebble/v2/sstable/block/blockkind"
)

// Compressor is used to compress blocks. Typical usage:
//
//	c := MakeCompressor(profile)
//	.. = c.Compress(..)
//	.. = c.Compress(..)
//	c.Close()
type Compressor struct {
	minReductionPercent   uint8
	dataBlocksCompressor  compression.Compressor
	valueBlocksCompressor compression.Compressor
	otherBlocksCompressor compression.Compressor

	stats CompressionStats
}

// MakeCompressor returns a Compressor that applies the given compression
// profile. Close must be called when the compressor is no longer needed.
func MakeCompressor(profile *CompressionProfile) Compressor {
	c := Compressor{
		minReductionPercent: profile.MinReductionPercent,
	}

	c.dataBlocksCompressor = maybeAdaptiveCompressor(profile, profile.DataBlocks)
	c.valueBlocksCompressor = maybeAdaptiveCompressor(profile, profile.ValueBlocks)
	c.otherBlocksCompressor = compression.GetCompressor(profile.OtherBlocks)
	return c
}

func maybeAdaptiveCompressor(
	profile *CompressionProfile, setting compression.Setting,
) compression.Compressor {
	if profile.AdaptiveReductionCutoffPercent != 0 && setting != profile.OtherBlocks {
		params := compression.AdaptiveCompressorParams{
			Slow:            setting,
			Fast:            profile.OtherBlocks,
			ReductionCutoff: float64(profile.AdaptiveReductionCutoffPercent) * 0.01,
			SampleEvery:     10,
			SampleHalfLife:  256 * 1024, // 256 KB
			SamplingSeed:    rand.Uint64(),
		}
		return compression.NewAdaptiveCompressor(params)
	}
	return compression.GetCompressor(setting)
}

// Close must be called when the Compressor is no longer needed.
// After Close is called, the Compressor must not be used again.
func (c *Compressor) Close() {
	c.dataBlocksCompressor.Close()
	c.valueBlocksCompressor.Close()
	c.otherBlocksCompressor.Close()
	*c = Compressor{}
}

// Compress a block, appending the compressed data to dst[:0].
//
// In addition to the buffer, returns the algorithm that was used.
func (c *Compressor) Compress(dst, src []byte, kind Kind) (CompressionIndicator, []byte) {
	var compressor compression.Compressor
	switch kind {
	case blockkind.SSTableData:
		compressor = c.dataBlocksCompressor
	case blockkind.SSTableValue, blockkind.BlobValue:
		compressor = c.valueBlocksCompressor
	default:
		compressor = c.otherBlocksCompressor
	}

	out, setting := compressor.Compress(dst, src)

	// Return the original data uncompressed if the reduction is less than the
	// minimum, i.e.:
	//
	//   after * 100
	//   -----------  >  100 - MinReductionPercent
	//      before
	if setting.Algorithm != compression.NoCompression &&
		int64(len(out))*100 > int64(len(src))*int64(100-c.minReductionPercent) {
		c.stats.add(compression.None, uint64(len(src)), uint64(len(src)))
		return NoCompressionIndicator, append(out[:0], src...)
	}
	c.stats.add(setting, uint64(len(src)), uint64(len(out)))
	return compressionIndicatorFromAlgorithm(setting.Algorithm), out
}

// UncompressedBlock informs the compressor that a block of the given size and
// kind was written uncompressed. This is used so that the final statistics are
// complete.
func (c *Compressor) UncompressedBlock(size int, kind Kind) {
	c.stats.add(compression.None, uint64(size), uint64(size))
}

// Stats returns the compression stats. The result can only be used until the
// next call to the Compressor.
func (c *Compressor) Stats() *CompressionStats {
	return &c.stats
}

// CompressionStats collects compression statistics for a single file - the
// total compressed and uncompressed sizes for each distinct compression.Setting
// used.
type CompressionStats struct {
	n int
	// Compression profiles have three settings (data, value, other) and
	// NoCompression can also be used for data that didn't compress.
	buf [4]CompressionStatsForSetting
}

type CompressionStatsForSetting struct {
	Setting           compression.Setting
	UncompressedBytes uint64
	CompressedBytes   uint64
}

// add updates the stats to reflect a block that was compressed with the given setting.
func (c *CompressionStats) add(
	setting compression.Setting, sizeUncompressed, sizeCompressed uint64,
) {
	for i := 0; i < c.n; i++ {
		if c.buf[i].Setting == setting {
			c.buf[i].UncompressedBytes += sizeUncompressed
			c.buf[i].CompressedBytes += sizeCompressed
			return
		}
	}
	if c.n >= len(c.buf) {
		panic("too many compression settings")
	}
	c.buf[c.n] = CompressionStatsForSetting{
		Setting:           setting,
		UncompressedBytes: sizeUncompressed,
		CompressedBytes:   sizeCompressed,
	}
	c.n++
}

// MergeWith updates the receiver stats to include the other stats.
func (c *CompressionStats) MergeWith(other *CompressionStats) {
	for i := 0; i < other.n; i++ {
		c.add(other.buf[i].Setting, other.buf[i].UncompressedBytes, other.buf[i].CompressedBytes)
	}
}

// All returns an iterator over the collected stats, in arbitrary order.
func (c CompressionStats) All() iter.Seq[CompressionStatsForSetting] {
	return func(yield func(cs CompressionStatsForSetting) bool) {
		for i := 0; i < c.n; i++ {
			if !yield(c.buf[i]) {
				return
			}
		}
	}
}

// String returns a string representation of the stats, in the format:
// "<setting1>:<compressed1>/<uncompressed1>,<setting2>:<compressed2>/<uncompressed2>,..."
func (c CompressionStats) String() string {
	var buf strings.Builder
	buf.Grow(c.n * 64)
	for i := 0; i < c.n; i++ {
		if i > 0 {
			buf.WriteString(",")
		}
		fmt.Fprintf(&buf, "%s:%d/%d", c.buf[i].Setting.String(), c.buf[i].CompressedBytes, c.buf[i].UncompressedBytes)
	}
	return buf.String()
}

type Decompressor = compression.Decompressor

func GetDecompressor(c CompressionIndicator) Decompressor {
	return compression.GetDecompressor(c.Algorithm())
}
