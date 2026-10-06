// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package block

import (
	"iter"
	"math/rand"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
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
	minReductionPercent uint8

	compressors ByKind[compression.Compressor]

	stats CompressionStats

	// inputBytes keeps track of the total number of bytes passed to the
	// compressor, by block kind.
	inputBytes [blockkind.NumKinds]uint64
}

// MakeCompressor returns a Compressor that applies the given compression
// profile. Close must be called when the compressor is no longer needed.
func MakeCompressor(profile *CompressionProfile) Compressor {
	c := Compressor{
		minReductionPercent: profile.MinReductionPercent,
	}

	c.compressors.DataBlocks = maybeAdaptiveCompressor(profile, profile.DataBlocks)
	c.compressors.ValueBlocks = maybeAdaptiveCompressor(profile, profile.ValueBlocks)
	c.compressors.OtherBlocks = compression.GetCompressor(profile.OtherBlocks)
	return c
}

func maybeAdaptiveCompressor(
	profile *CompressionProfile, setting CompressionSetting,
) compression.Compressor {
	if setting.AdaptiveReductionCutoffPercent != 0 && setting.Setting != profile.OtherBlocks {
		params := compression.AdaptiveCompressorParams{
			Slow:            setting.Setting,
			Fast:            profile.OtherBlocks,
			ReductionCutoff: float64(setting.AdaptiveReductionCutoffPercent) * 0.01,
			SampleEvery:     10,
			SampleHalfLife:  256 * 1024, // 256 KB
			SamplingSeed:    rand.Uint64(),
		}
		return compression.NewAdaptiveCompressor(params)
	}
	return compression.GetCompressor(setting.Setting)
}

// Close must be called when the Compressor is no longer needed.
// After Close is called, the Compressor must not be used again.
func (c *Compressor) Close() {
	c.compressors.DataBlocks.Close()
	c.compressors.ValueBlocks.Close()
	c.compressors.OtherBlocks.Close()
	*c = Compressor{}
}

// Compress a block, appending the compressed data to dst[:0].
//
// In addition to the buffer, returns the algorithm that was used.
func (c *Compressor) Compress(dst, src []byte, kind Kind) (CompressionIndicator, []byte) {
	c.inputBytes[kind] += uint64(len(src))

	compressor := *c.compressors.ForKind(kind)

	out, setting := compressor.Compress(dst, src)

	// Return the original data uncompressed if the reduction is less than the
	// minimum, i.e.:
	//
	//   after * 100
	//   -----------  >  100 - MinReductionPercent
	//      before
	if setting.Algorithm != compression.NoAlgorithm &&
		int64(len(out))*100 > int64(len(src))*int64(100-c.minReductionPercent) {
		setting.Algorithm = compression.NoAlgorithm
		out = append(out[:0], src...)
	}
	c.stats.addOne(setting, CompressionStatsForSetting{
		UncompressedBytes: uint64(len(src)),
		CompressedBytes:   uint64(len(out)),
	})
	return compressionIndicatorFromAlgorithm(setting.Algorithm), out
}

// UncompressedBlock informs the compressor that a block of the given size and
// kind was written uncompressed. This is used so that the final statistics are
// complete.
func (c *Compressor) UncompressedBlock(size int, kind Kind) {
	c.stats.addOne(compression.NoCompression, CompressionStatsForSetting{
		UncompressedBytes: uint64(size),
		CompressedBytes:   uint64(size),
	})
}

// CopiedBlock informs the compressor that a block that was already encoded
// (e.g. copied verbatim from another sstable) was written, so that the final
// statistics are complete. blockWithTrailer is the physical block, including
// its trailer.
//
// The compression algorithm and the decompressed size are determined from the
// block itself. The compression level cannot be; it is taken from sourceStats
// (the statistics of the table the block was copied from, if available) when
// they contain a single setting for the block's algorithm, and is otherwise
// left unset.
func (c *Compressor) CopiedBlock(blockWithTrailer []byte, sourceStats *CompressionStats) error {
	if len(blockWithTrailer) < TrailerLen {
		return base.CorruptionErrorf("pebble: invalid block of length %d", errors.Safe(len(blockWithTrailer)))
	}
	data := blockWithTrailer[:len(blockWithTrailer)-TrailerLen]
	indicator := CompressionIndicator(blockWithTrailer[len(data)])
	switch indicator {
	case NoCompressionIndicator, SnappyCompressionIndicator, ZstdCompressionIndicator, MinLZCompressionIndicator:
	default:
		return base.CorruptionErrorf("pebble: unsupported block compression indicator %d", errors.Safe(byte(indicator)))
	}
	algo := indicator.Algorithm()
	decompressedLen := len(data)
	if algo != compression.NoAlgorithm {
		d := compression.GetDecompressor(algo)
		var err error
		decompressedLen, err = d.DecompressedLen(data)
		d.Close()
		if err != nil {
			return err
		}
	}
	setting := compression.Setting{Algorithm: algo}
	if s, ok := sourceStats.uniqueSetting(algo); ok {
		setting = s
	}
	c.stats.addOne(setting, CompressionStatsForSetting{
		UncompressedBytes: uint64(decompressedLen),
		CompressedBytes:   uint64(len(data)),
	})
	return nil
}

// Stats returns the compression stats. The result can only be used until the
// next call to the Compressor.
func (c *Compressor) Stats() *CompressionStats {
	return &c.stats
}

// InputBytes returns an iterator over the total number of input bytes passed
// through the compressor, by block kind.
func (c *Compressor) InputBytes() iter.Seq2[Kind, uint64] {
	return func(yield func(blockkind.Kind, uint64) bool) {
		for k, v := range c.inputBytes {
			if v != 0 && !yield(blockkind.Kind(k), v) {
				return
			}
		}
	}
}

type Decompressor = compression.Decompressor

func GetDecompressor(c CompressionIndicator) Decompressor {
	return compression.GetDecompressor(c.Algorithm())
}
