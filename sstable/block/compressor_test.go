// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package block

import (
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble/internal/compression"
	"github.com/cockroachdb/pebble/sstable/block/blockkind"
	"github.com/stretchr/testify/require"
)

func TestCompressor(t *testing.T) {
	settings := []compression.Setting{
		compression.NoCompression,
		compression.SnappySetting,
		compression.MinLZFastest,
		compression.ZstdLevel3,
	}

	src := make([]byte, 1024)
	dst := make([]byte, 0, 1024)
	for runs := 0; runs < 100; runs++ {
		profile := &CompressionProfile{
			DataBlocks:          SimpleCompressionSetting(settings[rand.IntN(len(settings))]),
			ValueBlocks:         SimpleCompressionSetting(settings[rand.IntN(len(settings))]),
			OtherBlocks:         settings[rand.IntN(len(settings))],
			MinReductionPercent: 0,
		}

		compressor := MakeCompressor(profile)
		ci, _ := compressor.Compress(dst, src, blockkind.SSTableData)
		require.Equal(t, compressionIndicatorFromAlgorithm(profile.DataBlocks.Algorithm), ci)

		ci, _ = compressor.Compress(dst, src, blockkind.SSTableValue)
		require.Equal(t, compressionIndicatorFromAlgorithm(profile.ValueBlocks.Algorithm), ci)

		ci, _ = compressor.Compress(dst, src, blockkind.BlobValue)
		require.Equal(t, compressionIndicatorFromAlgorithm(profile.ValueBlocks.Algorithm), ci)

		ci, _ = compressor.Compress(dst, src, blockkind.SSTableIndex)
		require.Equal(t, compressionIndicatorFromAlgorithm(profile.OtherBlocks.Algorithm), ci)

		ci, _ = compressor.Compress(dst, src, blockkind.Metadata)
		require.Equal(t, compressionIndicatorFromAlgorithm(profile.OtherBlocks.Algorithm), ci)

		compressor.Close()
	}
}

// TestCompressorMinReductionStats verifies that a block abandoned because it
// did not meet MinReductionPercent is accounted as uncompressed, rather than
// under a synthetic setting that retains the profile's compression level.
func TestCompressorMinReductionStats(t *testing.T) {
	// Incompressible data, so that compression cannot meet the minimum
	// reduction.
	src := make([]byte, 1024)
	for i := range src {
		src[i] = byte(rand.Uint32())
	}
	profile := &CompressionProfile{
		DataBlocks:          SimpleCompressionSetting(compression.ZstdLevel3),
		ValueBlocks:         SimpleCompressionSetting(compression.ZstdLevel3),
		OtherBlocks:         compression.ZstdLevel3,
		MinReductionPercent: 20,
	}
	compressor := MakeCompressor(profile)
	defer compressor.Close()

	ci, out := compressor.Compress(nil, src, blockkind.SSTableData)
	require.Equal(t, NoCompressionIndicator, ci)
	require.Equal(t, src, out)
	require.Equal(t, "None:1024", compressor.Stats().String())
}

// TestCompressorCopiedBlock verifies that CopiedBlock derives the same stats as
// those recorded when the blocks were compressed.
func TestCompressorCopiedBlock(t *testing.T) {
	rng := rand.New(rand.NewPCG(0, 1))
	incompressible := make([]byte, 4096)
	for i := range incompressible {
		incompressible[i] = byte(rng.Uint32())
	}
	compressible := make([]byte, 4096)
	for i := range compressible {
		compressible[i] = byte(i % 7)
	}
	// makeBlocks returns the physical blocks (with trailers) produced by the
	// given profile, along with the compression stats of the compressor.
	makeBlocks := func(profile *CompressionProfile) ([][]byte, *CompressionStats) {
		var maker PhysicalBlockMaker
		maker.Init(profile, ChecksumTypeCRC32c, nil)
		defer maker.Close()
		var blocks [][]byte
		for _, data := range [][]byte{compressible, incompressible, compressible[:100]} {
			pb := maker.Make(data, blockkind.SSTableData, NoFlags)
			blocks = append(blocks, slices.Clone(pb.tb.Data()))
			pb.Release()
		}
		stats := maker.Compressor.Stats().Clone()
		return blocks, &stats
	}
	copiedStats := func(blocks [][]byte, sourceStats *CompressionStats) string {
		c := MakeCompressor(NoCompression)
		defer c.Close()
		for _, b := range blocks {
			require.NoError(t, c.CopiedBlock(b, sourceStats))
		}
		return c.Stats().String()
	}
	// zeroLevels returns the stats with the levels removed.
	zeroLevels := func(stats *CompressionStats) string {
		var res CompressionStats
		for s, cs := range stats.All() {
			res.addOne(compression.Setting{Algorithm: s.Algorithm}, cs)
		}
		return res.String()
	}

	for _, profile := range []*CompressionProfile{
		NoCompression, SnappyCompression, MinLZCompression, ZstdCompression,
	} {
		t.Run(profile.Name, func(t *testing.T) {
			blocks, stats := makeBlocks(profile)
			// With the source's stats, the levels are inferred.
			require.Equal(t, stats.String(), copiedStats(blocks, stats))
			// Without them, the levels are unknown.
			require.Equal(t, zeroLevels(stats), copiedStats(blocks, nil))
		})
	}

	t.Run("ambiguous-level", func(t *testing.T) {
		blocks, _ := makeBlocks(ZstdCompression)
		var sourceStats CompressionStats
		sourceStats.addOne(compression.ZstdLevel1, CompressionStatsForSetting{CompressedBytes: 1, UncompressedBytes: 2})
		sourceStats.addOne(compression.ZstdLevel3, CompressionStatsForSetting{CompressedBytes: 1, UncompressedBytes: 2})
		require.Contains(t, copiedStats(blocks, &sourceStats), "ZSTD:")
	})

	t.Run("invalid", func(t *testing.T) {
		c := MakeCompressor(NoCompression)
		defer c.Close()
		require.Error(t, c.CopiedBlock(make([]byte, TrailerLen-1), nil))
		b := make([]byte, 10+TrailerLen)
		b[10] = byte(ZlibCompressionIndicator)
		require.Error(t, c.CopiedBlock(b, nil))
		require.True(t, c.Stats().IsEmpty())
	})
}
