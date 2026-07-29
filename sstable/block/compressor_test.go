// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package block

import (
	"math/rand/v2"
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
