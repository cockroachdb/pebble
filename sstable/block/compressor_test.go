// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package block

import (
	"fmt"
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

// TestCompressorIncompressibleBlockStats verifies that a block which is stored
// uncompressed because it did not compress well enough is attributed to the
// no-compression setting, whatever setting was attempted.
func TestCompressorIncompressibleBlockStats(t *testing.T) {
	// Random data does not compress, so each compressor produces an output at
	// least as large as its input and the block is stored uncompressed.
	rng := rand.New(rand.NewPCG(1, 2))
	src := make([]byte, 4096)
	for i := range src {
		src[i] = byte(rng.Uint32())
	}

	for _, setting := range []compression.Setting{
		compression.NoCompression,
		compression.SnappySetting,
		compression.MinLZFastest,
		compression.MinLZBalanced,
		compression.ZstdLevel1,
		compression.ZstdLevel3,
	} {
		t.Run(setting.String(), func(t *testing.T) {
			profile := &CompressionProfile{
				Name:                setting.String(),
				DataBlocks:          SimpleCompressionSetting(setting),
				ValueBlocks:         SimpleCompressionSetting(setting),
				OtherBlocks:         setting,
				MinReductionPercent: 12,
			}
			compressor := MakeCompressor(profile)
			defer compressor.Close()

			ci, out := compressor.Compress(nil, src, blockkind.SSTableData)
			require.Equal(t, NoCompressionIndicator, ci)
			require.Equal(t, src, out)
			require.Equal(t, fmt.Sprintf("None:%d", len(src)), compressor.Stats().String())
		})
	}
}
