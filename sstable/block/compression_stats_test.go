// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package block

import (
	"math/rand/v2"
	"testing"

	"github.com/cockroachdb/pebble/internal/compression"
	"github.com/stretchr/testify/require"
)

func TestCompressionStatsString(t *testing.T) {
	var stats CompressionStats

	stats.addOne(compression.NoCompression, CompressionStatsForSetting{CompressedBytes: 100, UncompressedBytes: 100})
	require.Equal(t, "None:100", stats.String())

	stats.addOne(compression.SnappySetting, CompressionStatsForSetting{CompressedBytes: 100, UncompressedBytes: 200})
	require.Equal(t, "None:100,Snappy:100/200", stats.String())

	stats.addOne(compression.SnappySetting, CompressionStatsForSetting{CompressedBytes: 100, UncompressedBytes: 200})
	require.Equal(t, "None:100,Snappy:200/400", stats.String())

	stats.addOne(compression.MinLZFastest, CompressionStatsForSetting{CompressedBytes: 1000, UncompressedBytes: 4000})
	require.Equal(t, "None:100,Snappy:200/400,MinLZ1:1000/4000", stats.String())

	stats.addOne(compression.ZstdLevel1, CompressionStatsForSetting{CompressedBytes: 10000, UncompressedBytes: 80000})
	require.Equal(t, "None:100,Snappy:200/400,MinLZ1:1000/4000,ZSTD1:10000/80000", stats.String())

	stats.addOne(compression.ZstdLevel3, CompressionStatsForSetting{CompressedBytes: 5000, UncompressedBytes: 90000})
	require.Equal(t, "None:100,Snappy:200/400,MinLZ1:1000/4000,ZSTD1:10000/80000,ZSTD3:5000/90000", stats.String())

	stats = CompressionStats{}
	stats.addOne(compression.MinLZFastest, CompressionStatsForSetting{CompressedBytes: 1000, UncompressedBytes: 4000})
	require.Equal(t, "MinLZ1:1000/4000", stats.String())

	stats = CompressionStats{}
	stats.addOne(compression.SnappySetting, CompressionStatsForSetting{CompressedBytes: 1000, UncompressedBytes: 4000})
	require.Equal(t, "Snappy:1000/4000", stats.String())
}

func TestCompressionStatsRoundtrip(t *testing.T) {
	settings := []compression.Setting{compression.NoCompression, compression.SnappySetting, compression.MinLZFastest, compression.ZstdLevel1, compression.ZstdLevel3}
	for n := 0; n < 1000; n++ {
		var stats CompressionStats
		for _, i := range rand.Perm(len(settings))[:rand.IntN(len(settings)+1)] {
			compressed := rand.Uint64N(1_000_000)
			uncompressed := compressed
			if settings[i] != compression.NoCompression {
				uncompressed += compressed * rand.Uint64N(20) / 10
			}
			stats.addOne(settings[i], CompressionStatsForSetting{CompressedBytes: compressed, UncompressedBytes: uncompressed})
			str := stats.String()
			stats2, err := ParseCompressionStats(str)
			require.NoError(t, err)
			str2 := stats2.String()
			require.Equal(t, str, str2)
		}
	}
}

func TestParseCompressionStatsUnknown(t *testing.T) {
	stats, err := ParseCompressionStats("MinLZ1:100/200,ZSTD9:100/500,MiddleOut10:13/150000,Magic:15/10000000")
	require.NoError(t, err)
	expected := "MinLZ1:100/200,ZSTD9:100/500,unknown:28/10150000"
	require.Equal(t, expected, stats.String())
}

// TestParseCompressionStatsOld tests that we can parse the old format of
// compression stats.
func TestParseCompressionStatsOldFormat(t *testing.T) {
	stats, err := ParseCompressionStats("NoCompression:12345/12345,MinLZ1:100/200,ZSTD9:100/500")
	require.NoError(t, err)
	expected := "None:12345,MinLZ1:100/200,ZSTD9:100/500"
	require.Equal(t, expected, stats.String())
}
