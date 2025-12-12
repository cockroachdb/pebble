// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tieredmeta

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/testutils"
	"github.com/stretchr/testify/require"
)

// expectedStats tracks expected histogram values for test verification.
type expectedStats struct {
	totalBytes uint64
	totalCount uint64
	zeroBytes  uint64
}

// generateRandomRecords generates numRecords random (attr, bytes) pairs, calls
// recordFn for each, and returns the expected aggregate stats. Approximately
// 20% of records will have attr=0.
func generateRandomRecords(
	prng *rand.Rand, numRecords int, recordFn func(attr base.TieringAttribute, bytes uint64),
) expectedStats {
	var stats expectedStats
	for range numRecords {
		var attr base.TieringAttribute
		if prng.Float64() > 0.2 {
			attr = base.TieringAttribute(testutils.RandIntInRange(prng, 1, 1000))
		}
		bytes := uint64(testutils.RandIntInRange(prng, 1, 10000))

		recordFn(attr, bytes)

		stats.totalBytes += bytes
		stats.totalCount++
		if attr == 0 {
			stats.zeroBytes += bytes
		}
	}
	return stats
}

// requireStatsEqual asserts that the decoded histogram matches the expected stats.
func requireStatsEqual(t *testing.T, exp expectedStats, hist StatsHistogram, msgAndArgs ...any) {
	t.Helper()
	require.Equal(t, exp.totalBytes, hist.TotalBytes, msgAndArgs...)
	require.Equal(t, exp.totalCount, hist.TotalCount, msgAndArgs...)
	require.Equal(t, exp.zeroBytes, hist.BytesNoAttr, msgAndArgs...)
	require.Equal(t, exp.totalBytes-exp.zeroBytes, hist.BytesWithAttr(), msgAndArgs...)
}

func TestHistogramBlock_Randomized(t *testing.T) {
	prng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 0))

	for range 20 {
		w := NewTieringHistogramBlockWriter()
		expected := make(map[Key]expectedStats)

		// Generate random number of span IDs.
		numSpans := testutils.RandIntInRange(prng, 1, 10)
		spanIDs := make([]base.TieringSpanID, numSpans)
		for i := range spanIDs {
			spanIDs[i] = base.TieringSpanID(testutils.RandIntInRange(prng, 0, 1000))
		}

		// For each span, add records with random KindAndTier.
		for _, spanID := range spanIDs {
			numKinds := testutils.RandIntInRange(prng, 1, int(NumKindAndTiers)+1)
			for range numKinds {
				kt := KindAndTier(testutils.RandIntInRange(prng, 0, int(NumKindAndTiers)))
				numRecords := testutils.RandIntInRange(prng, 10, 100)
				key := Key{kt, spanID}

				stats := generateRandomRecords(prng, numRecords, func(attr base.TieringAttribute, bytes uint64) {
					w.Add(kt, spanID, attr, bytes)
				})

				// Accumulate stats for this key.
				existing := expected[key]
				existing.totalBytes += stats.totalBytes
				existing.totalCount += stats.totalCount
				existing.zeroBytes += stats.zeroBytes
				expected[key] = existing
			}
		}

		// Encode and decode the block.
		encoded := w.Flush()
		decoded, err := DecodeTieringHistogramBlock(encoded)
		require.NoError(t, err)

		// Verify all expected keys are present with correct statistics.
		require.Len(t, decoded.histograms, len(expected))
		for key, expStats := range expected {
			hist, ok := decoded.histograms[key]
			require.True(t, ok, "missing histogram for key %+v", key)
			requireStatsEqual(t, expStats, hist, "key %+v", key)
		}
	}
}

func TestHistogramEncoding_Roundtrip(t *testing.T) {
	prng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 0))

	for range 20 {
		hw := newHistogramWriter()
		numRecords := testutils.RandIntInRange(prng, 10, 200)

		exp := generateRandomRecords(prng, numRecords, hw.record)

		// Test encode/decode roundtrip.
		encoded := hw.encode()
		decoded, err := DecodeStatsHistogram(encoded)
		require.NoError(t, err)

		requireStatsEqual(t, exp, decoded)
	}
}
