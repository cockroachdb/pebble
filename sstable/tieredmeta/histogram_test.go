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

func TestHistogramWriter_Randomized(t *testing.T) {
	prng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 0))

	for range 20 {
		w := NewTieringHistogramBlockWriter()

		// Track expected values for verification.
		type expectedStats struct {
			totalBytes uint64
			totalCount uint64
			zeroBytes  uint64
		}
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

				for range numRecords {
					// Randomly choose attribute, with ~20% chance of zero.
					var attr base.TieringAttribute
					if prng.Float64() > 0.2 {
						attr = base.TieringAttribute(testutils.RandIntInRange(prng, 1, 1000))
					}
					bytes := uint64(testutils.RandIntInRange(prng, 1, 10000))

					w.Add(kt, spanID, attr, bytes)

					// Track expected values.
					key := Key{kt, spanID}
					stats := expected[key]
					stats.totalBytes += bytes
					stats.totalCount++
					if attr == 0 {
						stats.zeroBytes += bytes
					}
					expected[key] = stats
				}
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
			require.Equal(t, expStats.totalBytes, hist.TotalBytes,
				"TotalBytes mismatch for key %+v", key)
			require.Equal(t, expStats.totalCount, hist.TotalCount,
				"TotalCount mismatch for key %+v", key)
			require.Equal(t, expStats.zeroBytes, hist.ZeroBytes,
				"ZeroBytes mismatch for key %+v", key)
		}
	}
}

func TestHistogramEncoding_Roundtrip(t *testing.T) {
	prng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 0))

	for range 20 {
		hw := newHistogramWriter()

		var expectedTotalBytes, expectedZeroBytes, expectedCount uint64

		numRecords := testutils.RandIntInRange(prng, 10, 200)
		for range numRecords {
			var attr base.TieringAttribute
			if prng.Float64() > 0.2 {
				attr = base.TieringAttribute(testutils.RandIntInRange(prng, 1, 1000))
			}
			bytes := uint64(testutils.RandIntInRange(prng, 1, 10000))

			hw.record(attr, bytes)

			expectedTotalBytes += bytes
			expectedCount++
			if attr == 0 {
				expectedZeroBytes += bytes
			}
		}

		// Test encode/decode roundtrip.
		encoded := hw.encode()
		decoded, err := DecodeStatsHistogram(encoded)
		require.NoError(t, err)

		require.Equal(t, expectedTotalBytes, decoded.TotalBytes)
		require.Equal(t, expectedCount, decoded.TotalCount)
		require.Equal(t, expectedZeroBytes, decoded.ZeroBytes)
		require.Equal(t, expectedTotalBytes-expectedZeroBytes, decoded.NonZeroBytes())
	}
}
