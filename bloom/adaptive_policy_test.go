// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package bloom

import (
	"encoding/binary"
	"math/rand/v2"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/stretchr/testify/require"
)

func TestFilterSize(t *testing.T) {
	// Test edge cases.
	require.Equal(t, uint64(0), FilterSize(0, 10))
	require.Equal(t, uint64(0), FilterSize(10, 0))

	// Test that FilterSize matches the actual filter size produced by Finish.
	for range 10000 {
		numKeys := max(1, uint(rand.ExpFloat64()*100))
		bitsPerKey := 1 + rand.Uint32N(20)
		w := newTableFilterWriter(bitsPerKey)
		for i := range numKeys {
			key := []byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
			w.AddKey(key)
		}
		filterData, family, ok := w.Finish()
		require.True(t, ok)
		require.Equal(t, Family, family)
		expectedSize := FilterSize(numKeys, bitsPerKey)
		require.Equal(t, int(expectedSize), len(filterData),
			"FilterSize(%d, %d) = %d, but actual filter size = %d",
			numKeys, bitsPerKey, expectedSize, len(filterData))
	}
}

func TestMaxBitsPerKey(t *testing.T) {
	// Test edge cases.
	require.Equal(t, uint32(0), MaxBitsPerKey(0, 1000))
	require.Equal(t, uint32(0), MaxBitsPerKey(100, 0))
	require.Equal(t, uint32(0), MaxBitsPerKey(100, 5)) // Only trailer, no room for filter bits.

	check := func(numKeys uint, maxSize uint64) {
		result := MaxBitsPerKey(numKeys, maxSize)

		if result > 0 {
			// Verify FilterSize with result <= maxSize.
			size := FilterSize(numKeys, result)
			require.LessOrEqual(t, size, maxSize,
				"MaxBitsPerKey(%d, %d) = %d, but FilterSize = %d > maxSize",
				numKeys, maxSize, result, size)
		}

		// Verify FilterSize with result+1 > maxSize.
		if result < cacheLineBits {
			sizeNext := FilterSize(numKeys, result+1)
			require.Greater(t, sizeNext, maxSize,
				"MaxBitsPerKey(%d, %d) = %d, but FilterSize(%d, %d) = %d <= maxSize",
				numKeys, maxSize, result, numKeys, result+1, sizeNext)
		}
	}

	for range 1000 {
		numKeys := max(1, uint(rand.ExpFloat64()*1000))
		for i := uint32(1); i <= 20; i++ {
			size := FilterSize(numKeys, i)
			require.Greater(t, i, MaxBitsPerKey(numKeys, size-1), "%d keys, size %d", numKeys, size-1)
			require.LessOrEqual(t, i, MaxBitsPerKey(numKeys, size), "%d keys, size %d", numKeys, size)
		}
		for range 100 {
			maxSize := uint64(rand.ExpFloat64() * 10000)
			check(numKeys, maxSize)
		}
	}
}

func TestAdaptivePolicy(t *testing.T) {
	key := func(i int) []byte {
		return binary.LittleEndian.AppendUint32(nil, uint32(i))
	}

	build := func(t *testing.T, numKeys int, targetBitsPerKey uint32, maxSize uint64) (_ []byte, ok bool) {
		policy := AdaptivePolicy(targetBitsPerKey, maxSize)
		w := policy.NewWriter()

		for i := 0; i < numKeys; i++ {
			w.AddKey(key(i))
		}

		filter, family, ok := w.Finish()
		if !ok {
			require.Equal(t, base.TableFilterFamily(""), family)
			require.Nil(t, filter)
			return nil, false
		}
		require.True(t, ok)
		require.Equal(t, Family, family)
		require.NotNil(t, filter)
		require.LessOrEqual(t, uint64(len(filter)), maxSize,
			"filter size %d exceeds max size %d", len(filter), maxSize)
		return filter, true
	}

	mustBuild := func(t *testing.T, numKeys int, targetBitsPerKey uint32, maxSize uint64) []byte {
		filter, ok := build(t, numKeys, targetBitsPerKey, maxSize)
		require.True(t, ok)
		return filter
	}

	check := func(t *testing.T, filter []byte, numKeys int) {
		for i := range numKeys {
			require.True(t, mayContain(filter, hash(key(i))),
				"key %d not found in filter", i)
		}
		// Check that the filter doesn't always return true.
		fp := 0
		const numTests = 1000
		for i := range numTests {
			if mayContain(filter, hash(key(1_000_000_000+i))) {
				fp++
			}
		}
		if fp > numTests*3/4 {
			t.Fatalf("FPR too high: %f", float64(fp)/numTests)
		}
	}

	t.Run("basic", func(t *testing.T) {
		// Test that the adaptive policy respects the max size limit.
		const targetBitsPerKey = 10
		const numKeys = 10000

		// Calculate what size a non-adaptive filter would be.
		fullSize := FilterSize(numKeys, targetBitsPerKey)
		require.Greater(t, int(fullSize), 0)

		filter := mustBuild(t, numKeys, targetBitsPerKey, fullSize)
		require.Equal(t, len(filter), int(fullSize))
		check(t, filter, numKeys)

		// It should never use more than the target bits per key.
		filter = mustBuild(t, numKeys, targetBitsPerKey, fullSize*10)
		require.Equal(t, len(filter), int(fullSize))
		check(t, filter, numKeys)

		// Create an adaptive filter with max size less than full size.
		filter = mustBuild(t, numKeys, targetBitsPerKey, fullSize/2)
		check(t, filter, numKeys)

		filter = mustBuild(t, numKeys, targetBitsPerKey, fullSize/4)
		check(t, filter, numKeys)
	})

	t.Run("empty-filter", func(t *testing.T) {
		// Empty filter should return nil.
		_, ok := build(t, 0, 10, 100)
		require.False(t, ok)
	})

	t.Run("too-small-max-size", func(t *testing.T) {
		_, ok := build(t, 1000, 10, 100)
		require.False(t, ok)
	})

	t.Run("randomized", func(t *testing.T) {
		// Randomized test: verify filter size never exceeds max size.
		rng := rand.New(rand.NewPCG(1, 2))
		for range 100 {
			numKeys := max(1, int(rng.ExpFloat64()*1000))
			targetBitsPerKey := 1 + rng.Uint32N(20)
			fullSize := FilterSize(uint(numKeys), targetBitsPerKey)
			// Choose max size between 0 and 2x full size.
			maxSize := rng.Uint64N(2 * fullSize)

			policy := AdaptivePolicy(targetBitsPerKey, maxSize)
			w := policy.NewWriter()
			for i := range numKeys {
				w.AddKey(key(i))
			}
			if filter, ok := build(t, numKeys, targetBitsPerKey, maxSize); ok {
				check(t, filter, numKeys)
			}
		}
	})
}

func TestAdaptivePolicyFromName(t *testing.T) {
	// Test that PolicyFromName correctly parses adaptive_bloom names.
	testCases := []struct {
		name          string
		expectValid   bool
		expectBitsKey uint32
		expectMaxSize uint64
	}{
		{"adaptive_bloom(10,1000)", true, 10, 1000},
		{"adaptive_bloom(5,500)", true, 5, 500},
		{"adaptive_bloom(20,999999)", true, 20, 999999},
		{"adaptive_bloom(0,1000)", false, 0, 0},  // bitsPerKey must be > 0
		{"adaptive_bloom(10,0)", false, 0, 0},    // maxSize must be > 0
		{"adaptive_bloom(-1,1000)", false, 0, 0}, // negative bitsPerKey
		{"adaptive_bloom(10)", false, 0, 0},      // missing maxSize
		{"bloom(10)", true, 10, 0},               // regular bloom, not adaptive
		{"invalid", false, 0, 0},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			policy, ok := PolicyFromName(tc.name)
			if !tc.expectValid {
				require.False(t, ok, "expected PolicyFromName to return false for %q", tc.name)
				return
			}
			require.True(t, ok, "expected PolicyFromName to return true for %q", tc.name)
			require.NotNil(t, policy)

			if tc.expectMaxSize > 0 {
				// It's an adaptive policy.
				ap, ok := policy.(adaptivePolicyImpl)
				require.True(t, ok, "expected adaptive policy for %q", tc.name)
				require.Equal(t, tc.expectBitsKey, ap.TargetBitsPerKey)
				require.Equal(t, tc.expectMaxSize, ap.MaxSize)
				// Verify roundtrip through Name().
				require.Equal(t, tc.name, ap.Name())
			}
		})
	}
}
