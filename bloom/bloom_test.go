// Copyright 2013 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package bloom

import (
	crand "crypto/rand"
	"encoding/binary"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/stretchr/testify/require"
)

func (f tableFilter) String() string {
	var buf strings.Builder
	for i, x := range f {
		if i > 0 {
			if i%8 == 0 {
				buf.WriteString("\n")
			} else {
				buf.WriteString("  ")
			}
		}

		for j := uint(0); j < 8; j++ {
			if x&(1<<(7-j)) != 0 {
				buf.WriteString("1")
			} else {
				buf.WriteString(".")
			}
		}
	}
	buf.WriteString("\n")
	return buf.String()
}

func newTableFilter(bitsPerKey uint32, keys ...[]byte) tableFilter {
	w := FilterPolicy(bitsPerKey).NewWriter()
	for _, key := range keys {
		w.AddKey(key)
	}
	data, _, ok := w.Finish()
	if !ok {
		panic("failed to create filter")
	}
	return tableFilter(data)
}

func TestSmallBloomFilter(t *testing.T) {
	f := newTableFilter(10, []byte("hello"), []byte("world"))

	// The magic expected string comes from running RocksDB's util/bloom_test.cc:FullBloomTest.FullSmall.
	want := `
........  ........  ........  .......1  ........  ........  ........  ........
........  .1......  ........  .1......  ........  ........  ........  ........
...1....  ........  ........  ........  ........  ........  ........  ........
........  ........  ........  ........  ........  ........  ........  ...1....
........  ........  ........  ........  .....1..  ........  ........  ........
.......1  ........  ........  ........  ........  ........  .1......  ........
........  ........  ........  ........  ........  ...1....  ........  ........
.......1  ........  ........  ........  .1...1..  ........  ........  ........
.....11.  .......1  ........  ........  ........
`
	want = strings.TrimLeft(want, "\n")
	require.EqualValues(t, want, f.String())

	m := map[string]bool{
		"hello": true,
		"world": true,
		"x":     false,
		"foo":   false,
	}
	for k, want := range m {
		require.EqualValues(t, want, f.MayContain([]byte(k)))
	}
}

func TestBloomFilter(t *testing.T) {
	nextLength := func(x int) int {
		if x < 10 {
			return x + 1
		}
		if x < 100 {
			return x + 10
		}
		if x < 1000 {
			return x + 100
		}
		return x + 1000
	}
	le32 := func(i int) []byte {
		b := make([]byte, 4)
		b[0] = uint8(uint32(i) >> 0)
		b[1] = uint8(uint32(i) >> 8)
		b[2] = uint8(uint32(i) >> 16)
		b[3] = uint8(uint32(i) >> 24)
		return b
	}

	nMediocreFilters, nGoodFilters := 0, 0
loop:
	for length := 1; length <= 10000; length = nextLength(length) {
		keys := make([][]byte, 0, length)
		for i := 0; i < length; i++ {
			keys = append(keys, le32(i))
		}
		f := newTableFilter(10, keys...)
		// The size of the table bloom filter is measured in multiples of the
		// cache line size. The '+2' contribution captures the rounding up in the
		// length division plus preferring an odd number of cache lines. As such,
		// this formula isn't exact, but the exact formula is hard to read.
		maxLen := 5 + ((length*10)/cacheLineBits+2)*cacheLineSize
		if len(f) > maxLen {
			t.Errorf("length=%d: len(f)=%d > max len %d", length, len(f), maxLen)
			continue
		}

		// All added keys must match.
		for _, key := range keys {
			if !f.MayContain(key) {
				t.Errorf("length=%d: did not contain key %q", length, key)
				continue loop
			}
		}

		// Check false positive rate.
		nFalsePositive := 0
		for i := 0; i < 10000; i++ {
			if f.MayContain(le32(1e9 + i)) {
				nFalsePositive++
			}
		}
		if nFalsePositive > 0.02*10000 {
			t.Errorf("length=%d: %d false positives in 10000", length, nFalsePositive)
			continue
		}
		if nFalsePositive > 0.0125*10000 {
			nMediocreFilters++
		} else {
			nGoodFilters++
		}
	}

	if nMediocreFilters > nGoodFilters/5 {
		t.Errorf("%d mediocre filters but only %d good filters", nMediocreFilters, nGoodFilters)
	}
}

func TestHash(t *testing.T) {
	testCases := []struct {
		s        string
		expected uint32
	}{
		// The magic expected numbers come from RocksDB's util/hash_test.cc:TestHash.
		{"", 3164544308},
		{"\x08", 422599524},
		{"\x17", 3168152998},
		{"\x9a", 3195034349},
		{"\x1c", 2651681383},
		{"\x4d\x76", 2447836956},
		{"\x52\xd5", 3854228105},
		{"\x91\xf7", 31066776},
		{"\xd6\x27", 1806091603},
		{"\x30\x46\x0b", 3808221797},
		{"\x56\xdc\xd6", 2157698265},
		{"\xd4\x52\x33", 1721992661},
		{"\x6a\xb5\xf4", 2469105222},
		{"\x67\x53\x81\x1c", 118283265},
		{"\x69\xb8\xc0\x88", 3416318611},
		{"\x1e\x84\xaf\x2d", 3315003572},
		{"\x46\xdc\x54\xbe", 447346355},
		{"\xd0\x7a\x6e\xea\x56", 4255445370},
		{"\x86\x83\xd5\xa4\xd8", 2390603402},
		{"\xb7\x46\xbb\x77\xce", 2048907743},
		{"\x6c\xa8\xbc\xe5\x99", 2177978500},
		{"\x5c\x5e\xe1\xa0\x73\x81", 1036846008},
		{"\x08\x5d\x73\x1c\xe5\x2e", 229980482},
		{"\x42\xfb\xf2\x52\xb4\x10", 3655585422},
		{"\x73\xe1\xff\x56\x9c\xce", 3502708029},
		{"\x5c\xbe\x97\x75\x54\x9a\x52", 815120748},
		{"\x16\x82\x39\x49\x88\x2b\x36", 3056033698},
		{"\x59\x77\xf0\xa7\x24\xf4\x78", 587205227},
		{"\xd3\xa5\x7c\x0e\xc0\x02\x07", 2030937252},
		{"\x31\x1b\x98\x75\x96\x22\xd3\x9a", 469635402},
		{"\x38\xd6\xf7\x28\x20\xb4\x8a\xe9", 3530274698},
		{"\xbb\x18\x5d\xf4\x12\x03\xf7\x99", 1974545809},
		{"\x80\xd4\x3b\x3b\xae\x22\xa2\x78", 3563570120},
		{"\x1a\xb5\xd0\xfe\xab\xc3\x61\xb2\x99", 2706087434},
		{"\x8e\x4a\xc3\x18\x20\x2f\x06\xe6\x3c", 1534654151},
		{"\xb6\xc0\xdd\x05\x3f\xc4\x86\x4c\xef", 2355554696},
		{"\x9a\x5f\x78\x0d\xaf\x50\xe1\x1f\x55", 1400800912},
		{"\x22\x6f\x39\x1f\xf8\xdd\x4f\x52\x17\x94", 3420325137},
		{"\x32\x89\x2a\x75\x48\x3a\x4a\x02\x69\xdd", 3427803584},
		{"\x06\x92\x5c\xf4\x88\x0e\x7e\x68\x38\x3e", 1152407945},
		{"\xbd\x2c\x63\x38\xbf\xe9\x78\xb7\xbf\x15", 3382479516},
	}
	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			require.EqualValues(t, tc.expected, hash([]byte(tc.s)))
		})
	}
}

func BenchmarkBloomFilter(b *testing.B) {
	const keyLen = 128
	const numKeys = 1024
	keys := make([][]byte, numKeys)
	for i := range keys {
		keys[i] = make([]byte, keyLen)
		_, _ = crand.Read(keys[i])
	}
	b.ResetTimer()
	policy := FilterPolicy(10)
	for i := 0; i < b.N; i++ {
		w := policy.NewWriter()
		for _, key := range keys {
			w.AddKey(key)
		}
		w.Finish()
	}
}
func TestFilterSize(t *testing.T) {
	// Test edge cases.
	require.Equal(t, uint64(0), FilterSize(0, 10))
	require.Equal(t, uint64(0), FilterSize(10, 0))
	require.Equal(t, uint64(0), FilterSize(-1, 10))

	// Test that FilterSize matches the actual filter size produced by Finish.
	for range 10000 {
		numKeys := max(1, int(rand.ExpFloat64()*100))
		bitsPerKey := 1 + rand.Uint32N(20)
		w := newTableFilterWriter(bitsPerKey)
		for i := 0; i < numKeys; i++ {
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

	check := func(numKeys int, maxSize uint64) {
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
		numKeys := max(1, int(rand.ExpFloat64()*1000))
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
		tf := tableFilter(filter)
		for i := range numKeys {
			require.True(t, tf.MayContain(key(i)),
				"key %d not found in filter", i)
		}
		// Check that the filter doesn't always return true.
		fp := 0
		const numTests = 1000
		for i := range numTests {
			if tf.MayContain(key(1_000_000_000 + i)) {
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
			fullSize := FilterSize(numKeys, targetBitsPerKey)
			// Choose max size between 0 and 2x full size.
			maxSize := rng.Uint64N(2 * fullSize)

			policy := AdaptivePolicy(targetBitsPerKey, maxSize)
			w := policy.NewWriter()
			for i := 0; i < numKeys; i++ {
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
