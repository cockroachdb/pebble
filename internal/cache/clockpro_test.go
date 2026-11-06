// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package cache

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReserveColdTarget(t *testing.T) {
	// If coldTarget isn't updated when we call shard.Reserve,
	// then we unnecessarily remove nodes from the
	// cache.

	cache := NewWithShards(100, 1)
	defer cache.Unref()
	h := make([]*Handle, 50)
	for i := range h {
		h[i] = cache.NewHandle()
	}
	defer func() {
		for i := range h {
			h[i].Close()
		}
	}()

	for i := range h {
		setTestValue(h[i], 0, 0, "a", 1)
	}

	if cache.Size() != 50 {
		require.Equal(t, 50, cache.Size(), "nodes were unnecessarily evicted from the cache")
	}

	// There won't be enough space left for 50 nodes in the cache after
	// we call shard.Reserve. This should trigger a call to evict.
	cache.Reserve(51)

	// If we don't update coldTarget in Reserve then the cache gets emptied to
	// size 0. In shard.Evict, we loop until shard.Size() < shard.targetSize().
	// Therefore, 100 - 51 = 49, but we evict one more node.
	if cache.Size() != 48 {
		t.Fatalf("expected positive cache size %d, but found %d", 48, cache.Size())
	}
}

func TestSetColdEntryPromotesToHot(t *testing.T) {
	for _, tc := range []struct {
		name    string
		newSize int
	}{
		{"same size", 3},
		{"different size", 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache := NewWithShards(100, 1)
			defer cache.Unref()
			h := cache.NewHandle()
			defer h.Close()

			setTestValue(h, 0, 0, "a", 3)

			s := &cache.shards[0]
			require.Equal(t, int64(1), s.countCold)
			require.Equal(t, int64(0), s.countHot)

			setTestValue(h, 0, 0, "b", tc.newSize)

			require.Equal(t, int64(0), s.countCold)
			require.Equal(t, int64(0), s.sizeCold)
			require.Equal(t, int64(1), s.countHot)
			require.Equal(t, int64(tc.newSize), s.sizeHot)

			setTestValue(h, 0, 0, "c", 2)

			require.Equal(t, int64(1), s.countHot)
			require.Equal(t, int64(2), s.sizeHot)
		})
	}
}

func checkClockPROAccounting(t *testing.T, s *shard) {
	t.Helper()
	s.mu.RLock()
	defer s.mu.RUnlock()

	var counts, sizes [3]int64

	if head := s.handHot; head != nil {
		e := head
		for steps := 0; ; steps++ {
			if steps >= 1000 {
				t.Fatal("clock list failed to return to its starting entry")
			}

			require.NotNil(t, e)
			require.NotNil(t, e.next())
			require.True(t, e.next().prev() == e)

			switch e.ptype {
			case etTest, etCold, etHot:
			default:
				t.Fatalf("unexpected entry type: %d", e.ptype)
			}

			counts[e.ptype]++
			sizes[e.ptype] += e.size

			if e.ptype == etTest {
				require.Nil(t, e.val)
			} else {
				require.NotNil(t, e.val)
				require.Equal(t, e.size, int64(len(e.val.buf)))
			}

			e = e.next()
			if e == head {
				break
			}
		}
	}

	require.Equal(t,
		[3]int64{s.countTest, s.countCold, s.countHot}, counts)
	require.Equal(t,
		[3]int64{s.sizeTest, s.sizeCold, s.sizeHot}, sizes)
}

func TestSetColdEntryPromotionUnderEvictionPressure(t *testing.T) {
	cache := NewWithShards(24, 1)
	defer cache.Unref()

	h := cache.NewHandle()
	defer h.Close()

	s := &cache.shards[0]

	// First verify the cold-to-hot transition without eviction.
	setTestValue(h, 0, 0, "a", 4)
	setTestValue(h, 0, 0, "b", 4)

	require.Equal(t, int64(1), s.countHot)
	require.Equal(t, int64(0), s.countCold)
	checkClockPROAccounting(t, s)

	// Exercise replacements, size changes, and CLOCK-Pro sweeps
	// under a cache capacity much smaller than the working set.
	for i := 0; i < 300; i++ {
		offset := uint64(i % 17)

		setTestValue(h, 0, offset, "x", 1+i%7)
		setTestValue(h, 0, offset, "y", 1+(i+3)%9)

		checkClockPROAccounting(t, s)
	}

	// File eviction must clean up hot, cold, and test entries.
	h.EvictFile(0)
	checkClockPROAccounting(t, s)

	require.Zero(t, cache.Size())
	require.Nil(t, s.handHot)
	require.Nil(t, s.handCold)
	require.Nil(t, s.handTest)
}
