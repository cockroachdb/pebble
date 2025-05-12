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
