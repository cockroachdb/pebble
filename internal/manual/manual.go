// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manual

import (
	"fmt"
	"sync/atomic"

	"github.com/cockroachdb/pebble/v2/internal/invariants"
)

// Purpose identifies the use-case for an allocation.
type Purpose uint8

const (
	_ Purpose = iota

	BlockCacheMap
	BlockCacheEntry
	BlockCacheData
	MemTable

	NumPurposes
)

// Metrics contains memory statistics by purpose.
type Metrics [NumPurposes]struct {
	// InUseBytes is the total number of bytes currently allocated. This is just
	// the sum of the lengths of the allocations and does not include any overhead
	// or fragmentation.
	InUseBytes uint64
}

var counters [NumPurposes]struct {
	InUseBytes atomic.Int64
	// Pad to separate counters into cache lines. This reduces the overhead when
	// multiple purposes are used frequently. We assume 64 byte cache line size
	// which is the case for ARM64 servers and AMD64.
	_ [7]uint64
}

func recordAlloc(purpose Purpose, n int) {
	counters[purpose].InUseBytes.Add(int64(n))
}

func recordFree(purpose Purpose, n int) {
	newVal := counters[purpose].InUseBytes.Add(-int64(n))
	if invariants.Enabled && newVal < 0 {
		panic(fmt.Sprintf("negative counter value %d", newVal))
	}
}

// GetMetrics returns manual memory usage statistics.
func GetMetrics() Metrics {
	var res Metrics
	for i := range res {
		// We load the freed count first to avoid a negative value, since we don't load both counters atomically.
		res[i].InUseBytes = uint64(counters[i].InUseBytes.Load())
	}
	return res
}
