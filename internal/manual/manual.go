// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manual

import "sync/atomic"

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

	// InUseBytes is the total cumulative number of bytes allocated since the
	// process started. This is just the sum of the lengths of the allocations and
	// does not include any overhead or fragmentation.
	TotalBytes uint64
}

var counters [NumPurposes]struct {
	TotalAllocated atomic.Uint64
	TotalFreed     atomic.Uint64
	// Pad to separate counters into cache lines. This reduces the overhead when
	// multiple purposes are used frequently. We assume 64 byte cache line size
	// which is the case for ARM64 servers and AMD64.
	_ [6]uint64
}

// GetMetrics returns manual memory usage statistics.
func GetMetrics() Metrics {
	var res Metrics
	for i := range res {
		res[i].TotalBytes = counters[i].TotalAllocated.Load()
		res[i].InUseBytes = res[i].TotalBytes - counters[i].TotalFreed.Load()
	}
	return res
}
