// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manual

import (
	"fmt"
	"sync/atomic"
	"unsafe"

	"github.com/cockroachdb/pebble/v2/internal/invariants"
)

// Buf is a buffer allocated using this package.
type Buf struct {
	data unsafe.Pointer
	n    uintptr
}

// MakeBufUnsafe should be used with caution: the given data and n must match
// exactly the data and length of a Buf obtained from New. It is useful when
// these are stored implicitly in another type (like a []byte) and we want to
// reconstruct the Buf.
func MakeBufUnsafe(data unsafe.Pointer, n uintptr) Buf {
	return Buf{data: data, n: n}
}

// Data returns a pointer to the buffer data. If the buffer is not initialized
// (or is the result of calling New with a zero length), returns nil.
func (b Buf) Data() unsafe.Pointer {
	return b.data
}

func (b Buf) Len() uintptr {
	return b.n
}

// Slice converts the buffer to a byte slice.
func (b Buf) Slice() []byte {
	return unsafe.Slice((*byte)(b.data), b.n)
}

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

func recordAlloc(purpose Purpose, n uintptr) {
	counters[purpose].InUseBytes.Add(int64(n))
}

func recordFree(purpose Purpose, n uintptr) {
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
