// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build !cgo

package manual

import (
	"math/bits"
	"sync"
	"unsafe"

	"github.com/cockroachdb/pebble/internal/invariants"
)

// Provides versions of New and Free when cgo is not available (e.g. cross
// compilation).

// New allocates a slice of size n.
func New(purpose Purpose, n uintptr) Buf {
	if n == 0 {
		return Buf{}
	}
	recordAlloc(purpose, n)
	return Buf{
		data: pools[sizeClass(n)].Get().(unsafe.Pointer),
		n:    n,
	}
}

// Free frees the specified slice. It has to be exactly the slice that was
// returned by New.
func Free(purpose Purpose, b Buf) {
	invariants.MaybeMangle(b.Slice())
	if b.data == nil {
		return
	}

	// Clear the block for consistency with the cgo implementation (which uses calloc)
	clear(unsafe.Slice((*byte)(b.data), b.n))

	recordFree(purpose, b.n)
	pools[sizeClass(b.n)].Put(b.data)
}

var pools [bits.UintSize]sync.Pool // pools[n] is for allocs of size 1 << n

func init() {
	for i := range pools {
		allocSize := 1 << i
		pools[i].New = func() any {
			// Boxing an unsafe.Pointer into an interface does not require an allocation
			return unsafe.Pointer(unsafe.SliceData(make([]byte, allocSize)))
		}
	}
}

// sizeClass determines the smallest n such that 1 << n >= size
func sizeClass(size uintptr) int {
	if invariants.Enabled && size == 0 {
		panic("zero size should never get through New")
	}
	return bits.Len(uint(size - 1))
}
