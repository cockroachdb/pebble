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
	recordFree(purpose, b.n)
	pools[sizeClass(b.n)].Put(b.data)
}

var pools = mkPools() // pools[n] is for allocs of size 1 << n

func mkPools() [bits.UintSize]sync.Pool {
	var pools [bits.UintSize]sync.Pool
	for i := range pools {
		i := i // watch out for capture bugs
		pools[i].New = func() any {
			return unsafe.Pointer(unsafe.SliceData(make([]byte, 1<<i)))
		}
	}
	return pools
}

// sizeClass determines the smallest n such that 1 << n >= size
func sizeClass(size uintptr) int {
	return bits.UintSize - bits.LeadingZeros(uint(size-1))
}
