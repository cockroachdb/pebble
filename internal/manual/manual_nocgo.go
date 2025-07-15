// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build !cgo

package manual

import (
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
	slice := make([]byte, n)
	return Buf{
		data: unsafe.Pointer(unsafe.SliceData(slice)),
		n:    n,
	}
}

// Free frees the specified slice. It has to be exactly the slice that was
// returned by New.
func Free(purpose Purpose, b Buf) {
	invariants.MaybeMangle(b.Slice())
	recordFree(purpose, b.n)
}
