// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// +build !invariants,!tracing

package cache

import (
	"unsafe"

	"github.com/cockroachdb/pebble/internal/manual"
)

const valueSize = int(unsafe.Sizeof(Value{}))

func newManualValue(n int) *Value {
	if n == 0 {
		return nil
	}
	// When we're not performing leak detection, the lifetime of the returned
	// Value is exactly the lifetime of the backing buffer and we can manually
	// allocate both.
	b := allocNew(valueSize + n)
	v := (*Value)(unsafe.Pointer(&b[0]))
	v.buf = b[valueSize:]
	v.refs = 1
	return v
}

func (v *Value) free() {
	// When we're not performing leak detection, the Value and buffer were
	// allocated contiguously.
	n := valueSize + cap(v.buf)
	buf := (*[manual.MaxArrayLen]byte)(unsafe.Pointer(v))[:n:n]
	v.buf = nil
	allocFree(buf)
}
