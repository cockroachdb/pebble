// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// +build !tracing,!invariants

package cache

import (
	"unsafe"

	"github.com/cockroachdb/pebble/internal/manual"
)

// Value holds a reference counted immutable value.
//
// This is the definition of Value that is used in normal builds.
type Value struct {
	buf []byte
	// The number of references on the value. When refs drops to 0, the buf
	// associated with the value may be reused. This is a form of manual memory
	// management. See Cache.Free.
	//
	// Auto values are distinguished by setting their reference count to
	// -(1<<30).
	refs int32
}

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

func (v *Value) trace(msg string) {
}
