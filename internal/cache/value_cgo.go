// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build ((!invariants && !tracing) || race) && cgo
// +build !invariants,!tracing race
// +build cgo

package cache

import (
	"unsafe"

	"github.com/cockroachdb/pebble/internal/manual"
)

// ValueMetadataSize denotes the number of bytes of metadata allocated for a
// cache entry.
const ValueMetadataSize = int(unsafe.Sizeof(Value{}))

func newValue(n int) *Value {
	if n == 0 {
		return nil
	}

	// When we're not performing leak detection, the lifetime of the returned
	// Value is exactly the lifetime of the backing buffer and we can manually
	// allocate both.
	b := manual.New(ValueMetadataSize + n)
	v := (*Value)(unsafe.Pointer(&b[0]))
	v.buf = b[ValueMetadataSize:]
	v.ref.init(1)
	return v
}

func (v *Value) free() {
	// When we're not performing leak detection, the Value and buffer were
	// allocated contiguously.
	n := ValueMetadataSize + cap(v.buf)
	buf := (*[manual.MaxArrayLen]byte)(unsafe.Pointer(v))[:n:n]
	v.buf = nil
	manual.Free(buf)
}
