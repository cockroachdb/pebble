// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build ((!invariants && !tracing) || race) && !cgo
// +build !invariants,!tracing race
// +build !cgo

package cache

// ValueMetadataSize denotes the number of bytes of metadata allocated for a
// cache entry.
const ValueMetadataSize = 0

func newValue(n int) *Value {
	if n == 0 {
		return nil
	}

	// Since Cgo is disabled then all memory is allocated from the Go heap we
	// can't play the trick below to combine the Value and buffer allocation.
	v := &Value{buf: make([]byte, n)}
	v.ref.init(1)
	return v
}

func (v *Value) free() {
}
