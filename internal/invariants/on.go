// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build invariants || race
// +build invariants race

package invariants

// Enabled is true if we were built with the "invariants" or "race" build tags.
const Enabled = true

// CloseChecker is used to check that objects are closed exactly once.
type CloseChecker struct {
	closed bool
}

// Close panics if called twice on the same object (if we were built with the
// "invariants" or "race" build tags).
func (d *CloseChecker) Close() {
	if d.closed {
		// Note: to debug a double-close, you can add a stack field to CloseChecker
		// and set it to string(debug.Stack()) in Close, then print that in this
		// panic.
		panic("double close")
	}
	d.closed = true
}

// AssertClosed panics in invariant builds if Close was not called.
func (d *CloseChecker) AssertClosed() {
	if !d.closed {
		panic("not closed")
	}
}

// AssertNotClosed panics in invariant builds if Close was called.
func (d *CloseChecker) AssertNotClosed() {
	if d.closed {
		panic("closed")
	}
}

// Buffer is a buffer that may be used to run invariant checks that require
// preserving memory only when the invariants tag is enabled. The user calls
// Save to preserve memory until the next call to Save. If the invariants tag is
// not enabled, Save is a no-op and the buffer is an empty struct.
type BytesBuffer struct {
	v []byte
}

// Get returns the current contents of the buffer.
func (b *BytesBuffer) Get() []byte {
	return b.v
}

// Save saves the bytes when run with the invariants build tag.
func (b *BytesBuffer) Save(v []byte) {
	b.v = append(b.v[:0], v...)
}
