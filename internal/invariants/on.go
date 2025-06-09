// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build invariants || race
// +build invariants race

package invariants

import "fmt"

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

// Value is a generic container for a value that should only exist in invariant
// builds. In non-invariant builds, storing a value is a no-op, retrieving a
// value returns the type parameter's zero value, and the Value struct takes up
// no space.
type Value[V any] struct {
	v V
}

// Get returns the current value, or the zero-value if invariants are disabled.
func (v *Value[V]) Get() V {
	return v.v
}

// Store stores the value.
func (v *Value[V]) Store(inner V) {
	v.v = inner
}

// SafeSub returns a - b. If a < b, it panics in invariant builds and returns 0
// in non-invariant builds.
func SafeSub[T Integer](a, b T) T {
	if a < b {
		panic(fmt.Sprintf("underflow: %d - %d", a, b))
	}
	return a - b
}

// Integer is a constraint that permits any integer type.
type Integer interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr
}
