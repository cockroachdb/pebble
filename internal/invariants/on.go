// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build invariants || race

package invariants

import (
	"fmt"
	"math/rand/v2"
	"slices"
)

// Sometimes returns true percent% of the time if invariants are Enabled (i.e.
// we were built with the "invariants" or "race" build tags). Otherwise, always
// returns false.
func Sometimes(percent int) bool {
	return rand.Uint32N(100) < uint32(percent)
}

// CloseChecker is used to check that objects are closed exactly once. It is
// empty and does nothing in non-invariant builds.
//
// Note that in non-invariant builds, the struct is zero-sized but it can still
// increase the size of a parent struct if it is the last field (because Go must
// allow getting a valid pointer address of the field).
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
//
// Note that in non-invariant builds, the struct is zero-sized but it can still
// increase the size of a parent struct if it is the last field (because Go must
// allow getting a valid pointer address of the field).
type Value[V any] struct {
	v V
}

// Get the current value, or the zero-value if invariants are disabled.
func (v *Value[V]) Get() V {
	return v.v
}

// Mangle mangles a byte slice in invariant builds.
func Mangle(b []byte) {
	for i := range b {
		b[i] = 0xCC
	}
}

// BufMangler is a utility that can be used to test that the caller doesn't use
type BufMangler struct {
	lastReturnedBuf []byte
}

// MaybeMangleLater returns either the given buffer or a copy of it which will
// be mangled the next time this function is called.
func (bm *BufMangler) MaybeMangleLater(buf []byte) []byte {
	if bm.lastReturnedBuf != nil {
		for i := range bm.lastReturnedBuf {
			bm.lastReturnedBuf[i] = 0xCC
		}
		bm.lastReturnedBuf = nil
	}
	if rand.Uint32N(2) == 0 {
		bm.lastReturnedBuf = slices.Clone(buf)
		return bm.lastReturnedBuf
	}
	return buf
}

// Set the value; no-op in non-invariant builds.
func (v *Value[V]) Set(inner V) {
	v.v = inner
}

// CheckBounds panics if the index is not in the range [0, n). No-op in
// non-invariant builds.
func CheckBounds[T Integer](i T, n T) {
	if i < 0 || i >= n {
		panic(fmt.Sprintf("index %d out of bounds [0, %d)", i, n))
	}
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
