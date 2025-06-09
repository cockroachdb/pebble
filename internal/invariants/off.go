// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build !invariants && !race
// +build !invariants,!race

package invariants

// Enabled is true if we were built with the "invariants" or "race" build tags.
//
// Enabled should be used to gate invariant checks that may be expensive. It
// should not be used to unconditionally alter a code path significantly (e.g.
// wrapping an iterator - see #3678); Sometimes() should be used instead so that
// the production code path gets test coverage as well.
const Enabled = false

// CloseChecker is used to check that objects are closed exactly once.
type CloseChecker struct{}

// Close panics if called twice on the same object (if we were built with the
// "invariants" or "race" build tags).
func (d *CloseChecker) Close() {}

// AssertClosed panics in invariant builds if Close was not called.
func (d *CloseChecker) AssertClosed() {}

// AssertNotClosed panics in invariant builds if Close was called.
func (d *CloseChecker) AssertNotClosed() {}

// Value is a generic container for a value that should only exist in invariant
// builds. In non-invariant builds, storing a value is a no-op, retrieving a
// value returns the type parameter's zero value, and the Value struct takes up
// no space.
type Value[V any] struct{}

// Get returns the current value, or the zero-value if invariants are disabled.
func (*Value[V]) Get() V {
	var v V // zero value
	return v
}

// Store stores the value.
func (*Value[V]) Store(v V) {}

// SafeSub returns a - b. If a < b, it panics in invariant builds and returns 0
// in non-invariant builds.
func SafeSub[T Integer](a, b T) T {
	if a < b {
		return 0
	}
	return a - b
}

// Integer is a constraint that permits any integer type.
type Integer interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr
}
