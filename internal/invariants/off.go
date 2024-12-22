// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build !invariants && !race

package invariants

// Sometimes returns true percent% of the time if invariants are Enabled (i.e.
// we were built with the "invariants" or "race" build tags). Otherwise, always
// returns false.
func Sometimes(percent int) bool {
	return false
}

// CloseChecker is used to check that objects are closed exactly once. It is
// empty and does nothing in non-invariant builds.
//
// Note that in non-invariant builds, the struct is zero-sized but it can still
// increase the size of a parent struct if it is the last field (because Go must
// allow getting a valid pointer address of the field).
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
//
// Note that in non-invariant builds, the struct is zero-sized but it can still
// increase the size of a parent struct if it is the last field (because Go must
// allow getting a valid pointer address of the field).
type Value[V any] struct{}

// Get the current value, or the zero value if invariants are disabled.
func (*Value[V]) Get() V {
	var v V // zero value
	return v
}

// Set the value; no-op in non-invariant builds.
func (*Value[V]) Set(v V) {}
