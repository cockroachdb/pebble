// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build invariants || race
// +build invariants race

package invariants

// Enabled is true if we were built with the "invariants" or "race" build tags.
const Enabled = true

// DoubleCloseCheck is used to check that objects are not double-closed.
type DoubleCloseCheck struct {
	closed bool
}

// Close panics if called twice on the same object (if we were built with the
// "invariants" or "race" build tags).
func (d *DoubleCloseCheck) Close() {
	if d.closed {
		panic("double close")
	}
	d.closed = true
}
