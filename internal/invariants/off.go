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

// Buffer is a buffer that may be used to run invariant checks that require
// preserving memory only when the invariants tag is enabled. The user calls
// Save to preserve memory until the next call to Save. If the invariants tag is
// not enabled, Save is a no-op and the buffer is an empty struct.
type BytesBuffer struct{}

// Get returns the current contents of the buffer.
func (b *BytesBuffer) Get() []byte { return nil }

// Save saves the bytes when run with the invariants build tag.
func (b *BytesBuffer) Save(v []byte) {}
