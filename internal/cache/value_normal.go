// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// +build !tracing,!invariants

package cache

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

func newManualValue(n int) *Value {
	if n == 0 {
		return nil
	}
	b := allocNew(n)
	return &Value{buf: b, refs: 1}
}

func (v *Value) trace(msg string) {
}
