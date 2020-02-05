// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// +build !tracing

package cache

// Value holds a reference counted immutable value.
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

func (v *Value) trace(msg string) {
}

func (v *Value) traces() string {
	return ""
}
