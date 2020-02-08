// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package cache

import "sync/atomic"

func newAutoValue(b []byte) *Value {
	if b == nil {
		return nil
	}
	// A value starts with an invalid reference count. When the value is added to
	// the cache, the reference count will be set to 2: one reference for the
	// cache, and another for the returned Handle.
	return &Value{buf: b, refs: -(1 << 30)}
}

// Buf returns the buffer associated with the value. The contents of the buffer
// should not be changed once the value has been added to the cache. Instead, a
// new Value should be created and added to the cache to replace the existing
// value.
func (v *Value) Buf() []byte {
	if v == nil {
		return nil
	}
	return v.buf
}

// Truncate the buffer to the specified length. The buffer length should not be
// changed once the value has been added to the cache as there may be
// concurrent readers of the Value. Instead, a new Value should be created and
// added to the cache to replace the existing value.
func (v *Value) Truncate(n int) {
	v.buf = v.buf[:n]
}

func (v *Value) auto() bool {
	return atomic.LoadInt32(&v.refs) < 0
}

func (v *Value) manual() bool {
	return !v.auto()
}

func (v *Value) acquire() {
	atomic.AddInt32(&v.refs, 1)
	v.trace("acquire")
}

func (v *Value) release() {
	n := atomic.AddInt32(&v.refs, -1)
	v.trace("release")
	if n == 0 {
		v.free()
	}
}
