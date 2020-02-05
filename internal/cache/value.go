// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package cache

import (
	"fmt"
	"os"
	"runtime"
	"sync/atomic"

	"github.com/cockroachdb/pebble/internal/invariants"
)

func checkValue(obj interface{}) {
	v := obj.(*Value)
	if v.buf != nil {
		fmt.Fprintf(os.Stderr, "%p: cache value was not freed: refs=%d\n%s",
			v.buf, atomic.LoadInt32(&v.refs), v.traces())
		os.Exit(1)
	}
}

func newManualValue(b []byte) *Value {
	if b == nil {
		return nil
	}
	v := &Value{buf: b, refs: 1}
	if invariants.Enabled {
		v.trace("alloc")
		runtime.SetFinalizer(v, checkValue)
	}
	return v
}

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

func (v *Value) makeWeak() {
	if atomic.LoadInt32(&v.refs) > 0 {
		panic("pebble: cannot make manual Value into a weak Value")
	}
	// Add a reference to the value which will never be cleared. This is
	// necessary because WeakHandle.Get() performs an atomic load of the value,
	// but we need to ensure that nothing can concurrently be freeing the buffer
	// for reuse. Rather than add additional locking to this code path, we add a
	// reference here so that the underlying buffer can never be reused. And we
	// rely on the Go runtime to eventually GC the value and run the associated
	// finalizer.
	v.acquire()
}

func (v *Value) acquire() {
	atomic.AddInt32(&v.refs, 1)
	v.trace("acquire")
}

func (v *Value) release() bool {
	n := atomic.AddInt32(&v.refs, -1)
	v.trace("release")
	return n == 0
}
