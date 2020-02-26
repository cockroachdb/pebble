// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// +build !tracing

package cache

import "sync/atomic"

// refcnt provides an atomic reference count. This version is used when the
// "tracing" build tag is not enabled. See refcnt_tracing.go for the "tracing"
// enabled version.
type refcnt int32

// initialize the reference count to the specified value, optional setting the
// "weak" bit. The "weak" bit is used by entry and Value to indicate that a
// Handle can be converted to a WeakHandle. It is stored in the reference count
// for convenience, but is otherwise unrelated to the lifetime management that
// the reference count provides.
func (v *refcnt) init(val int32, weak bool) {
	*v = refcnt(val)
	if weak {
		*v |= refcnt(refcntWeakBit)
	}
}

func (v *refcnt) weak() bool {
	return (atomic.LoadInt32((*int32)(v)) & refcntWeakBit) != 0
}

func (v *refcnt) refs() int32 {
	return atomic.LoadInt32((*int32)(v)) & refcntRefsMask
}

func (v *refcnt) acquire() {
	atomic.AddInt32((*int32)(v), 1)
}

func (v *refcnt) release() bool {
	return (atomic.AddInt32((*int32)(v), -1) & refcntRefsMask) == 0
}

func (v *refcnt) trace(msg string) {
}

func (v *refcnt) traces() string {
	return ""
}

// Silence unused warning.
var _ = (*refcnt)(nil).traces
