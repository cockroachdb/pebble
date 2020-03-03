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

// initialize the reference count to the specified value.
func (v *refcnt) init(val int32) {
	*v = refcnt(val)
}

func (v *refcnt) refs() int32 {
	return atomic.LoadInt32((*int32)(v))
}

func (v *refcnt) acquire() {
	atomic.AddInt32((*int32)(v), 1)
}

func (v *refcnt) release() bool {
	return atomic.AddInt32((*int32)(v), -1) == 0
}

func (v *refcnt) trace(msg string) {
}

func (v *refcnt) traces() string {
	return ""
}

// Silence unused warning.
var _ = (*refcnt)(nil).traces
