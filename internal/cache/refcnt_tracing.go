// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// +build tracing

package cache

import (
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
)

// refcnt provides an atomic reference count, along with a tracing facility for
// debugging logic errors in manipulating the reference count. This version is
// used when the "tracing" build tag is enabled.
type refcnt struct {
	val int32
	sync.Mutex
	msgs []string
}

func (v *refcnt) init(val int32, weak bool) {
	v.val = val
	if weak {
		v.val |= refcntWeakBit
	}
	v.trace("init")
}

func (v *refcnt) weak() bool {
	return (atomic.LoadInt32(&v.val) & refcntWeakBit) != 0
}

func (v *refcnt) refs() int32 {
	return atomic.LoadInt32(&v.val) & refcntRefsMask
}

func (v *refcnt) acquire() {
	atomic.AddInt32(&v.val, 1)
	v.trace("acquire")
}

func (v *refcnt) release() bool {
	n := atomic.AddInt32(&v.val, -1)
	v.trace("release")
	return (n & refcntRefsMask) == 0
}

func (v *refcnt) trace(msg string) {
	s := fmt.Sprintf("%s: refs=%d weak=%t\n%s", msg, v.refs(), v.weak(), debug.Stack())
	v.Lock()
	v.msgs = append(v.msgs, s)
	v.Unlock()
}

func (v *refcnt) traces() string {
	v.Lock()
	s := strings.Join(v.msgs, "\n")
	v.Unlock()
	return s
}
