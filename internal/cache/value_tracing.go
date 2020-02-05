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

// Value holds a reference counted immutable value.
//
// This is the definition of Value that is used when the "tracing" build tag is
// specified.
type Value struct {
	buf []byte
	// The number of references on the value. When refs drops to 0, the buf
	// associated with the value may be reused. This is a form of manual memory
	// management. See Cache.Free.
	//
	// Auto values are distinguished by setting their reference count to
	// -(1<<30).
	refs int32
	// Traces recorded by Value.trace. Used for debugging.
	tr struct {
		sync.Mutex
		msgs []string
	}
}

func (v *Value) trace(msg string) {
	s := fmt.Sprintf("%s: refs=%d\n%s", msg, atomic.LoadInt32(&v.refs), debug.Stack())
	v.tr.Lock()
	v.tr.msgs = append(v.tr.msgs, s)
	v.tr.Unlock()
}

func (v *Value) traces() string {
	v.tr.Lock()
	s := strings.Join(v.tr.msgs, "\n")
	v.tr.Unlock()
	return s
}
