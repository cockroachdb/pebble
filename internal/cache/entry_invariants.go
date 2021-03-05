// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.
//
// +build invariants tracing

package cache

import (
	"fmt"
	"os"
	"runtime"
)

// When the "invariants" or "tracing" build tags are enabled, we need to
// allocate entries using the Go allocator so entry.val properly maintains a
// reference to the Value.
const entriesGoAllocated = true

func entryAllocNew() *entry {
	e := &entry{}
	return e
}

func entryAllocFree(e *entry) {
}
