// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.
//
// +build invariants tracing

package cache

// When the "invariants" or "tracing" build tags are enabled, we need to
// allocate entries using the Go allocator so entry.val properly maintains a
// reference to the Value.

func entryAllocNew() *entry {
	return &entry{}
}

func entryAllocFree(e *entry) {
}
