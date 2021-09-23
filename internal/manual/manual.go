// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manual

import (
	"sync"

	"modernc.org/memory"
)

// allocator is singleton manual memory allocator.
var allocator = struct {
	sync.Mutex
	memory.Allocator
}{}

// New allocates a slice of size n. The returned slice is from manually managed
// memory and MUST be released by calling Free. Failure to do so will result in
// a memory leak.
func New(n int) []byte {
	if n == 0 {
		return make([]byte, 0)
	}
	allocator.Lock()
	defer allocator.Unlock()
	b, err := allocator.Calloc(n)
	if err != nil {
		panic(err)
	}
	return b
}

// Free frees the specified slice.
func Free(b []byte) {
	if cap(b) == 0 {
		return
	}
	if len(b) == 0 {
		b = b[:cap(b)]
	}
	allocator.Lock()
	defer allocator.Unlock()
	allocator.Free(b)
}
