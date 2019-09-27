// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package cache

import (
	"testing"
	"unsafe"
)

func TestAllocCache(t *testing.T) {
	c := &allocCache{}
	for i := 0; i < 64; i++ {
		c.free(make([]byte, 1025))
		if c.size == 0 {
			t.Fatalf("expected cache size to be non-zero")
		}
	}
	m := make(map[unsafe.Pointer]bool)
	for i := 0; i < 64; i++ {
		b := c.alloc(1025)
		p := unsafe.Pointer(&b[0])
		if m[p] {
			t.Fatalf("%p already allocated", p)
		}
		m[p] = true
	}
	if c.size != 0 {
		t.Fatalf("expected cache size to be zero, found %d", c.size)
	}
}
