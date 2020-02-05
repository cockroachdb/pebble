// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package cache

import (
	"testing"
	"unsafe"
)

func TestAllocCache(t *testing.T) {
	c := newAllocCache()
	for i := 0; i < 64; i++ {
		c.free(manualNew(1025))
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

func TestAllocCacheEvict(t *testing.T) {
	c := newAllocCache()
	for i := 0; i < allocCacheCountLimit; i++ {
		c.free(manualNew(1024))
	}

	bufs := make([][]byte, allocCacheCountLimit)
	for j := range bufs {
		bufs[j] = c.alloc(2048)
	}
	for j := range bufs {
		c.free(bufs[j])
	}

	count := 0
	for i := range c.bufs {
		if cap(c.bufs[i]) == 2048 {
			count++
		}
	}

	if expected := allocCacheCountLimit / 4; count < expected {
		t.Errorf("expected at least %d cached 2KB buffers, but found %d", expected, count)
	}
}

func BenchmarkAllocCache(b *testing.B) {
	// Populate the cache with buffers if one size class.
	c := newAllocCache()
	for i := 0; i < allocCacheCountLimit; i++ {
		c.free(manualNew(1024))
	}

	// Benchmark allocating buffers of a different size class.
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v1 := c.alloc(2048)
		v2 := c.alloc(2048)
		c.free(v1)
		c.free(v2)
	}
}
