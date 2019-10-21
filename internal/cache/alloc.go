// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package cache

import (
	"sync"
	"time"

	"golang.org/x/exp/rand"
)

const (
	// The min size of a byte slice held in an allocCache instance. Byte slices
	// smaller than this value will not be cached.
	allocCacheMinSize = 1024
	// The max size of a byte slice held in an allocCache instance. Byte slices
	// larger than this value will not be cached.
	allocCacheMaxSize    = 64 * 1024
	allocCacheCountLimit = 16
	// The maximum size of data held in a single allocCache instance. There will
	// be O(num-cpus) allocCaches per Cache.
	allocCacheSizeLimit = 512 * 1024
)

var allocPool = sync.Pool{
	New: func() interface{} {
		return newAllocCache()
	},
}

// allocNew allocates a slice of size n. The use of sync.Pool provides a
// per-cpu cache of allocCache structures to allocate from.
func allocNew(n int) []byte {
	a := allocPool.Get().(*allocCache)
	b := a.alloc(n)
	allocPool.Put(a)
	return b
}

// allocFree releases the specified slice back to a per-cpu cache of buffers.
func allocFree(b []byte) {
	a := allocPool.Get().(*allocCache)
	a.free(b)
	allocPool.Put(a)
}

// allocCache implements a small cache for the byte slice allocations used by
// the Cache. If an allocCache is empty, allocations are passed through to the
// Go runtime allocator. An allocCache holds a list of recently freed buffers
// that is capped at 16 entries and 512KB in size. When a buffer is freed, the
// existing cached buffers are trimmed until the new entry can fit with the
// count and size limits. When a buffer is allocated, this cached list is
// walked from beginning to end looking for the first buffer which is the same
// size class as the allocation. Size classes are multiples of 1024.
type allocCache struct {
	size int
	rnd  rand.PCGSource
	bufs [][]byte
}

func sizeToClass(size int) int {
	return (size - 1) / 1024
}

func classToSize(class int) int {
	return (class + 1) * 1024
}

func newAllocCache() *allocCache {
	c := &allocCache{
		bufs: make([][]byte, 0, allocCacheCountLimit),
	}
	c.rnd.Seed(uint64(time.Now().UnixNano()))
	return c
}

func (c *allocCache) alloc(n int) []byte {
	if n < allocCacheMinSize || n >= allocCacheMaxSize {
		return make([]byte, n)
	}

	class := sizeToClass(n)
	for i := range c.bufs {
		if t := c.bufs[i]; sizeToClass(cap(t)) == class {
			j := len(c.bufs) - 1
			c.bufs[i], c.bufs[j] = c.bufs[j], nil
			c.bufs = c.bufs[:j]
			c.size -= cap(t)
			return t[:n]
		}
	}

	return make([]byte, n, classToSize(class))
}

func (c *allocCache) free(b []byte) {
	n := cap(b)
	if n < allocCacheMinSize || n >= allocCacheMaxSize {
		return
	}
	b = b[:n:n]

	// Rather than always evicting a specific slot, we randomly choose a slot to
	// evict. This ensures we will eventually evict buffers which are not being
	// used. Always evicting the first element and preserving exactly the N most
	// recently freed buffers would be expensive as we'd have to shift the
	// elements in the slice on every free.
	for c.size+n >= allocCacheSizeLimit || len(c.bufs) >= allocCacheCountLimit {
		// Randomly choose a victim buffer to evict. We swap the victim to the last
		// element in the slice and then trim the length of the slice.
		i := len(c.bufs) - 1
		// This is the integer multiplication version of bounding a random integer
		// to a range: (range * rand[0-65535]) / 65536. See
		// http://www.pcg-random.org/posts/bounded-rands.html. The random integers
		// are biased, but that is fine for the usage here.
		j := (uint32(len(c.bufs)) * (uint32(c.rnd.Uint64()) & ((1 << 16) - 1))) >> 16
		c.size -= cap(c.bufs[j])
		c.bufs[i], c.bufs[j] = nil, c.bufs[i]
		c.bufs = c.bufs[:i]
	}

	c.bufs = append(c.bufs, b)
	c.size += n
}
