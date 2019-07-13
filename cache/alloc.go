// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package cache

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
	bufs [][]byte
}

func sizeToClass(size int) int {
	return (size - 1) / 1024
}

func classToSize(class int) int {
	return (class + 1) * 1024
}

func (c *allocCache) alloc(n int) []byte {
	if n < allocCacheMinSize || n >= allocCacheMaxSize {
		return make([]byte, n)
	}

	class := sizeToClass(n)
	for i := range c.bufs {
		if t := c.bufs[i]; sizeToClass(len(t)) == class {
			j := len(c.bufs) - 1
			c.bufs[i], c.bufs[j] = c.bufs[j], c.bufs[i]
			c.bufs[j] = nil
			c.bufs = c.bufs[:j]
			c.size -= len(t)
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

	for c.size+n >= allocCacheSizeLimit || len(c.bufs) >= allocCacheCountLimit {
		i := len(c.bufs) - 1
		t := c.bufs[i]
		c.size -= len(t)
		c.bufs[i] = nil
		c.bufs = c.bufs[:i]
	}
	c.bufs = append(c.bufs, b)
	c.size += n
}
