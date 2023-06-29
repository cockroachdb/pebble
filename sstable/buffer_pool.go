// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/fastrand"
)

// A bufferHandle is a handle to manually-managed memory. The handle may point
// to a block in the block cache (h.Get() != nil), or a buffer that exists
// outside the block cache allocated from a BufferPool (h.b != nil).
type bufferHandle struct {
	h cache.Handle
	b *Buf
}

func (bh bufferHandle) Get() []byte {
	if v := bh.h.Get(); v != nil {
		return v
	} else if bh.b != nil {
		return bh.b.b
	}
	return nil
}

func (bh bufferHandle) Release() {
	bh.h.Release()
	if bh.b != nil {
		bh.b.Release()
	}
}

// A BufferPool holds a pool of buffers for holding sstable blocks.
//
// BufferPool is not thread-safe.
type BufferPool struct {
	cache *cache.Cache
	size  int
	// The first `size` elements are non-nil.
	pool []*Buf
}

// Init initializes the pool to use the provided cache for allocations, and to
// limit its pooled buffers to maxSize.
func (p *BufferPool) Init(cache *cache.Cache, maxSize int) {
	*p = BufferPool{
		cache: cache,
		pool:  make([]*Buf, maxSize),
	}
}

// Release releases all buffers held by the pool.
func (p *BufferPool) Release() {
	for i := 0; i < p.size; i++ {
		p.cache.Free(p.pool[i].v)
	}
	*p = BufferPool{}
}

// Alloc allocates a new buffer of size n. If the pool already holds a buffer at
// least as large as n, the pooled buffer is used instead.
func (p *BufferPool) Alloc(n int) *Buf {
	for i := p.size - 1; i >= 0; i-- {
		if len(p.pool[i].b) >= n {
			b := p.pool[i]
			b.b = b.b[:n]
			copy(p.pool[i:], p.pool[i+1:])
			p.size--
			return b
		}
	}
	// Allocate a new buffer.
	v := p.cache.Alloc(n)
	return &Buf{
		p: p,
		v: v,
		b: v.Buf(),
	}
}

// A Buf holds a manually-managed byte buffer.
type Buf struct {
	p *BufferPool
	v *cache.Value
	// b holds the current byte slice. It's backed by v, but may be a subslice
	// of v's memory.
	//
	// len(b) ≤ len(v.Buf())
	b []byte
}

// Release releases the buffer back to the pool.
func (b *Buf) Release() {
	// The caller may have resliced o.b to be a subslice of the underlying
	// cache.Value. Expand to the original slice.
	b.b = b.v.Buf()
	if b.p.size == len(b.p.pool) {
		// The pool is full. Evict a random item.
		i := int(fastrand.Uint32()) % len(b.p.pool)
		b.p.cache.Free(b.p.pool[i].v)
		b.p.pool[i] = b
	} else {
		b.p.pool[b.p.size] = b
		b.p.size++
	}
}
