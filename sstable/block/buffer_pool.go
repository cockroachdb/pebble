// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package block

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/invariants"
)

// Alloc allocates a new Value for a block of length n (excluding the block
// trailer, but including an associated Metadata). If bufferPool is non-nil,
// Alloc allocates the buffer from the pool. Otherwise it allocates it from the
// block cache.
func Alloc(n int, p *BufferPool) Value {
	if p != nil {
		return Value{buf: p.Alloc(MetadataSize + n)}
	}
	return Value{v: cache.Alloc(MetadataSize + n)}
}

// Value is a block buffer, either backed by the block cache or a BufferPool.
type Value struct {
	// buf.Valid() returns true if backed by a BufferPool.
	buf Buf
	// v is non-nil if backed by the block cache.
	v *cache.Value
}

// getInternalBuf gets the underlying buffer which includes the Metadata and the
// block.
func (b Value) getInternalBuf() []byte {
	if b.buf.Valid() {
		return b.buf.p.pool[b.buf.i].b
	}
	return b.v.RawBuffer()
}

// BlockData returns the byte slice for the block data.
func (b Value) BlockData() []byte {
	return b.getInternalBuf()[MetadataSize:]
}

// BlockMetadata returns the block metadata.
func (b Value) BlockMetadata() *Metadata {
	return (*Metadata)(b.getInternalBuf())
}

// MakeHandle constructs a BufferHandle from the Value.
func (b Value) MakeHandle() BufferHandle {
	if b.buf.Valid() {
		return BufferHandle{b: b.buf}
	}
	return BufferHandle{cv: b.v}
}

func (b *Value) SetInCacheForTesting(h *cache.Handle, fileNum base.DiskFileNum, offset uint64) {
	if b.buf.Valid() {
		panic("block value must be backed by a cache.Value")
	}
	h.Set(fileNum, offset, b.v)
	b.v.Release()
	b.v = nil
}

// Release releases the handle.
func (b Value) Release() {
	if b.buf.Valid() {
		b.buf.Release()
	} else {
		cache.Free(b.v)
	}
}

// Truncate truncates the block to n bytes.
func (b Value) Truncate(n int) {
	n += MetadataSize
	if b.buf.Valid() {
		b.buf.p.pool[b.buf.i].b = b.buf.p.pool[b.buf.i].b[:n]
	} else {
		b.v.Truncate(n)
	}
}

// A BufferHandle is a handle to manually-managed memory. The handle may point
// to a block in the block cache (h.cv != nil), or a buffer that exists outside
// the block cache allocated from a BufferPool (b.Valid()).
type BufferHandle struct {
	cv *cache.Value
	b  Buf
}

// CacheBufferHandle constructs a BufferHandle from a block cache Handle.
func CacheBufferHandle(cv *cache.Value) BufferHandle {
	return BufferHandle{cv: cv}
}

// Valid returns true if the BufferHandle holds a value.
func (bh BufferHandle) Valid() bool {
	return bh.cv != nil || bh.b.Valid()
}

func (bh BufferHandle) rawBuffer() []byte {
	if bh.cv != nil {
		return bh.cv.RawBuffer()
	}
	return bh.b.p.pool[bh.b.i].b
}

// BlockMetadata returns the buffer for the block metadata.
func (bh BufferHandle) BlockMetadata() *Metadata {
	return (*Metadata)(bh.rawBuffer())
}

// BlockData retrieves the buffer for the block data.
func (bh BufferHandle) BlockData() []byte {
	return (bh.rawBuffer())[MetadataSize:]
}

// Release releases the buffer, either back to the block cache or BufferPool. It
// is okay to call Release on a zero-value BufferHandle (to no effect).
func (bh BufferHandle) Release() {
	bh.cv.Release()
	bh.b.Release()
}

// A BufferPool holds a pool of buffers for holding sstable blocks. An initial
// size of the pool is provided on Init, but a BufferPool will grow to meet the
// largest working set size. It'll never shrink. When a buffer is released, the
// BufferPool recycles the buffer for future allocations.
//
// A BufferPool should only be used for short-lived allocations with
// well-understood working set sizes to avoid excessive memory consumption.
//
// BufferPool is not thread-safe.
type BufferPool struct {
	// pool contains all the buffers held by the pool, including buffers that
	// are in-use. For every i < len(pool): pool[i].v is non-nil.
	pool []AllocedBuffer
}

// AllocedBuffer is an allocated memory buffer.
type AllocedBuffer struct {
	v *cache.Value
	// b holds the current byte slice. It's backed by v, but may be a subslice
	// of v's memory while the buffer is in-use [ len(b) ≤ len(v.RawBuffer()) ].
	//
	// If the buffer is not currently in-use, b is nil. When being recycled, the
	// BufferPool.Alloc will reset b to be a subslice of v.RawBuffer().
	b []byte
}

// Init initializes the pool with an initial working set buffer size of
// `initialSize`.
func (p *BufferPool) Init(initialSize int) {
	*p = BufferPool{
		pool: make([]AllocedBuffer, 0, initialSize),
	}
}

// InitPreallocated is like Init but for internal sstable package use in
// instances where a pre-allocated slice of []allocedBuffer already exists. It's
// used to avoid an extra allocation initializing BufferPool.pool.
func (p *BufferPool) InitPreallocated(pool []AllocedBuffer) {
	*p = BufferPool{
		pool: pool[:0],
	}
}

// Release releases all buffers held by the pool and resets the pool to an
// uninitialized state.
func (p *BufferPool) Release() {
	for i := range p.pool {
		if p.pool[i].b != nil {
			panic(errors.AssertionFailedf("Release called on a BufferPool with in-use buffers"))
		}
		cache.Free(p.pool[i].v)
		p.pool[i].v = nil
	}
	p.pool = p.pool[:0]
}

// Alloc allocates a new buffer of size n. If the pool already holds a buffer at
// least as large as n, the pooled buffer is used instead.
//
// Alloc is O(MAX(N,M)) where N is the largest number of concurrently in-use
// buffers allocated and M is the initialSize passed to Init.
func (p *BufferPool) Alloc(n int) Buf {
	unusableBufferIdx := -1
	for i := 0; i < len(p.pool); i++ {
		if p.pool[i].b == nil {
			if len(p.pool[i].v.RawBuffer()) >= n {
				p.pool[i].b = p.pool[i].v.RawBuffer()[:n]
				return Buf{p: p, i: i}
			}
			unusableBufferIdx = i
		}
	}

	// If we would need to grow the size of the pool to allocate another buffer,
	// but there was a slot available occupied by a buffer that's just too
	// small, replace the too-small buffer.
	if len(p.pool) == cap(p.pool) && unusableBufferIdx >= 0 {
		i := unusableBufferIdx
		cache.Free(p.pool[i].v)
		p.pool[i].v = cache.Alloc(n)
		p.pool[i].b = p.pool[i].v.RawBuffer()
		return Buf{p: p, i: i}
	}

	// Allocate a new buffer.
	v := cache.Alloc(n)
	p.pool = append(p.pool, AllocedBuffer{v: v, b: v.RawBuffer()[:n]})
	return Buf{p: p, i: len(p.pool) - 1}
}

// A Buf holds a reference to a manually-managed, pooled byte buffer.
type Buf struct {
	p *BufferPool
	// i holds the index into p.pool where the buffer may be found. This scheme
	// avoids needing to allocate the handle to the buffer on the heap at the
	// cost of copying two words instead of one.
	i int
}

// Valid returns true if the buf holds a valid buffer.
func (b Buf) Valid() bool {
	return b.p != nil
}

// Release releases the buffer back to the pool.
func (b *Buf) Release() {
	if b.p == nil {
		return
	}
	if invariants.Enabled && invariants.Sometimes(10) {
		invariants.Mangle(b.p.pool[b.i].b)
	}
	// Clear the allocedBuffer's byte slice. This signals the allocated buffer
	// is no longer in use and a future call to BufferPool.Alloc may reuse this
	// buffer.
	b.p.pool[b.i].b = nil
	b.p = nil
}
