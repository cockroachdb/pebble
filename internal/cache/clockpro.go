// Copyright 2018. All rights reserved. Use of this source code is governed by
// an MIT-style license that can be found in the LICENSE file.

// Package cache implements the CLOCK-Pro caching algorithm.
//
// CLOCK-Pro is a patent-free alternative to the Adaptive Replacement Cache,
// https://en.wikipedia.org/wiki/Adaptive_replacement_cache.
// It is an approximation of LIRS ( https://en.wikipedia.org/wiki/LIRS_caching_algorithm ),
// much like the CLOCK page replacement algorithm is an approximation of LRU.
//
// This implementation is based on the python code from https://bitbucket.org/SamiLehtinen/pyclockpro .
//
// Slides describing the algorithm: http://fr.slideshare.net/huliang64/clockpro
//
// The original paper: http://static.usenix.org/event/usenix05/tech/general/full_papers/jiang/jiang_html/html.html
//
// It is MIT licensed, like the original.
package cache // import "github.com/cockroachdb/pebble/internal/cache"

import (
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

type entryType int8

const (
	etTest entryType = iota
	etCold
	etHot
)

func (p entryType) String() string {
	switch p {
	case etTest:
		return "test"
	case etCold:
		return "cold"
	case etHot:
		return "hot"
	}
	return "unknown"
}

type fileKey struct {
	dbNum   uint64
	fileNum uint64
}

type key struct {
	fileKey
	offset uint64
}

type value struct {
	buf  []byte
	refs int32
}

func newValue(b []byte) *value {
	if b == nil {
		return nil
	}
	// A value starts with 2 references. One for the cache, and one for the
	// handle that will be returned.
	return &value{buf: b, refs: 2}
}

func (v *value) acquire() {
	atomic.AddInt32(&v.refs, 1)
}

func (v *value) release() bool {
	return atomic.AddInt32(&v.refs, -1) == 0
}

type entry struct {
	key       key
	val       unsafe.Pointer
	blockLink struct {
		next *entry
		prev *entry
	}
	fileLink struct {
		next *entry
		prev *entry
	}
	size  int64
	ptype entryType
	ref   int32
}

func (e *entry) init() *entry {
	e.blockLink.next = e
	e.blockLink.prev = e
	e.fileLink.next = e
	e.fileLink.prev = e
	return e
}

func (e *entry) next() *entry {
	if e == nil {
		return nil
	}
	return e.blockLink.next
}

func (e *entry) prev() *entry {
	if e == nil {
		return nil
	}
	return e.blockLink.prev
}

func (e *entry) link(s *entry) {
	s.blockLink.prev = e.blockLink.prev
	s.blockLink.prev.blockLink.next = s
	s.blockLink.next = e
	s.blockLink.next.blockLink.prev = s
}

func (e *entry) unlink() *entry {
	next := e.blockLink.next
	e.blockLink.prev.blockLink.next = e.blockLink.next
	e.blockLink.next.blockLink.prev = e.blockLink.prev
	e.blockLink.prev = e
	e.blockLink.next = e
	return next
}

func (e *entry) linkFile(s *entry) {
	s.fileLink.prev = e.fileLink.prev
	s.fileLink.prev.fileLink.next = s
	s.fileLink.next = e
	s.fileLink.next.fileLink.prev = s
}

func (e *entry) unlinkFile() *entry {
	next := e.fileLink.next
	e.fileLink.prev.fileLink.next = e.fileLink.next
	e.fileLink.next.fileLink.prev = e.fileLink.prev
	e.fileLink.prev = e
	e.fileLink.next = e
	return next
}

func (e *entry) setValue(v *value, free func([]byte)) {
	if old := e.getValue(); old != nil {
		if old.release() && free != nil {
			free(old.buf)
		}
	}
	atomic.StorePointer(&e.val, unsafe.Pointer(v))
}

func (e *entry) getValue() *value {
	return (*value)(atomic.LoadPointer(&e.val))
}

func (e *entry) Get() []byte {
	v := e.getValue()
	if v == nil {
		return nil
	}
	atomic.StoreInt32(&e.ref, 1)
	return v.buf
}

// Handle provides a strong reference to an entry in the cache. The reference
// does not pin the entry in the cache, but it does prevent the underlying byte
// slice from being reused.
type Handle struct {
	entry *entry
	value *value
	free  func([]byte)
}

// Get returns the value stored in handle.
func (h Handle) Get() []byte {
	if h.value != nil {
		return h.value.buf
	}
	return nil
}

// Release releases the reference to the cache entry.
func (h Handle) Release() {
	if h.value != nil {
		if h.value.release() && h.free != nil {
			h.free(h.value.buf)
		}
		h.value = nil
	}
}

// Weak returns a weak handle and clears the strong reference, preventing the
// underlying data storage from being reused. Clearing the strong reference
// allows the underlying data to be evicted and GC'd, but the buffer will not
// be reused.
func (h Handle) Weak() WeakHandle {
	h.value = nil
	if h.entry == nil {
		return nil // return a nil interface, not (*entry)(nil)
	}
	return h.entry
}

// WeakHandle provides a "weak" reference to an entry in the cache. A weak
// reference allows the entry to be evicted, but also provides fast access
type WeakHandle interface {
	// Get retrieves the value associated with the weak handle, returning nil if
	// no value is present.
	Get() []byte
}

type shard struct {
	free func([]byte)

	mu sync.RWMutex

	maxSize  int64
	coldSize int64
	blocks   map[key]*entry     // fileNum+offset -> block
	files    map[fileKey]*entry // fileNum -> list of blocks

	handHot  *entry
	handCold *entry
	handTest *entry

	countHot  int64
	countCold int64
	countTest int64
}

func (c *shard) Get(dbNum, fileNum, offset uint64) Handle {
	c.mu.RLock()
	e := c.blocks[key{fileKey{dbNum, fileNum}, offset}]
	var value *value
	if e != nil {
		value = e.getValue()
		if value != nil {
			value.acquire()
			atomic.StoreInt32(&e.ref, 1)
		} else {
			e = nil
		}
	}
	c.mu.RUnlock()
	return Handle{value: value, free: c.free}
}

func (c *shard) Set(dbNum, fileNum, offset uint64, value []byte) Handle {
	c.mu.Lock()
	defer c.mu.Unlock()

	k := key{fileKey{dbNum, fileNum}, offset}
	e := c.blocks[k]
	v := newValue(value)

	switch {
	case e == nil:
		// no cache entry? add it
		e = &entry{ptype: etCold, key: k, size: int64(len(value))}
		e.init()
		e.setValue(v, c.free)
		c.metaAdd(k, e)
		c.countCold += e.size

	case e.getValue() != nil:
		// cache entry was a hot or cold page
		e.setValue(v, c.free)
		atomic.StoreInt32(&e.ref, 1)
		delta := int64(len(value)) - e.size
		e.size = int64(len(value))
		if e.ptype == etHot {
			c.countHot += delta
		} else {
			c.countCold += delta
		}
		c.evict()

	default:
		// cache entry was a test page
		c.coldSize += e.size
		if c.coldSize > c.maxSize {
			c.coldSize = c.maxSize
		}
		atomic.StoreInt32(&e.ref, 0)
		e.setValue(v, c.free)
		e.ptype = etHot
		c.countTest -= e.size
		c.metaDel(e)
		c.metaAdd(k, e)
		c.countHot += e.size
	}

	return Handle{entry: e, value: v, free: c.free}
}

// EvictFile evicts all of the cache values for the specified file.
func (c *shard) EvictFile(dbNum, fileNum uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	blocks := c.files[fileKey{dbNum, fileNum}]
	if blocks == nil {
		return
	}
	for b, n := blocks, (*entry)(nil); ; b = n {
		switch b.ptype {
		case etHot:
			c.countHot -= b.size
		case etCold:
			c.countCold -= b.size
		case etTest:
			c.countTest -= b.size
		}
		n = b.fileLink.next
		c.metaDel(b)
		if b == n {
			break
		}
	}
}

// Size returns the current space used by the cache.
func (c *shard) Size() int64 {
	c.mu.Lock()
	size := c.countHot + c.countCold
	c.mu.Unlock()
	return size
}

func (c *shard) metaAdd(key key, e *entry) {
	c.evict()

	c.blocks[key] = e

	if c.handHot == nil {
		// first element
		c.handHot = e
		c.handCold = e
		c.handTest = e
	} else {
		c.handHot.link(e)
	}

	if c.handCold == c.handHot {
		c.handCold = c.handCold.prev()
	}

	if fileBlocks := c.files[key.fileKey]; fileBlocks == nil {
		c.files[key.fileKey] = e
	} else {
		fileBlocks.linkFile(e)
	}
}

func (c *shard) metaDel(e *entry) {
	delete(c.blocks, e.key)

	if e == c.handHot {
		c.handHot = c.handHot.prev()
	}
	if e == c.handCold {
		c.handCold = c.handCold.prev()
	}
	if e == c.handTest {
		c.handTest = c.handTest.prev()
	}

	if e.unlink() == e {
		// This was the last entry in the cache.
		c.handHot = nil
		c.handCold = nil
		c.handTest = nil
	}

	if next := e.unlinkFile(); e == next {
		delete(c.files, e.key.fileKey)
	} else {
		c.files[e.key.fileKey] = next
	}
}

func (c *shard) evict() {
	for c.maxSize <= c.countHot+c.countCold && c.handCold != nil {
		c.runHandCold()
	}
}

func (c *shard) runHandCold() {
	e := c.handCold
	if e.ptype == etCold {
		if atomic.LoadInt32(&e.ref) == 1 {
			atomic.StoreInt32(&e.ref, 0)
			e.ptype = etHot
			c.countCold -= e.size
			c.countHot += e.size
		} else {
			e.setValue(nil, c.free)
			e.ptype = etTest
			c.countCold -= e.size
			c.countTest += e.size
			for c.maxSize < c.countTest && c.handTest != nil {
				c.runHandTest()
			}
		}
	}

	c.handCold = c.handCold.next()

	for c.maxSize-c.coldSize <= c.countHot && c.handHot != nil {
		c.runHandHot()
	}
}

func (c *shard) runHandHot() {
	if c.handHot == c.handTest && c.handTest != nil {
		c.runHandTest()
		if c.handHot == nil {
			return
		}
	}

	e := c.handHot
	if e.ptype == etHot {
		if atomic.LoadInt32(&e.ref) == 1 {
			atomic.StoreInt32(&e.ref, 0)
		} else {
			e.ptype = etCold
			c.countHot -= e.size
			c.countCold += e.size
		}
	}

	c.handHot = c.handHot.next()
}

func (c *shard) runHandTest() {
	if c.countCold > 0 && c.handTest == c.handCold && c.handCold != nil {
		c.runHandCold()
		if c.handTest == nil {
			return
		}
	}

	e := c.handTest
	if e.ptype == etTest {
		prev := c.handTest.prev()
		c.metaDel(c.handTest)
		c.handTest = prev

		c.countTest -= e.size
		c.coldSize -= e.size
		if c.coldSize < 0 {
			c.coldSize = 0
		}
	}

	c.handTest = c.handTest.next()
}

// Cache ...
type Cache struct {
	maxSize   int64
	shards    []shard
	allocPool *sync.Pool
}

// New creates a new cache of the specified size. Memory for the cache is
// allocated on demand, not during initialization.
func New(size int64) *Cache {
	return newShards(size, 2*runtime.NumCPU())
}

func newShards(size int64, shards int) *Cache {
	c := &Cache{
		maxSize: size,
		shards:  make([]shard, shards),
		allocPool: &sync.Pool{
			New: func() interface{} {
				return &allocCache{}
			},
		},
	}
	free := c.Free
	for i := range c.shards {
		c.shards[i] = shard{
			free:     free,
			maxSize:  size / int64(len(c.shards)),
			coldSize: size / int64(len(c.shards)),
			blocks:   make(map[key]*entry),
			files:    make(map[fileKey]*entry),
		}
	}
	return c
}

func (c *Cache) getShard(dbNum, fileNum, offset uint64) *shard {
	// Inlined version of fnv.New64 + Write.
	const offset64 = 14695981039346656037
	const prime64 = 1099511628211

	h := uint64(offset64)
	for i := 0; i < 8; i++ {
		h *= prime64
		h ^= uint64(dbNum & 0xff)
		dbNum >>= 8
	}
	for i := 0; i < 8; i++ {
		h *= prime64
		h ^= uint64(fileNum & 0xff)
		fileNum >>= 8
	}
	for i := 0; i < 8; i++ {
		h *= prime64
		h ^= uint64(offset & 0xff)
		offset >>= 8
	}

	return &c.shards[h%uint64(len(c.shards))]
}

// Get retrieves the cache value for the specified file and offset, returning
// nil if no value is present.
func (c *Cache) Get(dbNum, fileNum, offset uint64) Handle {
	return c.getShard(dbNum, fileNum, offset).Get(dbNum, fileNum, offset)
}

// Set sets the cache value for the specified file and offset, overwriting an
// existing value if present. A Handle is returned which provides faster
// retrieval of the cached value than Get (lock-free and avoidance of the map
// lookup).
func (c *Cache) Set(dbNum, fileNum, offset uint64, value []byte) Handle {
	return c.getShard(dbNum, fileNum, offset).Set(dbNum, fileNum, offset, value)
}

// EvictFile evicts all of the cache values for the specified file.
func (c *Cache) EvictFile(dbNum, fileNum uint64) {
	for i := range c.shards {
		c.shards[i].EvictFile(dbNum, fileNum)
	}
}

// MaxSize returns the max size of the cache.
func (c *Cache) MaxSize() int64 {
	return c.maxSize
}

// Size returns the current space used by the cache.
func (c *Cache) Size() int64 {
	var size int64
	for i := range c.shards {
		size += c.shards[i].Size()
	}
	return size
}

// Alloc allocates a byte slice of the specified size, possibly reusing
// previously allocated but unused memory.
func (c *Cache) Alloc(n int) []byte {
	a := c.allocPool.Get().(*allocCache)
	b := a.alloc(n)
	c.allocPool.Put(a)
	return b
}

// Free frees the specified slice of memory. The buffer will possibly be
// reused, making it invalid to use the buffer after calling Free.
func (c *Cache) Free(b []byte) {
	a := c.allocPool.Get().(*allocCache)
	a.free(b)
	c.allocPool.Put(a)
}
