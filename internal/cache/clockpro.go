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
	// id is the namespace for fileNums.
	id      uint64
	fileNum uint64
}

type key struct {
	fileKey
	offset uint64
}

type value struct {
	buf []byte
	// The number of references on the value. When refs drops to 0, the buf
	// associated with the value may be reused. This is a form of manual memory
	// management. See Cache.Free.
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
	// referenced is atomically set to indicate that this entry has been accessed
	// since the last time one of the clock hands swept it.
	referenced int32
	shard      *shard
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

func (e *entry) setValue(v *value) {
	if old := e.getValue(); old != nil {
		if old.release() {
			allocFree(old.buf)
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
	atomic.StoreInt32(&e.referenced, 1)
	// Record a cache hit because the entry is being used as a WeakHandle and
	// successfully avoided a more expensive shard.Get() operation.
	atomic.AddInt64(&e.shard.hits, 1)
	return v.buf
}

// Handle provides a strong reference to an entry in the cache. The reference
// does not pin the entry in the cache, but it does prevent the underlying byte
// slice from being reused.
type Handle struct {
	entry *entry
	value *value
}

// Get returns the value stored in handle.
func (h Handle) Get() []byte {
	if h.value != nil {
		// NB: We don't increment shard.hits in this code path because we only want
		// to record a hit when the handle is retrieved from the cache.
		return h.value.buf
	}
	return nil
}

// Release releases the reference to the cache entry.
func (h Handle) Release() {
	if h.value != nil {
		if h.value.release() {
			allocFree(h.value.buf)
		}
	}
}

// Weak converts the (strong) handle into a WeakHandle. A WeakHandle allows the
// underlying data to be evicted and GC'd, while allowing WeakHandle.Get() to
// be used to quickly determine if the data is still cached or not. Note that
// the reference count on the value is incremented which will prevent the
// associated buffer from ever being reused until it is GC'd by the Go
// runtime. It is not necessary to call Handle.Release() after calling Weak().
func (h Handle) Weak() WeakHandle {
	if h.entry == nil {
		return nil // return a nil interface, not (*entry)(nil)
	}
	// Add a reference to the value which will never be cleared. This is
	// necessary because WeakHandle.Get() performs an atomic load of the value,
	// but we need to ensure that nothing can concurrently be freeing the buffer
	// for reuse. Rather than add additional locking to this code path, we add a
	// reference here so that the underlying buffer can never be reused. And we
	// rely on the Go runtime to eventually GC the buffer.
	h.value.acquire()
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
	hits   int64
	misses int64

	mu sync.RWMutex

	reservedSize int64
	maxSize      int64
	coldSize     int64
	blocks       map[key]*entry     // fileNum+offset -> block
	files        map[fileKey]*entry // fileNum -> list of blocks

	handHot  *entry
	handCold *entry
	handTest *entry

	sizeHot  int64
	sizeCold int64
	sizeTest int64
}

func (c *shard) Get(id, fileNum, offset uint64) Handle {
	c.mu.RLock()
	e := c.blocks[key{fileKey{id, fileNum}, offset}]
	var value *value
	if e != nil {
		value = e.getValue()
		if value != nil {
			value.acquire()
			atomic.StoreInt32(&e.referenced, 1)
		} else {
			e = nil
		}
	}
	c.mu.RUnlock()
	if value == nil {
		atomic.AddInt64(&c.misses, 1)
	} else {
		atomic.AddInt64(&c.hits, 1)
	}
	return Handle{value: value}
}

func (c *shard) Set(id, fileNum, offset uint64, value []byte) Handle {
	c.mu.Lock()
	defer c.mu.Unlock()

	k := key{fileKey{id, fileNum}, offset}
	e := c.blocks[k]
	v := newValue(value)

	switch {
	case e == nil:
		// no cache entry? add it
		e = &entry{ptype: etCold, key: k, size: int64(len(value)), shard: c}
		e.init()
		e.setValue(v)
		if c.metaAdd(k, e) {
			c.sizeCold += e.size
		}

	case e.getValue() != nil:
		// cache entry was a hot or cold page
		e.setValue(v)
		atomic.StoreInt32(&e.referenced, 1)
		delta := int64(len(value)) - e.size
		e.size = int64(len(value))
		if e.ptype == etHot {
			c.sizeHot += delta
		} else {
			c.sizeCold += delta
		}
		c.evict()

	default:
		// cache entry was a test page
		c.coldSize += e.size
		if c.coldSize > c.targetSize() {
			c.coldSize = c.targetSize()
		}
		atomic.StoreInt32(&e.referenced, 0)
		e.setValue(v)
		e.ptype = etHot
		c.sizeTest -= e.size
		c.metaDel(e)
		if c.metaAdd(k, e) {
			c.sizeHot += e.size
		}
	}

	return Handle{entry: e, value: v}
}

// Delete deletes the cached value for the specified file and offset.
func (c *shard) Delete(id, fileNum, offset uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	e := c.blocks[key{fileKey{id, fileNum}, offset}]
	if e == nil {
		return
	}
	c.metaEvict(e)
}

// EvictFile evicts all of the cache values for the specified file.
func (c *shard) EvictFile(id, fileNum uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	blocks := c.files[fileKey{id, fileNum}]
	if blocks == nil {
		return
	}
	for b, n := blocks, (*entry)(nil); ; b = n {
		n = b.fileLink.next
		c.metaEvict(b)
		if b == n {
			break
		}
	}
}

func (c *shard) Reserve(n int) {
	c.mu.Lock()
	c.reservedSize += int64(n)
	c.evict()
	c.mu.Unlock()
}

// Size returns the current space used by the cache.
func (c *shard) Size() int64 {
	c.mu.RLock()
	size := c.sizeHot + c.sizeCold
	c.mu.RUnlock()
	return size
}

func (c *shard) targetSize() int64 {
	target := c.maxSize - c.reservedSize
	// Always return a positive integer for targetSize. This is so that we don't
	// end up in an infinite loop in evict(), in cases where reservedSize is
	// greater than or equal to maxSize.
	if target < 1 {
		return 1
	}
	return target
}

// Add the entry to the cache, returning true if the entry was added and false
// if it would not fit in the cache.
func (c *shard) metaAdd(key key, e *entry) bool {
	c.evict()
	if e.size > c.targetSize() {
		// The entry is larger than the target cache size.
		return false
	}

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
	return true
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

func (c *shard) metaEvict(e *entry) {
	switch e.ptype {
	case etHot:
		c.sizeHot -= e.size
	case etCold:
		c.sizeCold -= e.size
	case etTest:
		c.sizeTest -= e.size
	}
	c.metaDel(e)
}

func (c *shard) evict() {
	for c.targetSize() <= c.sizeHot+c.sizeCold && c.handCold != nil {
		c.runHandCold()
	}
}

func (c *shard) runHandCold() {
	e := c.handCold
	if e.ptype == etCold {
		if atomic.LoadInt32(&e.referenced) == 1 {
			atomic.StoreInt32(&e.referenced, 0)
			e.ptype = etHot
			c.sizeCold -= e.size
			c.sizeHot += e.size
		} else {
			e.setValue(nil)
			e.ptype = etTest
			c.sizeCold -= e.size
			c.sizeTest += e.size
			for c.targetSize() < c.sizeTest && c.handTest != nil {
				c.runHandTest()
			}
		}
	}

	c.handCold = c.handCold.next()

	for c.targetSize()-c.coldSize <= c.sizeHot && c.handHot != nil {
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
		if atomic.LoadInt32(&e.referenced) == 1 {
			atomic.StoreInt32(&e.referenced, 0)
		} else {
			e.ptype = etCold
			c.sizeHot -= e.size
			c.sizeCold += e.size
		}
	}

	c.handHot = c.handHot.next()
}

func (c *shard) runHandTest() {
	if c.sizeCold > 0 && c.handTest == c.handCold && c.handCold != nil {
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

		c.sizeTest -= e.size
		c.coldSize -= e.size
		if c.coldSize < 0 {
			c.coldSize = 0
		}
	}

	c.handTest = c.handTest.next()
}

// Metrics holds metrics for the cache.
type Metrics struct {
	// The number of bytes inuse by the cache.
	Size int64
	// The count of objects (blocks or tables) in the cache.
	Count int64
	// The number of cache hits.
	Hits int64
	// The number of cache misses.
	Misses int64
}

// Cache ...
type Cache struct {
	maxSize int64
	idAlloc uint64
	shards  []shard
}

// New creates a new cache of the specified size. Memory for the cache is
// allocated on demand, not during initialization.
func New(size int64) *Cache {
	return newShards(size, 2*runtime.NumCPU())
}

func newShards(size int64, shards int) *Cache {
	c := &Cache{
		maxSize: size,
		idAlloc: 1,
		shards:  make([]shard, shards),
	}
	for i := range c.shards {
		c.shards[i] = shard{
			maxSize:  size / int64(len(c.shards)),
			coldSize: size / int64(len(c.shards)),
			blocks:   make(map[key]*entry),
			files:    make(map[fileKey]*entry),
		}
	}
	return c
}

func (c *Cache) getShard(id, fileNum, offset uint64) *shard {
	if id == 0 {
		panic("pebble: 0 cache ID is invalid")
	}

	// Inlined version of fnv.New64 + Write.
	const offset64 = 14695981039346656037
	const prime64 = 1099511628211

	h := uint64(offset64)
	for i := 0; i < 8; i++ {
		h *= prime64
		h ^= uint64(id & 0xff)
		id >>= 8
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
func (c *Cache) Get(id, fileNum, offset uint64) Handle {
	return c.getShard(id, fileNum, offset).Get(id, fileNum, offset)
}

// Set sets the cache value for the specified file and offset, overwriting an
// existing value if present. A Handle is returned which provides faster
// retrieval of the cached value than Get (lock-free and avoidance of the map
// lookup).
func (c *Cache) Set(id, fileNum, offset uint64, value []byte) Handle {
	return c.getShard(id, fileNum, offset).Set(id, fileNum, offset, value)
}

// Delete deletes the cached value for the specified file and offset.
func (c *Cache) Delete(id, fileNum, offset uint64) {
	c.getShard(id, fileNum, offset).Delete(id, fileNum, offset)
}

// EvictFile evicts all of the cache values for the specified file.
func (c *Cache) EvictFile(id, fileNum uint64) {
	if id == 0 {
		panic("pebble: 0 cache ID is invalid")
	}
	for i := range c.shards {
		c.shards[i].EvictFile(id, fileNum)
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
	return allocNew(n)
}

// Free frees the specified slice of memory. The buffer will possibly be
// reused, making it invalid to use the buffer after calling Free.
func (c *Cache) Free(b []byte) {
	allocFree(b)
}

// Reserve N bytes in the cache. This effectively shrinks the size of the cache
// by N bytes, without actually consuming any memory. The returned closure
// should be invoked to release the reservation.
func (c *Cache) Reserve(n int) func() {
	// Round-up the per-shard reservation. Most reservations should be large, so
	// this probably doesn't matter in practice.
	shardN := (n + len(c.shards) - 1) / len(c.shards)
	for i := range c.shards {
		c.shards[i].Reserve(shardN)
	}
	return func() {
		if shardN == -1 {
			panic("pebble: cache reservation already released")
		}
		for i := range c.shards {
			c.shards[i].Reserve(-shardN)
		}
		shardN = -1
	}
}

// Metrics returns the metrics for the cache.
func (c *Cache) Metrics() Metrics {
	var m Metrics
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.RLock()
		m.Count += int64(len(s.blocks))
		m.Size += s.sizeHot + s.sizeCold
		s.mu.RUnlock()
		m.Hits += atomic.LoadInt64(&s.hits)
		m.Misses += atomic.LoadInt64(&s.misses)
	}
	return m
}

// NewID returns a new ID to be used as a namespace for cached file
// blocks.
func (c *Cache) NewID() uint64 {
	return atomic.AddUint64(&c.idAlloc, 1)
}
