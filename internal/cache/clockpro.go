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
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble/internal/invariants"
)

type fileKey struct {
	// id is the namespace for fileNums.
	id      uint64
	fileNum uint64
}

type key struct {
	fileKey
	offset uint64
}

// Handle provides a strong reference to an entry in the cache. The reference
// does not pin the entry in the cache, but it does prevent the underlying byte
// slice from being reused. When entry is non-nil, value is initialized to
// entry.val. Over the lifetime of the handle (until Release is called),
// entry.val may change, but value will remain unchanged.
type Handle struct {
	entry *entry
	value *Value
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
		h.value.release()
	}
}

// Weak converts the (strong) handle into a WeakHandle. A WeakHandle allows the
// underlying data to be evicted and GC'd, while allowing WeakHandle.Get() to
// be used to quickly determine if the data is still cached or not. Note that
// the reference count on the value is incremented which will prevent the
// associated buffer from ever being reused until it is GC'd by the Go
// runtime. It is not necessary to call Handle.Release() after calling Weak().
func (h Handle) Weak() *WeakHandle {
	if h.value.manual() {
		panic("pebble: cannot make manual Value into a WeakHandle")
	}
	return (*WeakHandle)(h.entry)
}

// WeakHandle provides a "weak" reference to an entry in the cache. A weak
// reference allows the entry to be evicted, but also provides fast access
type WeakHandle entry

// Get retrieves the value associated with the weak handle, returning nil if no
// value is present. The calls to Get must be balanced with the calls to
// Release.
func (h *WeakHandle) Get() []byte {
	e := (*entry)(h)
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

type shard struct {
	hits   int64
	misses int64

	mu sync.RWMutex

	reservedSize int64
	maxSize      int64
	coldTarget   int64
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
	var value *Value
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
		return Handle{}
	}
	atomic.AddInt64(&c.hits, 1)

	// Enforce the restriction that manually managed values cannot be converted
	// to weak handles.
	if e.manual {
		e = nil
	}
	return Handle{entry: e, value: value}
}

func (c *shard) Set(id, fileNum, offset uint64, value *Value) Handle {
	if n := atomic.LoadInt32(&value.refs); n != 1 && n >= 0 {
		panic(fmt.Sprintf("pebble: Value has already been added to the cache: refs=%d", n))
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	k := key{fileKey{id, fileNum}, offset}
	e := c.blocks[k]
	if e != nil && e.manual != value.manual() {
		panic(fmt.Sprintf("pebble: inconsistent caching of manual Value: entry=%t vs value=%t",
			e.manual, value.manual()))
	}

	switch {
	case e == nil:
		// no cache entry? add it
		e = newEntry(c, k, int64(len(value.buf)), value.manual())
		e.setValue(value)
		if c.metaAdd(k, e) {
			value.trace("add-cold")
			value.acquire() // add reference for the cache
			c.sizeCold += e.size
		} else {
			value.trace("skip-cold")
			e.free()
		}

	case e.getValue() != nil:
		// cache entry was a hot or cold page
		value.acquire() // add reference for the cache
		e.setValue(value)
		atomic.StoreInt32(&e.referenced, 1)
		delta := int64(len(value.buf)) - e.size
		e.size = int64(len(value.buf))
		if e.ptype == etHot {
			value.trace("add-hot")
			c.sizeHot += delta
		} else {
			value.trace("add-cold")
			c.sizeCold += delta
		}
		c.evict()

	default:
		// cache entry was a test page
		c.sizeTest -= e.size
		c.metaDel(e)

		c.coldTarget += e.size
		if c.coldTarget > c.targetSize() {
			c.coldTarget = c.targetSize()
		}

		atomic.StoreInt32(&e.referenced, 0)
		e.setValue(value)
		e.ptype = etHot
		if c.metaAdd(k, e) {
			value.trace("add-hot")
			value.acquire() // add reference for the cache
			c.sizeHot += e.size
		} else {
			value.trace("skip-hot")
			e.free()
		}
	}

	// Enforce the restriction that manually managed values cannot be converted
	// to weak handles.
	if e.manual {
		e = nil
	}
	// Values are initialized with a reference count of 1. That reference count
	// is being transferred to the returned Handle.
	return Handle{entry: e, value: value}
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

// Remove the entry from the cache. This removes the entry from the blocks map,
// the files map, and ensures that hand{Hot,Cold,Test} are not pointing at the
// entry.
func (c *shard) metaDel(e *entry) {
	if value := e.getValue(); value != nil {
		value.trace("metaDel")
	}
	e.setValue(nil)

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

	c.metaCheck(e)
}

// Check that the specified entry is not referenced by the cache.
func (c *shard) metaCheck(e *entry) {
	if invariants.Enabled {
		for _, t := range c.blocks {
			if e == t {
				panic("not reached")
			}
		}
		for _, t := range c.files {
			if e == t {
				panic("not reached")
			}
		}
		// NB: c.hand{Hot,Cold,Test} are pointers into a single linked list. We
		// only have to traverse one of them to check all of them.
		for t := c.handHot.next(); t != c.handHot; t = t.next() {
			if e == t {
				panic("not reached")
			}
		}
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
	e.free()
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

	for c.targetSize()-c.coldTarget <= c.sizeHot && c.handHot != nil {
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
		c.sizeTest -= e.size
		c.coldTarget -= e.size
		if c.coldTarget < 0 {
			c.coldTarget = 0
		}
		c.metaDel(e)
		e.free()
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

// Cache implements Pebble's sharded block cache. The Clock-PRO algorithm is
// used for page replacement
// (http://static.usenix.org/event/usenix05/tech/general/full_papers/jiang/jiang_html/html.html). In
// order to provide better concurrency, 2 x NumCPUs shards are created, with
// each shard being given 1/n of the target cache size. The Clock-PRO algorithm
// is run independently on each shard.
//
// Blocks are keyed by an (id, fileNum, offset) triple. The ID is a namespace
// for file numbers and allows a single Cache to be shared between multiple
// Pebble instances. The fileNum and offset refer to an sstable file number and
// the offset of the block within the file. Because sstables are immutable and
// file numbers are never reused, (fileNum,offset) are unique for the lifetime
// of a Pebble instance.
//
// In addition to maintaining a map from (fileNum,offset) to data, each shard
// maintains a map of the cached blocks for a particular fileNum. This allows
// efficient eviction of all of the blocks for a file which is used when an
// sstable is deleted from disk.
//
// Strong vs Weak Handles
//
// When a block is retrieved from the cache, a Handle is returned. The Handle
// maintains a reference to the associated value and will prevent the value
// from being removed. The caller must call Handle.Release() when they are done
// using the value (failure to do so will result in a memory leak). Until
// Handle.Release() is called, the value is pinned in memory.
//
// A Handle can be transformed into a WeakHandle by a call to Handle.Weak(). A
// WeakHandle does not pin the associated value in memory, and instead allows
// the value to be evicted from the cache and reclaimed by the Go GC. The value
// associated with a WeakHandle can be retrieved by WeakHandle.Get() which may
// return nil if the value has been evicted from the cache. WeakHandle's are
// useful for avoiding the overhad of a cache lookup. They are used for sstable
// index, filter, and range-del blocks, which are frequently accessed, almost
// always in the cache, but which we want to allow to be evicted under memory
// pressure.
//
// Memory Management
//
// In order to reduce pressure on the Go GC, manual memory management is
// performed for the majority of blocks in the cache. Manual memory management
// is selected using Cache.AllocManual() to allocate a value, rather than
// Cache.AllocAuto(). Note that manual values cannot be used for
// WeakHandles. The general rule is to use AllocAuto when a WeakHandle will be
// retrieved, and AllocManual in all other cases.
//
// Manual memory management is performed by calling into C.{malloc,free} to
// allocate memory. Cache.Values are reference counted and the memory backing a
// manual value is freed when the reference count drops to 0.
//
// Manual memory management brings the possibility of memory leaks. It is
// imperative that every Handle returned by Cache.{Get,Set} is eventually
// released. The "invariants" build tag enables a leak detection facility that
// places a GC finalizer on cache.Value. When the cache.Value finalizer is run,
// if the underlying buffer is still present a leak has occurred. The "tracing"
// build tag enables tracing of cache.Value reference count manipulation and
// eases finding where a leak has occurred. These two facilities are usually
// used in combination by specifying `-tags invariants,tracing`. Note that
// "tracing" produces a significant slowdown, while "invariants" does not.
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

func clearCache(obj interface{}) {
	c := obj.(*Cache)
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		s.maxSize = 0
		s.evict()
		s.mu.Unlock()
	}
}

func newShards(size int64, shards int) *Cache {
	c := &Cache{
		maxSize: size,
		idAlloc: 1,
		shards:  make([]shard, shards),
	}
	for i := range c.shards {
		c.shards[i] = shard{
			maxSize:    size / int64(len(c.shards)),
			coldTarget: size / int64(len(c.shards)),
			blocks:     make(map[key]*entry),
			files:      make(map[fileKey]*entry),
		}
	}
	// TODO(peter): This finalizer is used to clear the cache when the Cache
	// itself is GC'd. Investigate making this explicit, and then changing the
	// finalizer to only be installed when invariants.Enabled is true and to only
	// check that all of the manual memory has been freed.
	runtime.SetFinalizer(c, clearCache)
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
// lookup). The value must have been allocated by Cache.Alloc.
func (c *Cache) Set(id, fileNum, offset uint64, value *Value) Handle {
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

// AllocManual allocates a byte slice of the specified size, possibly reusing
// previously allocated but unused memory. The memory backing the value is
// manually managed. The caller MUST either add the value to the cache (via
// Cache.Set), or release the value (via Cache.Free). Failure to do so will
// result in a memory leak.
func (c *Cache) AllocManual(n int) *Value {
	return newManualValue(n)
}

// AllocAuto allocates an automatically managed value using buf as the internal
// buffer.
func (c *Cache) AllocAuto(buf []byte) *Value {
	return newAutoValue(buf)
}

// Free frees the specified value. The buffer associated with the value will
// possibly be reused, making it invalid to use the buffer after calling
// Free. Do not call Free on a value that has been added to the cache.
func (c *Cache) Free(v *Value) {
	if n := atomic.LoadInt32(&v.refs); n > 1 {
		panic(fmt.Sprintf("pebble: Value has been added to the cache: refs=%d", n))
	}
	v.release()
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
