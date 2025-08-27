// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package cache

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2/internal/base"
	"github.com/cockroachdb/pebble/v2/internal/invariants"
)

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
// order to provide better concurrency, 4 x NumCPUs shards are created, with
// each shard being given 1/n of the target cache size. The Clock-PRO algorithm
// is run independently on each shard.
//
// Blocks are keyed by an (handleID, fileNum, offset) triple. The handleID is a
// namespace for file numbers and allows a single Cache to be shared between
// multiple Pebble instances (via separate Handles). The fileNum and offset
// refer to an sstable file number and the offset of the block within the file.
// Because sstables are immutable and file numbers are never reused,
// (fileNum,offset) are unique for the lifetime of a Pebble instance.
//
// In addition to maintaining a map from (fileNum,offset) to data, each shard
// maintains a map of the cached blocks for a particular fileNum. This allows
// efficient eviction of all blocks for a file (is used when an sstable is
// deleted from disk).
//
// # Memory Management
//
// A normal implementation of the block cache would result in GC having to read
// through all the structures and keep track of the liveness of many objects.
// This was found to cause significant overhead in CRDB when compared to the
// earlier use of RocksDB.
//
// In order to reduce pressure on the Go GC, manual memory management is
// performed for the data stored in the cache. Manual memory management is
// performed by calling into C.{malloc,free} to allocate memory; this memory is
// outside the purview of the GC. Cache.Values are reference counted and the
// memory backing a manual value is freed when the reference count drops to 0.
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
	refs    atomic.Int64
	maxSize int64
	idAlloc atomic.Uint64
	shards  []shard

	// Traces recorded by Cache.trace. Used for debugging.
	tr struct {
		sync.Mutex
		msgs []string
	}
	stack string
}

// New creates a new cache of the specified size. Memory for the cache is
// allocated on demand, not during initialization. The cache is created with a
// reference count of 1. Each DB it is associated with adds a reference, so the
// creator of the cache should usually release their reference after the DB is
// created.
//
//	c := cache.New(...)
//	defer c.Unref()
//	d, err := pebble.Open(pebble.Options{Cache: c})
func New(size int64) *Cache {
	// How many cache shards should we create?
	//
	// Note that the probability two processors will try to access the same
	// shard at the same time increases superlinearly with the number of
	// processors (consider the birthday problem where each CPU is a person,
	// and each shard is a possible birthday).
	//
	// We could consider growing the number of shards superlinearly, but
	// increasing the shard count may reduce the effectiveness of the caching
	// algorithm if frequently-accessed blocks are insufficiently distributed
	// across shards.
	//
	// Experimentally, we've observed contention contributing to tail latencies
	// at 2 shards per processor. For now we use 4 shards per processor,
	// recognizing this may not be final word.
	m := 4 * runtime.GOMAXPROCS(0)

	// In tests we can use large CPU machines with small cache sizes and have
	// many caches in existence at a time. If sharding into m shards would
	// produce too small shards, constrain the number of shards to 4.
	const minimumShardSize = 4 << 20 // 4 MiB
	if m > 4 && int(size)/m < minimumShardSize {
		m = 4
	}
	return NewWithShards(size, m)
}

// NewWithShards creates a new cache with the specified size and number of
// shards.
func NewWithShards(size int64, shards int) *Cache {
	c := &Cache{
		maxSize: size,
		shards:  make([]shard, shards),
		stack:   string(debug.Stack()),
	}
	c.refs.Store(1)
	c.trace("alloc", c.refs.Load())
	for i := range c.shards {
		c.shards[i].init(size / int64(len(c.shards)))
	}

	// Note: this is a no-op if invariants are disabled or race is enabled.
	invariants.SetFinalizer(c, func(c *Cache) {
		if v := c.refs.Load(); v != 0 {
			c.tr.Lock()
			fmt.Fprintf(os.Stderr,
				"pebble: cache (%p) has non-zero reference count: %d\n\n%s\n\n", c, v, c.stack)
			if len(c.tr.msgs) > 0 {
				fmt.Fprintf(os.Stderr, "%s\n", strings.Join(c.tr.msgs, "\n"))
			}
			c.tr.Unlock()
			os.Exit(1)
		}
	})
	return c
}

// Ref adds a reference to the cache. The cache only remains valid as long a
// reference is maintained to it.
func (c *Cache) Ref() {
	v := c.refs.Add(1)
	if v <= 1 {
		panic(fmt.Sprintf("pebble: inconsistent reference count: %d", v))
	}
	c.trace("ref", v)
}

// Unref releases a reference on the cache.
func (c *Cache) Unref() {
	v := c.refs.Add(-1)
	c.trace("unref", v)
	switch {
	case v < 0:
		panic(fmt.Sprintf("pebble: inconsistent reference count: %d", v))
	case v == 0:
		for i := range c.shards {
			c.shards[i].Free()
		}
	}
}

func (c *Cache) NewHandle() *Handle {
	c.Ref()
	id := handleID(c.idAlloc.Add(1))
	return &Handle{
		cache: c,
		id:    id,
	}
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
		m.Count += int64(s.blocks.Len())
		m.Size += s.sizeHot + s.sizeCold
		s.mu.RUnlock()
		m.Hits += s.hits.Load()
		m.Misses += s.misses.Load()
	}
	return m
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

func (c *Cache) getShard(k key) *shard {
	idx := k.shardIdx(len(c.shards))
	return &c.shards[idx]
}

// Handle is the interface through which a store uses the cache. Each store uses
// a separate "handle". A handle corresponds to a separate "namespace" inside
// the cache; a handle cannot see another handle's blocks.
type Handle struct {
	cache *Cache
	id    handleID
}

// handleID is an ID associated with a Handle; it is unique in the context of a
// Cache instance and serves as a namespace for file numbers, allowing a single
// Cache to be shared among multiple Pebble instances.
type handleID uint64

// Cache returns the Cache instance associated with the handle.
func (c *Handle) Cache() *Cache {
	return c.cache
}

// Get retrieves the cache value for the specified file and offset, returning
// nil if no value is present.
func (c *Handle) Get(fileNum base.DiskFileNum, offset uint64) *Value {
	k := makeKey(c.id, fileNum, offset)
	cv, re := c.cache.getShard(k).getWithMaybeReadEntry(k, false /* desireReadEntry */)
	if invariants.Enabled && re != nil {
		panic("readEntry should be nil")
	}
	return cv
}

// GetWithReadHandle retrieves the cache value for the specified handleID, fileNum
// and offset. If found, a valid Handle is returned (with cacheHit set to
// true), else a valid ReadHandle is returned.
//
// See the ReadHandle declaration for the contract the caller must satisfy
// when getting a valid ReadHandle.
//
// This method can block before returning since multiple concurrent gets for
// the same cache value will take turns getting a ReadHandle, which represents
// permission to do the read. This blocking respects context cancellation, in
// which case an error is returned (and not a valid ReadHandle).
//
// When blocking, the errorDuration return value can be non-zero and is
// populated with the total duration that other readers that observed an error
// (see ReadHandle.SetReadError) spent in doing the read. This duration can be
// greater than the time spent blocked in this method, since some of these
// errors could have occurred prior to this call. But it serves as a rough
// indicator of whether turn taking could have caused higher latency due to
// context cancellation of other readers.
//
// While waiting, someone else may successfully read the value, which results
// in a valid Handle being returned. This is a case where cacheHit=false.
func (c *Handle) GetWithReadHandle(
	ctx context.Context, fileNum base.DiskFileNum, offset uint64,
) (cv *Value, rh ReadHandle, errorDuration time.Duration, cacheHit bool, err error) {
	k := makeKey(c.id, fileNum, offset)
	cv, re := c.cache.getShard(k).getWithMaybeReadEntry(k, true /* desireReadEntry */)
	if cv != nil {
		return cv, ReadHandle{}, 0, true, nil
	}
	cv, errorDuration, err = re.waitForReadPermissionOrHandle(ctx)
	if err != nil || cv != nil {
		re.unrefAndTryRemoveFromMap()
		return cv, ReadHandle{}, errorDuration, false, err
	}
	return nil, ReadHandle{entry: re}, errorDuration, false, nil
}

// Set sets the cache value for the specified file and offset, overwriting an
// existing value if present. The value must have been allocated by Cache.Alloc.
//
// The cache takes a reference on the Value and holds it until it gets evicted.
func (c *Handle) Set(fileNum base.DiskFileNum, offset uint64, value *Value) {
	k := makeKey(c.id, fileNum, offset)
	c.cache.getShard(k).set(k, value)
}

// Delete deletes the cached value for the specified file and offset.
func (c *Handle) Delete(fileNum base.DiskFileNum, offset uint64) {
	k := makeKey(c.id, fileNum, offset)
	c.cache.getShard(k).delete(k)
}

// EvictFile evicts all cache values for the specified file.
func (c *Handle) EvictFile(fileNum base.DiskFileNum) {
	for i := range c.cache.shards {
		c.cache.shards[i].evictFile(c.id, fileNum)
	}
}

func (c *Handle) Close() {
	c.cache.Unref()
	*c = Handle{}
}
