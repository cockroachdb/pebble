// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package genericcache

import (
	"context"
	"unsafe"

	"github.com/cockroachdb/pebble/v2/internal/invariants"
)

// Cache implements a generic cache that associates arbitrary keys with values.
// It uses multiple shards to reduce contention and uses the CLOCK-Pro
// algorithm.
//
// Values are initialized on demand and are automatically released when they are
// evicted.
type Cache[K Key, V any] struct {
	shards []shard[K, V]
}

// Key must be implemented by the key type used with a Cache.
type Key interface {
	comparable

	// Shard maps the key to a shard index between 0 and numShards-1.
	Shard(numShards int) int
}

// InitValueFn is called to initialize a new value that is being added to the cache.
//
// The function passes a ValueRef instead of *V only for special cases where we
// need to store the ValueRef in a closure (to Unref() it after a later
// FindOrCreate() call).
//
// It is guaranteed that there will be no concurrent calls to InitValueFn() with
// the same key.
type InitValueFn[K Key, V any] func(context.Context, K, ValueRef[K, V]) error

// ReleaseValueFn is called to release a value that is no longer used
// (specifically: it was evicted from the cache AND there are no outstanding
// ValueRefs on it).
type ReleaseValueFn[V any] func(*V)

// New creates a Cache with the given capacity and number of shards.
//
// initValue is used to initialize a new value when it is added to the cache.
// releaseValueFn is used to close a V when it is no longer needed.
func New[K Key, V any](
	capacity int, numShards int, initValueFn InitValueFn[K, V], releaseValueFn ReleaseValueFn[V],
) *Cache[K, V] {
	c := &Cache[K, V]{}
	c.Init(capacity, numShards, initValueFn, releaseValueFn)
	return c
}

// Init can be used instead of New when the cache is embedded in another struct.
func (c *Cache[K, V]) Init(
	capacity int, numShards int, initValueFn InitValueFn[K, V], releaseValueFn ReleaseValueFn[V],
) {
	c.shards = make([]shard[K, V], numShards)
	shardCapacity := (capacity + numShards - 1) / numShards
	for i := range c.shards {
		c.shards[i].Init(shardCapacity, initValueFn, releaseValueFn)
	}
}

// Close the cache, releasing all live values. There must not be any outstanding
// references on any of the values.
func (c *Cache[K, V]) Close() {
	for i := range c.shards {
		c.shards[i].Close()
	}
	c.shards = nil
}

// FindOrCreate retrieves an existing value or creates a new value for the given
// key. The result can be accessed via ValueRef.Value(). The caller must call
// ValueRef.Close() when it no longer needs the value.
func (c *Cache[K, V]) FindOrCreate(ctx context.Context, key K) (ValueRef[K, V], error) {
	shard := c.getShard(key)
	value := shard.findOrCreateValue(ctx, key)
	if err := value.err; err != nil {
		shard.UnrefValue(value)
		return ValueRef[K, V]{}, err
	}
	return ValueRef[K, V]{shard: shard, value: value}, nil
}

// ValueRef is returned by FindOrCreate. It holds a reference on a value; the
// value will be kept "alive" even if the cache decides to evict the value to
// make room for another one.
// The ValueRef is identical to the one passed to InitValueFn for this value.
type ValueRef[K Key, V any] struct {
	shard *shard[K, V]
	value *value[V]
}

// Value returns the value. This method and the returned value can only be used
// until ref.Close() is called.
func (ref ValueRef[K, V]) Value() *V {
	if invariants.Enabled && ref.value.err != nil {
		panic("ValueRef with error")
	}
	return &ref.value.v
}

// Unref releases the reference. This must be called or the underlying value
// will never be cleaned up.
func (ref ValueRef[K, V]) Unref() {
	ref.shard.UnrefValue(ref.value)
}

// Evict any entry associated with the given key. If there is a corresponding
// value in the cache, it is cleaned up before the function returns. There must
// not be any outstanding references on the value.
func (c *Cache[K, V]) Evict(key K) {
	c.getShard(key).Evict(key)
}

// EvictAll evicts all entries in the cache with a key that satisfies the given
// predicate. Any corresponding values are released before the function returns.
// There must not be any outstanding references on the values, and no keys that
// satisfy the predicate should be inserted while the method is running.
//
// It should be used sparingly as it is an O(n) operation. Returns the list of
// keys that were evicted (note that some may have been "ghost" entries without
// an associated value in the cache).
func (c *Cache[K, V]) EvictAll(predicate func(K) bool) []K {
	var keys []K
	for i := range c.shards {
		keys = append(keys, c.shards[i].EvictAll(predicate)...)
	}
	return keys
}

func (c *Cache[K, V]) getShard(key K) *shard[K, V] {
	return &c.shards[key.Shard(len(c.shards))]
}

// Metrics holds metrics for the cache.
type Metrics struct {
	// The number of bytes inuse by the cache. This includes the internal metadata
	// and storage for the V values present in the cache. Note that if V values
	// point to other objects, it is the caller's responsibility to account for
	// those as necessary.
	Size int64
	// The count of objects in the cache.
	Count int64
	// The number of cache hits.
	Hits int64
	// The number of cache misses.
	Misses int64
}

// Metrics retrieves metrics for the cache.
func (c *Cache[K, V]) Metrics() Metrics {
	var numHotOrCold, numTest int64
	var m Metrics
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.RLock()
		numHotOrCold += int64(s.mu.sizeHot) + int64(s.mu.sizeCold)
		numTest += int64(s.mu.sizeTest)
		s.mu.RUnlock()
		m.Hits += s.hits.Load()
		m.Misses += s.misses.Load()
	}
	// Only hot and cold entries actually contain an object.
	m.Count = numHotOrCold

	// All entries have a node and a key in the map.
	var k K
	m.Size = (numHotOrCold + numTest) * int64(
		unsafe.Sizeof(node[K, V]{})+ // nodes
			unsafe.Sizeof(k)+ // key in nodes map.
			unsafe.Sizeof((*node[K, V])(nil))) // value in nodes map (pointer).

	// Only hot or cold entries have an associated value.
	m.Size += numHotOrCold * int64(unsafe.Sizeof(value[V]{}))
	return m
}
