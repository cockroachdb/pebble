// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package genericcache

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/invariants"
)

type shard[K Key, V any, InitOpts any] struct {
	hits   atomic.Int64
	misses atomic.Int64

	capacity int

	mu struct {
		sync.RWMutex
		nodes map[K]*node[K, V]

		handHot  *node[K, V]
		handCold *node[K, V]
		handTest *node[K, V]

		coldTarget int
		sizeHot    int
		sizeCold   int
		sizeTest   int
	}
	releasingCh     chan *value[V]
	releaseLoopExit sync.WaitGroup

	// initWg tracks initValueFn calls in flight. Close waits on it so that
	// limbo nodes have transitioned out of limbo (or been cleaned up) before
	// teardown.
	initWg sync.WaitGroup

	initValueFn    InitValueFn[K, V, InitOpts]
	releaseValueFn ReleaseValueFn[V]
}

func (s *shard[K, V, InitOpts]) Init(
	capacity int, initValueFn InitValueFn[K, V, InitOpts], releaseValueFn ReleaseValueFn[V],
) {
	*s = shard[K, V, InitOpts]{
		capacity:       capacity,
		initValueFn:    initValueFn,
		releaseValueFn: releaseValueFn,
	}

	s.mu.nodes = make(map[K]*node[K, V])
	s.mu.coldTarget = capacity
	s.releasingCh = make(chan *value[V], 100)
	s.releaseLoopExit.Add(1)
	go s.releaseLoop()
}

// releaseLoop runs in the background for each shard, releasing values that are
// pushed to releasingCh.
func (s *shard[K, V, InitOpts]) releaseLoop() {
	defer s.releaseLoopExit.Done()
	for v := range s.releasingCh {
		<-v.initialized
		if v.err == nil {
			s.releaseValueFn(&v.v)
		}
	}
}

func (s *shard[K, V, InitOpts]) UnrefValue(v *value[V]) {
	if v.refCount.Add(-1) == 0 {
		s.releasingCh <- v
	}
}

// unlinkNode removes a node from the shard[K,V], leaving the shard
// reference in place.
//
// c.mu must be held when calling this.
func (s *shard[K, V, InitOpts]) unlinkNode(n *node[K, V]) {
	delete(s.mu.nodes, n.key)

	switch n.status {
	case hot:
		s.mu.sizeHot--
	case cold:
		s.mu.sizeCold--
	case test:
		s.mu.sizeTest--
	case limbo:
		panic(errors.AssertionFailedf("unlinkNode called on limbo node"))
	}

	if n == s.mu.handHot {
		s.mu.handHot = s.mu.handHot.prev()
	}
	if n == s.mu.handCold {
		s.mu.handCold = s.mu.handCold.prev()
	}
	if n == s.mu.handTest {
		s.mu.handTest = s.mu.handTest.prev()
	}

	if n.unlink() == n {
		// This was the last entry in the cache.
		s.mu.handHot = nil
		s.mu.handCold = nil
		s.mu.handTest = nil
	}

	n.links.prev = nil
	n.links.next = nil
}

func (s *shard[K, V, InitOpts]) clearNode(n *node[K, V]) {
	if v := n.value; v != nil {
		n.value = nil
		s.UnrefValue(v)
	}
}

// findOrCreateValue returns an initialized value for the key, taking a
// reference count on it. If the key is not already in the cache, a new value is
// created and initialized (evicting as necessary).
//
// The caller is responsible for unrefing the value.
func (s *shard[K, V, InitOpts]) findOrCreateValue(
	ctx context.Context, key K, opts InitOpts,
) *value[V] {
	// Fast-path for a hit in the cache.
	s.mu.RLock()
	if n := s.mu.nodes[key]; n != nil && n.value != nil {
		// Fast-path hit.
		v := n.value
		v.refCount.Add(1)
		s.mu.RUnlock()
		if !n.referenced.Load() {
			n.referenced.Store(true)
		}
		s.hits.Add(1)
		// TODO(dt): an atomic bool on value could let us skip the channel and
		// ctx checks entirely in the common (already-initialized) case, avoiding
		// even the overhead of the non-blocking select below.

		// Fast path: check if already initialized without involving ctx.Done()
		// to avoid the allocation overhead of reading ctx.Done() in the common
		// case where the value is already ready.
		select {
		case <-v.initialized:
			return v
		default:
		}
		// Slow path: value is still initializing; wait with cancellation.
		select {
		case <-v.initialized:
			return v
		case <-ctx.Done():
			s.UnrefValue(v)
			cancelled := &value[V]{
				err:         ctx.Err(),
				initialized: make(chan struct{}),
			}
			cancelled.refCount.Store(1)
			close(cancelled.initialized)
			return cancelled
		}
	}
	s.mu.RUnlock()

	s.mu.Lock()

	n := s.mu.nodes[key]
	switch {
	case n == nil:
		// Slow-path miss of a non-existent node. Create a limbo node: it
		// lives in the map so concurrent FindOrCreate calls find it, but
		// stays out of the CLOCK-Pro linked list while initValueFn runs.
		n = &node[K, V]{}
		n.key = key
		n.status = limbo
		n.limboTarget = cold
		s.mu.nodes[key] = n

	case n.value != nil:
		// Slow-path hit of a hot, cold, or limbo node.
		//
		// The caller is responsible for decrementing the refCount.
		v := n.value
		v.refCount.Add(1)
		n.referenced.Store(true)
		s.hits.Add(1)
		s.mu.Unlock()
		// Same fast/slow path as the read-lock hit above.
		select {
		case <-v.initialized:
			return v
		default:
		}
		select {
		case <-v.initialized:
			return v
		case <-ctx.Done():
			s.UnrefValue(v)
			cancelled := &value[V]{
				err:         ctx.Err(),
				initialized: make(chan struct{}),
			}
			cancelled.refCount.Store(1)
			close(cancelled.initialized)
			return cancelled
		}

	default:
		// Slow-path miss of a test node. Remove it from the linked list and
		// move it to limbo while we initialize a fresh value. On success it
		// will rejoin the cache as hot.
		s.unlinkNode(n)
		s.mu.coldTarget++
		if s.mu.coldTarget > s.capacity {
			s.mu.coldTarget = s.capacity
		}

		n.referenced.Store(false)
		n.status = limbo
		n.limboTarget = hot
		s.mu.nodes[key] = n
	}

	v := &value[V]{
		initialized: make(chan struct{}),
	}
	// One ref count for the shard, one for the caller.
	v.refCount.Store(2)
	n.value = v
	s.misses.Add(1)
	s.initWg.Add(1)

	s.mu.Unlock()

	vRef := ValueRef[K, V, InitOpts]{
		shard: s,
		value: v,
	}

	v.err = s.initValueFn(ctx, key, opts, vRef)

	s.mu.Lock()
	if v.err != nil {
		// Init failed: remove from the map (if still ours) and release the
		// shard's ref. The node never joins the linked list.
		if existing := s.mu.nodes[key]; existing == n {
			delete(s.mu.nodes, key)
		}
		n.value = nil
		s.mu.Unlock()
		s.UnrefValue(v)
	} else {
		// Init succeeded: leave limbo by joining the linked list with the
		// target status. evictNodes runs first to make room (matching the
		// original addNode ordering: evict, bump size, link).
		target := n.limboTarget
		n.status = target
		n.limboTarget = 0
		s.evictNodes()
		// linkNodeLocked always inserts at the boundary between the hot and
		// cold regions of the ring (right before handHot, with handCold
		// stepped back), so the same insertion position serves both targets;
		// the only thing that distinguishes hot from cold is which size
		// counter we bump (and therefore which clock hand will process it).
		switch target {
		case cold:
			s.mu.sizeCold++
		case hot:
			s.mu.sizeHot++
		}
		s.linkNodeLocked(n)
		s.mu.Unlock()
	}
	s.initWg.Done()
	close(v.initialized)
	return v
}

// linkNodeLocked inserts n into the CLOCK-Pro linked list. n must not
// already be linked. s.mu must be held.
func (s *shard[K, V, InitOpts]) linkNodeLocked(n *node[K, V]) {
	n.links.next = n
	n.links.prev = n
	if s.mu.handHot == nil {
		// First element.
		s.mu.handHot = n
		s.mu.handCold = n
		s.mu.handTest = n
	} else {
		s.mu.handHot.link(n)
	}
	if s.mu.handCold == s.mu.handHot {
		s.mu.handCold = s.mu.handCold.prev()
	}
}

func (s *shard[K, V, InitOpts]) evictNodes() {
	for s.capacity <= s.mu.sizeHot+s.mu.sizeCold && s.mu.handCold != nil {
		s.runHandCold()
	}
}

func (s *shard[K, V, InitOpts]) runHandCold() {
	n := s.mu.handCold
	if n.status == cold {
		if n.referenced.Load() {
			n.referenced.Store(false)
			n.status = hot
			s.mu.sizeCold--
			s.mu.sizeHot++
		} else {
			s.clearNode(n)
			n.status = test
			s.mu.sizeCold--
			s.mu.sizeTest++
			for s.capacity < s.mu.sizeTest && s.mu.handTest != nil {
				s.runHandTest()
			}
		}
	}

	s.mu.handCold = s.mu.handCold.next()

	for s.capacity-s.mu.coldTarget <= s.mu.sizeHot && s.mu.handHot != nil {
		s.runHandHot()
	}
}

func (s *shard[K, V, InitOpts]) runHandHot() {
	if s.mu.handHot == s.mu.handTest && s.mu.handTest != nil {
		s.runHandTest()
		if s.mu.handHot == nil {
			return
		}
	}

	n := s.mu.handHot
	if n.status == hot {
		if n.referenced.Load() {
			n.referenced.Store(false)
		} else {
			n.status = cold
			s.mu.sizeHot--
			s.mu.sizeCold++
		}
	}

	s.mu.handHot = s.mu.handHot.next()
}

func (s *shard[K, V, InitOpts]) runHandTest() {
	if s.mu.sizeCold > 0 && s.mu.handTest == s.mu.handCold && s.mu.handCold != nil {
		s.runHandCold()
		if s.mu.handTest == nil {
			return
		}
	}

	n := s.mu.handTest
	if n.status == test {
		s.mu.coldTarget--
		if s.mu.coldTarget < 0 {
			s.mu.coldTarget = 0
		}
		s.unlinkNode(n)
		s.clearNode(n)
	}

	s.mu.handTest = s.mu.handTest.next()
}

// Evict any entry associated with the given key. If there is a corresponding
// value in the shard, it is released before the function returns. There must
// not be any outstanding references on the value.
func (s *shard[K, V, InitOpts]) Evict(key K) {
	s.mu.Lock()
	n := s.mu.nodes[key]
	var v *value[V]
	if n != nil {
		if n.status == limbo {
			// initValueFn is in flight; the caller is holding a vRef, so
			// Evict's "no outstanding references" precondition is violated.
			s.mu.Unlock()
			panic(errors.AssertionFailedf("Evict called while initValueFn in flight"))
		}
		// NB: This is equivalent to UnrefValue, but we perform the releaseValueFn()
		// call synchronously below to free up any associated resources before
		// returning.
		s.unlinkNode(n)
		v = n.value
	}
	s.mu.Unlock()

	if v != nil {
		if v.refCount.Add(-1) != 0 {
			panic(errors.AssertionFailedf("element has outstanding references"))
		}
		<-v.initialized
		if v.err == nil {
			s.releaseValueFn(&v.v)
		}
	}
}

// EvictAll evicts all entries in the shard with a key that satisfies the given
// predicate. Any corresponding values are released before the function returns.
// There must not be any outstanding references on the values, and no keys that
// satisfy the predicate should be inserted while the method is running.
//
// It should be used sparingly as it is an O(n) operation.
func (s *shard[K, V, InitOpts]) EvictAll(predicate func(K) bool) []K {
	// Collect the keys which need to be evicted. Iterate the map (rather than
	// the linked list) so we also include any limbo nodes — though the
	// caller's "no outstanding references" precondition implies there shouldn't
	// be any in flight.
	var keys []K
	s.mu.RLock()
	for k := range s.mu.nodes {
		if predicate(k) {
			keys = append(keys, k)
		}
	}
	s.mu.RUnlock()

	for i := range keys {
		s.Evict(keys[i])
	}

	if invariants.Enabled {
		s.mu.RLock()
		defer s.mu.RUnlock()
		for k := range s.mu.nodes {
			if predicate(k) {
				panic(errors.AssertionFailedf("evictable key added in shard"))
			}
		}
	}
	return keys
}

// Close the shard, releasing all live values. There must not be any outstanding
// references on any of the values.
func (s *shard[K, V, InitOpts]) Close() {
	// Wait for any in-flight initValueFn calls to complete before tearing
	// down: they need the lock to transition out of limbo, and Close holds
	// it.
	s.initWg.Wait()

	s.mu.Lock()
	defer s.mu.Unlock()

	for s.mu.handHot != nil {
		n := s.mu.handHot
		if v := n.value; v != nil {
			if v.refCount.Add(-1) != 0 {
				panic(errors.AssertionFailedf("element has outstanding references"))
			}
			s.releasingCh <- v
		}
		s.unlinkNode(n)
	}

	s.mu.nodes = nil
	s.mu.handHot = nil
	s.mu.handCold = nil
	s.mu.handTest = nil

	close(s.releasingCh)
	s.releaseLoopExit.Wait()
}
