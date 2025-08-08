// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package genericcache

import (
	"context"
	"sync"
	"sync/atomic"

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
		<-v.initialized
		return v
	}
	s.mu.RUnlock()

	s.mu.Lock()

	n := s.mu.nodes[key]
	switch {
	case n == nil:
		// Slow-path miss of a non-existent node.
		n = &node[K, V]{}
		s.addNode(n, key, cold)
		s.mu.sizeCold++

	case n.value != nil:
		// Slow-path hit of a hot or cold node.
		//
		// The caller is responsible for decrementing the refCount.
		v := n.value
		v.refCount.Add(1)
		n.referenced.Store(true)
		s.hits.Add(1)
		s.mu.Unlock()
		<-v.initialized
		return v

	default:
		// Slow-path miss of a test node.
		s.unlinkNode(n)
		s.mu.coldTarget++
		if s.mu.coldTarget > s.capacity {
			s.mu.coldTarget = s.capacity
		}

		n.referenced.Store(false)
		s.addNode(n, key, hot)
		s.mu.sizeHot++
	}

	v := &value[V]{
		initialized: make(chan struct{}),
	}
	// One ref count for the shard, one for the caller.
	v.refCount.Store(2)
	n.value = v
	s.misses.Add(1)

	s.mu.Unlock()

	vRef := ValueRef[K, V, InitOpts]{
		shard: s,
		value: v,
	}

	v.err = s.initValueFn(ctx, key, opts, vRef)
	if v.err != nil {
		s.mu.Lock()
		defer s.mu.Unlock()
		// Lookup the node in the cache again as it might have already been
		// removed.
		if n := s.mu.nodes[key]; n != nil && n.value == v {
			s.unlinkNode(n)
			s.clearNode(n)
		}
	}
	close(v.initialized)
	return v
}

func (s *shard[K, V, InitOpts]) addNode(n *node[K, V], key K, status nodeStatus) {
	n.key = key
	n.status = status

	s.evictNodes()
	s.mu.nodes[n.key] = n

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
		// NB: This is equivalent to UnrefValue, but we perform the releaseValueFn()
		// call synchronously below to free up any associated resources before
		// returning.
		s.unlinkNode(n)
		v = n.value
	}
	s.mu.Unlock()

	if v != nil {
		if v.refCount.Add(-1) != 0 {
			panic("element has outstanding references")
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
	// Collect the keys which need to be evicted.
	var keys []K
	s.mu.RLock()
	s.forAllNodesLocked(func(n *node[K, V]) {
		if predicate(n.key) {
			keys = append(keys, n.key)
		}
	})
	s.mu.RUnlock()

	for i := range keys {
		s.Evict(keys[i])
	}

	if invariants.Enabled {
		s.mu.RLock()
		defer s.mu.RUnlock()
		s.forAllNodesLocked(func(n *node[K, V]) {
			if predicate(n.key) {
				panic("evictable key added in shard")
			}
		})
	}
	return keys
}

func (s *shard[K, V, InitOpts]) forAllNodesLocked(f func(n *node[K, V])) {
	if firstNode := s.mu.handHot; firstNode != nil {
		for node := firstNode; ; {
			f(node)
			if node = node.next(); node == firstNode {
				return
			}
		}
	}
}

// Close the shard, releasing all live values. There must not be any outstanding
// references on any of the values.
func (s *shard[K, V, InitOpts]) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for s.mu.handHot != nil {
		n := s.mu.handHot
		if v := n.value; v != nil {
			if v.refCount.Add(-1) != 0 {
				panic("element has outstanding references")
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
