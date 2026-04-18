// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package genericcache

import "sync/atomic"

// node is an entry in the cache. Normally half the nodes in the cache have a
// value, and half do not.
//
// A node in the limbo state is in the cache map but is NOT in the CLOCK-Pro
// linked list and does not count toward sizeHot/sizeCold/sizeTest. Limbo
// nodes hold a value whose initValueFn has not yet returned. Concurrent
// FindOrCreate calls for the same key still find a limbo node via the map
// and wait on v.initialized. Once initValueFn completes successfully, the
// node transitions out of limbo by being added to the linked list with
// status equal to limboTarget.
type node[K Key, V any] struct {
	key   K
	value *value[V]

	links struct {
		next *node[K, V]
		prev *node[K, V]
	}
	status nodeStatus
	// limboTarget is the status the node should transition to when its
	// initValueFn completes. Only meaningful when status == limbo.
	limboTarget nodeStatus
	// referenced is atomically set to indicate that this entry has been accessed
	// since the last time one of the clock hands swept it.
	referenced atomic.Bool
}

type nodeStatus int8

const (
	test = iota
	cold
	hot
	limbo
)

func (p nodeStatus) String() string {
	switch p {
	case test:
		return "test"
	case cold:
		return "cold"
	case hot:
		return "hot"
	case limbo:
		return "limbo"
	}
	return "unknown"
}

func (n *node[K, V]) next() *node[K, V] {
	if n == nil {
		return nil
	}
	return n.links.next
}

func (n *node[K, V]) prev() *node[K, V] {
	if n == nil {
		return nil
	}
	return n.links.prev
}

func (n *node[K, V]) link(s *node[K, V]) {
	s.links.prev = n.links.prev
	s.links.prev.links.next = s
	s.links.next = n
	s.links.next.links.prev = s
}

func (n *node[K, V]) unlink() *node[K, V] {
	next := n.links.next
	n.links.prev.links.next = n.links.next
	n.links.next.links.prev = n.links.prev
	n.links.prev = n
	n.links.next = n
	return next
}

type value[V any] struct {
	// v and err can only be used after initialized is closed.
	v   V
	err error

	initialized chan struct{}
	refCount    atomic.Int32
}
