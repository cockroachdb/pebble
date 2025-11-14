/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 * Modifications copyright (C) 2017 Andy Kimball and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package arenaskl

import (
	"context"
	"sync"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/treeprinter"
)

type splice struct {
	prev *node
	next *node
}

func (s *splice) init(prev, next *node) {
	s.prev = prev
	s.next = next
}

// Iterator is an iterator over the skiplist object. Use Skiplist.NewIter
// to construct an iterator. The current state of the iterator can be cloned by
// simply value copying the struct. All iterator methods are thread-safe.
type Iterator struct {
	list  *Skiplist
	nd    *node
	kv    base.InternalKV
	lower []byte
	upper []byte
	// {lower|upper}Node are lazily populated with an arbitrary node that is
	// beyond the lower or upper bound respectively. Note the node is
	// "arbitrary" because it may not be the first node that exceeds the bound.
	// Concurrent insertions into the skiplist may introduce new nodes with keys
	// that exceed the bounds but are closer to the bounds than the current
	// values of [lower|upper]Node.
	//
	// Once populated, [lower|upper]Node may be used to detect when iteration
	// has reached a bound without performing a key comparison. This may be
	// beneficial when performing repeated SeekGEs with TrySeekUsingNext and an
	// upper bound set. Once the upper bound has been met, no additional key
	// comparisons are necessary.
	lowerNode *node
	upperNode *node
}

// Iterator implements the base.InternalIterator interface.
var _ base.InternalIterator = (*Iterator)(nil)

var iterPool = sync.Pool{
	New: func() interface{} {
		return &Iterator{}
	},
}

// Close resets the iterator.
func (it *Iterator) Close() error {
	*it = Iterator{}
	iterPool.Put(it)
	return nil
}

func (it *Iterator) String() string {
	return "memtable"
}

// Error returns any accumulated error.
func (it *Iterator) Error() error {
	return nil
}

// SeekGE moves the iterator to the first entry whose key is greater than or
// equal to the given key. Returns the KV pair if the iterator is pointing at a
// valid entry, and nil otherwise. Note that SeekGE only checks the upper bound.
// It is up to the caller to ensure that key is greater than or equal to the
// lower bound.
func (it *Iterator) SeekGE(key []byte, flags base.SeekGEFlags) *base.InternalKV {
	if flags.TrySeekUsingNext() {
		if it.nd == it.list.tail || it.nd == it.upperNode {
			// Iterator is done.
			return nil
		}
		less := it.list.cmp(it.kv.K.UserKey, key) < 0
		if !less {
			// Current position already satisfies the seek.
			return &it.kv
		}
		// Current key < target key, use optimized forward search.
		it.trySeekUsingNext(key)
	} else {
		_, it.nd = it.seekForBaseSplice(key)
	}

	if it.nd == it.list.tail || it.nd == it.upperNode {
		return nil
	}
	it.decodeKey()
	if it.upper != nil && it.list.cmp(it.upper, it.kv.K.UserKey) <= 0 {
		it.upperNode = it.nd
		return nil
	}
	it.kv.V = base.MakeInPlaceValue(it.value())
	return &it.kv
}

// SeekPrefixGE moves the iterator to the first entry whose key is greater than
// or equal to the given key. This method is equivalent to SeekGE and is
// provided so that an arenaskl.Iterator implements the
// internal/base.InternalIterator interface.
func (it *Iterator) SeekPrefixGE(prefix, key []byte, flags base.SeekGEFlags) *base.InternalKV {
	return it.SeekGE(key, flags)
}

// SeekLT moves the iterator to the last entry whose key is less than the given
// key. Returns the KV pair if the iterator is pointing at a valid entry, and
// nil otherwise. Note that SeekLT only checks the lower bound. It is up to the
// caller to ensure that key is less than the upper bound.
func (it *Iterator) SeekLT(key []byte, flags base.SeekLTFlags) *base.InternalKV {
	// NB: the top-level Iterator has already adjusted key based on
	// the upper-bound.
	it.nd, _ = it.seekForBaseSplice(key)
	if it.nd == it.list.head || it.nd == it.lowerNode {
		return nil
	}
	it.decodeKey()
	if it.lower != nil && it.list.cmp(it.lower, it.kv.K.UserKey) > 0 {
		it.lowerNode = it.nd
		return nil
	}
	it.kv.V = base.MakeInPlaceValue(it.value())
	return &it.kv
}

// First seeks position at the first entry in list. Returns the KV pair if the
// iterator is pointing at a valid entry, and nil otherwise. Note that First
// only checks the upper bound. It is up to the caller to ensure that key is
// greater than or equal to the lower bound (e.g. via a call to SeekGE(lower)).
func (it *Iterator) First() *base.InternalKV {
	it.nd = it.list.getNext(it.list.head, 0)
	if it.nd == it.list.tail || it.nd == it.upperNode {
		return nil
	}
	it.decodeKey()
	if it.upper != nil && it.list.cmp(it.upper, it.kv.K.UserKey) <= 0 {
		it.upperNode = it.nd
		return nil
	}
	it.kv.V = base.MakeInPlaceValue(it.value())
	return &it.kv
}

// Last seeks position at the last entry in list. Returns the KV pair if the
// iterator is pointing at a valid entry, and nil otherwise. Note that Last only
// checks the lower bound. It is up to the caller to ensure that key is less
// than the upper bound (e.g. via a call to SeekLT(upper)).
func (it *Iterator) Last() *base.InternalKV {
	it.nd = it.list.getPrev(it.list.tail, 0)
	if it.nd == it.list.head || it.nd == it.lowerNode {
		return nil
	}
	it.decodeKey()
	if it.lower != nil && it.list.cmp(it.lower, it.kv.K.UserKey) > 0 {
		it.lowerNode = it.nd
		return nil
	}
	it.kv.V = base.MakeInPlaceValue(it.value())
	return &it.kv
}

// Next advances to the next position. Returns the KV pair if the iterator is
// pointing at a valid entry, and nil otherwise.
// Note: flushIterator.Next mirrors the implementation of Iterator.Next
// due to performance. Keep the two in sync.
func (it *Iterator) Next() *base.InternalKV {
	it.nd = it.list.getNext(it.nd, 0)
	if it.nd == it.list.tail || it.nd == it.upperNode {
		return nil
	}
	it.decodeKey()
	if it.upper != nil && it.list.cmp(it.upper, it.kv.K.UserKey) <= 0 {
		it.upperNode = it.nd
		return nil
	}
	it.kv.V = base.MakeInPlaceValue(it.value())
	return &it.kv
}

// NextPrefix advances to the next position with a new prefix. Returns the KV
// pair if the iterator is pointing at a valid entry and nil otherwise.
func (it *Iterator) NextPrefix(succKey []byte) *base.InternalKV {
	return it.SeekGE(succKey, base.SeekGEFlagsNone.EnableTrySeekUsingNext())
}

// Prev moves to the previous position. Returns the KV pair if the iterator is
// pointing at a valid entry and nil otherwise.
func (it *Iterator) Prev() *base.InternalKV {
	it.nd = it.list.getPrev(it.nd, 0)
	if it.nd == it.list.head || it.nd == it.lowerNode {
		return nil
	}
	it.decodeKey()
	if it.lower != nil && it.list.cmp(it.lower, it.kv.K.UserKey) > 0 {
		it.lowerNode = it.nd
		return nil
	}
	it.kv.V = base.MakeInPlaceValue(it.value())
	return &it.kv
}

// value returns the value at the current position.
func (it *Iterator) value() []byte {
	return it.nd.getValue(it.list.arena)
}

// SetBounds sets the lower and upper bounds for the iterator. Note that the
// result of Next and Prev will be undefined until the iterator has been
// repositioned with SeekGE, SeekPrefixGE, SeekLT, First, or Last.
func (it *Iterator) SetBounds(lower, upper []byte) {
	it.lower = lower
	it.upper = upper
	it.lowerNode = nil
	it.upperNode = nil
}

// SetContext implements base.InternalIterator.
func (it *Iterator) SetContext(_ context.Context) {}

// DebugTree is part of the InternalIterator interface.
func (it *Iterator) DebugTree(tp treeprinter.Node) {
	tp.Childf("%T(%p)", it, it)
}

func (it *Iterator) decodeKey() {
	it.kv.K.UserKey = it.list.arena.getBytes(it.nd.keyOffset, it.nd.keySize)
	it.kv.K.Trailer = it.nd.keyTrailer
}

// trySeekUsingNext attempts to seek to the target key by performing a forward
// search from the current position using the skiplist structure. This uses
// seekForBaseSpliceFrom which starts from the current node and uses binary
// search to find the appropriate starting level.
func (it *Iterator) trySeekUsingNext(key []byte) {
	startNode := it.nd

	// Use seekForBaseSpliceFrom which handles:
	// 1. Binary search to find the highest safe level to start from
	// 2. Standard skiplist search from that point
	// We start with the current node's height as the initial level.
	startLevel := int(startNode.height) - 1

	_, it.nd = it.seekForBaseSpliceFrom(startNode, startLevel, key)
}

func (it *Iterator) seekForBaseSplice(key []byte) (prev, next *node) {
	return it.seekForBaseSpliceFrom(it.list.head, int(it.list.Height()-1), key)
}

// seekForBaseSpliceFrom is like seekForBaseSplice but starts from a given node
// and level. It uses binary search to find the appropriate starting level when
// starting from an intermediate node.
func (it *Iterator) seekForBaseSpliceFrom(startNode *node, startLevel int, key []byte) (prev, next *node) {
	prev = startNode

	// If we're starting from an intermediate node (not head), we need to find
	// the highest level we can safely use. Use binary search to find it.
	if prev != it.list.head {
		// Binary search for the highest usable level.
		// We know level 0 always exists, and levels >= node.height don't exist.
		nodeHeight := int(prev.height)
		if startLevel >= nodeHeight {
			startLevel = nodeHeight - 1
		}

		// Binary search to find the highest level where prev < key.
		low, high := 0, startLevel
		for low < high {
			mid := (low + high + 1) / 2
			next := it.list.getNext(prev, mid)
			if next == nil || next == it.list.tail {
				// This level goes to tail, prev < key at this level.
				low = mid
				continue
			}
			offset, size := next.keyOffset, next.keySize
			nextKey := it.list.arena.buf[offset : offset+size]
			if it.list.cmp(nextKey, key) < 0 {
				// next < key, so we can potentially start at a higher level.
				low = mid
			} else {
				// next >= key, need to try lower levels.
				high = mid - 1
			}
		}
		startLevel = low
	}

	for level := startLevel; level >= 0; level-- {

		// Search this level for the key.
		prevLevelNext := next
		for {
			// Assume prev.key < key.
			next = it.list.getNext(prev, level)

			// Before performing a key comparison, check if the next pointer
			// equals prevLevelNext. The pointer comparison is significantly
			// cheaper than a key comparison.
			//
			// It's not unlikely for consecutive levels to have the same next
			// pointer. We use [maxHeight]=20 levels, and with each higher
			// height the probability a node extends one more rung of the tower
			// is 1/e.
			//
			// The skiplist may contain nodes with keys between the (prev,next)
			// pair of nodes that make up the previous level's splice. Let's
			// divide these nodes into the L nodes with keys < key and the R
			// nodes with keys > key. Only a subset of these nodes may have
			// towers that reach [level].
			//
			// Of the nodes in R that reach [level], we only care about the one
			// with the smallest key. If there are no nodes in R that reach
			// [level], then this level's splice's next pointer will be the same
			// as the level above's splice's next pointer. We can perform a
			// cheap pointer comparison of [next] and [prevLevelNext] to
			// determine this.
			//
			// (Note that we must still skip over any of the nodes in L that are
			// high enough to reach [level], and each of these nodes will
			// require a key comparison.)
			//
			//        (< key)                              (≥ key)
			//         prev                              prevLevelNext
			//      +---------+                          +---------+
			//      |         |                          |         |
			//      | level+1 |------------------------> |         |
			//      |         |                          |         |
			//      |         |          next            |         |
			//      |         |       +--------+         |         |
			//      | level   |--...--|        |--...--> |         |
			//      |         |       |        |         |         |
			//      |         |       |        |         |         |
			//      +---------+       +--------+         +---------+
			if next == prevLevelNext {
				break
			}
			if next == it.list.tail {
				// Tail node, so done.
				break
			}

			offset, size := next.keyOffset, next.keySize
			nextKey := it.list.arena.buf[offset : offset+size]
			cmp := it.list.cmp(key, nextKey)
			if cmp <= 0 {
				// We are done for this level, since prev.key < key <= next.key.
				break
			}
			// Keep moving right on this level.
			prev = next
		}
	}

	return prev, next
}
