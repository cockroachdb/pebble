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
	"encoding/binary"
	"sync"

	"github.com/petermattis/pebble/internal/base"
)

// FlushIterator is an iterator over the skiplist object. Use Skiplist.NewFlushIter
// to construct an iterator. The current state of the iterator can be cloned by
// simply value copying the struct.
type FlushIterator struct {
	list          *Skiplist
	nd            *node
	key           base.InternalKey
	lower         []byte
	upper         []byte
}

var flushIterPool = sync.Pool{
	New: func() interface{} {
		return &FlushIterator{}
	},
}

// Close resets the iterator.
func (it *FlushIterator) Close() error {
	it.list = nil
	it.nd = nil
	it.lower = nil
	it.upper = nil
	flushIterPool.Put(it)
	return nil
}

// Error returns any accumulated error.
func (it *FlushIterator) Error() error {
	return nil
}

func (it *FlushIterator) SeekGE(key []byte) (*base.InternalKey, []byte) {
	panic("pebble: SeekGE unimplemented")
}

func (it *FlushIterator) SeekPrefixGE(prefix, key []byte) (*base.InternalKey, []byte) {
	panic("pebble: SeekPrefixGE unimplemented")
}

func (it *FlushIterator) SeekLT(key []byte) (*base.InternalKey, []byte) {
	panic("pebble: SeekLT unimplemented")
}

// First seeks position at the first entry in list. Returns the key and value
// if the iterator is pointing at a valid entry, and (nil, nil) otherwise. Note
// that First only checks the upper bound. It is up to the caller to ensure
// that key is greater than or equal to the lower bound.
func (it *FlushIterator) First() (*base.InternalKey, []byte) {
	it.nd = it.list.getNext(it.list.head, 0)
	if it.nd == it.list.tail {
		return nil, nil
	}
	it.decodeKey()
	if it.upper != nil && it.list.cmp(it.upper, it.key.UserKey) <= 0 {
		it.nd = it.list.tail
		return nil, nil
	}
	it.list.bytesIterated = it.nd.allocSize
	return &it.key, it.Value()
}

// TODO(ryan): This doesn't make much sense in the context of a flush iterator,
// but it seems necessary in one function.
func (it *FlushIterator) Last() (*base.InternalKey, []byte) {
	it.nd = it.list.getPrev(it.list.tail, 0)
	if it.nd == it.list.head {
		return nil, nil
	}
	it.decodeKey()
	if it.lower != nil && it.list.cmp(it.lower, it.key.UserKey) > 0 {
		it.nd = it.list.head
		return nil, nil
	}
	return &it.key, it.Value()
}

// Next advances to the next position. Returns the key and value if the
// iterator is pointing at a valid entry, and (nil, nil) otherwise.
func (it *FlushIterator) Next() (*base.InternalKey, []byte) {
	it.nd = it.list.getNext(it.nd, 0)
	if it.nd == it.list.tail {
		return nil, nil
	}
	it.decodeKey()
	if it.upper != nil && it.list.cmp(it.upper, it.key.UserKey) <= 0 {
		it.nd = it.list.tail
		return nil, nil
	}
	it.list.bytesIterated += it.nd.allocSize
	return &it.key, it.Value()
}

func (it *FlushIterator) Prev() (*base.InternalKey, []byte) {
	panic("pebble: Prev unimplemented")
}

// Key returns the key at the current position.
func (it *FlushIterator) Key() *base.InternalKey {
	return &it.key
}

// Value returns the value at the current position.
func (it *FlushIterator) Value() []byte {
	return it.nd.getValue(it.list.arena)
}

// Head true iff the iterator is positioned at the sentinel head node.
func (it *FlushIterator) Head() bool {
	return it.nd == it.list.head
}

// Tail true iff the iterator is positioned at the sentinel tail node.
func (it *FlushIterator) Tail() bool {
	return it.nd == it.list.tail
}

// Valid returns true iff the iterator is positioned at a valid node.
func (it *FlushIterator) Valid() bool {
	return it.nd != it.list.head && it.nd != it.list.tail
}

// SetBounds sets the lower and upper bounds for the iterator. Note that the
// result of Next will be undefined until the iterator has been repositioned
// with First.
func (it *FlushIterator) SetBounds(lower, upper []byte) {
	it.lower = lower
	it.upper = upper
}

func (it *FlushIterator) decodeKey() {
	b := it.list.arena.getBytes(it.nd.keyOffset, it.nd.keySize)
	// This is a manual inline of base.DecodeInternalKey, because the Go compiler
	// seems to refuse to automatically inline it currently.
	l := len(b) - 8
	if l >= 0 {
		it.key.Trailer = binary.LittleEndian.Uint64(b[l:])
		it.key.UserKey = b[:l:l]
	} else {
		it.key.Trailer = uint64(base.InternalKeyKindInvalid)
		it.key.UserKey = nil
	}
}

func (it *FlushIterator) seekForBaseSplice(key []byte) (prev, next *node, found bool) {
	ikey := base.MakeSearchKey(key)
	level := int(it.list.Height() - 1)

	prev = it.list.head
	for {
		prev, next, found = it.list.findSpliceForLevel(ikey, level, prev)

		if found {
			if level != 0 {
				// next is pointing at the target node, but we need to find previous on
				// the bottom level.
				prev = it.list.getPrev(next, 0)
			}
			break
		}

		if level == 0 {
			break
		}

		level--
	}

	return
}
