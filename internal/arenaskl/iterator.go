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

	"github.com/petermattis/pebble/db"
)

type splice struct {
	prev *node
	next *node
}

func (s *splice) init(prev, next *node) {
	s.prev = prev
	s.next = next
}

// Iterator is an iterator over the skiplist object. Use Skiplist.NewIterator
// to construct an iterator. The current state of the iterator can be cloned by
// simply value copying the struct. All iterator methods are thread-safe.
type Iterator struct {
	list *Skiplist
	nd   *node
}

// Close resets the iterator.
func (it *Iterator) Close() error {
	it.list = nil
	it.nd = nil
	return nil
}

// Error returns any accumulated error.
func (it *Iterator) Error() error {
	return nil
}

// SeekGE moves the iterator to the first entry whose key is greater than or
// equal to the given key. Returns true if the iterator is pointing at a
// valid entry and false otherwise.
func (it *Iterator) SeekGE(key []byte) bool {
	_, it.nd, _ = it.seekForBaseSplice(key)
	return it.nd != it.list.tail
}

// SeekLT moves the iterator to the last entry whose key is less than the given
// key. Returns true if the iterator is pointing at a valid entry and false
// otherwise.
func (it *Iterator) SeekLT(key []byte) bool {
	it.nd, _, _ = it.seekForBaseSplice(key)
	return it.nd != it.list.head
}

// First seeks position at the first entry in list. Final state of iterator is
// Valid() iff list is not empty.
func (it *Iterator) First() bool {
	it.nd = it.list.getNext(it.list.head, 0)
	return it.nd != it.list.tail
}

// Last seeks position at the last entry in list. Final state of iterator is
// Valid() iff list is not empty.
func (it *Iterator) Last() bool {
	it.nd = it.list.getPrev(it.list.tail, 0)
	return it.nd != it.list.head
}

// Next advances to the next position. If there are no following nodes, then
// Valid() will be false after this call.
func (it *Iterator) Next() bool {
	it.nd = it.list.getNext(it.nd, 0)
	return it.nd != it.list.tail
}

// Prev moves to the previous position. If there are no previous nodes, then
// Valid() will be false after this call.
func (it *Iterator) Prev() bool {
	it.nd = it.list.getPrev(it.nd, 0)
	return it.nd != it.list.head
}

// Key returns the key at the current position.
func (it *Iterator) Key() db.InternalKey {
	b := it.list.arena.getBytes(it.nd.keyOffset, it.nd.keySize)
	// This is a manual inline of db.DecodeInternalKey, because the Go compiler
	// seems to refuse to automatically inline it currently.
	l := len(b) - 8
	var trailer uint64
	if l >= 0 {
		trailer = binary.LittleEndian.Uint64(b[l:])
		b = b[:l:l]
	} else {
		trailer = uint64(db.InternalKeyKindInvalid)
	}
	return db.InternalKey{
		UserKey: b,
		Trailer: trailer,
	}
}

// Value returns the value at the current position.
func (it *Iterator) Value() []byte {
	return it.nd.getValue(it.list.arena)
}

// Head true iff the iterator is positioned at the sentinel head node.
func (it *Iterator) Head() bool {
	return it.nd == it.list.head
}

// Tail true iff the iterator is positioned at the sentinel tail node.
func (it *Iterator) Tail() bool {
	return it.nd == it.list.tail
}

// Valid returns true iff the iterator is positioned at a valid node.
func (it *Iterator) Valid() bool {
	return it.nd != it.list.head && it.nd != it.list.tail
}

func (it *Iterator) seekForBaseSplice(key []byte) (prev, next *node, found bool) {
	ikey := db.MakeSearchKey(key)
	level := int(it.list.Height() - 1)

	prev = it.list.head
	for {
		prev, next, found = it.list.findSpliceForLevel(ikey, level, prev)

		if found {
			break
		}

		if level == 0 {
			break
		}

		level--
	}

	return
}
