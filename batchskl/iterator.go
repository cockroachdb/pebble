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

package batchskl

type splice struct {
	prev uint32
	next uint32
}

func (s *splice) init(prev, next uint32) {
	s.prev = prev
	s.next = next
}

// Iterator is an iterator over the skiplist object. Use Skiplist.NewIterator
// to construct an iterator. The current state of the iterator can be cloned
// by simply value copying the struct.
type Iterator struct {
	list *Skiplist
	nd   uint32
}

// Valid returns nil iff the iterator is positioned at a valid node.
func (it *Iterator) Valid() error {
	if it.nd != it.list.head && it.nd != it.list.tail {
		return nil
	}
	return ErrInvalid
}

// Key returns the key at the current position.
func (it *Iterator) Key() []byte {
	return it.list.storage.Get(it.list.getKey(it.nd))
}

// Next advances to the next position. If there are no following nodes, then
// Valid() will be false after this call.
func (it *Iterator) Next() {
	it.nd = it.list.getNext(it.nd, 0)
}

// Prev moves to the previous position. If there are no previous nodes, then
// Valid() will be false after this call.
func (it *Iterator) Prev() {
	it.nd = it.list.getPrev(it.nd, 0)
}

// SeekGE moves the iterator to the first entry whose key is greater than or
// equal to the given key. Returns true if the given key exists and false
// otherwise.
func (it *Iterator) SeekGE(key []byte) (found bool) {
	_, it.nd, found = it.seekForBaseSplice(key, it.list.storage.Prefix(key))
	return found
}

// SeekLE moves the iterator to the first entry whose key is less than or equal
// to the given key. Returns true if the given key exists and false otherwise.
func (it *Iterator) SeekLE(key []byte) (found bool) {
	var prev, next uint32
	prev, next, found = it.seekForBaseSplice(key, it.list.storage.Prefix(key))

	if found {
		it.nd = next
	} else {
		it.nd = prev
	}

	return found
}

// First seeks position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (it *Iterator) First() {
	it.nd = it.list.getNext(it.list.head, 0)
}

// Last seeks position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (it *Iterator) Last() {
	it.nd = it.list.getPrev(it.list.tail, 0)
}

func (it *Iterator) seekForBaseSplice(
	key []byte, prefix KeyPrefix,
) (prev, next uint32, found bool) {
	prev = it.list.head
	for level := it.list.height - 1; ; level-- {
		prev, next, found = it.list.findSpliceForLevel(key, prefix, level, prev)
		if found {
			break
		}
		if level == 0 {
			break
		}
	}

	return
}
