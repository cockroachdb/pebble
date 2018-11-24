// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rangedel

import (
	"github.com/petermattis/pebble/db"
)

// Iter is an iterator over a set of fragmented tombstones.
type Iter struct {
	cmp        db.Compare
	tombstones []Tombstone
	index      int
}

// NewIter returns a new iterator over a set of fragmented tombstones.
func NewIter(cmp db.Compare, tombstones []Tombstone) *Iter {
	return &Iter{
		cmp:        cmp,
		tombstones: tombstones,
		index:      -1,
	}
}

// SeekGE implements internalIterator.SeekGE, as documented in the pebble
// package.
func (i *Iter) SeekGE(key []byte) {
	// TODO(peter): Use binary search.
	for i.index = 0; i.index < len(i.tombstones); i.index++ {
		if i.cmp(key, i.Key().UserKey) <= 0 {
			break
		}
	}
}

// SeekLT implements internalIterator.SeekLT, as documented in the pebble
// package.
func (i *Iter) SeekLT(key []byte) {
	// TODO(peter): Use binary search.
	for i.index = len(i.tombstones) - 1; i.index >= 0; i.index-- {
		if i.cmp(key, i.Key().UserKey) > 0 {
			break
		}
	}
}

// First implements internalIterator.First, as documented in the pebble
// package.
func (i *Iter) First() {
	i.index = 0
}

// Last implements internalIterator.Last, as documented in the pebble
// package.
func (i *Iter) Last() {
	i.index = len(i.tombstones) - 1
}

// Next implements internalIterator.Next, as documented in the pebble
// package.
func (i *Iter) Next() bool {
	if i.index == len(i.tombstones) {
		return false
	}
	i.index++
	return i.index < len(i.tombstones)
}

// Prev implements internalIterator.Prev, as documented in the pebble
// package.
func (i *Iter) Prev() bool {
	if i.index < 0 {
		return false
	}
	i.index--
	return i.index >= 0
}

// Key implements internalIterator.Key, as documented in the pebble
// package.
func (i *Iter) Key() db.InternalKey {
	return i.tombstones[i.index].Start
}

// Value implements internalIterator.Value, as documented in the pebble
// package.
func (i *Iter) Value() []byte {
	return i.tombstones[i.index].End
}

// Valid implements internalIterator.Valid, as documented in the pebble
// package.
func (i *Iter) Valid() bool {
	return i.index >= 0 && i.index < len(i.tombstones)
}

// Error implements internalIterator.Error, as documented in the pebble
// package.
func (i *Iter) Error() error {
	return nil
}

// Close implements internalIterator.Close, as documented in the pebble
// package.
func (i *Iter) Close() error {
	return nil
}
