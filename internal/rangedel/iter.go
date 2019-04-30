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
func (i *Iter) SeekGE(key []byte) (*db.InternalKey, []byte) {
	// NB: manually inlined sort.Seach is ~5% faster.
	//
	// Define f(-1) == false and f(n) == true.
	// Invariant: f(index-1) == false, f(upper) == true.
	ikey := db.MakeSearchKey(key)
	i.index = 0
	upper := len(i.tombstones)
	for i.index < upper {
		h := int(uint(i.index+upper) >> 1) // avoid overflow when computing h
		// i.index ≤ h < upper
		if db.InternalCompare(i.cmp, ikey, i.tombstones[h].Start) >= 0 {
			i.index = h + 1 // preserves f(i-1) == false
		} else {
			upper = h // preserves f(j) == true
		}
	}
	// i.index == upper, f(i.index-1) == false, and f(upper) (= f(i.index)) ==
	// true => answer is i.index.
	if i.index >= len(i.tombstones) {
		return nil, nil
	}
	t := &i.tombstones[i.index]
	return &t.Start, t.End
}

// SeekLT implements internalIterator.SeekLT, as documented in the pebble
// package.
func (i *Iter) SeekLT(key []byte) (*db.InternalKey, []byte) {
	// NB: manually inlined sort.Search is ~5% faster.
	//
	// Define f(-1) == false and f(n) == true.
	// Invariant: f(index-1) == false, f(upper) == true.
	ikey := db.MakeSearchKey(key)
	i.index = 0
	upper := len(i.tombstones)
	for i.index < upper {
		h := int(uint(i.index+upper) >> 1) // avoid overflow when computing h
		// i.index ≤ h < upper
		if db.InternalCompare(i.cmp, ikey, i.tombstones[h].Start) > 0 {
			i.index = h + 1 // preserves f(i-1) == false
		} else {
			upper = h // preserves f(j) == true
		}
	}
	// i.index == upper, f(i.index-1) == false, and f(upper) (= f(i.index)) ==
	// true => answer is i.index.

	// Since keys are strictly increasing, if i.index > 0 then i.index-1 will be
	// the largest whose key is < the key sought.
	i.index--
	if i.index < 0 {
		return nil, nil
	}
	t := &i.tombstones[i.index]
	return &t.Start, t.End
}

// First implements internalIterator.First, as documented in the pebble
// package.
func (i *Iter) First() (*db.InternalKey, []byte) {
	if len(i.tombstones) == 0 {
		return nil, nil
	}
	i.index = 0
	t := &i.tombstones[i.index]
	return &t.Start, t.End
}

// Last implements internalIterator.Last, as documented in the pebble
// package.
func (i *Iter) Last() (*db.InternalKey, []byte) {
	if len(i.tombstones) == 0 {
		return nil, nil
	}
	i.index = len(i.tombstones) - 1
	t := &i.tombstones[i.index]
	return &t.Start, t.End
}

// Next implements internalIterator.Next, as documented in the pebble
// package.
func (i *Iter) Next() (*db.InternalKey, []byte) {
	if i.index == len(i.tombstones) {
		return nil, nil
	}
	i.index++
	if i.index == len(i.tombstones) {
		return nil, nil
	}
	t := &i.tombstones[i.index]
	return &t.Start, t.End
}

// Prev implements internalIterator.Prev, as documented in the pebble
// package.
func (i *Iter) Prev() (*db.InternalKey, []byte) {
	if i.index < 0 {
		return nil, nil
	}
	i.index--
	if i.index < 0 {
		return nil, nil
	}
	t := &i.tombstones[i.index]
	return &t.Start, t.End
}

// Key implements internalIterator.Key, as documented in the pebble
// package.
func (i *Iter) Key() *db.InternalKey {
	return &i.tombstones[i.index].Start
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
