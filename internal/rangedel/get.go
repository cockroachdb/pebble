// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rangedel

import (
	"github.com/petermattis/pebble/db"
)

// iterator is a subset of the pebble.internalIterator interface needed for
// range deletion iteration.
type iterator interface {
	// SeekLT moves the iterator to the last key/value pair whose key is less
	// than the given key.
	SeekLT(key []byte) bool

	// First moves the iterator the the first key/value pair.
	First() bool

	// Last moves the iterator the the last key/value pair.
	Last() bool

	// Next moves the iterator to the next key/value pair.
	// It returns whether the iterator is exhausted.
	Next() bool

	// Prev moves the iterator to the previous key/value pair.
	// It returns whether the iterator is exhausted.
	Prev() bool

	// Key returns the encoded internal key of the current key/value pair, or nil
	// if done. The caller should not modify the contents of the returned slice,
	// and its contents may change on the next call to Next.
	Key() db.InternalKey

	// Value returns the value of the current key/value pair, or nil if done.
	// The caller should not modify the contents of the returned slice, and
	// its contents may change on the next call to Next.
	Value() []byte

	// Valid returns true if the iterator is positioned at a valid key/value pair
	// and false otherwise.
	Valid() bool
}

// Get returns the newest tombstone that contains the target key. If no
// tombstone contains the target key, an empty tombstone is returned. The
// snapshot parameter controls the visibility of tombstones (only tombstones
// older than the snapshot sequence number are visible). The iterator must
// contain fragmented tombstones: any overlapping tombstones must have the same
// start and end key.
func Get(cmp db.Compare, iter iterator, key []byte, snapshot uint64) Tombstone {
	// NB: We use SeekLT in order to land on the proper tombstone for a search
	// key that resides in the middle of a tombstone. Consider the scenario:
	//
	//     a---e
	//         e---i
	//
	// The tombstones are indexed by their start keys `a` and `e`. If the
	// search key is `c` we want to land on the tombstone [a,e). If we were to
	// use SeekGE then the search key `c` would land on the tombstone [e,i) and
	// we'd have to backtrack. The one complexity here is what happens for the
	// search key `e`. In that case SeekLT will land us on the tombstone [a,e)
	// and we'll have to move forward.
	if !iter.SeekLT(key) {
		if !iter.Next() {
			// The iterator is empty.
			return Tombstone{}
		}
		if cmp(key, iter.Key().UserKey) < 0 {
			// The search key lies before the first tombstone.
			return Tombstone{}
		}
	}

	// Invariant: key >= iter.Key().UserKey

	if cmp(key, iter.Value()) < 0 {
		// The current tombstone contains the search key, but SeekLT returns the
		// oldest entry for a key, so backup until we hit the previous tombstone or
		// an entry which is not visible.
		for {
			if !iter.Prev() || cmp(key, iter.Value()) >= 0 || !iter.Key().Visible(snapshot) {
				iter.Next()
				break
			}
		}
	} else {
		// The current tombstone lies before the search key. Advance the iterator
		// as long as the search key lies past the end of the tombstone. See the
		// comment at the start of this function about why this is necessary.
		for {
			if !iter.Next() || cmp(key, iter.Key().UserKey) < 0 {
				// We've run out of tombstones or we've moved on to a tombstone which
				// starts after our search key.
				return Tombstone{}
			}
			if cmp(key, iter.Value()) < 0 {
				break
			}
		}
	}

	for {
		if start := iter.Key(); start.Visible(snapshot) {
			// The tombstone is visible at our read sequence number.
			return Tombstone{
				Start: start,
				End:   iter.Value(),
			}
		}
		if !iter.Next() || cmp(key, iter.Key().UserKey) < 0 {
			// We've run out of tombstones or we've moved on to a tombstone which
			// starts after our search key.
			return Tombstone{}
		}
	}
}
