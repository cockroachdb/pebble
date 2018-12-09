// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rangedel

import (
	"github.com/petermattis/pebble/db"
)

// invalidate the specified iterator by moving it past the last entry.
func invalidate(iter iterator) {
	iter.Last()
	iter.Next()
}

// SeekGE seeks to the newest tombstone that contains or is past the target
// key. The snapshot parameter controls the visibility of tombstones (only
// tombstones older than the snapshot sequence number are visible). The
// iterator must contain fragmented tombstones: any overlapping tombstones must
// have the same start and end key.
func SeekGE(cmp db.Compare, iter iterator, key []byte, snapshot uint64) Tombstone {
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
	valid := iter.SeekLT(key)

	// Invariant: key < iter.Key().UserKey

	if valid && cmp(key, iter.Value()) < 0 {
		// The current tombstones contains or is past the search key, but SeekLT
		// returns the oldest entry for a key, so backup until we hit the previous
		// tombstone or an entry which is not visible.
		for savedKey := iter.Key().UserKey; ; {
			if !iter.Prev() || cmp(savedKey, iter.Value()) >= 0 || !iter.Key().Visible(snapshot) {
				iter.Next()
				break
			}
		}
	} else {
		// The current tombstone lies before the search key. Advance the iterator
		// as long as the search key lies past the end of the tombstone. See the
		// comment at the start of this function about why this is necessary.
		for {
			if !iter.Next() {
				// We've run out of tombstones.
				return Tombstone{}
			}
			if cmp(key, iter.Value()) < 0 {
				break
			}
		}
	}

	// Walk through the tombstones to find one the newest one that is visible
	// (i.e. has a sequence number less than the snapshot sequence number).
	for {
		if start := iter.Key(); start.Visible(snapshot) {
			// The tombstone is visible at our read sequence number.
			return Tombstone{
				Start: start,
				End:   iter.Value(),
			}
		}
		if !iter.Next() {
			// We've run out of tombstones.
			return Tombstone{}
		}
	}
}

// SeekLE seeks to the newest tombstone that contains or is before the target
// key. The snapshot parameter controls the visibility of tombstones (only
// tombstones older than the snapshot sequence number are visible). The
// iterator must contain fragmented tombstones: any overlapping tombstones must
// have the same start and end key.
func SeekLE(cmp db.Compare, iter iterator, key []byte, snapshot uint64) Tombstone {
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
			iter.Prev()
			return Tombstone{}
		}
		// Advance the iterator until we find a visible version or we hit the next
		// tombstone.
		for {
			if start := iter.Key(); start.Visible(snapshot) {
				return Tombstone{
					Start: start,
					End:   iter.Value(),
				}
			}
			if !iter.Next() {
				// We've run out of tombstones.
				return Tombstone{}
			}
			if cmp(key, iter.Key().UserKey) < 0 {
				// We've hit the next tombstone.
				invalidate(iter)
				return Tombstone{}
			}
		}
	}

	// Invariant: key >= iter.Key().UserKey

	if cmp(key, iter.Value()) >= 0 {
		// The current tombstone lies before the search key. Check to see if the
		// next tombstone contains the search key. If it doesn't, we'll backup and
		// use the current tombstone.
		if !iter.Next() || cmp(key, iter.Key().UserKey) < 0 {
			iter.Prev()
		} else {
			// Advance the iterator until we find a visible version or we hit the
			// next tombstone.
			for {
				if start := iter.Key(); start.Visible(snapshot) {
					return Tombstone{
						Start: start,
						End:   iter.Value(),
					}
				}
				if !iter.Next() || cmp(key, iter.Key().UserKey) < 0 {
					iter.Prev()
					break
				}
			}
		}
	}

	// We're now positioned at a tombstone that contains or is before the search
	// key and we're positioned at either the oldest of the versions or a visible
	// version. Walk backwards through the tombstones to find the newest one that
	// is visible (i.e. has a sequence number less than the snapshot sequence
	// number).
	for savedKey := iter.Key().UserKey; ; {
		valid := iter.Key().Visible(snapshot)
		if !iter.Prev() {
			break
		}
		if valid {
			start := iter.Key()
			if !start.Visible(snapshot) {
				break
			}
			if cmp(savedKey, start.UserKey) != 0 {
				break
			}
		}
	}

	iter.Next()
	start := iter.Key()
	if cmp(key, start.UserKey) < 0 {
		// The current tombstone is after our search key.
		invalidate(iter)
		return Tombstone{}
	}
	if !start.Visible(snapshot) {
		// The current tombstone is not visible at our read sequence number.
		invalidate(iter)
		return Tombstone{}
	}
	// The tombstone is visible at our read sequence number.
	return Tombstone{
		Start: start,
		End:   iter.Value(),
	}
}
