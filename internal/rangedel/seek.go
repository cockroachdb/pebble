// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rangedel

import "github.com/cockroachdb/pebble/internal/base"

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
func SeekGE(cmp base.Compare, iter iterator, key []byte, snapshot uint64) Tombstone {
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
	iterKey, iterValue := iter.SeekLT(key)

	// Invariant: key < iter.Key().UserKey

	if iterKey != nil && cmp(key, iterValue) < 0 {
		// The current tombstones contains or is past the search key, but SeekLT
		// returns the oldest entry for a key, so backup until we hit the previous
		// tombstone or an entry which is not visible.
		for savedKey := iterKey.UserKey; ; {
			iterKey, iterValue = iter.Prev()
			if iterKey == nil || cmp(savedKey, iterValue) >= 0 || !iterKey.Visible(snapshot) {
				iterKey, iterValue = iter.Next()
				break
			}
		}
	} else {
		// The current tombstone lies before the search key. Advance the iterator
		// as long as the search key lies past the end of the tombstone. See the
		// comment at the start of this function about why this is necessary.
		for {
			iterKey, iterValue = iter.Next()
			if iterKey == nil {
				// We've run out of tombstones.
				return Tombstone{}
			}
			if cmp(key, iterValue) < 0 {
				break
			}
		}
	}

	// Walk through the tombstones to find one the newest one that is visible
	// (i.e. has a sequence number less than the snapshot sequence number).
	for {
		if start := iterKey; start.Visible(snapshot) {
			// The tombstone is visible at our read sequence number.
			return Tombstone{
				Start: *start,
				End:   iterValue,
			}
		}
		iterKey, iterValue = iter.Next()
		if iterKey == nil {
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
func SeekLE(cmp base.Compare, iter iterator, key []byte, snapshot uint64) Tombstone {
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
	iterKey, iterValue := iter.SeekLT(key)
	if iterKey == nil {
		iterKey, iterValue = iter.Next()
		if iterKey == nil {
			// The iterator is empty.
			return Tombstone{}
		}
		if cmp(key, iterKey.UserKey) < 0 {
			// The search key lies before the first tombstone.
			//
			// TODO(peter): why is this call to iter.Prev() here?
			iterKey, iterValue = iter.Prev()
			return Tombstone{}
		}
		// Advance the iterator until we find a visible version or we hit the next
		// tombstone.
		for {
			if start := iterKey; start.Visible(snapshot) {
				return Tombstone{
					Start: *start,
					End:   iterValue,
				}
			}
			iterKey, iterValue = iter.Next()
			if iterKey == nil {
				// We've run out of tombstones.
				return Tombstone{}
			}
			if cmp(key, iterKey.UserKey) < 0 {
				// We've hit the next tombstone.
				invalidate(iter)
				return Tombstone{}
			}
		}
	}

	// Invariant: key >= iter.Key().UserKey

	if cmp(key, iterValue) >= 0 {
		// The current tombstone lies before the search key. Check to see if the
		// next tombstone contains the search key. If it doesn't, we'll backup and
		// use the current tombstone.
		iterKey, iterValue = iter.Next()
		if iterKey == nil || cmp(key, iterKey.UserKey) < 0 {
			iterKey, iterValue = iter.Prev()
		} else {
			// Advance the iterator until we find a visible version or we hit the
			// next tombstone.
			for {
				if start := iterKey; start.Visible(snapshot) {
					return Tombstone{
						Start: *start,
						End:   iterValue,
					}
				}
				iterKey, iterValue = iter.Next()
				if iterKey == nil || cmp(key, iterKey.UserKey) < 0 {
					iterKey, iterValue = iter.Prev()
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
	for savedKey := iterKey.UserKey; ; {
		valid := iterKey.Visible(snapshot)
		iterKey, iterValue = iter.Prev()
		if iterKey == nil {
			break
		}
		if valid {
			if !iterKey.Visible(snapshot) {
				break
			}
			if cmp(savedKey, iterKey.UserKey) != 0 {
				break
			}
		}
	}

	iterKey, iterValue = iter.Next()
	start := iterKey
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
		Start: *start,
		End:   iterValue,
	}
}
