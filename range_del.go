// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/internal/rangedel"
)

func rangeDelGet(
	cmp db.Compare,
	i internalIterator,
	key []byte,
	snapshot uint64,
) rangedel.Tombstone {
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
	i.SeekLT(key)
	if !i.Valid() {
		i.Next()
		if !i.Valid() {
			return rangedel.Tombstone{}
		}
	}

	if cmp(key, i.Key().UserKey) < 0 {
		// The search key lies before the start of the tombstone.
		return rangedel.Tombstone{}
	}

	// Advance the iterator as long as the search key lies past the end of the
	// tombstone. See the comment above about why this is necessary.
	for cmp(key, i.Value()) >= 0 {
		i.Next()
		if !i.Valid() || cmp(key, i.Key().UserKey) < 0 {
			// We've run out of tombstones or we've moved on to a tombstone which
			// starts after our search key.
			return rangedel.Tombstone{}
		}
	}

	// At this point, key >= tombstone-start and key < tombstone-end. Walk
	// through the tombstones to find one that is both visible and newer than
	// our key's sequence number. SeekLT returns the oldest entry for a key, so
	// back up to find the newest.
	for {
		i.Prev()
		if !i.Valid() || cmp(key, i.Value()) >= 0 {
			i.Next()
			break
		}
	}

	for {
		tStart := i.Key()
		tSeqNum := tStart.SeqNum()
		if tSeqNum < snapshot || (tSeqNum&db.InternalKeySeqNumBatch) != 0 {
			// The tombstone is visible at our read sequence number.
			return rangedel.Tombstone{
				Start: tStart,
				End:   i.Value(),
			}
		}
		i.Next()
		if !i.Valid() || cmp(key, i.Key().UserKey) < 0 {
			// We've run out of tombstones or we've moved on to a tombstone which
			// starts after our search key.
			return rangedel.Tombstone{}
		}
	}
}

// rangeDelSeekGE seeks to the newest tombstone that contains or is past
// the target key. The snapshot parameter controls the visibility of tombstones
// (only tombstones older than the snapshot sequence number are visible). The
// internal iterator must contain fragmented tombstones: any overlapping
// tombstones must have the same start and end key.
func rangeDelSeekGE(
	cmp db.Compare,
	i internalIterator,
	key []byte,
	snapshot uint64,
) rangedel.Tombstone {
	return rangeDelGet(cmp, i, key, snapshot)
}

// rangeTombstoneSeekGE seeks to the newest tombstone that contains or is
// before the target key. The snapshot parameter controls the visibility of
// tombstones (only tombstones older than the snapshot sequence number are
// visible). The internal iterator must contain fragmented tombstones: any
// overlapping tombstones must have the same start and end key.
func rangeDelSeekLE(
	cmp db.Compare,
	i internalIterator,
	key []byte,
	snapshot uint64,
) rangedel.Tombstone {
	return rangeDelGet(cmp, i, key, snapshot)
}
