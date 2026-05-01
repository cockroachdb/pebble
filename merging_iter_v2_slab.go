// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"iter"
	"slices"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/iterv2"
)

// See the mergingIterV2 doc comment for an overview of the slab concept.

// slabState manages the per-level span state, the per-level visibility ranges
// ([minSeqNum, maxSeqNum) on each level), and the slab boundary for the
// merging iterator.
type slabState struct {
	cmp base.Compare
	// snapshot is the SeqNum at which we are reading; only point/span keys with
	// lower seq nums are visible.
	snapshot base.SeqNum
	// batchSnapshot is non-zero if and only if a level is the indexed batch.
	batchSnapshot base.SeqNum
	// batchLevelIdx is the index of the batch level; only meaningful when
	// batchSnapshot != 0. Defaults to 0.
	batchLevelIdx int

	// levels aliases m.levels, sharing the same backing array. Indexed by
	// level index (0 = highest / most recent level).
	levels []mergingIterV2Level

	// nextBoundary is the nearest unshadowed span.Boundary across all
	// non-parked levels. Only computed and used in invariant builds, for
	// assertion checking in handleBoundaryForward / handleBoundaryBackward.
	nextBoundary invariants.Value[[]byte]
}

// Build returns an iterator over (levelIdx, parked) pairs for levels 0 through
// len(s.levels)-1. For each yielded level, the caller must position the level's
// iterator (which updates the stashed span) before continuing the loop, unless
// parked is true (in which case the iterator should not be positioned). The
// caller should also set the level's parked field accordingly.
//
// After the caller continues, Build reads the level's span, detects visible
// RANGEDELs, sets the level's [minSeqNum, maxSeqNum) visibility range, and
// computes whether the next level should be parked.
//
// Under invariants, asserts that a lower-level RANGEDEL has a strictly
// smaller seqnum than any higher-level RANGEDEL.
func (s *slabState) Build(dir int8) iter.Seq2[int, bool] {
	return func(yield func(int, bool) bool) {
		// INVARIANT: highestRangeDelSeqNum > 0 <=> parked
		highestRangeDelSeqNum := base.SeqNum(0)

		parked := false
		for levelIdx := range s.levels {
			if !yield(levelIdx, parked) {
				return
			}
			// After yield, the caller has positioned the level's iterator
			// (updating the span) or left it unpositioned if parked.
			if parked {
				continue
			}

			sl := &s.levels[levelIdx]
			// Set maxSeqNum: batch level uses batchSnapshot, others use snapshot.
			if s.batchSnapshot != 0 && levelIdx == s.batchLevelIdx {
				sl.maxSeqNum = s.batchSnapshot
			} else {
				sl.maxSeqNum = s.snapshot
			}

			if highestRangeDelSeqNum == 0 {
				// No RANGEDEL found yet; check this level's span.
				highestRangeDelSeqNum = visibleRangeDelSeqNum(sl.span, sl.maxSeqNum)
			} else if invariants.Enabled {
				// A higher level already has a visible RANGEDEL. By the LSM
				// invariant, any RANGEDEL in this level must have a smaller
				// seqnum.
				rdSeqNum := visibleRangeDelSeqNum(sl.span, sl.maxSeqNum)
				if rdSeqNum >= highestRangeDelSeqNum {
					panic(errors.AssertionFailedf("lower level has visible RANGEDEL with seqnum >= higher level's"))
				}
			}
			sl.minSeqNum = highestRangeDelSeqNum

			// All lower levels are parked if we have any visible RANGEDEL.
			parked = highestRangeDelSeqNum > 0
		}
		s.calcNextBoundary(dir)
	}
}

// calcNextBoundary computes the nearest unshadowed span boundary across all
// non-parked levels and stores it in s.nextBoundary. Only does work in
// invariant builds; in production builds this is a no-op.
//
// dir indicates the comparison direction: +1 for forward iteration (minimum
// boundary), -1 for backward iteration (maximum boundary).
func (s *slabState) calcNextBoundary(dir int8) {
	if !invariants.Enabled {
		return
	}
	var nb []byte
	for i := range s.levels {
		if s.levels[i].parked {
			continue
		}
		if boundary := s.levels[i].span.Boundary; boundary != nil {
			if nb == nil || s.cmp(boundary, nb) == -int(dir) {
				nb = boundary
			}
		}
	}
	s.nextBoundary.Set(slices.Clone(nb))
}

func (s *slabState) assertNextBoundary(key []byte) {
	if invariants.Enabled && s.cmp(s.nextBoundary.Get(), key) != 0 {
		panic(errors.AssertionFailedf("boundary key %s does not match slab boundary %v", key, s.nextBoundary))
	}
}

// visibleRangeDelSeqNum returns the sequence number of the highest-seqnum
// RANGEDEL key in span that is visible given the provided maxSeqNum threshold.
// A RANGEDEL key is visible iff its sequence number is strictly less than
// maxSeqNum. Because span.Keys are ordered by trailer descending, the first
// matching entry is the one with the largest SeqNum. Returns 0 if no visible
// RANGEDEL exists.
func visibleRangeDelSeqNum(span *iterv2.Span, maxSeqNum base.SeqNum) base.SeqNum {
	for i := range span.Keys {
		if span.Keys[i].Kind() == base.InternalKeyKindRangeDelete &&
			span.Keys[i].SeqNum() < maxSeqNum {
			return span.Keys[i].SeqNum()
		}
	}
	return 0
}
