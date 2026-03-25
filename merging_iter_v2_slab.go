// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"iter"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/iterv2"
)

// See the mergingIterV2 doc comment for an overview of the slab concept.

// slabState manages the per-level span state, the per-level visibility ranges
// ([minSeqNum, maxSeqNum) on each level), and the slab boundary for the
// merging iterator. It is direction-aware: BuildForward uses span.Boundary
// (which is the exclusive End during forward iteration); BuildBackward uses
// span.Boundary (which is the inclusive Start during backward iteration).
type slabState struct {
	cmp base.Compare
	// snapshot is the SeqNum at which we are reading; only point/span keys with
	// lower seq nums are visible.
	snapshot base.SeqNum
	// batchSnapshot is non-zero if and only if the first level is a batch.
	batchSnapshot base.SeqNum

	// levels aliases m.levels, sharing the same backing array. Indexed by
	// level index (0 = highest / most recent level).
	levels []mergingIterV2Level

	// nextBoundary is the user key where the current slab ends (forward
	// iteration) or starts (backward iteration). It is the nearest
	// unshadowed span.Boundary across all levels. nil means no boundary
	// was found (the slab extends to the iterator bound).
	nextBoundary    []byte
	nextBoundaryBuf []byte
}

// BuildForward returns an iterator over (levelIdx, parked) pairs for levels
// 0 through len(s.levels)-1. For each yielded level, the caller must position
// the level's iterator (which updates the stashed span) before continuing the
// loop, unless parked is true (in which case the iterator should not be
// positioned). The caller should also set the level's parked field accordingly.
//
// After the caller continues, BuildForward reads the level's span, detects
// visible RANGEDELs, sets the level's [minSeqNum, maxSeqNum) visibility range,
// updates the nextBoundary, and computes whether the next level should be
// parked.
//
// Under invariants, asserts that a lower-level RANGEDEL has a strictly
// smaller seqnum than any higher-level RANGEDEL.
//
// TODO(radu): avoid building and maintaining the slab in the common case where
// no levels have any spans.
func (s *slabState) BuildForward() iter.Seq2[int, bool] {
	return func(yield func(int, bool) bool) {
		s.nextBoundary = nil
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
			if levelIdx == 0 && s.batchSnapshot != 0 {
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
					panic("lower level has visible RANGEDEL with seqnum >= higher level's")
				}
			}
			sl.minSeqNum = highestRangeDelSeqNum

			// Incremental nextBoundary: minimum span.Boundary across all
			// active (non-parked) levels. During forward iteration,
			// span.Boundary is the exclusive End of the span.
			if boundary := sl.span.Boundary; boundary != nil {
				if s.nextBoundary == nil || s.cmp(boundary, s.nextBoundary) < 0 {
					s.nextBoundaryBuf = append(s.nextBoundaryBuf[:0], boundary...)
					s.nextBoundary = s.nextBoundaryBuf
				}
			}

			// All lower levels are parked if we have any visible RANGEDEL.
			parked = highestRangeDelSeqNum > 0
		}
	}
}

// BuildBackward is the backward-iteration counterpart of BuildForward. It
// returns an iterator over (levelIdx, parked) pairs. For each yielded level,
// the caller must position the level's iterator (which updates the stashed
// span) before continuing the loop, unless parked is true. During backward
// iteration, span.Boundary is the inclusive Start of the span.
//
// After the caller continues, BuildBackward reads the level's span, detects
// visible RANGEDELs, sets the level's [minSeqNum, maxSeqNum) visibility range,
// updates the nextBoundary, and computes whether the next level should be
// parked.
//
// The nextBoundary is the maximum span.Boundary across all non-parked levels
// (the nearest boundary in the backward direction).
func (s *slabState) BuildBackward() iter.Seq2[int, bool] {
	return func(yield func(int, bool) bool) {
		s.nextBoundary = nil
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
			if levelIdx == 0 && s.batchSnapshot != 0 {
				sl.maxSeqNum = s.batchSnapshot
			} else {
				sl.maxSeqNum = s.snapshot
			}

			if highestRangeDelSeqNum == 0 {
				highestRangeDelSeqNum = visibleRangeDelSeqNum(sl.span, sl.maxSeqNum)
			} else if invariants.Enabled {
				rdSeqNum := visibleRangeDelSeqNum(sl.span, sl.maxSeqNum)
				if rdSeqNum >= highestRangeDelSeqNum {
					panic("lower level has visible RANGEDEL with seqnum >= higher level's")
				}
			}
			sl.minSeqNum = highestRangeDelSeqNum

			// Incremental nextBoundary: maximum span.Boundary across all
			// active (non-parked) levels. During backward iteration,
			// span.Boundary is the inclusive Start of the span.
			if boundary := sl.span.Boundary; boundary != nil {
				if s.nextBoundary == nil || s.cmp(boundary, s.nextBoundary) > 0 {
					s.nextBoundaryBuf = append(s.nextBoundaryBuf[:0], boundary...)
					s.nextBoundary = s.nextBoundaryBuf
				}
			}

			// All lower levels are parked if we have any visible RANGEDEL.
			parked = highestRangeDelSeqNum > 0
		}
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
