// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"context"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/iterv2"
	"github.com/cockroachdb/pebble/internal/treesteps"
)

// mergingIterV2 provides a merged view of point keys from multiple iterv2.Iters
// from different levels of the LSM, hiding point keys that are shadowed by
// range dels.
//
// mergingIterV2 does not produce any spans; it is a simple
// base.InternalIterator and not a iterv2.Iter.
//
// The core is a heap of iterv2.Iter children (see mergingIterV2Heap). The heap
// operates as a min-heap during forward iteration (First, SeekGE, Next) or a
// max-heap during reverse iteration (Last, SeekLT, Prev). A call to Next or
// Prev takes the current top element on the heap, advances its iterator, and
// then fixes the heap property. When a child iterator is exhausted it is
// removed from the heap.
//
// Range keys are handled outside mergingIterV2 (at a higher level in the
// iterator stack), since range keys cannot be shadowed by range deletions and
// integrating them into the slab-based design would force scanning through
// shadowed regions.
//
// # Range Deletions and Slabs
//
// mergingIterV2 uses a "slab-based" approach that cleanly separates range
// deletion logic from point key merging.
//
// Each level iterator (iterv2.Iter) partitions the key space into contiguous
// spans, where each span may contain zero or more RANGEDEL keys. Spans are
// exposed as "half spans" (iterv2.Span): only the next boundary in the
// iteration direction is available (the exclusive End during forward
// iteration, the inclusive Start during backward iteration). When a level
// iterator crosses a span boundary it emits a synthetic boundary key.
//
// A "slab" is a contiguous region of the user key space within which no
// unshadowed span boundary exists across any level. "Unshadowed" means the
// boundary is not contained within a range deletion from a higher
// (lower-numbered) level. Because no unshadowed boundary exists inside a
// slab, the set of active range deletions is constant throughout it.
//
// Within a slab, each level has a [minSeqNum, maxSeqNum) visibility range
// computed by the slab code. A point key is visible iff
// minSeqNum <= seqNum < maxSeqNum. maxSeqNum encodes the snapshot
// visibility (snapshot for committed levels, batchSnapshot for the batch
// level). minSeqNum encodes the range deletion threshold: it is set to the
// highest visible RANGEDEL sequence number from a higher-numbered level.
// By the LSM invariant (each level has seqnums strictly greater than all
// seqnums in any lower level), a visible RANGEDEL in level L guarantees
// that ALL keys in levels L+1, L+2, ... have smaller seqnums. Therefore
// when a visible RANGEDEL is found, all lower levels are fully parked
// (their iterators are not advanced and they are excluded from the heap).
// The findNextEntry / findPrevEntry loop checks the per-level
// [minSeqNum, maxSeqNum) range once per key — no per-key CoversAt call
// or separate Visible call is needed.
//
// The slab ends at the nearest unshadowed span boundary across all levels.
// The slab state — per-level [minSeqNum, maxSeqNum) ranges, the next
// boundary, and which levels are parked — is computed by
// slabState.BuildForward / slabState.BuildBackward (see
// merging_iter_v2_slab.go).
//
// # Worked Example
//
// Consider the following setup:
//
//	Level 0: points {a, d};  rangeDel [b, e)
//	Level 1: points {b, c, f};  no rangeDel
//
// Level 0's iterv2 spans (forward): [?,b) empty, [?,e) RANGEDEL, [?,∞) empty
// Level 1's iterv2 spans (forward): single [?,∞) empty span
//
// SeekGE("a"):
//
//   - L0 positioned at a#SET, span [?,b) empty
//
//   - L1 positioned at b#SET, span [?,∞) empty
//
//   - Slab: [a, b) — L0's unshadowed boundary at "b" is nearest.
//     highestRangeDelSeqNum = 0.
//
//   - Heap: {a#SET(L0), b#SET(L1)}
//
//     Call       Heap root      Action
//     SeekGE(a)  a#SET(L0)      return point
//     Next       b#∞,BDRY       boundary at "b" (slab end); consumed internally
//     (cont.)   advance slab   L0 span now RANGEDEL. L1 parked.
//     d#SET(L0)      return point
//     Next       e#∞,BDRY       boundary at "e" (slab end); consumed internally
//     (cont.)   advance slab   L0 span now empty. L1 unparked, SeekGE("e")→f.
//     f#SET(L1)      return point
//
// # Boundary Keys in the Heap
//
// Each level iter emits boundary keys (InternalKeyKindSpanBoundary,
// SeqNumMax) as part of its normal iteration sequence. These appear in the
// heap alongside point keys. Because boundary keys use SeqNumMax, they sort
// before any point key at the same user key in the min-heap, which is the
// correct behavior — we want to handle the slab boundary before returning
// point keys in the new slab. Boundary keys are consumed internally by
// findNextEntry / findPrevEntry and are never returned to callers.
//
// When the heap root is a boundary key from a level, it always matches the
// slab boundary (nextBoundary), triggering a slab transition
// (advanceSlabForward / advanceSlabBackward). This is guaranteed because
// nextBoundary is the minimum (forward) / maximum (backward) span boundary
// across all non-parked levels, parked levels are excluded from the heap,
// and any level with a boundary beyond nextBoundary cannot surface before
// the slab transition.
//
// # Slab Transitions
//
// When a slab boundary is reached (handleBoundaryForward /
// handleBoundaryBackward), the merging iterator performs a slab transition:
//
//  1. Drain all co-located boundary keys at the same user key from the heap,
//     advancing each level past its boundary. Multiple levels may have span
//     boundaries at the same user key.
//  2. Recompute the slab state via BuildForward / BuildBackward. This reads
//     each level's updated span (the stashed span pointer is updated in
//     place by the level iterator), finds visible RANGEDELs, sets per-level
//     visibility ranges, and computes the next slab boundary.
//  3. Unpark levels that were previously parked but are no longer shadowed
//     (seek them to the slab boundary via SeekGE / SeekLT).
//  4. Park levels that are now fully shadowed (clear their iterKV and
//     exclude them from the heap).
//  5. Rebuild the heap.
//
// # Parked Levels
//
// Levels fully shadowed by a higher-level RANGEDEL are "parked": their
// iterators are not advanced and they produce no keys. This avoids doing
// work on keys that will be discarded. When a slab transition unshadows a
// level, it is repositioned via SeekGE (forward) or SeekLT (backward) to
// the slab boundary. During direction switches (switchToMinHeap /
// switchToMaxHeap), parked levels are similarly repositioned.
//
// # Error Handling
//
// Any iterator operation may fail. The InternalIterator contract dictates
// that an iterator must return a nil internal key when an error occurs, and
// a subsequent call to Error() should return the error value. The merging
// iterator sets m.err when a child iterator error is detected (via
// levelHasError) and clears the heap to stop iteration.
type mergingIterV2 struct {
	logger Logger
	split  Split
	dir    int8 // +1 forward, -1 backward, 0 unpositioned
	levels []mergingIterV2Level
	heap   mergingIterV2Heap
	slab   slabState
	err    error
	prefix []byte
	lower  []byte
	upper  []byte
	stats  *InternalIteratorStats

	// slabBoundaryBuf holds a copy of the slab boundary key during slab
	// transitions. This is necessary because BuildForward/BuildBackward
	// reuses slab.nextBoundaryBuf, which would overwrite the boundary key
	// that advanceSlabForward/advanceSlabBackward still needs for unparking
	// levels via SeekGE/SeekLT.
	slabBoundaryBuf []byte
}

var _ base.TopLevelIterator = (*mergingIterV2)(nil)

// mergingIterV2Level represents a single level in the merging iterator.
type mergingIterV2Level struct {
	index  int
	iter   iterv2.Iter      // levelIterV2 or batch/memtable V2 iter
	iterKV *base.InternalKV // cached current key from iter (nil if exhausted or parked)
	// span is the pointer returned by the level iterator's Span() method.
	//
	// Per the iterv2.Iter contract the caller may stash this pointer; the
	// level iterator updates it in place whenever the span changes (i.e.
	// when the iterator crosses a span boundary). We call Span() once
	// during initialisation and reuse the pointer thereafter.
	span *iterv2.Span
	// minSeqNum is the minimum visible sequence number (inclusive) for keys
	// from this level in the current slab. Set by the slab code to the
	// highest visible range deletion sequence number; keys with
	// seqNum < minSeqNum are deleted. 0 means no range deletion filtering.
	minSeqNum base.SeqNum
	// maxSeqNum is the maximum visible sequence number (exclusive) for keys
	// from this level. For the batch level (level 0 when batchSnapshot != 0),
	// this is batchSnapshot; for all other levels, this is snapshot.
	// A key is visible iff minSeqNum <= seqNum < maxSeqNum.
	maxSeqNum base.SeqNum
	// parked is true when the level is fully shadowed by a higher-level
	// RANGEDEL. A parked level's iterator is not positioned and produces
	// no keys until unparked.
	parked bool
}

func (level *mergingIterV2Level) Compare(cmp base.Compare, other *mergingIterV2Level) int {
	return base.InternalCompare(cmp, level.iterKV.K, other.iterKV.K)
}

// newMergingIterV2 creates a new merging iterator V2 over the given level
// iterators. The iterators must implement iterv2.Iter.
func newMergingIterV2(
	cmp base.Compare, split Split, snapshot base.SeqNum, iters ...iterv2.Iter,
) *mergingIterV2 {
	m := &mergingIterV2{}
	m.levels = make([]mergingIterV2Level, len(iters))
	for i, iter := range iters {
		m.levels[i] = mergingIterV2Level{
			index: i,
			iter:  iter,
		}
	}
	m.split = split
	m.heap.cmp = cmp
	m.heap.items = make([]mergingIterV2HeapItem, 0, len(iters))
	m.slab.cmp = cmp
	m.slab.snapshot = snapshot
	m.slab.levels = m.levels
	// Stash the span pointers from each level iter.
	for i := range m.levels {
		m.levels[i].span = m.levels[i].iter.Span()
	}
	return m
}

func (m *mergingIterV2) initHeap(dir int) {
	m.dir = int8(dir)
	m.heap.Reset()
	for i := range m.levels {
		if m.levels[i].iterKV != nil {
			m.heap.Append(&m.levels[i])
		}
	}
	// For forward iteration we need a min-heap; for reverse iteration we need a
	// max heap.
	m.heap.Init(-dir)
}

// nextEntry advances the given level's iterator and fixes the heap.
func (m *mergingIterV2) nextEntry(level *mergingIterV2Level) {
	level.iterKV = level.iter.Next()
	if level.iterKV == nil {
		if m.levelHasError(level) {
			return
		}
		m.heap.PopTop()
	} else {
		m.heap.FixTop()
	}
}

// prevEntry backs up the given level's iterator and fixes the heap.
func (m *mergingIterV2) prevEntry(level *mergingIterV2Level) {
	level.iterKV = level.iter.Prev()
	if level.iterKV == nil {
		if m.levelHasError(level) {
			return
		}
		m.heap.PopTop()
	} else {
		m.heap.FixTop()
	}
}

// First implements base.InternalIterator.
func (m *mergingIterV2) First() (kv *base.InternalKV) {
	if treesteps.Enabled && treesteps.IsRecording(m) {
		op := treesteps.StartOpf(m, "First()")
		defer func() {
			op.Finishf("= %s", kv.String())
		}()
	}
	m.err = nil
	m.prefix = nil
	for levelIdx, parked := range m.slab.BuildForward() {
		level := &m.levels[levelIdx]
		level.parked = parked
		if parked {
			level.iterKV = nil
		} else {
			level.iterKV = level.iter.First()
			if level.iterKV == nil && m.levelHasError(level) {
				return nil
			}
		}
	}
	m.initHeap(+1)
	return m.findNextEntry()
}

// seekGE positions all levels at or after key. Levels that are fully
// shadowed by a higher-level RANGEDEL are parked (not seeked).
func (m *mergingIterV2) seekGE(key []byte, flags base.SeekGEFlags) {
	if invariants.Enabled && flags.RelativeSeek() {
		panic(errors.AssertionFailedf("invalid use of relative seek"))
	}
	// TODO(radu): support TrySeekUsingNext.
	flags = flags.DisableTrySeekUsingNext()
	for levelIdx, parked := range m.slab.BuildForward() {
		level := &m.levels[levelIdx]
		level.parked = parked
		if parked {
			level.iterKV = nil
		} else {
			if m.prefix != nil {
				level.iterKV = level.iter.SeekPrefixGE(m.prefix, key, flags)
			} else {
				level.iterKV = level.iter.SeekGE(key, flags)
			}
			if level.iterKV == nil && m.levelHasError(level) {
				return
			}
		}
	}
	m.initHeap(+1)
}

// SeekGE implements base.InternalIterator.
func (m *mergingIterV2) SeekGE(key []byte, flags base.SeekGEFlags) (kv *base.InternalKV) {
	if treesteps.Enabled && treesteps.IsRecording(m) {
		op := treesteps.StartOpf(m, "SeekGE(%q, %d)", key, flags)
		defer func() {
			op.Finishf("= %s", kv.String())
		}()
	}
	m.err = nil
	m.prefix = nil
	m.seekGE(key, flags)
	return m.findNextEntry()
}

// Next implements base.InternalIterator.
func (m *mergingIterV2) Next() (kv *base.InternalKV) {
	if treesteps.Enabled && treesteps.IsRecording(m) {
		op := treesteps.StartOpf(m, "Next()")
		defer func() {
			op.Finishf("= %s", kv.String())
		}()
	}
	if m.err != nil {
		return nil
	}
	if m.dir != +1 {
		return m.switchToMinHeap()
	}
	if m.heap.Len() == 0 {
		return nil
	}
	m.nextEntry(m.heap.Top())
	return m.findNextEntry()
}

// findNextEntry returns the next visible point key, handling boundary keys
// and range deletion filtering internally.
func (m *mergingIterV2) findNextEntry() *base.InternalKV {
	for m.heap.Len() > 0 && m.err == nil {
		level := m.heap.Top()

		// Handle boundary keys from level iters.
		if level.iterKV.K.Kind() == base.InternalKeyKindSpanBoundary {
			m.handleBoundaryForward(level)
			continue
		}

		// Per-level visibility check: a key is visible iff its sequence
		// number is in [minSeqNum, maxSeqNum). minSeqNum encodes range
		// deletion filtering; maxSeqNum encodes snapshot visibility.
		if seqNum := level.iterKV.SeqNum(); seqNum < level.minSeqNum || seqNum >= level.maxSeqNum {
			m.nextEntry(level)
			continue
		}

		if invariants.Enabled && m.prefix != nil {
			if !bytes.Equal(m.prefix, m.split.Prefix(level.iterKV.K.UserKey)) {
				panic("mergingIterV2: prefix violation: returning key without matching prefix")
			}
		}
		m.addLevelStats(level)
		return level.iterKV
	}
	return nil
}

func (m *mergingIterV2) addLevelStats(l *mergingIterV2Level) {
	if m.stats != nil {
		m.stats.PointCount++
		m.stats.KeyBytes += uint64(len(l.iterKV.K.UserKey))
		m.stats.ValueBytes += uint64(l.iterKV.V.InternalLen())
	}
}

// handleBoundaryForward processes a boundary key at the top of the heap.
// The boundary always matches the slab boundary, triggering a slab transition.
func (m *mergingIterV2) handleBoundaryForward(level *mergingIterV2Level) {
	if invariants.Enabled {
		if m.slab.nextBoundary == nil || m.slab.cmp(level.iterKV.K.UserKey, m.slab.nextBoundary) != 0 {
			panic(errors.AssertionFailedf(
				"mergingIterV2: boundary key %s does not match slab boundary %v",
				level.iterKV.K, m.slab.nextBoundary,
			))
		}
	}
	m.advanceSlabForward()
}

// advanceSlabForward handles a slab transition during forward iteration.
func (m *mergingIterV2) advanceSlabForward() {
	// Copy the slab boundary into a stable buffer. BuildForward below will
	// reuse slab.nextBoundaryBuf, invalidating slab.nextBoundary.
	m.slabBoundaryBuf = append(m.slabBoundaryBuf[:0], m.slab.nextBoundary...)
	slabBoundary := m.slabBoundaryBuf

	// Drain all level boundary keys at slabBoundary from the heap.
	for m.heap.Len() > 0 {
		level := m.heap.Top()
		if level.iterKV.K.Kind() != base.InternalKeyKindSpanBoundary {
			break
		}
		if m.slab.cmp(level.iterKV.K.UserKey, slabBoundary) != 0 {
			break
		}
		// Advance this level past its boundary. The level's stashed span
		// pointer (level.span) is updated in-place by the iterator.
		// TODO(radu): if the level becomes parked, this Next() call is not
		// necessary and it can cause opening a new file in a level iterator.
		level.iterKV = level.iter.Next()
		if level.iterKV == nil {
			if m.levelHasError(level) {
				return
			}
			m.heap.PopTop()
		} else {
			m.heap.FixTop()
		}
	}

	// If in prefix mode and the slab boundary is past the prefix, exhaust
	// all levels. All matching-prefix keys have been returned. Don't seek
	// any level past the prefix (preserves TrySeekUsingNext correctness
	// for future seeks to different prefixes).
	if m.prefix != nil && !bytes.Equal(m.prefix, m.split.Prefix(slabBoundary)) {
		for i := range m.levels {
			m.levels[i].iterKV = nil
		}
		m.heap.Reset()
		return
	}

	// Recompute the slab via BuildForward. Levels that were previously
	// parked are unparked and seeked to the slab boundary; newly parked
	// levels have their iterKV cleared.
	for levelIdx, parked := range m.slab.BuildForward() {
		level := &m.levels[levelIdx]
		wasParked := level.parked
		level.parked = parked
		if parked {
			level.iterKV = nil
		} else if wasParked {
			// Unpark: seek to slab boundary.
			if m.prefix != nil {
				// TODO(radu): use TrySeekUsingNext (we'll have to keep track of whether
				// the iteration direction has changed).
				level.iterKV = level.iter.SeekPrefixGE(
					m.prefix, slabBoundary, base.SeekGEFlagsNone,
				)
			} else {
				level.iterKV = level.iter.SeekGE(
					slabBoundary, base.SeekGEFlagsNone,
				)
			}
			if level.iterKV == nil && m.levelHasError(level) {
				return
			}
		}
		// else: level was active and stays active; span already current.
	}

	// Rebuild the heap.
	m.initHeap(+1)
}

// SeekPrefixGE implements base.InternalIterator.
func (m *mergingIterV2) SeekPrefixGE(prefix, key []byte, flags base.SeekGEFlags) *base.InternalKV {
	return m.SeekPrefixGEStrict(prefix, key, flags)
}

// SeekPrefixGEStrict implements base.TopLevelIterator. The V2 merging iterator
// always filters keys by prefix, so this is identical to SeekPrefixGE.
func (m *mergingIterV2) SeekPrefixGEStrict(
	prefix, key []byte, flags base.SeekGEFlags,
) (kv *base.InternalKV) {
	if treesteps.Enabled && treesteps.IsRecording(m) {
		op := treesteps.StartOpf(m, "SeekPrefixGE(%q, %d)", key, flags)
		defer func() {
			op.Finishf("= %s", kv.String())
		}()
	}
	m.err = nil
	m.prefix = prefix
	m.seekGE(key, flags)
	return m.findNextEntry()
}

// SeekLT implements base.InternalIterator.
func (m *mergingIterV2) SeekLT(key []byte, flags base.SeekLTFlags) (kv *base.InternalKV) {
	if treesteps.Enabled && treesteps.IsRecording(m) {
		op := treesteps.StartOpf(m, "SeekLT(%q, %d)", key, flags)
		defer func() {
			op.Finishf("= %s", kv.String())
		}()
	}
	m.err = nil
	m.prefix = nil
	m.seekLT(key, flags)
	return m.findPrevEntry()
}

// seekLT positions all levels before key. Levels that are fully shadowed
// by a higher-level RANGEDEL are parked (not seeked).
func (m *mergingIterV2) seekLT(key []byte, flags base.SeekLTFlags) {
	if invariants.Enabled && flags.RelativeSeek() {
		panic(errors.AssertionFailedf("invalid use of relative seek"))
	}
	for levelIdx, parked := range m.slab.BuildBackward() {
		level := &m.levels[levelIdx]
		level.parked = parked
		if parked {
			level.iterKV = nil
		} else {
			level.iterKV = level.iter.SeekLT(key, flags)
			if level.iterKV == nil && m.levelHasError(level) {
				return
			}
		}
	}
	m.initHeap(-1)
}

// Last implements base.InternalIterator.
func (m *mergingIterV2) Last() (kv *base.InternalKV) {
	if treesteps.Enabled && treesteps.IsRecording(m) {
		op := treesteps.StartOpf(m, "Last()")
		defer func() {
			op.Finishf("= %s", kv.String())
		}()
	}
	m.err = nil
	m.prefix = nil
	for levelIdx, parked := range m.slab.BuildBackward() {
		level := &m.levels[levelIdx]
		level.parked = parked
		if parked {
			level.iterKV = nil
		} else {
			level.iterKV = level.iter.Last()
			if level.iterKV == nil && m.levelHasError(level) {
				return nil
			}
		}
	}
	m.initHeap(-1)
	return m.findPrevEntry()
}

// NextPrefix implements base.InternalIterator.
func (m *mergingIterV2) NextPrefix(succKey []byte) *base.InternalKV {
	// TODO(radu): implement this using NextPrefix.
	return m.SeekGE(succKey, 0)
}

// Prev implements base.InternalIterator.
func (m *mergingIterV2) Prev() (kv *base.InternalKV) {
	if treesteps.Enabled && treesteps.IsRecording(m) {
		op := treesteps.StartOpf(m, "Prev()")
		defer func() {
			op.Finishf("= %s", kv.String())
		}()
	}
	if m.err != nil {
		return nil
	}
	if m.dir != -1 {
		return m.switchToMaxHeap()
	}
	if m.heap.Len() == 0 {
		return nil
	}
	m.prevEntry(m.heap.Top())
	return m.findPrevEntry()
}

// findPrevEntry returns the previous visible point key, handling boundary keys
// and range deletion filtering internally.
func (m *mergingIterV2) findPrevEntry() *base.InternalKV {
	for m.heap.Len() > 0 && m.err == nil {
		level := m.heap.Top()

		// Handle boundary keys from level iters.
		if level.iterKV.K.Kind() == base.InternalKeyKindSpanBoundary {
			m.handleBoundaryBackward(level)
			continue
		}

		// Per-level visibility check: a key is visible iff its sequence
		// number is in [minSeqNum, maxSeqNum). minSeqNum encodes range
		// deletion filtering; maxSeqNum encodes snapshot visibility.
		if seqNum := level.iterKV.SeqNum(); seqNum < level.minSeqNum || seqNum >= level.maxSeqNum {
			m.prevEntry(level)
			continue
		}

		m.addLevelStats(level)
		return level.iterKV
	}
	return nil
}

// handleBoundaryBackward processes a boundary key at the top of the heap
// during backward iteration. The boundary always matches the slab boundary,
// triggering a slab transition.
func (m *mergingIterV2) handleBoundaryBackward(level *mergingIterV2Level) {
	if invariants.Enabled {
		if m.slab.nextBoundary == nil || m.slab.cmp(level.iterKV.K.UserKey, m.slab.nextBoundary) != 0 {
			panic(errors.AssertionFailedf(
				"mergingIterV2: boundary key %s does not match slab boundary %v",
				level.iterKV.K, m.slab.nextBoundary,
			))
		}
	}
	m.advanceSlabBackward()
}

// advanceSlabBackward handles a slab transition during backward iteration.
func (m *mergingIterV2) advanceSlabBackward() {
	// Copy the slab boundary into a stable buffer. BuildBackward below will
	// reuse slab.nextBoundaryBuf, invalidating slab.nextBoundary.
	m.slabBoundaryBuf = append(m.slabBoundaryBuf[:0], m.slab.nextBoundary...)
	slabBoundary := m.slabBoundaryBuf

	// Drain all level boundary keys at slabBoundary from the heap.
	for m.heap.Len() > 0 {
		level := m.heap.Top()
		if level.iterKV.K.Kind() != base.InternalKeyKindSpanBoundary {
			break
		}
		if m.slab.cmp(level.iterKV.K.UserKey, slabBoundary) != 0 {
			break
		}
		// Advance this level past its boundary (backward). The level's
		// stashed span pointer (level.span) is updated in-place by the
		// iterator.
		level.iterKV = level.iter.Prev()
		if level.iterKV == nil {
			if m.levelHasError(level) {
				return
			}
			m.heap.PopTop()
		} else {
			m.heap.FixTop()
		}
	}

	// Recompute the slab via BuildBackward. Levels that were previously
	// parked are unparked and seeked to the slab boundary; newly parked
	// levels have their iterKV cleared.
	for level, parked := range m.slab.BuildBackward() {
		wasParked := m.levels[level].parked
		m.levels[level].parked = parked
		if parked {
			m.levels[level].iterKV = nil
		} else if wasParked {
			// Unpark: seek to before slab boundary.
			m.levels[level].iterKV = m.levels[level].iter.SeekLT(
				slabBoundary, base.SeekLTFlagsNone,
			)
			if m.levels[level].iterKV == nil && m.levelHasError(&m.levels[level]) {
				return
			}
		}
		// else: level was active and stays active; span already current.
	}

	// Rebuild the heap.
	m.initHeap(-1)
}

// switchToMinHeap switches from backward to forward iteration. All levels are
// repositioned so that they are at a key strictly after the current key (the
// heap root before switching).
func (m *mergingIterV2) switchToMinHeap() *base.InternalKV {
	if m.heap.Len() == 0 {
		if m.lower != nil {
			return m.SeekGE(m.lower, base.SeekGEFlagsNone)
		}
		return m.First()
	}

	cur := m.heap.Top()
	key := cur.iterKV.K

	for i := range m.levels {
		l := &m.levels[i]
		if l == cur {
			continue
		}
		if l.parked {
			// Parked levels are not positioned. Seek forward, then advance
			// past key. Clear parked so we can position the iterator;
			// BuildForward below may re-park it.
			l.iterKV = l.iter.SeekGE(key.UserKey, base.SeekGEFlagsNone)
			l.parked = false
			for l.iterKV != nil {
				if base.InternalCompare(m.heap.cmp, key, l.iterKV.K) < 0 {
					break
				}
				l.iterKV = l.iter.Next()
			}
		} else {
			// Non-parked: advance from current position. Start with Next()
			// to handle both positioned and exhausted (iterKV == nil) levels.
			for l.iterKV = l.iter.Next(); l.iterKV != nil; l.iterKV = l.iter.Next() {
				if base.InternalCompare(m.heap.cmp, key, l.iterKV.K) < 0 {
					break
				}
			}
		}
		if l.iterKV == nil && m.levelHasError(l) {
			return nil
		}
	}

	// Advance the current level past key.
	cur.iterKV = cur.iter.Next()
	if cur.iterKV == nil && m.levelHasError(cur) {
		return nil
	}

	// Recompute slab for forward iteration. All levels are positioned;
	// BuildForward reads their spans.
	for level, parked := range m.slab.BuildForward() {
		m.levels[level].parked = parked
		if parked {
			m.levels[level].iterKV = nil
		}
	}

	m.initHeap(+1)
	return m.findNextEntry()
}

// switchToMaxHeap switches from forward to backward iteration. All levels are
// repositioned so that they are at a key strictly before the current key (the
// heap root before switching).
func (m *mergingIterV2) switchToMaxHeap() *base.InternalKV {
	if m.heap.Len() == 0 {
		if m.upper != nil {
			return m.SeekLT(m.upper, base.SeekLTFlagsNone)
		}
		return m.Last()
	}

	cur := m.heap.Top()
	key := cur.iterKV.K

	for i := range m.levels {
		l := &m.levels[i]
		if l == cur {
			continue
		}
		if l.parked {
			// Parked levels are not positioned. SeekLT positions before
			// key.UserKey, which is correct: by the LSM invariant, a parked
			// (lower) level has smaller seqnums, so no same-userKey entry
			// from this level would sort before key in internal order.
			// Clear parked so we can position the iterator; BuildBackward
			// below may re-park it.
			l.iterKV = l.iter.SeekLT(key.UserKey, base.SeekLTFlagsNone)
			l.parked = false
		} else {
			// Back up until strictly before key in internal order.
			for l.iterKV = l.iter.Prev(); l.iterKV != nil; l.iterKV = l.iter.Prev() {
				if base.InternalCompare(m.heap.cmp, key, l.iterKV.K) > 0 {
					// key > iter-key: iter is before key, stop.
					break
				}
			}
		}
		if l.iterKV == nil && m.levelHasError(l) {
			return nil
		}
	}

	// Back up the current level past key.
	cur.iterKV = cur.iter.Prev()
	if cur.iterKV == nil && m.levelHasError(cur) {
		return nil
	}

	// Recompute slab for backward iteration. All levels are positioned;
	// BuildBackward reads their spans.
	for level, parked := range m.slab.BuildBackward() {
		m.levels[level].parked = parked
		if parked {
			m.levels[level].iterKV = nil
		}
	}

	m.initHeap(-1)
	return m.findPrevEntry()
}

// Error implements base.InternalIterator.
func (m *mergingIterV2) Error() error {
	return m.err
}

// Close implements base.InternalIterator.
func (m *mergingIterV2) Close() error {
	m.heap.Reset()
	for i := range m.levels {
		m.levels[i].iterKV = nil
		if err := m.levels[i].iter.Close(); err != nil && m.err == nil {
			m.err = err
		}
	}
	m.levels = nil
	return m.err
}

// SetBounds implements base.InternalIterator.
func (m *mergingIterV2) SetBounds(lower, upper []byte) {
	m.prefix = nil
	m.dir = 0
	m.heap.Reset()
	m.lower = lower
	m.upper = upper
	for i := range m.levels {
		m.levels[i].iterKV = nil
		m.levels[i].parked = false
		m.levels[i].iter.SetBounds(lower, upper)
	}
}

// SetContext implements base.InternalIterator.
func (m *mergingIterV2) SetContext(ctx context.Context) {
	for i := range m.levels {
		m.levels[i].iter.SetContext(ctx)
	}
}

// String implements fmt.Stringer.
func (m *mergingIterV2) String() string {
	return "merging-v2"
}

// TreeStepsNode implements treesteps.Node.
func (m *mergingIterV2) TreeStepsNode() treesteps.NodeInfo {
	info := treesteps.NodeInfof(m, "mergingIterV2(%p)", m)
	for i := range m.levels {
		info.AddChildren(m.levels[i].iter)
	}
	return info
}

func (m *mergingIterV2) levelHasError(level *mergingIterV2Level) bool {
	if err := level.iter.Error(); err != nil {
		m.err = err
		m.heap.Reset()
		return true
	}
	return false
}

// ----------------------------------------------------------------------------
// mergingIterV2Heap
// ----------------------------------------------------------------------------

// mergingIterV2Heap is a heap of mergingIterV2Levels. Adapted from
// mergingIterHeap.
type mergingIterV2Heap struct {
	cmp Compare
	// lessCmp is -1 if this is a min heap or +1 if it is a max heap.
	lessCmp int
	items   []mergingIterV2HeapItem
}

type mergingIterV2HeapItem struct {
	level       *mergingIterV2Level
	winnerChild winnerChild
}

func (h *mergingIterV2Heap) Len() int {
	return len(h.items)
}

func (h *mergingIterV2Heap) Reset() {
	h.items = h.items[:0]
}

func (h *mergingIterV2Heap) less(i, j int) bool {
	return h.items[i].level.Compare(h.cmp, h.items[j].level) == h.lessCmp
}

func (h *mergingIterV2Heap) swap(i, j int) {
	h.items[i].level, h.items[j].level = h.items[j].level, h.items[i].level
}

// Append an element. After elements are appended, Init must be called.
func (h *mergingIterV2Heap) Append(level *mergingIterV2Level) {
	h.items = append(h.items, mergingIterV2HeapItem{level: level})
}

// Init creates the heap from the elements that have been appended.
// lessCmp is either -1 (min heap) or +1 (max heap).
func (h *mergingIterV2Heap) Init(lessCmp int) {
	h.lessCmp = lessCmp
	n := h.Len()
	for i := n/2 - 1; i >= 0; i-- {
		h.down(i, n)
	}
}

func (h *mergingIterV2Heap) Top() *mergingIterV2Level {
	return h.items[0].level
}

func (h *mergingIterV2Heap) FixTop() {
	h.down(0, h.Len())
}

func (h *mergingIterV2Heap) PopTop() {
	n := h.Len() - 1
	h.swap(0, n)
	h.down(0, n)
	h.items = h.items[:n]
}

func (h *mergingIterV2Heap) down(i, n int) {
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 {
			break
		}
		j := j1
		if j2 := j1 + 1; j2 < n {
			if h.items[i].winnerChild == winnerChildUnknown {
				if h.less(j2, j1) {
					h.items[i].winnerChild = winnerChildRight
				} else {
					h.items[i].winnerChild = winnerChildLeft
				}
			} else if invariants.Enabled {
				if (h.items[i].winnerChild == winnerChildLeft && h.less(j2, j1)) ||
					(h.items[i].winnerChild == winnerChildRight && h.less(j1, j2)) {
					panic("incorrect winnerChild")
				}
			}
			if h.items[i].winnerChild == winnerChildRight {
				j = j2
			}
		}
		if !h.less(j, i) {
			break
		}
		h.swap(i, j)
		h.items[i].winnerChild = winnerChildUnknown
		i = j
	}
}
