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
// For example, consider three levels with span boundaries at "b":
//
//	L0: span [b, f)
//	L1: span [b, e)
//	L2: span [b, d)
//
// If none of the spans are range deletions, all three boundaries (d, e, f)
// are unshadowed and the slab starting at "b" ends at "d" (the nearest):
//
//	       b     d     e     f
//	L0:    |−−−−−−−−−−-−−−−−−|
//	L1:    |−−−−−−−−−−-|
//	L2:    |−−−−−|
//	slab:  [b,  d)
//
// If L1 is a range deletion [b, e), it shadows L2's boundary at "d" (L1 is
// a higher level than L2). The unshadowed boundaries are now "e" and "f",
// so the slab is [b, e):
//
//	       b     d     e     f
//	L0:    |−−−−−−−−−−−-−−−−−|
//	L1:    |==RANGEDEL=|          ← shadows L2's boundary at "d"
//	L2:    |−−−−−|
//	slab:  [b,        e)
//
// If instead L0 is a range deletion [b, f), it shadows both L1's boundary
// at "e" and L2's boundary at "d". The only unshadowed boundary is "f",
// so the slab is [b, f):
//
//	       b     d     e     f
//	L0:    |=====RANGEDEL====|    ← shadows L1 and L2 boundaries
//	L1:    |−−−−−−−−−−|
//	L2:    |−−−−−|
//	slab:  [b,              f)
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
// slabState.Build (see merging_iter_v2_slab.go).
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
//  1. Mark all co-located boundary keys at the same user key, removing them
//     from the heap. The Next()/Prev() call to cross each boundary is
//     deferred to avoid unnecessary file opens in level iterators.
//  2. Recompute the slab state via Build. For each level, advance past the
//     boundary (via Next()/Prev()) only if the level is not parked. Parked
//     levels skip the call entirely — this is the key optimization. Unpark
//     levels that were previously parked but are no longer shadowed by seeking
//     them to the slab boundary.
//  3. Rebuild the heap.
//
// # Parked Levels
//
// Levels fully shadowed by a higher-level RANGEDEL are "parked": their
// iterators are not advanced and they produce no keys. This avoids doing
// work on keys that will be discarded. When a slab transition unshadows a
// level, it is repositioned via SeekGE (forward) or SeekLT (backward) to
// the slab boundary. During direction switches (switchToMinHeapAndNext /
// switchToMaxHeapAndPrev), parked levels are similarly repositioned.
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

	keyBuf []byte
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
	// atBoundary is true when this level has a boundary key at the current
	// slab boundary. The Next()/Prev() to cross it is deferred until we
	// know whether the level will be parked.
	atBoundary bool
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
			// We stash the pointer so we don't have to keep calling Span().
			span: iter.Span(),
		}
	}
	m.split = split
	m.heap.cmp = cmp
	m.heap.items = make([]mergingIterV2HeapItem, 0, len(iters))
	m.slab.cmp = cmp
	m.slab.snapshot = snapshot
	m.slab.levels = m.levels
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

// nextEntry advances the given level's iterator and fixes the heap; level is
// the current top of the heap.
func (m *mergingIterV2) nextEntry(level *mergingIterV2Level) {
	if invariants.Enabled && level != m.heap.Top() {
		panic(errors.AssertionFailedf("level not the top"))
	}
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

// prevEntry backs up the given level's iterator and fixes the heap; level is
// the current top of the heap.
func (m *mergingIterV2) prevEntry(level *mergingIterV2Level) {
	if invariants.Enabled && level != m.heap.Top() {
		panic(errors.AssertionFailedf("level not the top"))
	}
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
	for levelIdx, parked := range m.slab.Build(+1) {
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
	for levelIdx, parked := range m.slab.Build(+1) {
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
		return m.switchToMinHeapAndNext()
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
			m.slab.assertNextBoundary(level.iterKV.K.UserKey)
			m.advanceSlabForward()
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
				panic(errors.AssertionFailedf("mergingIterV2: prefix violation: returning key without matching prefix"))
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

// advanceSlabForward handles a slab transition during forward iteration.
func (m *mergingIterV2) advanceSlabForward() {
	top := m.heap.Top()
	boundaryKey := top.iterKV.K.UserKey

	// If in prefix mode and the boundary key is past the prefix, exhaust
	// all levels. All matching-prefix keys have been returned. Don't seek
	// any level past the prefix (preserves TrySeekUsingNext correctness
	// for future seeks to different prefixes).
	if m.prefix != nil && !bytes.Equal(m.prefix, m.split.Prefix(boundaryKey)) {
		for i := range m.levels {
			m.levels[i].iterKV = nil
		}
		m.heap.Reset()
		return
	}

	// Optimization: when a single level has a boundary here, and the current span
	// on that level has no span keys, try to advance without rebuilding the slab.
	// TODO(radu): we can relax these conditions to cover more cases.
	if len(top.span.Keys) == 0 && !m.heap.MultipleLevelsAtSameBoundary() {
		top.iterKV = top.iter.Next()
		if top.iterKV == nil {
			if m.levelHasError(top) {
				return
			}
			m.heap.PopTop()
		} else {
			m.heap.FixTop()
		}
		if len(top.span.Keys) == 0 {
			// This was a spurious boundary: no keys before, no keys after. Nothing
			// else in the slab has changed.
			m.slab.calcNextBoundary(+1)
			return
		}
		// We have to rebuild the slab in case some levels become parked.
	} else {
		// Mark all levels at the slab boundary. The actual Next() call is deferred to
		// the Build loop below, where we can skip it for levels that become parked
		// (avoiding unnecessary file opens in level iterators).
		top.atBoundary = true
		m.heap.PopTop()
		for m.heap.Len() > 0 {
			level := m.heap.Top()
			if level.iterKV.K.Kind() != base.InternalKeyKindSpanBoundary {
				break
			}
			if m.slab.cmp(level.iterKV.K.UserKey, boundaryKey) != 0 {
				break
			}
			level.atBoundary = true
			m.heap.PopTop()
		}
	}

	// If some levels are parked, we will need the boundaryKey after potentially
	// moving the iterator that produced it; make a copy.
	if m.anyLevelParked() {
		m.keyBuf = append(m.keyBuf[:0], boundaryKey...)
		boundaryKey = m.keyBuf
	}

	// Recompute the slab. For levels at the boundary, call Next() to cross it
	// (updating the span in place) only if the level is not parked. Parked levels
	// skip the Next() entirely.
	for levelIdx, parked := range m.slab.Build(+1) {
		level := &m.levels[levelIdx]
		wasParked := level.parked
		level.parked = parked
		if parked {
			level.iterKV = nil
			level.atBoundary = false
		} else if level.atBoundary {
			// Cross the boundary via Next(). This updates the level's
			// stashed span pointer in place.
			level.atBoundary = false
			level.iterKV = level.iter.Next()
			if level.iterKV == nil && m.levelHasError(level) {
				return
			}
		} else if wasParked {
			// Unpark: seek to slab boundary.
			if m.prefix != nil {
				// TODO(radu): use TrySeekUsingNext (we'll have to keep track of whether
				// the iteration direction has changed).
				level.iterKV = level.iter.SeekPrefixGE(m.prefix, boundaryKey, base.SeekGEFlagsNone)
			} else {
				level.iterKV = level.iter.SeekGE(boundaryKey, base.SeekGEFlagsNone)
			}
			if level.iterKV == nil && m.levelHasError(level) {
				return
			}
		}
		// else: level was active, not at boundary, stays active; span
		// already current.
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
	for levelIdx, parked := range m.slab.Build(-1) {
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
	for levelIdx, parked := range m.slab.Build(-1) {
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
		return m.switchToMaxHeapAndPrev()
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
			m.slab.assertNextBoundary(level.iterKV.K.UserKey)
			m.advanceSlabBackward()
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

// advanceSlabBackward handles a slab transition during backward iteration.
func (m *mergingIterV2) advanceSlabBackward() {
	top := m.heap.Top()
	boundaryKey := top.iterKV.K.UserKey

	// Optimization: when a single level has a boundary here, and the current span
	// on that level has no span keys, try to advance without rebuilding the slab.
	// TODO(radu): we can relax these conditions to cover more cases.
	if len(top.span.Keys) == 0 && !m.heap.MultipleLevelsAtSameBoundary() {
		top.iterKV = top.iter.Prev()
		if top.iterKV == nil {
			if m.levelHasError(top) {
				return
			}
			m.heap.PopTop()
		} else {
			m.heap.FixTop()
		}
		if len(top.span.Keys) == 0 {
			// This was a spurious boundary: no keys before, no keys after. Nothing
			// else in the slab has changed.
			m.slab.calcNextBoundary(-1)
			return
		}
		// We have to rebuild the slab in case some levels become parked.
	} else {
		// Mark all levels at the slab boundary (see advanceSlabForward). The actual
		// Prev() call is deferred to the Build loop below, where we can skip it for
		// levels that become parked (avoiding unnecessary file opens in level
		// iterators).
		top.atBoundary = true
		m.heap.PopTop()
		for m.heap.Len() > 0 {
			level := m.heap.Top()
			if level.iterKV.K.Kind() != base.InternalKeyKindSpanBoundary {
				break
			}
			if m.slab.cmp(level.iterKV.K.UserKey, boundaryKey) != 0 {
				break
			}
			level.atBoundary = true
			m.heap.PopTop()
		}
	}

	// Unless some levels are parked, we are only using boundary in the first loop
	// below (before messing with the iterator).
	if m.anyLevelParked() {
		m.keyBuf = append(m.keyBuf[:0], boundaryKey...)
		boundaryKey = m.keyBuf
	}

	// Recompute the slab. For levels at the boundary, call Prev() to cross it
	// only if the level is not parked.
	for levelIdx, parked := range m.slab.Build(-1) {
		level := &m.levels[levelIdx]
		wasParked := level.parked
		level.parked = parked
		if parked {
			level.iterKV = nil
			level.atBoundary = false
		} else if level.atBoundary {
			// Cross the boundary via Prev(). This updates the level's
			// stashed span pointer in place.
			level.atBoundary = false
			level.iterKV = level.iter.Prev()
			if level.iterKV == nil && m.levelHasError(level) {
				return
			}
		} else if wasParked {
			// Unpark: seek to before slab boundary.
			level.iterKV = level.iter.SeekLT(boundaryKey, base.SeekLTFlagsNone)
			if level.iterKV == nil && m.levelHasError(level) {
				return
			}
		}
		// else: level was active, not at boundary, stays active; span
		// already current.
	}

	// Rebuild the heap.
	m.initHeap(-1)
}

// switchToMinHeapAndNext switches from backward to forward iteration. All levels are
// repositioned so that they are at a key strictly after the current key (the
// heap root before switching).
func (m *mergingIterV2) switchToMinHeapAndNext() *base.InternalKV {
	if m.heap.Len() == 0 {
		if m.lower != nil {
			return m.SeekGE(m.lower, base.SeekGEFlagsNone)
		}
		return m.First()
	}

	cur := m.heap.Top()
	key := cur.iterKV.K
	m.keyBuf = append(m.keyBuf[:0], key.UserKey...)
	key.UserKey = m.keyBuf

	// Recompute slab for forward iteration.
	for levelIdx, parked := range m.slab.Build(+1) {
		level := &m.levels[levelIdx]
		if parked {
			level.parked = true
			level.iterKV = nil
			continue
		}
		if level.parked {
			level.parked = false
			level.iterKV = level.iter.SeekGE(key.UserKey, base.SeekGEFlagsNone)
		} else {
			level.iterKV = level.iter.Next()
		}
		// Make sure we are beyond the previous top key. This might be necessary for
		// levels unparked with SeekGE. Also, in some iterators (memtable), points
		// that weren't there can "show up" (at seq nums that will make them
		// invisible in the end), so a Prev followed by a Next might not return us
		// to the same place.
		for level.iterKV != nil && base.InternalCompare(m.heap.cmp, level.iterKV.K, key) <= 0 {
			level.iterKV = level.iter.Next()
		}
		if level.iterKV == nil && m.levelHasError(level) {
			return nil
		}
	}

	m.initHeap(+1)
	return m.findNextEntry()
}

// switchToMaxHeapAndPrev switches from forward to backward iteration. All levels are
// repositioned so that they are at a key strictly before the current key (the
// heap root before switching).
func (m *mergingIterV2) switchToMaxHeapAndPrev() *base.InternalKV {
	if m.heap.Len() == 0 {
		if m.upper != nil {
			return m.SeekLT(m.upper, base.SeekLTFlagsNone)
		}
		return m.Last()
	}

	cur := m.heap.Top()
	key := cur.iterKV.K
	m.keyBuf = append(m.keyBuf[:0], key.UserKey...)
	key.UserKey = m.keyBuf

	// Recompute slab for backward iteration.
	for levelIdx, parked := range m.slab.Build(-1) {
		level := &m.levels[levelIdx]
		if parked {
			level.parked = true
			level.iterKV = nil
			continue
		}
		if level.parked {
			level.parked = false
			level.iterKV = level.iter.SeekLT(key.UserKey, base.SeekLTFlagsNone)
		} else {
			level.iterKV = level.iter.Prev()
		}
		// Make sure we are beyond the previous top key. In some iterators
		// (memtable), points that weren't there can "show up" (at seq nums that
		// will make them invisible in the end), so a Next followed by a Prev might
		// not return us to the same place.
		for level.iterKV != nil && base.InternalCompare(m.heap.cmp, level.iterKV.K, key) >= 0 {
			level.iterKV = level.iter.Prev()
		}
		if level.iterKV == nil && m.levelHasError(level) {
			return nil
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

func (m *mergingIterV2) anyLevelParked() bool {
	for i := range m.levels {
		if m.levels[i].parked {
			return true
		}
	}
	return false
}

func (m *mergingIterV2) levelHasError(level *mergingIterV2Level) bool {
	if err := level.iter.Error(); err != nil {
		m.err = err
		for i := range m.levels {
			m.levels[i].atBoundary = false
		}
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
	li := h.items[i].level
	lj := h.items[j].level
	if cmp := li.Compare(h.cmp, lj); cmp != 0 {
		return cmp == h.lessCmp
	}
	// When multiple levels have the same key, put higher levels (smaller index)
	// at the top. This can only happen for boundary keys (real internal keys with
	// the same user key must have different seq nums).
	if invariants.Enabled && h.items[i].level.iterKV.Kind() != base.InternalKeyKindSpanBoundary {
		panic(errors.AssertionFailedf("duplicate non-boundary key"))
	}
	return li.index < lj.index
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
		h.down(i)
	}
}

func (h *mergingIterV2Heap) Top() *mergingIterV2Level {
	return h.items[0].level
}

// MultipleLevelsAtSameBoundary returns true if the "second best" in the heap is at
// the same boundary key as the top.
func (h *mergingIterV2Heap) MultipleLevelsAtSameBoundary() bool {
	if invariants.Enabled && h.Top().iterKV.Kind() != base.InternalKeyKindSpanBoundary {
		panic(errors.AssertionFailedf("not at boundary"))
	}

	if h.Len() < 2 {
		return false
	}
	secondBest := h.items[1].level
	if h.Len() > 2 {
		if h.items[0].winnerChild == winnerChildUnknown {
			if h.less(2, 1) {
				h.items[0].winnerChild = winnerChildRight
			} else {
				h.items[0].winnerChild = winnerChildLeft
			}
		}
		if h.items[0].winnerChild == winnerChildRight {
			secondBest = h.items[2].level
		}
	}
	top := h.items[0].level
	// Note: the index comparison is just an optimization: we know that if there
	// are multiple levels with the same key, the smallest index is on top. We
	// expect the bottom level to have the most boundaries, so it can save some
	// comparisons.
	return secondBest.index > top.index &&
		secondBest.iterKV.Kind() == base.InternalKeyKindSpanBoundary &&
		h.cmp(secondBest.iterKV.K.UserKey, top.iterKV.K.UserKey) == 0
}

func (h *mergingIterV2Heap) FixTop() {
	h.down(0)
}

func (h *mergingIterV2Heap) PopTop() {
	n := h.Len() - 1
	h.swap(0, n)
	h.items = h.items[:n]
	h.down(0)
}

func (h *mergingIterV2Heap) down(i int) {
	n := len(h.items)
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
					panic(errors.AssertionFailedf("mergingIterV2Heap: winnerChild cache inconsistency"))
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
