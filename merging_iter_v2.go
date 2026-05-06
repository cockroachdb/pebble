// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"context"
	"fmt"
	"iter"
	"slices"
	"strings"

	"github.com/cockroachdb/crlib/crstrings"
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

	// lastPrefixCopy stores a copy of the prefix from the most recent
	// SeekPrefixGE call. It is used in invariants builds to verify that
	// callers do not pass the same prefix when using TrySeekUsingNext (see
	// SeekPrefixGEStrict).
	lastPrefixCopy invariants.Value[[]byte]
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
	// onlyFwdSinceParked is true when the level was parked during a forward slab
	// advance and the merging iterator has only moved forward since then. When
	// true, we can use TrySeekUsingNext when unparking the level in
	// advanceSlabForward. Only valid when parked is true.
	//
	// This field is per-level because different levels get parked/unparked at
	// different times.
	onlyFwdSinceParked bool
	// atBoundary is true when this level has a boundary key at the current
	// slab boundary. The Next()/Prev() to cross it is deferred until we
	// know whether the level will be parked.
	atBoundary bool
}

func (level *mergingIterV2Level) Name() string {
	return string('A' + byte(level.index))
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
	m.prepareForLevelOp(level)
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
	m.prepareForLevelOp(level)
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
			level.onlyFwdSinceParked = false
			level.iterKV = nil
		} else {
			m.prepareForLevelOp(level)
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
	for levelIdx, parked := range m.slab.Build(+1) {
		level := &m.levels[levelIdx]
		wasParked := level.parked
		level.parked = parked
		if parked {
			if flags.TrySeekUsingNext() {
				if !wasParked {
					// We are parking the level now. We know that it is positioned at or
					// behind <key>.
					level.onlyFwdSinceParked = true
				}
				// If the level was already parked, onlyFwdSinceParked stays as-is
				// because we are moving forward.
			} else {
				// Without TrySeekUsingNext, seekGE is an absolute positioning operation
				// and we can't rely on any existing state.
				level.onlyFwdSinceParked = false
			}

			level.iterKV = nil
		} else {
			levelFlags := flags
			if wasParked && !level.onlyFwdSinceParked {
				// The level could be positioned in an arbitrary place, so we need to do
				// an absolute seek.
				levelFlags = levelFlags.DisableTrySeekUsingNext()
			}
			m.prepareForLevelOp(level)
			if m.prefix != nil {
				level.iterKV = level.iter.SeekPrefixGE(m.prefix, key, levelFlags)
			} else {
				level.iterKV = level.iter.SeekGE(key, levelFlags)
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
		op := treesteps.StartOpf(m, "SeekGE(%q%s)", key, crstrings.If(flags != 0, ", "+flags.String()))
		defer func() {
			op.Finishf("= %s", kv.String())
		}()
	}
	if flags.TrySeekUsingNext() {
		if m.err != nil || m.dir != +1 || m.prefix != nil {
			panic(errors.AssertionFailedf("invalid use of TrySeekUsingNext"))
		}
		if m.heap.Len() == 0 {
			return nil
		}
		if top := m.heap.Top(); m.heap.cmp(key, top.iterKV.K.UserKey) <= 0 {
			// The iterator is already at the right position.
			//
			// It is necessary to check for this case to avoid passing down
			// TrySeekUsingNext incorrectly: it is possible multiple slab transitions
			// are necessary between <key> and <iterKV.K>, which would mean some
			// levels would go backwards if we seeked them at <key>.
			//
			// For example, consider two levels:
			//  L1: a  [b, c):RANGEDEL d
			//  L2:      b1
			//
			// A SeekGE(a1) on the merging iterator would cause slab transitions
			// through boundaries b and c to produce the resulting key d. A subsequent
			// SeekGE(b, TrySeekUsingNext) is legal because (from an external
			// perspective) it doesn't move back the merging iterator. However, L1 is
			// now positioned at d, so it would be illegal to re-seek it to b using
			// TrySeekUsingNext.
			return top.iterKV
		}
		// Try the single-level advance fast path.
		if top := m.heap.Top(); m.shouldTrySingleLevelAdvance(top, key) {
			m.prepareForLevelOp(top)
			top.iterKV = top.iter.SeekGE(key, flags)
			if m.finishSingleLevelAdvance(top) {
				return m.findNextEntry()
			}
			// The top.span keys might have changed; fall through to the full slab
			// rebuild below. m.seekGE will re-seek top with TSUN (a cheap no-op since
			// top is now at >= key) before rebuilding.
		}
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
				panic(errors.AssertionFailedf("mergingIterV2: prefix violation: returning key %q without matching prefix %q",
					level.iterKV.K.UserKey, m.prefix))
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
		m.prepareForLevelOp(top)
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
			if !wasParked {
				// Note that advanceSlabForward is only used when the iterator is
				// already in forward mode; the last operation on any non-parked
				// iterator must have been a forward operation.
				level.onlyFwdSinceParked = true
				level.iterKV = nil
			}
			level.atBoundary = false
		} else if level.atBoundary {
			// Cross the boundary via Next(). This updates the level's
			// stashed span pointer in place.
			level.atBoundary = false
			m.prepareForLevelOp(level)
			level.iterKV = level.iter.Next()
			if level.iterKV == nil && m.levelHasError(level) {
				return
			}
		} else if wasParked {
			// Unpark: seek to slab boundary. If the merging iterator has only moved
			// forward since the level was parked, use TrySeekUsingNext.
			flags := base.SeekGEFlagsNone
			if level.onlyFwdSinceParked {
				flags = flags.EnableTrySeekUsingNext()
			}
			m.prepareForLevelOp(level)
			if m.prefix != nil {
				level.iterKV = level.iter.SeekPrefixGE(m.prefix, boundaryKey, flags)
			} else {
				level.iterKV = level.iter.SeekGE(boundaryKey, flags)
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
//
// When flags.TrySeekUsingNext() is set, the caller MUST pass a prefix that
// differs from the prefix supplied to the most recent SeekPrefixGE call.
// Reusing the same prefix with TrySeekUsingNext is illegal and is checked in
// invariants builds. This is a particularity of this iterator and not the
// general base.InternalIterator contract.
func (m *mergingIterV2) SeekPrefixGEStrict(
	prefix, key []byte, flags base.SeekGEFlags,
) (kv *base.InternalKV) {
	if treesteps.Enabled && treesteps.IsRecording(m) {
		op := treesteps.StartOpf(m, "SeekPrefixGE(%q%s)", key, crstrings.If(flags != 0, ", "+flags.String()))
		defer func() {
			op.Finishf("= %s", kv.String())
		}()
	}
	if invariants.Enabled {
		if flags.TrySeekUsingNext() {
			// Verify that the prefix is strictly greater than the last prefix. We can't
			// use m.prefix, since it's a shallow copy that is no longer guaranteed to
			// be stable.
			//
			// The reason we require this is that we otherwise can't tell if the
			// iterator is already at the correct position for the seek key (in which
			// case, re-seeking some levels could move them back; see the "iterator is
			// already at the right position" case in SeekGE). Making that
			// determination would require remembering a copy of the prefix or of the
			// last point key.
			if prev := m.lastPrefixCopy.Get(); prev == nil || bytes.Compare(prefix, prev) <= 0 {
				panic(errors.AssertionFailedf(
					"mergingIterV2.SeekPrefixGE(TrySeekUsingNext): prefix %q must be > previous %q", prefix, prev))
			}
		}
		m.lastPrefixCopy.Set(append(m.lastPrefixCopy.Get()[:0], prefix...))
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
			level.onlyFwdSinceParked = false
			level.iterKV = nil
		} else {
			m.prepareForLevelOp(level)
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
			level.onlyFwdSinceParked = false
			level.iterKV = nil
		} else {
			m.prepareForLevelOp(level)
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
func (m *mergingIterV2) NextPrefix(succKey []byte) (kv *base.InternalKV) {
	if treesteps.Enabled && treesteps.IsRecording(m) {
		op := treesteps.StartOpf(m, "NextPrefix(%q)", succKey)
		defer func() {
			op.Finishf("= %s", kv.String())
		}()
	}
	if m.dir != +1 {
		panic(errors.AssertionFailedf("pebble: cannot switch directions with NextPrefix"))
	}
	if m.err != nil || m.heap.Len() == 0 {
		return nil
	}
	if invariants.Enabled && m.prefix != nil {
		panic(errors.AssertionFailedf("pebble: NextPrefix in prefix iteration mode"))
	}
	// NextPrefix can only be called when the iterator last returned a
	// non-boundary key.
	if invariants.Enabled && m.heap.Top().iterKV.Kind() == base.InternalKeyKindSpanBoundary {
		panic(errors.AssertionFailedf("pebble: NextPrefix called on boundary key"))
	}
	// Try the single-level advance fast path.
	if top := m.heap.Top(); m.shouldTrySingleLevelAdvance(top, succKey) {
		m.prepareForLevelOp(top)
		// We can always call NextPrefix here (see assertion above).
		top.iterKV = top.iter.NextPrefix(succKey)
		if m.finishSingleLevelAdvance(top) {
			return m.findNextEntry()
		}
		// The top.span keys might have changed; fall through to the full slab
		// rebuild below. Note that top will not be re-seeked (already at >=
		// succKey).
	}
	for levelIdx, parked := range m.slab.Build(+1) {
		level := &m.levels[levelIdx]
		wasParked := level.parked
		level.parked = parked
		if parked {
			if !wasParked {
				// Newly parking the level. NextPrefix is forward-only and the level
				// is currently at or before succKey, so any future unpark seek is to
				// a key >= succKey and TrySeekUsingNext is safe.
				level.onlyFwdSinceParked = true
			}
			// If the level was already parked, onlyFwdSinceParked stays as-is
			// because we are moving forward.
			level.iterKV = nil
			continue
		}
		if wasParked {
			// Unpark: seek to succKey. If the merging iterator has only moved forward
			// since the level was parked, use TrySeekUsingNext.
			flags := base.SeekGEFlagsNone
			if level.onlyFwdSinceParked {
				flags = flags.EnableTrySeekUsingNext()
			}
			level.iterKV = level.iter.SeekGE(succKey, flags)
		} else if level.iterKV != nil && m.heap.cmp(level.iterKV.K.UserKey, succKey) < 0 {
			// Level is positioned before succKey.
			if level.iterKV.K.Kind() != base.InternalKeyKindSpanBoundary {
				m.prepareForLevelOp(level)
				level.iterKV = level.iter.NextPrefix(succKey)
			} else {
				m.prepareForLevelOp(level)
				// We cannot call NextPrefix at a boundary.
				level.iterKV = level.iter.SeekGE(succKey, base.SeekGEFlagsNone.EnableTrySeekUsingNext())
			}
		}
		if level.iterKV == nil && m.levelHasError(level) {
			return nil
		}
	}
	m.initHeap(+1)
	return m.findNextEntry()
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
		m.prepareForLevelOp(top)
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
		if invariants.Enabled && wasParked && level.onlyFwdSinceParked {
			// The iterator is moving in reverse direction; all onlyFwdSinceParked
			// flags should have been reset.
			panic(errors.AssertionFailedf("onlyFwdSinceParked set in reverse iteration mode"))
		}
		level.parked = parked
		if parked {
			level.onlyFwdSinceParked = false
			level.iterKV = nil
			level.atBoundary = false
		} else if level.atBoundary {
			// Cross the boundary via Prev(). This updates the level's
			// stashed span pointer in place.
			level.atBoundary = false
			m.prepareForLevelOp(level)
			level.iterKV = level.iter.Prev()
			if level.iterKV == nil && m.levelHasError(level) {
				return
			}
		} else if wasParked {
			// Unpark: seek to before slab boundary.
			m.prepareForLevelOp(level)
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
			level.onlyFwdSinceParked = false
			level.iterKV = nil
			continue
		}
		if level.parked {
			level.parked = false
			m.prepareForLevelOp(level)
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
			m.prepareForLevelOp(level)
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
			level.onlyFwdSinceParked = false
			level.iterKV = nil
			continue
		}
		m.prepareForLevelOp(level)
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
			m.prepareForLevelOp(level)
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
	var buf strings.Builder
	buf.WriteString("merging-v2(")
	if m.err != nil {
		fmt.Fprintf(&buf, "err: %s", m.err)
	} else if m.dir == 0 {
		buf.WriteString("not positioned")
	} else {
		if m.dir > 0 {
			buf.WriteString("fwd")
		} else {
			buf.WriteString("bwd")
		}
		buf.WriteString(", heap:")
		for level := range m.heap.DebugItems() {
			fmt.Fprintf(&buf, " %s:%s %s", level.Name(), level.iterKV.K, level.span)
		}
	}
	buf.WriteByte(')')
	return buf.String()
}

type treestepsV2DummyNode struct {
	info treesteps.NodeInfo
}

func (d *treestepsV2DummyNode) TreeStepsNode() treesteps.NodeInfo {
	return d.info
}

// TreeStepsNode implements treesteps.Node.
func (m *mergingIterV2) TreeStepsNode() treesteps.NodeInfo {
	info := treesteps.NodeInfof(m, "mergingIterV2")
	if m.err != nil {
		info.AddPropf("ERROR", "%s", m.err)
	}
	if m.heap.Len() > 0 {
		heapProp := crstrings.IfElse(m.heap.lessCmp == -1, "min heap", "max heap")
		var str strings.Builder
		for i, item := range m.heap.items {
			if i > 0 {
				str.WriteString(" ")
			}
			if item.level.iterKV != nil {
				fmt.Fprintf(&str, "%s:%s", item.level.Name(), item.level.iterKV.K)
			} else {
				fmt.Fprintf(&str, "%s:<nil>", item.level.Name())
			}
		}
		info.AddPropf(heapProp, "%s", str.String())
	}
	if m.prefix != nil {
		info.AddPropf("prefix", "%s", m.prefix)
	}
	for i := range m.levels {
		l := &m.levels[i]
		name := l.Name()
		if l.iterKV != nil {
			name = fmt.Sprintf("%s:%s", name, l.iterKV.K)
		}
		if l.parked {
			name += " [parked]"
		}
		d := &treestepsV2DummyNode{}
		if l.span != nil && l.span.Valid() {
			name += fmt.Sprintf(" %s", l.span)
		}
		d.info = treesteps.NodeInfof(d, "%s", name)
		d.info.AddChildren(l.iter)
		info.AddChildren(d)
	}
	return info
}

// prepareForLevelOp is a no-op in production; in treesteps builds, it resets
// level.iterKV before an operation on that level's iterator starts. This
// prevents TreeStepsNode from printing an invalid key slice.
func (m *mergingIterV2) prepareForLevelOp(level *mergingIterV2Level) {
	if treesteps.Enabled && treesteps.IsRecording(m) {
		level.iterKV = nil
	}
}

// shouldTrySingleLevelAdvance is used to implement a fast path when seeking the
// iterator forward (SeekGE with TrySeekUsingNext or NextPrefix). The fast path
// involves advancing a single level and skipping the slab rebuild.
//
// The slab's per-level visibility (minSeqNum/maxSeqNum/parked) is a pure
// function of each level's span keys. If we know that no level's span keys
// change as a result of the advance, the slab is unchanged and we can advance
// just the one level.
//
// We establish two conditions:
//
//  1. Only the heap's top level moves (i.e. no other level needs to advance).
//     Verified here by comparing the seek key to the second-best heap entry.
//
//  2. The top level's span keys don't change as a result of the advance.
//     Currently checked conservatively: top.span must have no keys before
//     AND after the advance. The pre-check is here; the post-check is in
//     finishSingleLevelAdvance.
//     TODO(radu): relax this (e.g. snapshot the span keys and compare).
func (m *mergingIterV2) shouldTrySingleLevelAdvance(top *mergingIterV2Level, seekKey []byte) bool {
	if len(top.span.Keys) != 0 {
		return false
	}
	// Condition 1 is satisfied iff the second-best level is at a key >= the
	// seek key.
	sb := m.heap.SecondBest()
	return sb == nil || m.heap.cmp(sb.iterKV.K.UserKey, seekKey) >= 0
}

// finishSingleLevelAdvance is used after shouldTrySingleLevelAdvance returned
// true and the top level was advanced while holding the slab fixed. It inspects
// the result of the single-level advance: it completes condition 2 from
// shouldTrySingleLevelAdvance (top.span keys unchanged) by verifying the new
// top.span also has no keys.
//
// Returns true if the fast path was successful; false if the slab must be
// rebuilt. On success, it updates the heap.
func (m *mergingIterV2) finishSingleLevelAdvance(top *mergingIterV2Level) (ok bool) {
	if top.iterKV == nil {
		if m.levelHasError(top) {
			return true
		}
		m.heap.PopTop()
		m.slab.calcNextBoundary(+1)
		return true
	}
	if len(top.span.Keys) == 0 {
		m.heap.FixTop()
		m.slab.calcNextBoundary(+1)
		return true
	}
	return false
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

// SecondBest returns the level with the second-smallest key in the heap (i.e.
// the level that would become the top if the current top were popped), or nil
// if the heap has fewer than two elements. SecondBest populates the
// winnerChild cache for items[0]; subsequent FixTop / down(0) calls reuse it.
func (h *mergingIterV2Heap) SecondBest() *mergingIterV2Level {
	if h.Len() < 2 {
		return nil
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
	return secondBest
}

// MultipleLevelsAtSameBoundary returns true if the "second best" in the heap is at
// the same boundary key as the top.
func (h *mergingIterV2Heap) MultipleLevelsAtSameBoundary() bool {
	if invariants.Enabled && h.Top().iterKV.Kind() != base.InternalKeyKindSpanBoundary {
		panic(errors.AssertionFailedf("not at boundary"))
	}
	secondBest := h.SecondBest()
	if secondBest == nil {
		return false
	}
	top := h.Top()
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

// DebugItems returns an iterator over the heap items in sorted order.
func (h *mergingIterV2Heap) DebugItems() iter.Seq[*mergingIterV2Level] {
	return func(yield func(*mergingIterV2Level) bool) {
		indices := make([]int, len(h.items))
		for i := range indices {
			indices[i] = i
		}
		slices.SortFunc(indices, func(i, j int) int {
			if h.less(i, j) {
				return -1
			}
			return 1
		})
		for _, idx := range indices {
			if !yield(h.items[idx].level) {
				return
			}
		}
	}
}
