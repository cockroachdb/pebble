// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package iterv2

import (
	"context"
	"slices"
	"sort"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/internal/treesteps"
)

// keyCmp is the comparison function used by TestIter.
var keyCmp = testkeys.Comparer.Compare

// testEntry is a single element in the flat entry list used by TestIter. Each
// entry is either a point key or a boundary key. The skipFwd/skipBwd flags
// control visibility: forward operations skip entries with skipFwd=true, and
// backward operations skip entries with skipBwd=true.
type testEntry struct {
	kv      base.InternalKV
	spanIdx int  // index into boundaries/spanKeys
	skipFwd bool // skip during forward operations (First, SeekGE, Next)
	skipBwd bool // skip during backward operations (Last, SeekLT, Prev)
}

// TestIter is a simple implementation of InterleavingIter when all the point
// keys and spans are known upfront.
//
// It operates over real InternalKey and keyspan.Span inputs, using a
// pre-computed flat entry list with skip flags for directional boundary
// emission.
type TestIter struct {
	// Immutable inputs.
	points   []base.InternalKV
	spans    []keyspan.Span
	startKey []byte // overall range start (default lower)
	endKey   []byte // overall range end (default upper)

	// Rebuilt by init():
	boundaries [][]byte
	spanKeys   [][]keyspan.Key
	entries    []testEntry
	nilLower   bool // true if the effective lower bound is unbounded
	nilUpper   bool // true if the effective upper bound is unbounded

	idx         int  // -1 = before start, len(entries) = past end
	dir         int8 // +1 forward, -1 backward; set in every positioning method
	kv          base.InternalKV
	currentSpan Span

	// filter, when non-nil, is checked during forward iteration in Next().
	// Entries where the filter returns false are skipped. The function takes
	// an index into t.entries.
	filter func(idx int) bool
}

var _ Iter = (*TestIter)(nil)

// NewTestIter creates a TestIter.
//
// Parameters:
//   - points: point keys to include.
//   - spans: span definitions. Note that a span with Start=End can be used to
//     induce a spurious boundary inside a gap between spans.
//   - startKey: the overall start key of the iterator's range (can be nil).
//   - endKey: the overall exclusive end key of the iterator's range (can be
//     nil).
//   - lower: the initial lower bound (nil for unbounded below).
//   - upper: the initial exclusive upper bound (nil for unbounded above).
func NewTestIter(
	points []base.InternalKV, spans []keyspan.Span, startKey, endKey, lower, upper []byte,
) *TestIter {
	t := &TestIter{
		points:   points,
		spans:    spans,
		startKey: startKey,
		endKey:   endKey,
		idx:      -1,
	}
	t.init(lower, upper)
	return t
}

// init rebuilds boundaries, spanKeys, and entries for the given [lower, upper)
// range. Called by the constructor and SetBounds. Either or both bounds can be
// nil, meaning unbounded.
func (t *TestIter) init(lower, upper []byte) {
	t.nilLower = lower == nil && t.startKey == nil
	t.nilUpper = upper == nil && t.endKey == nil

	effectiveLower := t.startKey
	if lower != nil {
		if t.startKey != nil && keyCmp(lower, t.startKey) <= 0 {
			lower = nil
		} else {
			effectiveLower = lower
		}
	}

	effectiveUpper := t.endKey
	if upper != nil {
		if t.endKey != nil && keyCmp(upper, t.endKey) >= 0 {
			upper = nil
		} else {
			effectiveUpper = upper
		}
	}

	if effectiveLower != nil && effectiveUpper != nil && keyCmp(effectiveLower, effectiveUpper) >= 0 {
		// Empty range.
		t.boundaries = t.boundaries[:0]
		t.spanKeys = nil
		t.entries = t.entries[:0]
		t.idx = -1
		t.currentSpan = Span{}
		return
	}

	// Filter points to [lower, upper).
	var filteredPoints []base.InternalKV
	for _, p := range t.points {
		if lower != nil && keyCmp(p.K.UserKey, lower) < 0 {
			continue
		}
		if upper != nil && keyCmp(p.K.UserKey, upper) >= 0 {
			continue
		}
		filteredPoints = append(filteredPoints, p)
	}

	// Filter spans overlapping [lower, upper), clip Start/End.
	var filteredSpans []keyspan.Span
	for _, s := range t.spans {
		if upper != nil && keyCmp(s.Start, upper) >= 0 {
			continue
		}
		if lower != nil && keyCmp(s.End, lower) <= 0 {
			continue
		}
		clipped := s
		if lower != nil && keyCmp(clipped.Start, lower) < 0 {
			clipped.Start = lower
		}
		if upper != nil && keyCmp(clipped.End, upper) > 0 {
			clipped.End = upper
		}
		filteredSpans = append(filteredSpans, clipped)
	}

	// Build boundaries: effective lower and upper bounds, plus all filtered span
	// Start/End. Sort and dedup.
	t.boundaries = t.boundaries[:0]
	if lower != nil {
		t.boundaries = append(t.boundaries, lower)
	} else if t.startKey != nil {
		t.boundaries = append(t.boundaries, t.startKey)
	}
	if upper != nil {
		t.boundaries = append(t.boundaries, upper)
	} else if t.endKey != nil {
		t.boundaries = append(t.boundaries, t.endKey)
	}
	for _, s := range filteredSpans {
		t.boundaries = append(t.boundaries, s.Start, s.End)
	}
	slices.SortFunc(t.boundaries, keyCmp)
	t.boundaries = slices.CompactFunc(t.boundaries, func(a, b []byte) bool {
		return keyCmp(a, b) == 0
	})

	if lower == nil && t.startKey == nil {
		t.boundaries = append([][]byte{nil}, t.boundaries...)
	}
	if upper == nil && t.endKey == nil {
		t.boundaries = append(t.boundaries, nil)
	}

	// Build spanKeys[i]: for each region [boundaries[i], boundaries[i+1]),
	// find the filtered span covering it (if any) and assign its Keys.
	numRegions := len(t.boundaries) - 1
	t.spanKeys = make([][]keyspan.Key, numRegions)
	for i := range numRegions {
		if t.boundaries[i] == nil || t.boundaries[i+1] == nil {
			continue
		}
		for _, s := range filteredSpans {
			if keyCmp(s.Start, t.boundaries[i]) <= 0 && keyCmp(s.End, t.boundaries[i+1]) >= 0 {
				if t.spanKeys[i] != nil {
					panic(errors.AssertionFailedf("input spans must be non-overlapping"))
				}
				t.spanKeys[i] = s.Keys
			}
		}
	}

	// Build entries.
	t.entries = t.entries[:0]

	// Point entries.
	for _, p := range filteredPoints {
		spanIdx := sort.Search(numRegions-1, func(i int) bool {
			return keyCmp(t.boundaries[i+1], p.K.UserKey) > 0
		})
		t.entries = append(t.entries, testEntry{
			kv:      p,
			spanIdx: spanIdx,
		})
	}

	// Boundary entries. At each boundary position i:
	//   - Forward boundary (i > 0): exiting span[i-1] going forward, skipBwd.
	//   - Backward boundary (i < len-1): exiting span[i] going backward, skipFwd.
	// Forward boundary is added first so stable sort places it before
	// backward boundary at the same position.
	//
	// When nilUpper, skip the forward boundary at the last position (no
	// terminal boundary for unbounded upper). When nilLower, skip the backward
	// boundary at the first position (no terminal boundary for unbounded lower).
	for i, bdry := range t.boundaries {
		bdryKey := base.MakeInternalKey(
			bdry,
			base.SeqNumMax,
			base.InternalKeyKindSpanBoundary,
		)
		if i > 0 && !(t.nilUpper && i == len(t.boundaries)-1) {
			t.entries = append(t.entries, testEntry{
				kv:      base.InternalKV{K: bdryKey},
				spanIdx: i - 1,
				skipBwd: true,
			})
		}
		if i < len(t.boundaries)-1 && !(t.nilLower && i == 0) {
			t.entries = append(t.entries, testEntry{
				kv:      base.InternalKV{K: bdryKey},
				spanIdx: i,
				skipFwd: true,
			})
		}
	}

	// Stable sort by InternalCompare.
	slices.SortStableFunc(t.entries, func(a, b testEntry) int {
		return base.InternalCompare(keyCmp, a.kv.K, b.kv.K)
	})

	t.idx = -1
	t.currentSpan = Span{}
}

// emitEntry sets kv and currentSpan from the given entry and returns &kv.
func (t *TestIter) emitEntry(e *testEntry) *base.InternalKV {
	t.kv = e.kv
	if t.dir >= 0 {
		// Forward: Boundary is the End of the current region.
		t.currentSpan.BoundaryType = BoundaryEnd
		if t.nilUpper && e.spanIdx == len(t.boundaries)-2 {
			t.currentSpan.Boundary = nil
		} else {
			t.currentSpan.Boundary = t.boundaries[e.spanIdx+1]
		}
	} else {
		// Backward: Boundary is the Start of the current region.
		t.currentSpan.BoundaryType = BoundaryStart
		if t.nilLower && e.spanIdx == 0 {
			t.currentSpan.Boundary = nil
		} else {
			t.currentSpan.Boundary = t.boundaries[e.spanIdx]
		}
	}
	t.currentSpan.Keys = t.spanKeys[e.spanIdx]
	return &t.kv
}

// Span implements Iter.
func (t *TestIter) Span() *Span {
	return &t.currentSpan
}

// First implements InternalIterator.
func (t *TestIter) First() *base.InternalKV {
	t.filter = nil
	t.dir = +1
	for t.idx = 0; t.idx < len(t.entries); t.idx++ {
		if !t.entries[t.idx].skipFwd {
			return t.emitEntry(&t.entries[t.idx])
		}
	}
	t.idx = len(t.entries)
	t.currentSpan = Span{}
	return nil
}

// Last implements InternalIterator.
func (t *TestIter) Last() *base.InternalKV {
	t.filter = nil
	t.dir = -1
	for t.idx = len(t.entries) - 1; t.idx >= 0; t.idx-- {
		if !t.entries[t.idx].skipBwd {
			return t.emitEntry(&t.entries[t.idx])
		}
	}
	t.idx = -1
	t.currentSpan = Span{}
	return nil
}

// seekGEInternal contains the core SeekGE logic.
func (t *TestIter) seekGEInternal(key []byte) *base.InternalKV {
	t.idx = sort.Search(len(t.entries), func(i int) bool {
		cmp := keyCmp(t.entries[i].kv.K.UserKey, key)
		// When we land exactly on a boundary, we don't need to emit it.
		return cmp > 0 || (cmp == 0 && t.entries[i].kv.K.Kind() != base.InternalKeyKindSpanBoundary)
	})
	for ; t.idx < len(t.entries); t.idx++ {
		if !t.entries[t.idx].skipFwd {
			return t.emitEntry(&t.entries[t.idx])
		}
	}
	t.idx = len(t.entries)
	t.currentSpan = Span{}
	return nil
}

// SeekGE implements InternalIterator.
func (t *TestIter) SeekGE(key []byte, flags base.SeekGEFlags) *base.InternalKV {
	t.filter = nil
	t.dir = +1
	return t.seekGEInternal(key)
}

// SeekPrefixGE implements InternalIterator.
func (t *TestIter) SeekPrefixGE(prefix, key []byte, flags base.SeekGEFlags) *base.InternalKV {
	if t.SeekGE(key, flags) == nil {
		return nil
	}
	// Set up a filter that accepts entries with the given prefix, plus one
	// terminal span boundary past the prefix. A non-matching boundary is
	// accepted if the previous non-skipped boundary (since the seek start)
	// has a prefix <= the target prefix, or if there is no earlier boundary.
	prefix = slices.Clone(prefix)

	// Find the first boundary that has a different prefix.
	endBoundary := len(t.entries)
	for i := t.idx; i < len(t.entries); i++ {
		if !t.entries[i].skipFwd && t.entries[i].kv.K.Kind() == base.InternalKeyKindSpanBoundary &&
			!testkeys.Comparer.HasPrefix(t.entries[i].kv.K.UserKey, prefix) {
			endBoundary = i
			break
		}
	}

	t.filter = func(idx int) bool {
		if idx > endBoundary {
			return false
		}
		k := t.entries[idx].kv.K
		return k.Kind() == base.InternalKeyKindSpanBoundary || testkeys.Comparer.HasPrefix(k.UserKey, prefix)
	}
	// Check if the current entry passes the filter; if not, advance.
	if !t.filter(t.idx) {
		return t.Next()
	}
	return &t.kv
}

// seekLTInternal contains the core SeekLT logic.
func (t *TestIter) seekLTInternal(key []byte) *base.InternalKV {
	t.idx = sort.Search(len(t.entries), func(i int) bool {
		return keyCmp(t.entries[i].kv.K.UserKey, key) >= 0
	}) - 1
	for ; t.idx >= 0; t.idx-- {
		if !t.entries[t.idx].skipBwd {
			return t.emitEntry(&t.entries[t.idx])
		}
	}
	t.idx = -1
	t.currentSpan = Span{}
	return nil
}

// SeekLT implements InternalIterator.
func (t *TestIter) SeekLT(key []byte, flags base.SeekLTFlags) *base.InternalKV {
	t.filter = nil
	t.dir = -1
	return t.seekLTInternal(key)
}

// Next implements InternalIterator.
func (t *TestIter) Next() *base.InternalKV {
	t.dir = +1
	t.idx++
	for ; t.idx < len(t.entries); t.idx++ {
		if t.entries[t.idx].skipFwd {
			continue
		}
		if t.filter != nil && !t.filter(t.idx) {
			continue
		}
		return t.emitEntry(&t.entries[t.idx])
	}
	t.idx = len(t.entries)
	t.currentSpan = Span{}
	return nil
}

// NextPrefix implements InternalIterator.
func (t *TestIter) NextPrefix(succKey []byte) *base.InternalKV {
	return t.SeekGE(succKey, base.SeekGEFlagsNone)
}

// Prev implements InternalIterator.
func (t *TestIter) Prev() *base.InternalKV {
	t.filter = nil
	t.dir = -1
	t.idx--
	for ; t.idx >= 0; t.idx-- {
		if !t.entries[t.idx].skipBwd {
			return t.emitEntry(&t.entries[t.idx])
		}
	}
	t.idx = -1
	t.currentSpan = Span{}
	return nil
}

// Error implements InternalIterator.
func (t *TestIter) Error() error { return nil }

// Close implements InternalIterator.
func (t *TestIter) Close() error { return nil }

// SetBounds implements InternalIterator.
func (t *TestIter) SetBounds(lower, upper []byte) {
	t.filter = nil
	t.init(lower, upper)
}

// SetContext implements InternalIterator.
func (t *TestIter) SetContext(_ context.Context) {}

// String implements fmt.Stringer.
func (t *TestIter) String() string { return "test-iter" }

// TreeStepsNode implements treesteps.Node.
func (t *TestIter) TreeStepsNode() treesteps.NodeInfo {
	return treesteps.NodeInfof(t, "TestIter")
}
