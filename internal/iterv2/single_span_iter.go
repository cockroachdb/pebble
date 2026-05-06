// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package iterv2

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/treesteps"
)

// SingleSpanIter is an iterv2.Iter that exposes a single span containing a
// single key (typically a RANGEDEL). It is functionally equivalent to an
// InterleavingIter wrapping an empty point iterator and a keyspan.Iter
// containing one span, but with much less overhead.
type SingleSpanIter struct {
	cmp       *base.Comparer
	spanStart []byte
	spanEnd   []byte

	// boundaries[0..numRegions] are the partition points of [lower, upper)
	// induced by spanStart/spanEnd. numRegions is always > 0, and we populate
	// numRegions+1 entries: boundaries[0] is the iteration's left edge (lower
	// or nil), boundaries[numRegions] is the right edge (upper or nil), and
	// the inner slots are spanStart and/or spanEnd as applicable.
	//
	// Examples (S = spanStart, E = spanEnd; L = lower, U = upper):
	//	L=nil, U=nil:                  boundaries=(nil, S, E, nil),  numRegions=3, keyRegionIdx=1
	//	L<S<E<U:                       boundaries=(L, S, E, U),      numRegions=3, keyRegionIdx=1
	//	S<=L<U<=E (encompasses span):  boundaries=(L, U),            numRegions=1, keyRegionIdx=0
	//	span entirely below [L, U):    boundaries=(L, U),            numRegions=1, keyRegionIdx=-1
	//	span entirely above [L, U):    boundaries=(L, U),            numRegions=1, keyRegionIdx=-1
	//	L>=U (empty range):            boundaries=(nil, nil),        numRegions=1, keyRegionIdx=-1
	//
	// When positioned in currentRegion, Span().Boundary is
	// boundaries[currentRegion+1] for forward iteration or
	// boundaries[currentRegion] for backward; the returned internal key has the
	// same user key.
	//
	// The iterator is exhausted when presentedSpan is not valid. Typical
	// conditions that lead to exhaustion are when currentRegion goes outside
	// [0, numRegions) or we reach a nil boundary.
	boundaries [4][]byte
	numRegions int8
	// keyRegionIdx is the index of the region containing the keyspan.Key, or
	// -1 if the span does not intersect [lower, upper).
	keyRegionIdx int8

	// currentRegion is the region the iterator is currently positioned in.
	currentRegion int8
	// dir tracks the last direction (+1, -1, or 0 = unpositioned). Preserved
	// across exhaustion so direction switches can fall back to First/Last.
	dir int8

	presentedSpan Span
	presentedKV   base.InternalKV
	// keys holds the single keyspan.Key; placed in presentedSpan.Keys whenever
	// the iterator is positioned in the region containing the span (currentRegion
	// == keyRegionIdx).
	keys [1]keyspan.Key
	// prefix is set by SeekPrefixGE; nil otherwise.
	prefix []byte
}

var _ Iter = (*SingleSpanIter)(nil)

// Init initializes the SingleSpanIter for a span [spanStart, spanEnd) with a
// single keyspan.Key with the given Trailer. spanStart and spanEnd must be
// non-nil with spanStart < spanEnd. lower and upper are optional dynamic
// bounds; nil means unbounded in that direction.
func (i *SingleSpanIter) Init(
	cmp *base.Comparer,
	spanStart, spanEnd []byte,
	trailer base.InternalKeyTrailer,
	lower, upper []byte,
) {
	if invariants.Enabled {
		if spanStart == nil || spanEnd == nil {
			panic(errors.AssertionFailedf("SingleSpanIter: span Start/End must be non-nil"))
		}
		if cmp.Compare(spanStart, spanEnd) >= 0 {
			panic(errors.AssertionFailedf("SingleSpanIter: invalid span [%q, %q)", spanStart, spanEnd))
		}
	}
	*i = SingleSpanIter{
		cmp:       cmp,
		spanStart: spanStart,
		spanEnd:   spanEnd,
	}
	i.keys[0] = keyspan.Key{Trailer: trailer}
	i.computeRegions(lower, upper)
}

// computeRegions populates boundaries, numRegions, and keyRegionIdx based on
// the given bounds and the span.
func (i *SingleSpanIter) computeRegions(lower, upper []byte) {
	i.boundaries = [4][]byte{}
	// Empty range: collapse to a single region with nil boundaries. The
	// resulting iterator is permanently exhausted: First/Last/SeekGE/SeekLT
	// all observe a nil edge boundary and exhaust immediately.
	if lower != nil && upper != nil && i.cmp.Compare(lower, upper) >= 0 {
		i.numRegions = 1
		i.keyRegionIdx = -1
		return
	}
	i.boundaries[0] = lower

	// First, check if there is no overlap with the span (this will be the common
	// case in practice).
	if (lower != nil && i.cmp.Compare(lower, i.spanEnd) >= 0) ||
		(upper != nil && i.cmp.Compare(i.spanStart, upper) >= 0) {
		i.keyRegionIdx = -1
		i.boundaries[1] = upper
		i.numRegions = 1
		return
	}

	n := int8(0)
	if lower == nil || i.cmp.Compare(lower, i.spanStart) < 0 {
		n++
		i.boundaries[n] = i.spanStart
	}
	i.keyRegionIdx = n
	if upper == nil || i.cmp.Compare(i.spanEnd, upper) < 0 {
		n++
		i.boundaries[n] = i.spanEnd
	}
	n++
	i.boundaries[n] = upper
	i.numRegions = n
}

// regionKeys returns keys[:] when the given region holds the keyspan.Key.
func (i *SingleSpanIter) regionKeys(region int8) []keyspan.Key {
	if region == i.keyRegionIdx {
		return i.keys[:]
	}
	return nil
}

// Span implements Iter.
func (i *SingleSpanIter) Span() *Span {
	return &i.presentedSpan
}

// emitForward positions the iterator in region for forward iteration. The
// boundary key is boundaries[region+1]. If that boundary is nil, exhausts.
func (i *SingleSpanIter) emitForward(region int8) *base.InternalKV {
	boundary := i.boundaries[region+1]
	i.dir = +1
	if boundary == nil {
		i.exhaust()
		return nil
	}
	i.currentRegion = region
	i.presentedSpan.BoundaryType = BoundaryEnd
	i.presentedSpan.Boundary = boundary
	i.presentedSpan.Keys = i.regionKeys(region)
	i.presentedKV = base.InternalKV{
		K: base.MakeInternalKey(boundary, base.SeqNumMax, base.InternalKeyKindSpanBoundary),
	}
	return &i.presentedKV
}

// emitBackward positions the iterator in region for backward iteration. The
// boundary key is boundaries[region]. If that boundary is nil, exhausts.
func (i *SingleSpanIter) emitBackward(region int8) *base.InternalKV {
	boundary := i.boundaries[region]
	i.dir = -1
	if boundary == nil {
		i.exhaust()
		return nil
	}
	i.currentRegion = region
	i.presentedSpan.BoundaryType = BoundaryStart
	i.presentedSpan.Boundary = boundary
	i.presentedSpan.Keys = i.regionKeys(region)
	i.presentedKV = base.InternalKV{
		K: base.MakeInternalKey(boundary, base.SeqNumMax, base.InternalKeyKindSpanBoundary),
	}
	return &i.presentedKV
}

// exhaust clears presentedSpan (which makes the iterator exhausted). Note that
// i.dir indicates the direction of exhaustion.
func (i *SingleSpanIter) exhaust() {
	i.presentedSpan = Span{}
}

// First implements InternalIterator.
func (i *SingleSpanIter) First() *base.InternalKV {
	if invariants.Enabled && i.boundaries[0] != nil {
		panic(errors.AssertionFailedf("First with lower bound set"))
	}
	i.prefix = nil
	return i.emitForward(0)
}

// Last implements InternalIterator.
func (i *SingleSpanIter) Last() *base.InternalKV {
	if invariants.Enabled && i.boundaries[i.numRegions] != nil {
		panic(errors.AssertionFailedf("Last with upper bound set"))
	}
	i.prefix = nil
	return i.emitBackward(i.numRegions - 1)
}

// SeekGE implements InternalIterator.
func (i *SingleSpanIter) SeekGE(key []byte, _ base.SeekGEFlags) *base.InternalKV {
	if invariants.Enabled && i.boundaries[0] != nil && i.cmp.Compare(key, i.boundaries[0]) < 0 {
		panic(errors.AssertionFailedf("forward seek (%q) before lower bound %q", key, i.boundaries[0]))
	}
	i.prefix = nil
	return i.seekGEInternal(key)
}

// SeekPrefixGE implements InternalIterator.
func (i *SingleSpanIter) SeekPrefixGE(prefix, key []byte, _ base.SeekGEFlags) *base.InternalKV {
	if invariants.Enabled {
		if i.boundaries[0] != nil && i.cmp.Compare(key, i.boundaries[0]) < 0 {
			panic(errors.AssertionFailedf("forward seek (%q) before lower bound %q", key, i.boundaries[0]))
		}
		if !i.cmp.HasPrefix(key, prefix) {
			panic(errors.AssertionFailedf("prefix %q does not match key %q", prefix, key))
		}
	}
	i.prefix = prefix
	return i.seekGEInternal(key)
}

func (i *SingleSpanIter) seekGEInternal(key []byte) *base.InternalKV {
	// SeekGE(key) where key is at or past upper exhausts (matches
	// InterleavingIter behavior).
	if upper := i.boundaries[i.numRegions]; upper != nil && i.cmp.Compare(key, upper) >= 0 {
		i.dir = +1
		i.exhaust()
		return nil
	}
	// Find the region containing key: smallest R such that key < boundaries[R+1].
	// At most numRegions-1 (<= 2) comparisons.
	//
	// We only need to check whether the key belongs in regions
	// 0..numRegions-2; if not, it must belong in the last region (we already
	// returned early above if key was at or past the upper boundary).
	var region int8
	for region < i.numRegions-1 && i.cmp.Compare(key, i.boundaries[region+1]) >= 0 {
		region++
	}
	return i.emitForward(region)
}

// SeekLT implements InternalIterator.
func (i *SingleSpanIter) SeekLT(key []byte, _ base.SeekLTFlags) *base.InternalKV {
	if invariants.Enabled {
		if upper := i.boundaries[i.numRegions]; upper != nil && i.cmp.Compare(key, upper) > 0 {
			panic(errors.AssertionFailedf("SeekLT(%q) after upper bound %q", key, upper))
		}
	}
	i.prefix = nil
	// SeekLT(key) where key is at or before lower exhausts.
	if lower := i.boundaries[0]; lower != nil && i.cmp.Compare(key, lower) <= 0 {
		i.dir = -1
		i.exhaust()
		return nil
	}
	// Find the region containing the predecessor of key: largest R such that
	// boundaries[R] < key.
	//
	// We only need to check whether the predecessor belongs in regions
	// 1..numRegions-1; if not, it must belong in region 0 (we already
	// returned early above if key was at or before the lower boundary).
	region := i.numRegions - 1
	for region > 0 && i.cmp.Compare(key, i.boundaries[region]) <= 0 {
		region--
	}
	return i.emitBackward(region)
}

// Next implements InternalIterator.
func (i *SingleSpanIter) Next() *base.InternalKV {
	if i.dir == -1 {
		// Switch to forward.
		if !i.presentedSpan.Valid() {
			// Exhausted backward: equivalent to a fresh forward start.
			return i.emitForward(0)
		}
		// Re-emit the forward boundary at the current region.
		return i.emitForward(i.currentRegion)
	}
	// dir == +1 (or 0 = unpositioned).
	if !i.presentedSpan.Valid() {
		return nil
	}
	// Prefix exhaustion: if the current boundary doesn't match the seek
	// prefix, exhaust without advancing.
	if i.prefix != nil && !i.cmp.HasPrefix(i.presentedSpan.Boundary, i.prefix) {
		i.exhaust()
		return nil
	}
	if i.currentRegion+1 >= i.numRegions {
		i.exhaust()
		return nil
	}
	return i.emitForward(i.currentRegion + 1)
}

// Prev implements InternalIterator.
func (i *SingleSpanIter) Prev() *base.InternalKV {
	if i.dir == +1 {
		// Switch to backward.
		if !i.presentedSpan.Valid() {
			// Exhausted forward: equivalent to a fresh backward start.
			return i.emitBackward(i.numRegions - 1)
		}
		// Re-emit the backward boundary at the current region.
		return i.emitBackward(i.currentRegion)
	}
	// dir == -1 (or 0 = unpositioned).
	if !i.presentedSpan.Valid() {
		return nil
	}
	if i.currentRegion == 0 {
		i.exhaust()
		return nil
	}
	return i.emitBackward(i.currentRegion - 1)
}

// NextPrefix implements InternalIterator. It is illegal to call NextPrefix on
// a SingleSpanIter: NextPrefix is only meaningful when positioned at a point
// key, and this iterator only produces span boundary keys.
func (i *SingleSpanIter) NextPrefix(_ []byte) *base.InternalKV {
	panic(errors.AssertionFailedf("NextPrefix called on SingleSpanIter"))
}

// SetBounds implements InternalIterator.
func (i *SingleSpanIter) SetBounds(lower, upper []byte) {
	i.prefix = nil
	i.dir = 0
	i.exhaust()
	i.computeRegions(lower, upper)
}

// Error implements InternalIterator.
func (i *SingleSpanIter) Error() error {
	return nil
}

// Close implements InternalIterator.
func (i *SingleSpanIter) Close() error {
	return nil
}

// SetContext implements InternalIterator.
func (i *SingleSpanIter) SetContext(_ context.Context) {}

// String implements fmt.Stringer.
func (i *SingleSpanIter) String() string {
	t := i.keys[0].Trailer
	return fmt.Sprintf("single-span-iter([%q, %q) %s#%d)",
		i.spanStart, i.spanEnd, t.Kind(), t.SeqNum())
}

// TreeStepsNode implements treesteps.Node.
func (i *SingleSpanIter) TreeStepsNode() treesteps.NodeInfo {
	return treesteps.NodeInfof(i, "SingleSpanIter")
}
