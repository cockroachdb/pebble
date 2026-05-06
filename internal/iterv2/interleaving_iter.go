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

// InterleavingIter wraps a base.InternalIterator (points) and a
// keyspan.FragmentIterator (spans) and implements the iterv2.Iter interface.
//
// Given an overall [startKey, endKey) range, it partitions the key space into
// contiguous spans (from the fragment iterator, plus synthesized empty gap
// spans) and emits synthetic boundary keys when crossing span boundaries
// during Next/Prev.
type InterleavingIter struct {
	cmp       *base.Comparer
	pointIter base.InternalIterator
	spanIter  keyspan.FragmentIterator

	// [startKey, endKey) is the static key range for this iterator. The
	// wrapped iterators are assumed to only return keys within this range (no
	// key comparisons are performed to enforce it). These bounds are used as
	// fallback gap boundaries when lower/upper are not set.
	startKey []byte
	endKey   []byte

	// lower and upper are optional dynamic bounds that restrict the iteration
	// range within [startKey, endKey). Unlike startKey/endKey, these are
	// enforced via key comparisons. When set, First/Last panic (the caller
	// must use SeekGE/SeekLT instead). Nil means unbounded in that direction.
	lower []byte
	upper []byte

	// pointKV is the last KV returned by pointIter. This is also the last KV
	// returned by this iterator, unless the last KV returned was a boundary key
	// (atBoundary=true); in the latter case, pointKV is the next point in the
	// direction of iteration.
	pointKV *base.InternalKV

	// span is the current position of spanIter (the last result of a spanIter
	// operation). If inSpan is true, the current position of this iterator is
	// inside this span; otherwise span is the next span in the direction of
	// iteration.
	span *keyspan.Span

	// dir is +1 for forward, -1 for backward (and 0 for unpositioned).
	dir int8
	// inSpan indicates whether the current position is inside span. When inSpan is true,
	// the presentedSpan Boundary matches span.End (forward iteration) or span.End
	// (backward iteration).
	inSpan bool
	// atBoundary indicates if the last returned key was a boundary key.
	atBoundary bool
	// presentedSpan is the iterv2.Span presented by this iterator via Span().
	presentedSpan Span
	// kv is used to avoid allocations when returning boundary keys.
	kv     base.InternalKV
	prefix []byte
	err    error

	tmpBuf []byte
}

var _ Iter = (*InterleavingIter)(nil)

// Init initializes the InterleavingIter.
//
// spanIter may be nil, in which case the iterator behaves as if there are no
// spans (the entire key range is one empty gap).
//
// There are two sets of bounds:
//
// [startKey, endKey) defines the static key range owned by this iterator.
// Everything returned by the wrapped iterators is assumed to already be within
// this range (no key comparisons are performed to enforce it). These are used
// to determine gap span boundaries when no dynamic bounds are set. In the level
// iterator, startKey/endKey are derived from file boundaries and divide the
// entire keyspace exactly between InterleavingIters. Forward seeks at keys
// below startKey are equivalent doing the seek at startKey. Similarly, SeekLT
// at a key above endKey is equivalent to SeekLT(endKey).
//
// lower and upper are optional dynamic bounds; nil means unbounded in that
// direction. They restrict the iteration range within [startKey, endKey) and
// are enforced via key comparisons. When set, First/Last cannot be used (the
// caller must use SeekGE/SeekLT instead); they are equivalent to SetBounds
// arguments.
func (i *InterleavingIter) Init(
	cmp *base.Comparer,
	pointIter base.InternalIterator,
	spanIter keyspan.FragmentIterator,
	startKey, endKey []byte,
	lower, upper []byte,
) {
	*i = InterleavingIter{
		cmp:       cmp,
		pointIter: pointIter,
		spanIter:  spanIter,
		startKey:  startKey,
		endKey:    endKey,
		lower:     lower,
		upper:     upper,
	}
	i.checkBounds(lower, upper)
}

func (i *InterleavingIter) checkBounds(lower, upper []byte) {
	if invariants.Enabled {
		if lower == nil && upper == nil {
			return
		}
		if i.startKey != nil {
			if (lower != nil && i.cmp.Compare(lower, i.startKey) < 0) ||
				(upper != nil && i.cmp.Compare(upper, i.startKey) <= 0) {
				panic(errors.AssertionFailedf("bounds %q %q before startKey %q", lower, upper, i.startKey))
			}
		}
		if i.endKey != nil {
			if (upper != nil && i.cmp.Compare(upper, i.endKey) > 0) ||
				(lower != nil && i.cmp.Compare(lower, i.endKey) >= 0) {
				panic(errors.AssertionFailedf("bounds %q %q after endKey %q", lower, upper, i.endKey))
			}
		}
	}
}

// effectiveLower returns the effective lower bound: lower if set, otherwise
// startKey.
func (i *InterleavingIter) effectiveLower() []byte {
	if i.lower != nil {
		return i.lower
	}
	return i.startKey
}

// effectiveUpper returns the effective upper bound: upper if set, otherwise
// endKey.
func (i *InterleavingIter) effectiveUpper() []byte {
	if i.upper != nil {
		return i.upper
	}
	return i.endKey
}

// geUpper returns true if key is at or past the upper bound. Returns false when
// upper is nil (unbounded above) or key is nil.
func (i *InterleavingIter) geUpper(key []byte) bool {
	return i.upper != nil && key != nil && i.cmp.Compare(key, i.upper) >= 0
}

// geEffectiveUpper returns true if key is at or past the effective upper bound.
func (i *InterleavingIter) geEffectiveUpper(key []byte) bool {
	upper := i.effectiveUpper()
	return upper != nil && key != nil && i.cmp.Compare(key, upper) >= 0
}

// leLower returns true if key is at or before the lower bound. Returns false
// when lower is nil (unbounded below) or key is nil.
func (i *InterleavingIter) leLower(key []byte) bool {
	return i.lower != nil && key != nil && i.cmp.Compare(key, i.lower) <= 0
}

// leEffectiveLower returns true if key is at or before the effective lower bound.
func (i *InterleavingIter) leEffectiveLower(key []byte) bool {
	lower := i.effectiveLower()
	return lower != nil && key != nil && i.cmp.Compare(key, lower) <= 0
}

// computeCurrentSpan sets presentedSpan based on inSpan, span, and dir.
//
// Boundary is set to the next boundary in the current iteration direction,
// clamped to iteration bounds. BoundaryType is set to BoundaryEnd (forward) or
// BoundaryStart (backward).
//
// Boundary can be nil when the range is unbounded in that direction.
func (i *InterleavingIter) computeCurrentSpan() {
	if i.inSpan {
		i.presentedSpan.Keys = i.span.Keys
		if i.dir >= 0 {
			// Forward: boundary is the span's End (clamped to upper).
			end := i.span.End
			if i.geUpper(end) {
				end = i.upper
			}
			i.presentedSpan.BoundaryType = BoundaryEnd
			i.presentedSpan.Boundary = end
		} else {
			// Backward: boundary is the span's Start (clamped to lower).
			start := i.span.Start
			if i.leLower(start) {
				start = i.lower
			}
			i.presentedSpan.BoundaryType = BoundaryStart
			i.presentedSpan.Boundary = start
		}
	} else {
		i.presentedSpan.Keys = nil
		if i.dir >= 0 {
			// Forward: boundary is the next span's Start or upper.
			i.presentedSpan.BoundaryType = BoundaryEnd
			if i.span != nil {
				i.presentedSpan.Boundary = i.span.Start
			} else {
				i.presentedSpan.Boundary = i.effectiveUpper() // can be nil
			}
		} else {
			// Backward: boundary is the previous span's End or lower.
			i.presentedSpan.BoundaryType = BoundaryStart
			if i.span != nil {
				i.presentedSpan.Boundary = i.span.End
			} else {
				i.presentedSpan.Boundary = i.effectiveLower() // can be nil
			}
		}
	}
}

// emitBoundary sets atBoundary=true, builds a synthetic key with
// SpanBoundary kind + SeqNumMax. Does NOT update presentedSpan (stays as
// the exiting span).
func (i *InterleavingIter) emitBoundary(userKey []byte) *base.InternalKV {
	i.atBoundary = true
	i.kv = base.InternalKV{
		K: base.MakeInternalKey(userKey, base.SeqNumMax, base.InternalKeyKindSpanBoundary),
	}
	return &i.kv
}

// positionSpanIterForward positions the fragment iterator for forward iteration
// starting from pos. Sets span and inSpan.
//
// After this call, span is the first span with End > pos, or nil if none. When
// inSpan is true, pos is inside span.
func (i *InterleavingIter) positionSpanIterForward(pos []byte, flags base.SeekGEFlags) {
	if i.spanIter == nil {
		i.inSpan = false
		i.span = nil
		i.computeCurrentSpan()
		return
	}
	// Try to avoid reseeking. We can do this in TrySeekUsingNext mode if pos is
	// not beyond the current known span; otherwise we can do this if pos happens
	// to be inside the known span.
	var currentSpanOK bool
	if flags.TrySeekUsingNext() {
		currentSpanOK = i.span == nil || i.cmp.Compare(pos, i.span.End) < 0
	} else {
		currentSpanOK = i.span != nil && i.span.Contains(i.cmp.Compare, pos)
	}
	if !currentSpanOK {
		i.nextSpan(i.spanIter.SeekGE(pos))
	}
	i.inSpan = false
	if i.span != nil {
		if i.startKey != nil && i.cmp.Compare(pos, i.startKey) < 0 {
			pos = i.startKey
		}
		i.inSpan = i.cmp.Compare(pos, i.span.Start) >= 0
	}
	i.computeCurrentSpan()
}

func (i *InterleavingIter) nextSpan(s *keyspan.Span, err error) {
	if err != nil {
		i.setError(err)
		return
	}
	i.checkSpan(s)
	if s != nil && i.geUpper(s.Start) {
		s = nil
	}
	i.span = s
}

// positionSpanIterBackward positions the fragment iterator for backward
// iteration before pos. Sets span and inSpan.
//
// After this call, span is the last span with Start < pos, or nil if none. When
// inSpan is true, pos is inside span (or touching the end of the span).
func (i *InterleavingIter) positionSpanIterBackward(pos []byte) {
	if i.spanIter == nil {
		i.span = nil
		i.inSpan = false
		i.computeCurrentSpan()
		return
	}
	i.prevSpan(i.spanIter.SeekLT(pos))
	i.inSpan = false
	if i.span != nil {
		if i.endKey != nil && i.cmp.Compare(pos, i.endKey) > 0 {
			pos = i.endKey
		}
		i.inSpan = i.cmp.Compare(pos, i.span.End) <= 0
	}
	i.computeCurrentSpan()
}

func (i *InterleavingIter) prevSpan(s *keyspan.Span, err error) {
	if err != nil {
		i.setError(err)
		return
	}
	i.checkSpan(s)
	if s != nil && i.leLower(s.End) {
		s = nil
	}
	i.span = s
}

// exhaust zeros out the current span. Called when the iterator is
// exhausted or hits an error.
func (i *InterleavingIter) exhaust() {
	i.presentedSpan = Span{}
	i.inSpan = false
}

func (i *InterleavingIter) isExhausted() bool {
	return !i.presentedSpan.Valid()
}

func (i *InterleavingIter) setError(err error) {
	i.err = err
	i.dir = 0
	i.pointKV = nil
	i.exhaust()
}

// Span implements Iter.
func (i *InterleavingIter) Span() *Span {
	return &i.presentedSpan
}

// SeekGE implements InternalIterator.
func (i *InterleavingIter) SeekGE(key []byte, flags base.SeekGEFlags) *base.InternalKV {
	return i.seekGEHelper(key, flags, i.pointIter.SeekGE(key, flags))
}

func (i *InterleavingIter) seekGEHelper(
	key []byte, flags base.SeekGEFlags, kv *base.InternalKV,
) *base.InternalKV {
	if invariants.Enabled && i.lower != nil && i.cmp.Compare(key, i.lower) < 0 {
		panic(errors.AssertionFailedf("forward seek (%q) before lower bound %q", key, i.lower))
	}
	i.checkPoint(kv)
	i.prefix = nil
	i.dir = +1
	i.atBoundary = false
	i.err = nil
	i.pointKV = kv
	if kv == nil {
		if err := i.pointIter.Error(); err != nil {
			i.setError(err)
			return nil
		}
		if i.geEffectiveUpper(key) {
			i.exhaust()
			return nil
		}
	}
	i.positionSpanIterForward(key, flags)
	return i.resolveForward()
}

// SeekPrefixGE implements InternalIterator.
func (i *InterleavingIter) SeekPrefixGE(
	prefix, key []byte, flags base.SeekGEFlags,
) *base.InternalKV {
	if invariants.Enabled {
		if i.lower != nil && i.cmp.Compare(key, i.lower) < 0 {
			panic(errors.AssertionFailedf("forward seek (%q) before lower bound %q", key, i.lower))
		}
		if !i.cmp.HasPrefix(key, prefix) {
			panic(errors.AssertionFailedf("prefix %q does not match key %q", prefix, key))
		}
	}
	kv := i.pointIter.SeekPrefixGE(prefix, key, flags)
	i.checkPoint(kv)
	if invariants.Enabled && kv != nil && !i.cmp.HasPrefix(kv.K.UserKey, prefix) {
		panic(errors.AssertionFailedf("pointIter %T did not enforce strict prefix iteration", i.pointIter))
	}
	i.prefix = prefix
	i.dir = +1
	i.atBoundary = false
	i.err = nil
	i.pointKV = kv
	if kv == nil {
		if err := i.pointIter.Error(); err != nil {
			i.setError(err)
			return nil
		}
		if i.geEffectiveUpper(key) {
			i.exhaust()
			return nil
		}
	}
	i.positionSpanIterForward(key, flags)
	return i.resolveForward()
}

// SeekLT implements InternalIterator.
func (i *InterleavingIter) SeekLT(key []byte, flags base.SeekLTFlags) *base.InternalKV {
	return i.seekLTHelper(key, i.pointIter.SeekLT(key, flags))
}

func (i *InterleavingIter) seekLTHelper(key []byte, kv *base.InternalKV) *base.InternalKV {
	if invariants.Enabled && i.upper != nil && i.cmp.Compare(key, i.upper) > 0 {
		panic(errors.AssertionFailedf("SeekLT(%q) after upper bound %q", key, i.upper))
	}
	i.checkPoint(kv)
	i.prefix = nil
	i.dir = -1
	i.atBoundary = false
	i.err = nil
	i.pointKV = kv

	if kv == nil {
		if err := i.pointIter.Error(); err != nil {
			i.setError(err)
			return nil
		}
		if i.leEffectiveLower(key) {
			i.exhaust()
			return nil
		}
	}
	i.positionSpanIterBackward(key)
	return i.resolveBackward()
}

// First implements InternalIterator.
func (i *InterleavingIter) First() *base.InternalKV {
	i.dir = +1
	i.prefix = nil
	i.atBoundary = false
	i.err = nil
	if invariants.Enabled && i.lower != nil {
		panic(errors.AssertionFailedf("First with lower bound set"))
	}

	i.pointKV = i.pointIter.First()
	if i.pointKV == nil {
		if err := i.pointIter.Error(); err != nil {
			i.setError(err)
			return nil
		}
	}

	if i.pointKV == nil && i.geUpper(i.startKey) {
		// Empty range.
		i.exhaust()
		return nil
	}

	if i.spanIter != nil {
		i.nextSpan(i.spanIter.First())
		i.inSpan = i.span != nil && i.leEffectiveLower(i.span.Start)
	} else {
		i.span = nil
		i.inSpan = false
	}
	i.computeCurrentSpan()
	return i.resolveForward()
}

// Last implements InternalIterator.
func (i *InterleavingIter) Last() *base.InternalKV {
	i.dir = -1
	i.prefix = nil
	i.atBoundary = false
	i.err = nil
	if invariants.Enabled && i.upper != nil {
		panic(errors.AssertionFailedf("Last with upper bound set"))
	}

	i.pointKV = i.pointIter.Last()
	if i.pointKV == nil {
		if err := i.pointIter.Error(); err != nil {
			i.setError(err)
			return nil
		}
	}

	if i.pointKV == nil && i.leLower(i.endKey) {
		// Empty range.
		i.exhaust()
		return nil
	}

	if i.spanIter != nil {
		i.prevSpan(i.spanIter.Last())
		i.inSpan = i.span != nil && i.geEffectiveUpper(i.span.End)
	} else {
		i.span = nil
		i.inSpan = false
	}
	i.computeCurrentSpan()
	return i.resolveBackward()
}

// Next implements InternalIterator.
func (i *InterleavingIter) Next() *base.InternalKV {
	if i.dir == -1 {
		return i.switchToForward()
	}

	if i.atBoundary {
		i.atBoundary = false
		if i.prefix != nil && !i.cmp.HasPrefix(i.presentedSpan.Boundary, i.prefix) {
			// Exhaust the iterator but don't invalidate i.span; it will be useful for
			// TrySeekUsingNext.
			i.presentedSpan = Span{}
			return nil
		}
		i.updateSpanAfterForwardBoundary()
	} else if i.pointKV != nil {
		i.pointKV = i.pointIter.Next()
		if i.pointKV == nil {
			if err := i.pointIter.Error(); err != nil {
				i.setError(err)
				return nil
			}
		} else if invariants.Enabled && i.prefix != nil && !i.cmp.HasPrefix(i.pointKV.K.UserKey, i.prefix) {
			panic(errors.AssertionFailedf("pointIter %T did not enforce strict prefix iteration", i.pointIter))
		}
		// If pointKV is outside the current span, we will emit a boundary in
		// resolveForward.
	}
	return i.resolveForward()
}

// resolveForward returns either the next point (pointKV) or a span boundary key,
// whichever comes first.
func (i *InterleavingIter) resolveForward() *base.InternalKV {
	if i.err != nil {
		return nil
	}

	if i.isExhausted() {
		return nil
	}

	if i.presentedSpan.Boundary == nil {
		// No boundary in this direction (unbounded). Return the point key
		// if we have one; otherwise, the iterator is exhausted.
		if i.pointKV == nil {
			i.exhaust()
		}
		return i.pointKV
	}

	if i.pointKV != nil {
		if i.cmp.Compare(i.pointKV.K.UserKey, i.presentedSpan.Boundary) < 0 {
			return i.pointKV
		}
	}

	return i.emitBoundary(i.presentedSpan.Boundary)
}

// updateSpanAfterForwardBoundary advances fragment state after a forward
// boundary was emitted.
func (i *InterleavingIter) updateSpanAfterForwardBoundary() {
	if i.span == nil || i.geEffectiveUpper(i.presentedSpan.Boundary) {
		// We reached the end.
		i.exhaust()
		return
	}
	if i.inSpan {
		// Was in fragment span, now potentially entering gap after it.
		i.tmpBuf = append(i.tmpBuf[:0], i.span.End...)
		lastEnd := i.tmpBuf
		i.nextSpan(i.spanIter.Next())
		i.inSpan = i.span != nil && i.cmp.Compare(i.span.Start, lastEnd) == 0
	} else {
		// Was in a gap, now entering the fragment span.
		i.inSpan = true
	}
	i.computeCurrentSpan()
}

// Prev implements InternalIterator.
func (i *InterleavingIter) Prev() *base.InternalKV {
	if i.dir == +1 {
		return i.switchToBackward()
	}

	if i.atBoundary {
		i.atBoundary = false
		i.updateSpanAfterBackwardBoundary()
	} else if i.pointKV != nil {
		i.pointKV = i.pointIter.Prev()
		if i.pointKV == nil {
			if err := i.pointIter.Error(); err != nil {
				i.setError(err)
				return nil
			}
		}
		// If pointKV is outside the current span, we will emit a boundary in
		// resolveBackward.
	}
	return i.resolveBackward()
}

// resolveBackward checks if the current pointKV has crossed a span boundary
// going backward. If so, it emits a boundary key. Otherwise returns pointKV.
func (i *InterleavingIter) resolveBackward() *base.InternalKV {
	if i.err != nil {
		return nil
	}

	if i.isExhausted() {
		return nil
	}

	if i.presentedSpan.Boundary == nil {
		// No boundary in this direction (unbounded). Return the point key
		// if we have one; otherwise, the iterator is exhausted.
		if i.pointKV == nil {
			i.exhaust()
		}
		return i.pointKV
	}

	if i.pointKV != nil {
		if i.cmp.Compare(i.pointKV.K.UserKey, i.presentedSpan.Boundary) >= 0 {
			return i.pointKV
		}
	}

	return i.emitBoundary(i.presentedSpan.Boundary)
}

// updateSpanAfterBackwardBoundary updates fragment state after a backward
// boundary was emitted.
func (i *InterleavingIter) updateSpanAfterBackwardBoundary() {
	if i.span == nil || i.leEffectiveLower(i.presentedSpan.Boundary) {
		// We reached the end.
		i.exhaust()
		return
	}
	if i.inSpan {
		// Was in fragment span, now potentially entering gap before it.
		i.tmpBuf = append(i.tmpBuf[:0], i.span.Start...)
		lastStart := i.tmpBuf
		i.prevSpan(i.spanIter.Prev())
		i.inSpan = i.span != nil && i.cmp.Compare(i.span.End, lastStart) == 0
	} else {
		// Was in a gap, now entering the fragment span.
		i.inSpan = true
	}
	i.computeCurrentSpan()
}

// switchToForward handles a direction change from backward to forward.
func (i *InterleavingIter) switchToForward() *base.InternalKV {
	if i.err != nil {
		return nil
	}

	if i.isExhausted() {
		if i.lower == nil {
			return i.First()
		}
		return i.seekGEHelper(i.lower, base.SeekGEFlagsNone, i.pointIter.SeekGE(i.lower, base.SeekGEFlagsNone))
	}

	i.dir = +1
	i.atBoundary = false
	i.pointKV = i.pointIter.Next()
	if i.pointKV == nil {
		if err := i.pointIter.Error(); err != nil {
			i.setError(err)
			return nil
		}
	}
	if !i.inSpan && i.spanIter != nil {
		i.nextSpan(i.spanIter.Next())
	}
	i.computeCurrentSpan()
	return i.resolveForward()
}

// switchToBackward handles a direction change from forward to backward.
func (i *InterleavingIter) switchToBackward() *base.InternalKV {
	if i.err != nil {
		return nil
	}

	if i.isExhausted() {
		if i.upper == nil {
			return i.Last()
		}
		return i.seekLTHelper(i.upper, i.pointIter.SeekLT(i.upper, base.SeekLTFlagsNone))
	}

	i.dir = -1
	i.atBoundary = false
	i.pointKV = i.pointIter.Prev()
	if i.pointKV == nil {
		if err := i.pointIter.Error(); err != nil {
			i.setError(err)
			return nil
		}
	}
	if !i.inSpan && i.spanIter != nil {
		i.prevSpan(i.spanIter.Prev())
	}
	i.computeCurrentSpan()
	return i.resolveBackward()
}

// NextPrefix implements InternalIterator.
func (i *InterleavingIter) NextPrefix(succKey []byte) *base.InternalKV {
	if invariants.Enabled && i.atBoundary {
		panic(errors.AssertionFailedf("NextPrefix called when at boundary key"))
	}
	var kv *base.InternalKV
	if i.pointKV != nil {
		kv = i.pointIter.NextPrefix(succKey)
	}
	return i.seekGEHelper(succKey, base.SeekGEFlagsNone.EnableTrySeekUsingNext(), kv)
}

// SetBounds implements InternalIterator.
func (i *InterleavingIter) SetBounds(lower, upper []byte) {
	i.checkBounds(lower, upper)
	i.pointIter.SetBounds(lower, upper)
	i.lower = lower
	i.upper = upper
	i.dir = 0
	i.atBoundary = false
	i.pointKV = nil
	i.span = nil
	i.inSpan = false
	i.err = nil
	i.exhaust()
}

// Error implements InternalIterator.
func (i *InterleavingIter) Error() error {
	if i.err != nil {
		return i.err
	}
	return i.pointIter.Error()
}

// Close implements InternalIterator.
func (i *InterleavingIter) Close() error {
	var err error
	if i.pointIter != nil {
		err = i.pointIter.Close()
		i.pointIter = nil
	}
	if i.spanIter != nil {
		i.spanIter.Close()
	}
	return err
}

// SetContext implements InternalIterator.
func (i *InterleavingIter) SetContext(ctx context.Context) {
	i.pointIter.SetContext(ctx)
	if i.spanIter != nil {
		i.spanIter.SetContext(ctx)
	}
}

// String implements fmt.Stringer.
func (i *InterleavingIter) String() string {
	if i.spanIter != nil {
		return fmt.Sprintf("interleaving(%s, %s)", i.pointIter.String(), i.spanIter)
	}
	return fmt.Sprintf("interleaving(%s, nil)", i.pointIter.String())
}

// TreeStepsNode implements treesteps.Node.
func (i *InterleavingIter) TreeStepsNode() treesteps.NodeInfo {
	info := treesteps.NodeInfof(i, "InterleavingIter")
	info.AddChildren(i.pointIter)
	if i.spanIter != nil {
		info.AddChildren(i.spanIter)
	}
	return info
}

func (i *InterleavingIter) checkPoint(kv *base.InternalKV) {
	if invariants.Enabled && kv != nil {
		if i.startKey != nil && i.cmp.Compare(kv.K.UserKey, i.startKey) < 0 {
			panic(errors.AssertionFailedf("point %s returned despite absolute bounds [%q, %q)", kv.K, i.startKey, i.endKey))
		}
		if i.endKey != nil && i.cmp.Compare(kv.K.UserKey, i.endKey) >= 0 {
			panic(errors.AssertionFailedf("point %s returned despite absolute bounds [%q, %q)", kv.K, i.startKey, i.endKey))
		}
		if i.lower != nil && i.cmp.Compare(kv.K.UserKey, i.lower) < 0 {
			panic(errors.AssertionFailedf("point %s returned despite lower bound %q", kv.K, i.lower))
		}
		if i.upper != nil && i.cmp.Compare(kv.K.UserKey, i.upper) >= 0 {
			panic(errors.AssertionFailedf("point %s returned despite upper bound %q", kv.K, i.upper))
		}
		if kind := kv.Kind(); kind == base.InternalKeyKindRangeDelete || (kind >= base.InternalKeyKindRangeKeyMin && kind <= base.InternalKeyKindRangeKeyMax) {
			panic(errors.AssertionFailedf("point %s has invalid kind", kv.K))
		}
	}
}

func (i *InterleavingIter) checkSpan(span *keyspan.Span) {
	if invariants.Enabled && span != nil {
		if i.startKey != nil && i.cmp.Compare(span.Start, i.startKey) < 0 {
			panic(errors.AssertionFailedf("span %s returned despite absolute bounds [%q, %q)", span, i.startKey, i.endKey))
		}
		if i.endKey != nil && i.cmp.Compare(span.End, i.endKey) > 0 {
			panic(errors.AssertionFailedf("span %s returned despite absolute bounds [%q, %q)", span, i.startKey, i.endKey))
		}
	}
}
