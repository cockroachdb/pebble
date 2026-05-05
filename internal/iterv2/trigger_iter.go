// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package iterv2

import (
	"context"

	"github.com/RaduBerinde/axisds/v3/regiontree"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/treesteps"
)

// BoundaryTrigger is called when the TriggerIter fires. The key is the
// boundary position; dir is +1 (forward) or -1 (backward).
type BoundaryTrigger interface {
	// Trigger is called when a TriggerIter reaches a region in the regiontree.
	// The key slice comes from the regiontree.
	Trigger(key []byte, dir int8)
}

// TriggerIter is an iterv2.Iter that consults a regiontree to emit a synthetic
// BOUNDARY key at the first position where iteration would touch a region with
// a non-zero count. When the merging iterator reaches that boundary (calling
// Next/Prev to advance past it), the TriggerIter fires its callback and
// permanently exhausts itself.
//
// This is used for lazy combined iteration: the TriggerIter is placed as a
// top-level child of the merging iterator to detect when point iteration
// reaches a region that may contain range key sets.
//
// When bounds are set, the TriggerIter will check the entire region and elide
// any work during iterator operations if there are no possible range key sets
// within the bounds.
type TriggerIter struct {
	cmp     base.Compare
	tree    *regiontree.T[[]byte, int]
	trigger BoundaryTrigger // set to nil after firing
	lower   []byte
	upper   []byte

	noRegions bool // true if no regions exist within [lower, upper)
	dir       int8 // +1 forward, -1 backward, 0 unpositioned or exhausted.
	// kv contains the closest boundary key in the iteration direction; empty if
	// dir is 0.
	kv base.InternalKV
	// span is presented via Span(). It presents the same boundary in kv; empty if
	// dir is 0.
	span Span
}

var _ Iter = (*TriggerIter)(nil)

// checkNoRegions sets noRegions to true if both bounds are set and there are no
// regions with non-zero count within [lower, upper).
func (t *TriggerIter) checkNoRegions() {
	if t.trigger == nil {
		t.noRegions = true
		return
	}
	l := regiontree.Min[[]byte]()
	if t.lower != nil {
		l = regiontree.GE(t.lower)
	}
	u := regiontree.Max[[]byte]()
	if t.upper != nil {
		u = regiontree.LT(t.upper)
	}
	t.noRegions = !t.tree.Any(l, u, func(v int) bool { return v > 0 })
}

// Init initializes the TriggerIter.
func (t *TriggerIter) Init(
	cmp base.Compare, tree *regiontree.T[[]byte, int], trigger BoundaryTrigger, lower, upper []byte,
) {
	*t = TriggerIter{
		cmp:     cmp,
		tree:    tree,
		trigger: trigger,
		lower:   lower,
		upper:   upper,
	}
	t.checkNoRegions()
}

// Reset the iterator and change the trigger. The trigger can be nil, in which
// case the iterator will be exhausted for all operations until the next Reset.
func (t *TriggerIter) Reset(trigger BoundaryTrigger) {
	t.dir = 0
	t.span = Span{}
	t.trigger = trigger
	t.kv = base.InternalKV{}
	t.checkNoRegions()
}

// makeBoundaryKey creates a synthetic boundary key at the given user key.
func makeBoundaryKey(key []byte) base.InternalKey {
	return base.MakeInternalKey(key, base.SeqNumMax, base.InternalKeyKindSpanBoundary)
}

// upperBound returns the regiontree upper bound for queries.
func (t *TriggerIter) upperBound() regiontree.UpperBound[[]byte] {
	if t.upper != nil {
		return regiontree.LT(t.upper)
	}
	return regiontree.Max[[]byte]()
}

// lowerBound returns the regiontree lower bound for queries.
func (t *TriggerIter) lowerBound() regiontree.LowerBound[[]byte] {
	if t.lower != nil {
		return regiontree.GE(t.lower)
	}
	return regiontree.Min[[]byte]()
}

// exhaust sets the iterator to the exhausted state.
func (t *TriggerIter) exhaust() {
	t.dir = 0
	t.span = Span{}
	t.kv = base.InternalKV{}
}

// fire triggers the callback and permanently exhausts the iterator.
func (t *TriggerIter) fire(key []byte, dir int8) {
	t.trigger.Trigger(key, dir)
	t.trigger = nil
	t.noRegions = true
	t.exhaust()
}

// seekForward implements the shared logic for SeekGE, First, SeekPrefixGE, and
// NextPrefix. If seekKey is non-nil and the seek key lands inside a region, the
// trigger fires immediately.
func (t *TriggerIter) seekForward(
	lower regiontree.LowerBound[[]byte], seekKey []byte,
) *base.InternalKV {
	if t.noRegions {
		return nil
	}
	for interval := range t.tree.Enumerate(lower, t.upperBound()) {
		// When seekKey is set, interval.Start is clipped to seekKey by
		// Enumerate, so interval.Start == seekKey means the seek key is
		// inside a region.
		if seekKey != nil && t.cmp(interval.Start, seekKey) == 0 {
			t.fire(seekKey, +1)
			return nil
		}
		// Region is ahead of the starting position.
		t.dir = +1
		t.span = Span{
			BoundaryType: BoundaryEnd,
			Boundary:     interval.Start,
		}
		t.kv = base.InternalKV{K: makeBoundaryKey(interval.Start)}
		return &t.kv
	}
	// No region found.
	t.exhaust()
	return nil
}

// seekBackward implements the shared logic for SeekLT and Last. If seekKey is
// non-nil and the seek key lands inside a region, the trigger fires
// immediately.
func (t *TriggerIter) seekBackward(
	upper regiontree.UpperBound[[]byte], seekKey []byte,
) *base.InternalKV {
	if t.noRegions {
		return nil
	}
	for interval := range t.tree.EnumerateDesc(upper, t.lowerBound()) {
		// When seekKey is set, interval.End is clipped to seekKey by EnumerateDesc,
		// so interval.End == seekKey means that either the seek key is inside a
		// region, or a region ends exactly at seekKey. In both cases, we should
		// fire (we are doing reverse iteration so the seekKey is an exclusive upper
		// bound).
		if seekKey != nil && t.cmp(interval.End, seekKey) == 0 {
			t.fire(seekKey, -1)
			return nil
		}
		// Region is below the starting position.
		t.dir = -1
		t.span = Span{
			BoundaryType: BoundaryStart,
			Boundary:     interval.End,
		}
		t.kv = base.InternalKV{K: makeBoundaryKey(interval.End)}
		return &t.kv
	}
	// No region found.
	t.exhaust()
	return nil
}

// First implements Iter. First must not be called when a lower bound is set;
// use SeekGE(lower) instead.
func (t *TriggerIter) First() *base.InternalKV {
	return t.seekForward(regiontree.Min[[]byte](), nil)
}

// Last implements Iter. Last must not be called when an upper bound is set;
// use SeekLT(upper) instead.
func (t *TriggerIter) Last() *base.InternalKV {
	return t.seekBackward(regiontree.Max[[]byte](), nil)
}

// SeekGE implements Iter.
func (t *TriggerIter) SeekGE(key []byte, flags base.SeekGEFlags) *base.InternalKV {
	if flags.TrySeekUsingNext() && invariants.Sometimes(50) {
		if invariants.Enabled && t.dir == -1 {
			panic(errors.AssertionFailedf("SeekGE with TrySeekUsingNext after backward positioning"))
		}
		if t.dir == 0 {
			// Iterator already exhausted.
			return nil
		}
		if t.cmp(key, t.kv.K.UserKey) <= 0 {
			// Fast path: the seek did not reach the boundary.
			return &t.kv
		}
		// Fall through.
	}
	return t.seekForward(regiontree.GE(key), key)
}

// SeekPrefixGE implements Iter.
func (t *TriggerIter) SeekPrefixGE(_, key []byte, flags base.SeekGEFlags) *base.InternalKV {
	return t.SeekGE(key, flags)
}

// SeekLT implements Iter.
func (t *TriggerIter) SeekLT(key []byte, _ base.SeekLTFlags) *base.InternalKV {
	return t.seekBackward(regiontree.LT(key), key)
}

// Next implements Iter.
func (t *TriggerIter) Next() *base.InternalKV {
	if t.noRegions {
		return nil
	}
	switch t.dir {
	case +1:
		// Positioned forward: advancing past the boundary fires the trigger.
		t.fire(t.kv.K.UserKey, +1)
		return nil
	case -1:
		// Was positioned backward; re-seek forward from the current boundary
		// to handle direction switches correctly. t.kv.K.UserKey is the exclusive
		// end key of the closest region before our position, and we know we are
		// inside a gap (or we would have fired already). So seeking forward from
		// this key will find the next boundary forward.
		//
		// For example, if we have regions [a, c) and [e, g) and last operation was
		// SeekLT(d), the current boundary is c. seeking forward from c will find
		// the [e, g) region.
		return t.seekForward(regiontree.GE(t.kv.K.UserKey), nil)
	default:
		// Exhausted (e.g. a previous SeekLT found nothing). Seek forward from the
		// lower bound.
		return t.seekForward(t.lowerBound(), nil)
	}
}

// NextPrefix implements Iter.
func (t *TriggerIter) NextPrefix(succKey []byte) *base.InternalKV {
	return t.SeekGE(succKey, base.SeekGEFlagsNone.EnableTrySeekUsingNext())
}

// Prev implements Iter.
func (t *TriggerIter) Prev() *base.InternalKV {
	if t.noRegions {
		return nil
	}
	switch t.dir {
	case -1:
		// Positioned backward: advancing past the boundary fires the trigger.
		t.fire(t.kv.K.UserKey, -1)
		return nil
	case +1:
		// Was positioned forward; re-seek backward from before current boundary to
		// handle direction switches correctly. t.kv.K.UserKey is the start key of
		// the closest region after our position, and we know we are inside a gap
		// (or we would have fired already). So seeking backward from this key will
		// find the next boundary backward.
		//
		// For example, if we have regions [a, c) and [e, g) and last operation was
		// SeekGE(d), the current boundary is e. seeking backward from e will find
		// the [a, c) region.
		return t.seekBackward(regiontree.LT(t.kv.K.UserKey), nil)
	default:
		// Exhausted (e.g. a previous SeekGE found nothing). Seek backward from the
		// upper bound.
		return t.seekBackward(t.upperBound(), nil)
	}
}

// Span implements Iter.
func (t *TriggerIter) Span() *Span {
	return &t.span
}

// SetBounds implements Iter.
func (t *TriggerIter) SetBounds(lower, upper []byte) {
	t.lower = lower
	t.upper = upper
	t.dir = 0
	t.span = Span{}
	t.kv = base.InternalKV{}
	t.checkNoRegions()
}

// Error implements Iter.
func (t *TriggerIter) Error() error { return nil }

// Close implements Iter.
func (t *TriggerIter) Close() error { return nil }

// SetContext implements Iter.
func (t *TriggerIter) SetContext(_ context.Context) {}

// String implements fmt.Stringer.
func (t *TriggerIter) String() string { return "trigger-iter" }

// TreeStepsNode implements treesteps.Node.
func (t *TriggerIter) TreeStepsNode() treesteps.NodeInfo {
	description := "TriggerIter"
	if t.trigger == nil {
		description += " (disabled)"
	} else if t.noRegions {
		description += " (no regions)"
	}
	return treesteps.NodeInfof(t, "%s", description)
}
