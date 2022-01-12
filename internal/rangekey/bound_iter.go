// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rangekey

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
)

type boundKind int8

const (
	boundKindInvalid boundKind = iota
	boundKindFragmentStart
	boundKindFragmentEnd
	// TODO(jackson): Once there's a levelIter-type that provides a continuous
	// iterator over the range key blocks of a level, we'll also need bound
	// kinds for the table bounds.
	// boundKindTableStart
	// boundKindTableEnd
)

type boundKey struct {
	kind boundKind
	key  []byte
	// fragments holds a set of fragments with identical user key bounds, all
	// sharing this bound. The ordering of fragments is unspecified and depends
	// on internal iteration implementation details.
	//
	// All fragments have the same user key bounds but vary in sequence number
	// and kind. If the boundKind is boundKindFragmentStart, all fragments'
	// Start keys have user keys equal to the boundKey's key field. If the
	// boundKind is boundKindFragmentEnd, all fragments' End keys equal the
	// boundKey's key field.
	fragments []keyspan.Span
}

func (k boundKey) valid() bool {
	return k.kind != boundKindInvalid
}

func (k boundKey) String() string {
	var buf bytes.Buffer
	switch k.kind {
	case boundKindInvalid:
		fmt.Fprint(&buf, "invalid")
	case boundKindFragmentStart:
		fmt.Fprint(&buf, "fragment-start")
	case boundKindFragmentEnd:
		fmt.Fprint(&buf, "fragment-end  ")
	default:
		fmt.Fprintf(&buf, "unknown-kind(%d)", k.kind)
	}
	fmt.Fprintf(&buf, " %s [", k.key)
	for i := range k.fragments {
		if i > 0 {
			fmt.Fprint(&buf, ", ")
		}
		fmt.Fprintf(&buf, "%s", k.fragments[i])
	}
	fmt.Fprint(&buf, "]")
	return buf.String()
}

// fragmentBoundIterator wraps a fragment iterator, exposing an iterator over
// the fragments' individual bounds. Consider a fragment iterator containing the
// following fragments:
//
//    [a, c)#2  [a, c)#1  [c, d)#1
//
// A fragmentBoundIterator wrapping the above fragments exposes 4 entries:
//
//     fragment-start a [a-c#2, a-c#1]
//     fragment-end   c [a-c#2, a-c#1]
//     fragment-start c [c-d#1]
//     fragment-end   d [c-d#1]
//
// Along with the key and bound kind, fragmentBoundIterator surfaces the set of
// all the fragments that share that bound. fragmentBoundIterator is used
// internally by the range-key merging iterator.
//
// Note that fragmentBoundIterator's interface differs from FragmentIterator.
// The fragmentBoundIterator iterates over individual bounds, including end
// boundaries, whereas FragmentIterator returns whole keyspan.Spans ordered by
// Start key. The fragmentBoundIterator's interface is designed this way to
// simplify the mergingIter implementation, which must include both start and
// end boundaries in its heap.
type fragmentBoundIterator struct {
	// iter holds the underlying fragment iterator. iterSpan is always set to
	// the span at iter's current position.
	iter     keyspan.FragmentIterator
	iterSpan keyspan.Span
	// start and end hold the user keys for the iterator's current fragment. One
	// of either the start or the end is the current iterator position,
	// indicated by boundKind. All of the underlying iterators fragments with
	// the bounds [start, end) are contained within the fragments field.
	start     []byte
	end       []byte
	fragments []keyspan.Span
	boundKind boundKind
	dir       int8
}

func (i *fragmentBoundIterator) init(fragmentIter keyspan.FragmentIterator) {
	*i = fragmentBoundIterator{iter: fragmentIter}
}

// seekGE returns the smallest bound ≥ key. It searches through both start and
// end bounds, and the returned bound may represent fragments' exclusive end
// boundary. Note that this differs from FragmentIterator.SeekGE's semantics,
// which returns the fragment with the smallest start key ≥ key. This interface
// is useful within the merging iterator implementation, which must include all
// bounds, including both start and end boundaries, in its heap.
func (i *fragmentBoundIterator) seekGE(cmp base.Compare, key []byte) boundKey {
	// Search for the span with the largest Start < key.
	_, _ = i.iter.SeekLT(key)
	i.iterSpan = i.iter.Current()

	if !i.iterSpan.Empty() && cmp(i.iterSpan.End, key) >= 0 {
		// i.iterSpan.End ≥ key
		// We need to return this span's end bound. Since we SeekLT'd, we're on
		// the last fragment with these bounds. We need to init backwards as a
		// result.
		return i.initBackwardSpan(cmp)
	}
	// i.iterSpan.End < key

	// This span ends before key. Next to the first span with a Start ≥ key,
	// and use that.
	_, _ = i.iter.Next()
	i.iterSpan = i.iter.Current()
	return i.initForwardSpan(cmp)
}

func (i *fragmentBoundIterator) seekLT(cmp base.Compare, key []byte) boundKey {
	_, _ = i.iter.SeekLT(key)
	i.iterSpan = i.iter.Current()
	bk := i.initBackwardSpan(cmp)
	// initBackwardSpan returns the end bound. If it's < key, return that.
	// Otherwise Prev to the Start bound which we already know is < key from
	// our i.iter.SeekLT(key).
	if cmp(bk.key, key) < 0 {
		return bk
	}
	return i.prev(cmp)
}

func (i *fragmentBoundIterator) first(cmp base.Compare) boundKey {
	_, _ = i.iter.First()
	i.iterSpan = i.iter.Current()
	return i.initForwardSpan(cmp)
}

func (i *fragmentBoundIterator) last(cmp base.Compare) boundKey {
	_, _ = i.iter.Last()
	i.iterSpan = i.iter.Current()
	return i.initBackwardSpan(cmp)
}

func (i *fragmentBoundIterator) next(cmp base.Compare) boundKey {
	if i.boundKind == boundKindFragmentStart {
		i.boundKind = boundKindFragmentEnd
		return boundKey{
			kind:      boundKindFragmentEnd,
			key:       i.end,
			fragments: i.fragments,
		}
	}
	if i.dir == -1 {
		// We were previously iterating in the reverse direction, so i.iter is
		// positioned at the last fragment with bounds less than the current
		// iterator position. We need to move it to be at the first fragment
		// with bounds greater than the current iterator position. We can avoid
		// the key comparisons because we know how many fragments exist at the
		// current iterator position.
		//
		//    [c, d)   [c, d)   [e, f)   [e, f)   [e, f)   [g, h)   [g, h)
		//               ^      \                      /     ^
		//             i.iter          current pos         i.iter
		//             before                              after
		for j := 0; j < 1+len(i.fragments); j++ {
			k, _ := i.iter.Next()
			if invariants.Enabled && j < len(i.fragments) && cmp(k.UserKey, i.start) > 0 {
				panic("pebble: invariant violation: switching direction")
			}
		}
		i.iterSpan = i.iter.Current()
	}
	return i.initForwardSpan(cmp)
}

func (i *fragmentBoundIterator) prev(cmp base.Compare) boundKey {
	if i.boundKind == boundKindFragmentEnd {
		i.boundKind = boundKindFragmentStart
		return boundKey{
			kind:      boundKindFragmentStart,
			key:       i.start,
			fragments: i.fragments,
		}
	}
	if i.dir == +1 {
		// If we were previously iterating in the forward direction, i.iter is
		// positioned at the first fragment with bounds greater than the current
		// iterator position. We need to move it to the last fragment with
		// bounds less than the current iterator position. We can avoid the key
		// comparisons because we know how many fragments exist at the current
		// iterator position.
		//
		//    [c, d)   [c, d)   [e, f)   [e, f)   [e, f)   [g, h)  [g, h)
		//               ^      \                      /     ^
		//             i.iter          current pos         i.iter
		//             after                               before
		//
		for j := 0; j < 1+len(i.fragments); j++ {
			k, _ := i.iter.Prev()
			if invariants.Enabled && j < len(i.fragments) && cmp(k.UserKey, i.start) < 0 {
				panic("pebble: invariant violation: switching direction")
			}
		}
		i.iterSpan = i.iter.Current()
	}
	return i.initBackwardSpan(cmp)
}

func (i *fragmentBoundIterator) initForwardSpan(cmp base.Compare) boundKey {
	i.dir = +1
	i.clearFragments()
	if i.iterSpan.Empty() {
		i.start, i.end = nil, nil
		i.boundKind = boundKindInvalid
		return boundKey{kind: boundKindInvalid}
	}
	i.start = i.iterSpan.Start.UserKey
	i.end = i.iterSpan.End

	// TODO(jackson): Consider lazily accumulating fragments with identical
	// bounds when the caller requests them. It would increase complexity, but
	// would save at least 1 key comparison when we don't require the fragments.

	// Accumulate all of the fragments with these identical bounds.
	for k, _ := i.iter.Next(); k != nil && cmp(k.UserKey, i.start) == 0; k, _ = i.iter.Next() {
		i.fragments = append(i.fragments, i.iterSpan)
		i.iterSpan = i.iter.Current()
	}
	i.fragments = append(i.fragments, i.iterSpan)
	i.iterSpan = i.iter.Current()

	// Since we're going forward, start at the smaller start boundary.
	i.boundKind = boundKindFragmentStart
	return boundKey{
		kind:      boundKindFragmentStart,
		key:       i.start,
		fragments: i.fragments,
	}
}

func (i *fragmentBoundIterator) initBackwardSpan(cmp base.Compare) boundKey {
	i.dir = -1
	i.clearFragments()
	if i.iterSpan.Empty() {
		i.start, i.end = nil, nil
		i.boundKind = boundKindInvalid
		return boundKey{kind: boundKindInvalid}
	}

	i.start = i.iterSpan.Start.UserKey
	i.end = i.iterSpan.End

	// Accumulate all of the fragments with these identical bounds.
	for k, _ := i.iter.Prev(); k != nil && cmp(k.UserKey, i.start) == 0; k, _ = i.iter.Prev() {
		i.fragments = append(i.fragments, i.iterSpan)
		i.iterSpan = i.iter.Current()
	}
	i.fragments = append(i.fragments, i.iterSpan)
	i.iterSpan = i.iter.Current()

	// Since we're going backwards, start at the larger end boundary.
	i.boundKind = boundKindFragmentEnd
	return boundKey{
		kind:      boundKindFragmentEnd,
		key:       i.end,
		fragments: i.fragments,
	}
}

func (i *fragmentBoundIterator) clearFragments() {
	for j := range i.fragments {
		// Clear any pointers into range key blocks to avoid retaining them.
		i.fragments[j] = keyspan.Span{}
	}
	i.fragments = i.fragments[:0]
}
