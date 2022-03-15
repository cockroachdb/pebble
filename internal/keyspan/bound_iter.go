// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/pebble/internal/base"
)

type boundKind int8

const (
	boundKindInvalid boundKind = iota
	boundKindFragmentStart
	boundKindFragmentEnd
)

type boundKey struct {
	kind boundKind
	key  []byte
	// span holds the span the bound key comes from.
	//
	// If kind is boundKindFragmentStart, then key is span.Start. If kind is
	// boundKindFragmentEnd, then key is span.End.
	span Span
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
	fmt.Fprintf(&buf, "%s", k.span)
	fmt.Fprint(&buf, "]")
	return buf.String()
}

// fragmentBoundIterator wraps a fragment iterator, exposing an iterator over
// the fragments' individual bounds. Consider a fragment iterator containing the
// following spans:
//
//    [a, c){#2,#1},  [c, d){#1}
//
// A fragmentBoundIterator wrapping the above fragments exposes 4 entries:
//
//     fragment-start a [a-c){#2,#1}
//     fragment-end   c [a-c){#2,#1}
//     fragment-start c [c-d){#1}
//     fragment-end   d [c-d){#1}
//
// Along with the key and bound kind, fragmentBoundIterator surfaces the span
// itself. fragmentBoundIterator is used internally by the span merging
// iterator.
//
// Note that fragmentBoundIterator's interface differs from FragmentIterator.
// The fragmentBoundIterator iterates over individual bounds, including end
// boundaries, whereas FragmentIterator returns whole Spans ordered by
// Start key. The fragmentBoundIterator's interface is designed this way to
// simplify the mergingIter implementation, which must include both start and
// end boundaries in its heap.
type fragmentBoundIterator struct {
	// iter holds the underlying fragment iterator. iterSpan is always set to
	// the span at iter's current position.
	iter      FragmentIterator
	iterSpan  Span
	boundKind boundKind
}

func (i *fragmentBoundIterator) init(fragmentIter FragmentIterator) {
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
	i.iterSpan = i.iter.SeekLT(key)

	if i.iterSpan.Valid() && cmp(i.iterSpan.End, key) >= 0 {
		// i.iterSpan.End ≥ key
		// We need to return this span's end bound.
		return i.yieldEndBound()
	}
	// i.iterSpan.End < key
	// This span ends before key. Next to the first span with a Start ≥ key, and
	// use that.
	i.iterSpan = i.iter.Next()
	return i.yieldStartBound()
}

func (i *fragmentBoundIterator) seekLT(cmp base.Compare, key []byte) boundKey {
	i.iterSpan = i.iter.SeekLT(key)
	bk := i.yieldEndBound()
	// yieldEndBound returns the end bound. If it's < key, return that.
	// Otherwise Prev to the Start bound which we already know is < key from
	// our i.iter.SeekLT(key).
	if cmp(bk.key, key) < 0 {
		return bk
	}
	return i.prev()
}

func (i *fragmentBoundIterator) first() boundKey {
	i.iterSpan = i.iter.First()
	return i.yieldStartBound()
}

func (i *fragmentBoundIterator) last() boundKey {
	i.iterSpan = i.iter.Last()
	return i.yieldEndBound()
}

func (i *fragmentBoundIterator) next() boundKey {
	if i.boundKind == boundKindFragmentStart {
		i.boundKind = boundKindFragmentEnd
		return boundKey{
			kind: boundKindFragmentEnd,
			key:  i.iterSpan.End,
			span: i.iterSpan,
		}
	}
	i.iterSpan = i.iter.Next()
	return i.yieldStartBound()
}

func (i *fragmentBoundIterator) prev() boundKey {
	if i.boundKind == boundKindFragmentEnd {
		i.boundKind = boundKindFragmentStart
		return boundKey{
			kind: boundKindFragmentStart,
			key:  i.iterSpan.Start,
			span: i.iterSpan,
		}
	}
	i.iterSpan = i.iter.Prev()
	return i.yieldEndBound()
}

func (i *fragmentBoundIterator) yieldStartBound() boundKey {
	if !i.iterSpan.Valid() {
		i.boundKind = boundKindInvalid
		return boundKey{kind: boundKindInvalid}
	}
	i.boundKind = boundKindFragmentStart
	return boundKey{
		kind: boundKindFragmentStart,
		key:  i.iterSpan.Start,
		span: i.iterSpan,
	}
}

func (i *fragmentBoundIterator) yieldEndBound() boundKey {
	if !i.iterSpan.Valid() {
		i.boundKind = boundKindInvalid
		return boundKey{kind: boundKindInvalid}
	}
	i.boundKind = boundKindFragmentEnd
	return boundKey{
		kind: boundKindFragmentEnd,
		key:  i.iterSpan.End,
		span: i.iterSpan,
	}
}
