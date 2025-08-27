// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"context"

	"github.com/cockroachdb/pebble/v2/internal/base"
	"github.com/cockroachdb/pebble/v2/internal/treeprinter"
)

// TODO(jackson): Consider removing this type and adding bounds enforcement
// directly to the MergingIter. This type is probably too lightweight to warrant
// its own type, but for now we implement it separately for expediency.

// boundedIterPos records the position of the BoundedIter relative to the
// underlying iterator's position. It's used to avoid Next/Prev-ing the iterator
// if there can't possibly be another span within bounds, because the current
// span overlaps the bound.
//
// Imagine bounds [a,c) and an iterator that seeks to a span [b,d). The span
// [b,d) overlaps some portion of the iterator bounds, so the iterator must
// return it. If the iterator is subsequently Nexted, Next can tell that the
// iterator is exhausted without advancing the underlying iterator because the
// current span's end bound of d is ≥ the upper bound of c. In this case, the
// bounded iterator returns nil and records i.pos as posAtUpperLimit to remember
// that the underlying iterator position does not match the current BoundedIter
// position.
type boundedIterPos int8

const (
	posAtLowerLimit boundedIterPos = -1
	posAtIterSpan   boundedIterPos = 0
	posAtUpperLimit boundedIterPos = +1
)

// BoundedIter implements FragmentIterator and enforces bounds.
//
// Like the point InternalIterator interface, the bounded iterator's forward
// positioning routines (SeekGE, First, and Next) only check the upper bound.
// The reverse positioning routines (SeekLT, Last, and Prev) only check the
// lower bound. It is up to the caller to ensure that the forward positioning
// routines respect the lower bound and the reverse positioning routines respect
// the upper bound (i.e. calling SeekGE instead of First if there is a lower
// bound, and SeekLT instead of Last if there is an upper bound).
//
// When the hasPrefix parameter indicates that the iterator is in prefix
// iteration mode, BoundedIter elides any spans that do not overlap with the
// prefix's keyspace. In prefix iteration mode, reverse iteration is disallowed,
// except for an initial SeekLT with a seek key greater than or equal to the
// prefix. In prefix iteration mode, the first seek must position the iterator
// at or immediately before the first fragment covering a key greater than or
// equal to the prefix.
type BoundedIter struct {
	iter      FragmentIterator
	iterSpan  *Span
	cmp       base.Compare
	split     base.Split
	lower     []byte
	upper     []byte
	hasPrefix *bool
	prefix    *[]byte
	pos       boundedIterPos
}

// Init initializes the bounded iterator.
//
// In addition to the iterator bounds, Init takes pointers to a boolean
// indicating whether the iterator is in prefix iteration mode and the prefix
// key if it is. This is used to exclude spans that are outside the iteration
// prefix.
//
// hasPrefix and prefix are allowed to be nil, however if hasPrefix != nil,
// prefix must also not be nil.
func (i *BoundedIter) Init(
	cmp base.Compare,
	split base.Split,
	iter FragmentIterator,
	lower, upper []byte,
	hasPrefix *bool,
	prefix *[]byte,
) {
	*i = BoundedIter{
		iter:      iter,
		cmp:       cmp,
		split:     split,
		lower:     lower,
		upper:     upper,
		hasPrefix: hasPrefix,
		prefix:    prefix,
	}
}

var _ FragmentIterator = (*BoundedIter)(nil)

// Seek calls.
//
// Seek calls check iterator bounds in the direction of the seek. Additionally,
// if the iterator is in prefix iteration mode, seek calls check both start and
// end bounds against the prefix's bounds. We check both bounds for defense in
// depth. This optimization has been a source of various bugs due to various
// other prefix iteration optimizations that can result in seek keys that don't
// respect the prefix bounds.

// SeekGE implements FragmentIterator.
func (i *BoundedIter) SeekGE(key []byte) (*Span, error) {
	s, err := i.iter.SeekGE(key)
	s, err = i.checkPrefixSpanStart(s, err)
	s, err = i.checkPrefixSpanEnd(s, err)
	return i.checkForwardBound(s, err)
}

// SeekLT implements FragmentIterator.
func (i *BoundedIter) SeekLT(key []byte) (*Span, error) {
	s, err := i.iter.SeekLT(key)
	s, err = i.checkPrefixSpanStart(s, err)
	s, err = i.checkPrefixSpanEnd(s, err)
	return i.checkBackwardBound(s, err)
}

// First implements FragmentIterator.
func (i *BoundedIter) First() (*Span, error) {
	s, err := i.iter.First()
	s, err = i.checkPrefixSpanStart(s, err)
	return i.checkForwardBound(s, err)
}

// Last implements FragmentIterator.
func (i *BoundedIter) Last() (*Span, error) {
	s, err := i.iter.Last()
	s, err = i.checkPrefixSpanEnd(s, err)
	return i.checkBackwardBound(s, err)
}

// Next implements FragmentIterator.
func (i *BoundedIter) Next() (*Span, error) {
	switch i.pos {
	case posAtLowerLimit:
		// The BoundedIter had previously returned nil, because it knew from
		// i.iterSpan's bounds that there was no previous span. To Next, we only
		// need to return the current iter span and reset i.pos to reflect that
		// we're no longer positioned at the limit.
		i.pos = posAtIterSpan
		return i.iterSpan, nil
	case posAtIterSpan:
		// If the span at the underlying iterator position extends to or beyond the
		// upper bound, we can avoid advancing because the next span is necessarily
		// out of bounds.
		if i.iterSpan != nil && i.upper != nil && i.cmp(i.iterSpan.End, i.upper) >= 0 {
			i.pos = posAtUpperLimit
			return nil, nil
		}
		// Similarly, if the span extends to the next prefix and we're in prefix
		// iteration mode, we can avoid advancing.
		if i.iterSpan != nil && i.hasPrefix != nil && *i.hasPrefix {
			ei := i.split(i.iterSpan.End)
			if i.cmp(i.iterSpan.End[:ei], *i.prefix) > 0 {
				i.pos = posAtUpperLimit
				return nil, nil
			}
		}
		return i.checkForwardBound(i.checkPrefixSpanStart(i.iter.Next()))
	case posAtUpperLimit:
		// Already exhausted.
		return nil, nil
	default:
		panic("unreachable")
	}
}

// Prev implements FragmentIterator.
func (i *BoundedIter) Prev() (*Span, error) {
	switch i.pos {
	case posAtLowerLimit:
		// Already exhausted.
		return nil, nil
	case posAtIterSpan:
		// If the span at the underlying iterator position extends to or beyond
		// the lower bound, we can avoid advancing because the previous span is
		// necessarily out of bounds.
		if i.iterSpan != nil && i.lower != nil && i.cmp(i.iterSpan.Start, i.lower) <= 0 {
			i.pos = posAtLowerLimit
			return nil, nil
		}
		// Similarly, if the span extends to or beyond the current prefix and
		// we're in prefix iteration mode, we can avoid advancing.
		if i.iterSpan != nil && i.hasPrefix != nil && *i.hasPrefix {
			si := i.split(i.iterSpan.Start)
			if i.cmp(i.iterSpan.Start[:si], *i.prefix) < 0 {
				i.pos = posAtLowerLimit
				return nil, nil
			}
		}
		return i.checkBackwardBound(i.checkPrefixSpanEnd(i.iter.Prev()))
	case posAtUpperLimit:
		// The BoundedIter had previously returned nil, because it knew from
		// i.iterSpan's bounds that there was no next span. To Prev, we only
		// need to return the current iter span and reset i.pos to reflect that
		// we're no longer positioned at the limit.
		i.pos = posAtIterSpan
		return i.iterSpan, nil
	default:
		panic("unreachable")
	}
}

// Close implements FragmentIterator.
func (i *BoundedIter) Close() {
	i.iter.Close()
}

// SetContext is part of the FragmentIterator interface.
func (i *BoundedIter) SetContext(ctx context.Context) {
	i.iter.SetContext(ctx)
}

// SetBounds modifies the FragmentIterator's bounds.
func (i *BoundedIter) SetBounds(lower, upper []byte) {
	i.lower, i.upper = lower, upper
}

func (i *BoundedIter) checkPrefixSpanStart(span *Span, err error) (*Span, error) {
	// Compare to the prefix's bounds, if in prefix iteration mode.
	if span != nil && i.hasPrefix != nil && *i.hasPrefix {
		si := i.split(span.Start)
		if i.cmp(span.Start[:si], *i.prefix) > 0 {
			// This span starts at a prefix that sorts after our current prefix.
			span = nil
		}
	}
	return span, err
}

// checkForwardBound enforces the upper bound, returning nil if the provided
// span is wholly outside the upper bound. It also updates i.pos and i.iterSpan
// to reflect the new iterator position.
func (i *BoundedIter) checkForwardBound(span *Span, err error) (*Span, error) {
	// Compare to the upper bound.
	if span != nil && i.upper != nil && i.cmp(span.Start, i.upper) >= 0 {
		span = nil
	}
	i.iterSpan = span
	if i.pos != posAtIterSpan {
		i.pos = posAtIterSpan
	}
	return span, err
}

func (i *BoundedIter) checkPrefixSpanEnd(span *Span, err error) (*Span, error) {
	// Compare to the prefix's bounds, if in prefix iteration mode.
	if span != nil && i.hasPrefix != nil && *i.hasPrefix && i.cmp(span.End, *i.prefix) <= 0 {
		// This span ends before the current prefix.
		span = nil
	}
	return span, err
}

// checkBackward enforces the lower bound, returning nil if the provided span is
// wholly outside the lower bound.  It also updates i.pos and i.iterSpan to
// reflect the new iterator position.
func (i *BoundedIter) checkBackwardBound(span *Span, err error) (*Span, error) {
	// Compare to the lower bound.
	if span != nil && i.lower != nil && i.cmp(span.End, i.lower) <= 0 {
		span = nil
	}
	i.iterSpan = span
	if i.pos != posAtIterSpan {
		i.pos = posAtIterSpan
	}
	return span, err
}

// WrapChildren implements FragmentIterator.
func (i *BoundedIter) WrapChildren(wrap WrapFn) {
	i.iter = wrap(i.iter)
}

// DebugTree is part of the FragmentIterator interface.
func (i *BoundedIter) DebugTree(tp treeprinter.Node) {
	n := tp.Childf("%T(%p)", i, i)
	if i.iter != nil {
		i.iter.DebugTree(n)
	}
}
