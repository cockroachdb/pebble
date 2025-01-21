// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2/internal/base"
	"github.com/cockroachdb/pebble/v2/internal/invariants"
	"github.com/cockroachdb/pebble/v2/internal/treeprinter"
)

// Assert wraps an iterator which asserts that operations return sane results.
func Assert(iter FragmentIterator, cmp base.Compare) FragmentIterator {
	return &assertIter{
		iter: iter,
		cmp:  cmp,
	}
}

// MaybeAssert potentially wraps an iterator with Assert and/or
// NewInvalidatingIter if we are in testing mode.
func MaybeAssert(iter FragmentIterator, cmp base.Compare) FragmentIterator {
	if invariants.Enabled {
		if invariants.Sometimes(60 /* percent */) {
			iter = NewInvalidatingIter(iter)
		}
		if invariants.Sometimes(60 /* percent */) {
			iter = Assert(iter, cmp)
		}
	}
	return iter
}

// AssertUserKeyBounds wraps an iterator and asserts that all spans are within
// the given bounds [lower, upper).
func AssertUserKeyBounds(
	iter FragmentIterator, lower, upper []byte, cmp base.Compare,
) FragmentIterator {
	return AssertBounds(iter, base.MakeSearchKey(lower), upper, cmp)
}

// AssertBounds wraps an iterator and asserts that all spans are within the
// given bounds [lower.UserKey, upper), and that all keys in a span that starts
// exactly at lower.UserKey are >= lower.
//
// The asymmetry here is due to fragment spans having exclusive end user keys.
func AssertBounds(
	iter FragmentIterator, lower base.InternalKey, upper []byte, cmp base.Compare,
) FragmentIterator {
	i := &assertIter{
		iter: iter,
		cmp:  cmp,
	}
	i.checkBounds.enabled = true
	i.checkBounds.lower = lower
	i.checkBounds.upper = upper
	return i
}

// assertIter is a pass-through FragmentIterator wrapper which performs checks
// on what the wrapped iterator returns.
//
// It verifies that results for various operations are sane, and it optionally
// verifies that spans are within given bounds.
type assertIter struct {
	iter        FragmentIterator
	cmp         base.Compare
	checkBounds struct {
		enabled bool
		lower   base.InternalKey
		upper   []byte
	}
	lastSpanStart []byte
	lastSpanEnd   []byte
}

var _ FragmentIterator = (*assertIter)(nil)

func (i *assertIter) panicf(format string, args ...interface{}) {
	str := fmt.Sprintf(format, args...)
	panic(errors.AssertionFailedf("%s; wraps %T", str, i.iter))
}

func (i *assertIter) check(span *Span) {
	i.lastSpanStart = i.lastSpanStart[:0]
	i.lastSpanEnd = i.lastSpanEnd[:0]
	if span == nil {
		return
	}
	if i.checkBounds.enabled {
		lower := i.checkBounds.lower
		switch startCmp := i.cmp(span.Start, lower.UserKey); {
		case startCmp < 0:
			i.panicf("lower bound %q violated by span %s", lower.UserKey, span)
		case startCmp == 0:
			// Note: trailers are in descending order.
			if len(span.Keys) > 0 && span.SmallestKey().Trailer > lower.Trailer {
				i.panicf("lower bound %s violated by key %s", lower, span.SmallestKey())
			}
		}
		if i.cmp(span.End, i.checkBounds.upper) > 0 {
			i.panicf("upper bound %q violated by span %s", i.checkBounds.upper, span)
		}
	}
	// Save the span to check Next/Prev operations.
	i.lastSpanStart = append(i.lastSpanStart, span.Start...)
	i.lastSpanEnd = append(i.lastSpanEnd, span.End...)
}

// SeekGE implements FragmentIterator.
func (i *assertIter) SeekGE(key []byte) (*Span, error) {
	span, err := i.iter.SeekGE(key)
	if span != nil && i.cmp(span.End, key) <= 0 {
		i.panicf("incorrect SeekGE(%q) span %s", key, span)
	}
	i.check(span)
	return span, err
}

// SeekLT implements FragmentIterator.
func (i *assertIter) SeekLT(key []byte) (*Span, error) {
	span, err := i.iter.SeekLT(key)
	if span != nil && i.cmp(span.Start, key) >= 0 {
		i.panicf("incorrect SeekLT(%q) span %s", key, span)
	}
	i.check(span)
	return span, err
}

// First implements FragmentIterator.
func (i *assertIter) First() (*Span, error) {
	span, err := i.iter.First()
	i.check(span)
	return span, err
}

// Last implements FragmentIterator.
func (i *assertIter) Last() (*Span, error) {
	span, err := i.iter.Last()
	i.check(span)
	return span, err
}

// Next implements FragmentIterator.
func (i *assertIter) Next() (*Span, error) {
	span, err := i.iter.Next()
	if span != nil && len(i.lastSpanEnd) > 0 && i.cmp(i.lastSpanEnd, span.Start) > 0 {
		i.panicf("Next span %s not after last span end %q", span, i.lastSpanEnd)
	}
	i.check(span)
	return span, err
}

// Prev implements FragmentIterator.
func (i *assertIter) Prev() (*Span, error) {
	span, err := i.iter.Prev()
	if span != nil && len(i.lastSpanStart) > 0 && i.cmp(i.lastSpanStart, span.End) < 0 {
		i.panicf("Prev span %s not before last span start %q", span, i.lastSpanStart)
	}
	i.check(span)
	return span, err
}

// SetContext is part of the FragmentIterator interface.
func (i *assertIter) SetContext(ctx context.Context) {
	i.iter.SetContext(ctx)
}

// Close implements FragmentIterator.
func (i *assertIter) Close() {
	i.iter.Close()
}

// WrapChildren implements FragmentIterator.
func (i *assertIter) WrapChildren(wrap WrapFn) {
	i.iter = wrap(i.iter)
}

// DebugTree is part of the FragmentIterator interface.
func (i *assertIter) DebugTree(tp treeprinter.Node) {
	n := tp.Childf("%T(%p)", i, i)
	if i.iter != nil {
		i.iter.DebugTree(n)
	}
}
