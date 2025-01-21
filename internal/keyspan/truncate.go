// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"context"

	"github.com/cockroachdb/pebble/v2/internal/base"
	"github.com/cockroachdb/pebble/v2/internal/invariants"
	"github.com/cockroachdb/pebble/v2/internal/treeprinter"
)

// Truncate creates a new iterator where every span in the supplied iterator is
// truncated to be contained within the given user key bounds.
//
// Note that fragment iterator Spans always have exclusive end-keys; if the
// given bounds have an inclusive end key, then the input iterator must not
// produce a span that contains that key. The only difference between bounds.End
// being inclusive vs exclusive is this extra check.
func Truncate(cmp base.Compare, iter FragmentIterator, bounds base.UserKeyBounds) FragmentIterator {
	return &truncatingIter{
		iter:   iter,
		cmp:    cmp,
		bounds: bounds,
	}
}

type truncatingIter struct {
	iter FragmentIterator
	cmp  base.Compare

	bounds base.UserKeyBounds

	span Span
}

// SeekGE implements FragmentIterator.
func (i *truncatingIter) SeekGE(key []byte) (*Span, error) {
	span, err := i.iter.SeekGE(key)
	if err != nil {
		return nil, err
	}
	span, spanBoundsChanged, err := i.nextSpanWithinBounds(span, +1)
	if err != nil {
		return nil, err
	}
	// nextSpanWithinBounds could return a span that's less than key, if the end
	// bound was truncated to end at a key less than or equal to `key`. Detect
	// this case and next/invalidate the iter.
	if spanBoundsChanged && i.cmp(span.End, key) <= 0 {
		return i.Next()
	}
	return span, nil
}

// SeekLT implements FragmentIterator.
func (i *truncatingIter) SeekLT(key []byte) (*Span, error) {
	span, err := i.iter.SeekLT(key)
	if err != nil {
		return nil, err
	}
	span, spanBoundsChanged, err := i.nextSpanWithinBounds(span, -1)
	if err != nil {
		return nil, err
	}
	// nextSpanWithinBounds could return a span that's >= key, if the start bound
	// was truncated to start at a key greater than or equal to `key`. Detect this
	// case and prev/invalidate the iter.
	if spanBoundsChanged && i.cmp(span.Start, key) >= 0 {
		return i.Prev()
	}
	return span, nil
}

// First implements FragmentIterator.
func (i *truncatingIter) First() (*Span, error) {
	span, err := i.iter.First()
	if err != nil {
		return nil, err
	}
	span, _, err = i.nextSpanWithinBounds(span, +1)
	return span, err
}

// Last implements FragmentIterator.
func (i *truncatingIter) Last() (*Span, error) {
	span, err := i.iter.Last()
	if err != nil {
		return nil, err
	}
	span, _, err = i.nextSpanWithinBounds(span, -1)
	return span, err
}

// Next implements FragmentIterator.
func (i *truncatingIter) Next() (*Span, error) {
	span, err := i.iter.Next()
	if err != nil {
		return nil, err
	}
	span, _, err = i.nextSpanWithinBounds(span, +1)
	return span, err
}

// Prev implements FragmentIterator.
func (i *truncatingIter) Prev() (*Span, error) {
	span, err := i.iter.Prev()
	if err != nil {
		return nil, err
	}
	span, _, err = i.nextSpanWithinBounds(span, -1)
	return span, err
}

// SetContext is part of the FragmentIterator interface.
func (i *truncatingIter) SetContext(ctx context.Context) {
	i.iter.SetContext(ctx)
}

// Close implements FragmentIterator.
func (i *truncatingIter) Close() {
	i.iter.Close()
}

// nextSpanWithinBounds returns the first span (starting with the given span and
// advancing in the given direction) that intersects the bounds. It returns a
// span that is entirely within the bounds; spanBoundsChanged indicates if the span
// bounds had to be truncated.
func (i *truncatingIter) nextSpanWithinBounds(
	span *Span, dir int8,
) (_ *Span, spanBoundsChanged bool, _ error) {
	var err error
	for span != nil {
		if i.bounds.End.Kind == base.Inclusive && span.Contains(i.cmp, i.bounds.End.Key) {
			err := base.AssertionFailedf("inclusive upper bound %q inside span %s", i.bounds.End.Key, span)
			if invariants.Enabled {
				panic(err)
			}
			return nil, false, err
		}
		// Intersect [span.Start, span.End) with [i.bounds.Start, i.bounds.End.Key).
		spanBoundsChanged = false
		start := span.Start
		if i.cmp(start, i.bounds.Start) < 0 {
			spanBoundsChanged = true
			start = i.bounds.Start
		}
		end := span.End
		if i.cmp(end, i.bounds.End.Key) > 0 {
			spanBoundsChanged = true
			end = i.bounds.End.Key
		}
		if !spanBoundsChanged {
			return span, false, nil
		}
		if i.cmp(start, end) < 0 {
			i.span = Span{
				Start:     start,
				End:       end,
				Keys:      span.Keys,
				KeysOrder: span.KeysOrder,
			}
			return &i.span, true, nil
		}
		// Span is outside of bounds, find the next one.
		if dir == +1 {
			span, err = i.iter.Next()
		} else {
			span, err = i.iter.Prev()
		}
	}
	// NB: err may be nil or non-nil.
	return nil, false, err
}

// WrapChildren implements FragmentIterator.
func (i *truncatingIter) WrapChildren(wrap WrapFn) {
	i.iter = wrap(i.iter)
}

// DebugTree is part of the FragmentIterator interface.
func (i *truncatingIter) DebugTree(tp treeprinter.Node) {
	n := tp.Childf("%T(%p)", i, i)
	if i.iter != nil {
		i.iter.DebugTree(n)
	}
}
