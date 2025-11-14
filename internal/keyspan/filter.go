// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"context"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/treesteps"
)

// FilterFunc is a callback that allows filtering keys from a Span. The result
// is the set of keys that should be retained (using buf as a buffer). If the
// result has no keys, the span is skipped altogether.
type FilterFunc func(span *Span, buf []Key) []Key

// filteringIter is a FragmentIterator that uses a FilterFunc to select which
// Spans from the input iterator are returned in the output.
//
// A note on Span lifetimes: as the FilterFunc reuses a Span with a mutable
// slice of Keys to reduce allocations, Spans returned by this iterator are only
// valid until the next relative or absolute positioning method is called.
type filteringIter struct {
	iter     FragmentIterator
	filterFn FilterFunc
	cmp      base.Compare

	// span is a mutable Span passed to the filterFn. The filterFn is free to
	// mutate this Span. The slice of Keys in the Span is reused with every call
	// to the filterFn.
	span Span
}

var _ FragmentIterator = (*filteringIter)(nil)

// Filter returns a new filteringIter that will filter the Spans from the
// provided child iterator using the provided FilterFunc.
func Filter(iter FragmentIterator, filter FilterFunc, cmp base.Compare) FragmentIterator {
	return MaybeAssert(&filteringIter{iter: iter, filterFn: filter, cmp: cmp}, cmp)
}

// SeekGE implements FragmentIterator.
func (i *filteringIter) SeekGE(key []byte) (*Span, error) {
	s, err := i.iter.SeekGE(key)
	if err != nil {
		return nil, err
	}
	return i.filter(s, +1)
}

// SeekLT implements FragmentIterator.
func (i *filteringIter) SeekLT(key []byte) (*Span, error) {
	span, err := i.iter.SeekLT(key)
	if err != nil {
		return nil, err
	}
	return i.filter(span, -1)
}

// First implements FragmentIterator.
func (i *filteringIter) First() (*Span, error) {
	s, err := i.iter.First()
	if err != nil {
		return nil, err
	}
	return i.filter(s, +1)
}

// Last implements FragmentIterator.
func (i *filteringIter) Last() (*Span, error) {
	s, err := i.iter.Last()
	if err != nil {
		return nil, err
	}
	return i.filter(s, -1)
}

// Next implements FragmentIterator.
func (i *filteringIter) Next() (*Span, error) {
	s, err := i.iter.Next()
	if err != nil {
		return nil, err
	}
	return i.filter(s, +1)
}

// Prev implements FragmentIterator.
func (i *filteringIter) Prev() (*Span, error) {
	s, err := i.iter.Prev()
	if err != nil {
		return nil, err
	}
	return i.filter(s, -1)
}

// SetContext is part of the FragmentIterator interface.
func (i *filteringIter) SetContext(ctx context.Context) {
	i.iter.SetContext(ctx)
}

// Close implements FragmentIterator.
func (i *filteringIter) Close() {
	i.iter.Close()
}

// filter uses the filterFn (if configured) to filter and possibly mutate the
// given Span. If the current Span is to be skipped, the iterator continues
// iterating in the given direction until it lands on a Span that should be
// returned, or the iterator becomes invalid.
func (i *filteringIter) filter(span *Span, dir int8) (*Span, error) {
	if i.filterFn == nil {
		return span, nil
	}
	var err error
	for span != nil {
		keys := i.filterFn(span, i.span.Keys[:0])
		if len(keys) > 0 {
			i.span = Span{
				Start:     span.Start,
				End:       span.End,
				Keys:      keys,
				KeysOrder: span.KeysOrder,
			}
			return &i.span, nil
		}

		if dir == +1 {
			span, err = i.iter.Next()
		} else {
			span, err = i.iter.Prev()
		}
	}
	// NB: err may be nil or non-nil.
	return span, err
}

// WrapChildren implements FragmentIterator.
func (i *filteringIter) WrapChildren(wrap WrapFn) {
	i.iter = wrap(i.iter)
}

// TreeStepsNode is part of the FragmentIterator interface.
func (i *filteringIter) TreeStepsNode() treesteps.NodeInfo {
	info := treesteps.NodeInfof(i, "%T(%p)", i, i)
	info.AddChildren(i.iter)
	return info
}
