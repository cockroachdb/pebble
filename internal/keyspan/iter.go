// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"context"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/treesteps"
)

// FragmentIterator defines an iterator interface over spans. The spans
// surfaced by a FragmentIterator must be non-overlapping. This is achieved by
// fragmenting spans at overlap points (see Fragmenter).
//
// A Span returned by a FragmentIterator is only valid until the next
// positioning method. Some implementations (eg, keyspan.Iter) may provide
// longer lifetimes but implementations need only guarantee stability until the
// next positioning method.
//
// If any positioning method fails to find a span, the iterator is left
// positioned at an exhausted position in the direction of iteration. For
// example, a caller than finds SeekGE(k)=nil may call Prev to move the iterator
// to the last span.
//
// If an error occurs during any positioning method, the method returns a nil
// span and a non-nil error.
type FragmentIterator interface {
	// SeekGE moves the iterator to the first span covering a key greater than
	// or equal to the given key. This is equivalent to seeking to the first
	// span with an end key greater than the given key.
	SeekGE(key []byte) (*Span, error)

	// SeekLT moves the iterator to the last span covering a key less than the
	// given key. This is equivalent to seeking to the last span with a start
	// key less than the given key.
	SeekLT(key []byte) (*Span, error)

	// First moves the iterator to the first span.
	First() (*Span, error)

	// Last moves the iterator to the last span.
	Last() (*Span, error)

	// Next moves the iterator to the next span.
	//
	// It is valid to call Next when the iterator is positioned before the first
	// key/value pair due to either a prior call to SeekLT or Prev which
	// returned an invalid span. It is not allowed to call Next when the
	// previous call to SeekGE, SeekPrefixGE or Next returned an invalid span.
	Next() (*Span, error)

	// Prev moves the iterator to the previous span.
	//
	// It is valid to call Prev when the iterator is positioned after the last
	// key/value pair due to either a prior call to SeekGE or Next which
	// returned an invalid span. It is not allowed to call Prev when the
	// previous call to SeekLT or Prev returned an invalid span.
	Prev() (*Span, error)

	// Close closes the iterator. It is not in general valid to call Close
	// multiple times. Other methods should not be called after the iterator has
	// been closed. Spans returned by a previous method should also not be used
	// after the iterator has been closed.
	Close()

	// WrapChildren wraps any child iterators using the given function. The
	// function can call WrapChildren to recursively wrap an entire iterator
	// stack. Used only for debug logging.
	WrapChildren(wrap WrapFn)

	// SetContext replaces the context provided at iterator creation, or the last
	// one provided by SetContext.
	SetContext(ctx context.Context)

	treesteps.Node
}

// SpanIterOptions is a subset of IterOptions that are necessary to instantiate
// per-sstable span iterators.
type SpanIterOptions struct {
	// RangeKeyFilters can be used to avoid scanning tables and blocks in tables
	// when iterating over range keys.
	RangeKeyFilters []base.BlockPropertyFilter
}

// Iter is an iterator over a set of fragmented spans.
type Iter struct {
	cmp   base.Compare
	spans []Span
	index int
}

// Iter implements the FragmentIterator interface.
var _ FragmentIterator = (*Iter)(nil)

// NewIter returns a new iterator over a set of fragmented spans.
func NewIter(cmp base.Compare, spans []Span) *Iter {
	i := &Iter{}
	i.Init(cmp, spans)
	return i
}

// Count returns the number of spans contained by Iter.
func (i *Iter) Count() int {
	return len(i.spans)
}

// Init initializes an Iter with the provided spans.
func (i *Iter) Init(cmp base.Compare, spans []Span) {
	*i = Iter{
		cmp:   cmp,
		spans: spans,
		index: -1,
	}
}

// SeekGE implements FragmentIterator.SeekGE.
func (i *Iter) SeekGE(key []byte) (*Span, error) {
	// NB: manually inlined sort.Search is ~5% faster.
	//
	// Define f(j) = false iff the span i.spans[j] is strictly before `key`
	// (equivalently, i.spans[j].End ≤ key.)
	//
	// Define f(-1) == false and f(n) == true.
	// Invariant: f(index-1) == false, f(upper) == true.
	i.index = 0
	upper := len(i.spans)
	for i.index < upper {
		h := int(uint(i.index+upper) >> 1) // avoid overflow when computing h
		// i.index ≤ h < upper
		if i.cmp(key, i.spans[h].End) >= 0 {
			i.index = h + 1 // preserves f(i-1) == false
		} else {
			upper = h // preserves f(j) == true
		}
	}

	// i.index == upper, f(i.index-1) == false, and f(upper) (= f(i.index)) ==
	// true => answer is i.index.
	if i.index >= len(i.spans) {
		return nil, nil
	}
	return &i.spans[i.index], nil
}

// SeekLT implements FragmentIterator.SeekLT.
func (i *Iter) SeekLT(key []byte) (*Span, error) {
	// NB: manually inlined sort.Search is ~5% faster.
	//
	// Define f(-1) == false and f(n) == true.
	// Invariant: f(index-1) == false, f(upper) == true.
	i.index = 0
	upper := len(i.spans)
	for i.index < upper {
		h := int(uint(i.index+upper) >> 1) // avoid overflow when computing h
		// i.index ≤ h < upper
		if i.cmp(key, i.spans[h].Start) > 0 {
			i.index = h + 1 // preserves f(i-1) == false
		} else {
			upper = h // preserves f(j) == true
		}
	}
	// i.index == upper, f(i.index-1) == false, and f(upper) (= f(i.index)) ==
	// true => answer is i.index.

	// Since keys are strictly increasing, if i.index > 0 then i.index-1 will be
	// the largest whose key is < the key sought.
	i.index--
	if i.index < 0 {
		return nil, nil
	}
	return &i.spans[i.index], nil
}

// First implements FragmentIterator.First.
func (i *Iter) First() (*Span, error) {
	if len(i.spans) == 0 {
		return nil, nil
	}
	i.index = 0
	return &i.spans[i.index], nil
}

// Last implements FragmentIterator.Last.
func (i *Iter) Last() (*Span, error) {
	if len(i.spans) == 0 {
		return nil, nil
	}
	i.index = len(i.spans) - 1
	return &i.spans[i.index], nil
}

// Next implements FragmentIterator.Next.
func (i *Iter) Next() (*Span, error) {
	if i.index >= len(i.spans) {
		return nil, nil
	}
	i.index++
	if i.index >= len(i.spans) {
		return nil, nil
	}
	return &i.spans[i.index], nil
}

// Prev implements FragmentIterator.Prev.
func (i *Iter) Prev() (*Span, error) {
	if i.index < 0 {
		return nil, nil
	}
	i.index--
	if i.index < 0 {
		return nil, nil
	}
	return &i.spans[i.index], nil
}

// SetContext is part of the FragmentIterator interface.
func (i *Iter) SetContext(ctx context.Context) {}

// Close implements FragmentIterator.Close.
func (i *Iter) Close() {}

func (i *Iter) String() string {
	return "keyspan.Iter"
}

// WrapChildren implements FragmentIterator.
func (i *Iter) WrapChildren(wrap WrapFn) {}

// TreeStepsNode is part of the FragmentIterator interface.
func (i *Iter) TreeStepsNode() treesteps.NodeInfo {
	return treesteps.NodeInfof(i, "%T(%p)", i, i)
}
