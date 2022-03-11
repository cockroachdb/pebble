// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"sort"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
)

// FragmentIterator defines an iterator interface over spans. The spans
// surfaced by a FragmentIterator must be non-overlapping. This is achieved by
// fragmenting spans at overlap points (see Fragmenter).
//
// A Span returned by a FragmentIterator is only valid until the next
// positioning method. Some implementations (eg, keyspan.Iter) may provide
// longer lifetimes but implementations need only guarantee stability until the
// next positioning method.
type FragmentIterator interface {
	// SeekGE moves the iterator to the first span whose start key is greater
	// than or equal to the given key.
	SeekGE(key []byte) Span

	// SeekLT moves the iterator to the last span whose start key is less than
	// the given key.
	SeekLT(key []byte) Span

	// First moves the iterator to the first span.
	First() Span

	// Last moves the iterator to the last span.
	Last() Span

	// Next moves the iterator to the next span.
	//
	// It is valid to call Next when the iterator is positioned before the first
	// key/value pair due to either a prior call to SeekLT or Prev which
	// returned an invalid span. It is not allowed to call Next when the
	// previous call to SeekGE, SeekPrefixGE or Next returned an invalid span.
	Next() Span

	// Prev moves the iterator to the previous span.
	//
	// It is valid to call Prev when the iterator is positioned after the last
	// key/value pair due to either a prior call to SeekGE or Next which
	// returned an invalid span. It is not allowed to call Prev when the
	// previous call to SeekLT or Prev returned an invalid span.
	Prev() Span

	// Clone returns an unpositioned iterator containing the same spans.
	Clone() FragmentIterator

	// Error returns any accumulated error.
	Error() error

	// Close closes the iterator and returns any accumulated error. Exhausting
	// the iterator is not considered to be an error. It is valid to call Close
	// multiple times. Other methods should not be called after the iterator has
	// been closed.
	Close() error

	// SetBounds sets the lower (inclusive) and upper (exclusive) bounds for the
	// iterator. Note that the result of Next and Prev will be undefined until
	// the iterator has been repositioned with SeekGE, SeekLT, First, or Last.
	//
	// Spans that extend beyond either bound will NOT be truncated. Spans are
	// considered out of bounds only if they have no overlap with the bound span
	// [lower, upper).
	SetBounds(lower, upper []byte)
}

// TableNewRangeKeyIter creates a new range key iterator for the given file.
type TableNewRangeKeyIter func(file *manifest.FileMetadata, iterOptions *RangeIterOptions) (FragmentIterator, error)

// RangeIterOptions is a subset of IterOptions that are necessary to instantiate
// per-sstable range key iterators.
type RangeIterOptions struct {
	// LowerBound specifies the smallest userkey (inclusive) that the iterator will
	// return during iteration. If the iterator is seeked or iterated past this
	// boundary the iterator will return Valid()==false. Setting LowerBound
	// effectively truncates the key space visible to the iterator. Iterators are
	// allowed to reuse this slice, so it should not be modified once passed in.
	LowerBound []byte
	// UpperBound specifies the largest key (exclusive) that the iterator will
	// return during iteration. If the iterator is seeked or iterated past this
	// boundary the iterator will return Valid()==false. Setting UpperBound
	// effectively truncates the key space visible to the iterator. Iterators are
	// allowed to reuse this slice, so it should not be modified once passed in.
	UpperBound []byte
	// Filters can be used to avoid scanning tables and blocks in tables
	// when iterating over range keys.
	Filters []base.BlockPropertyFilter
}

// Iter is an iterator over a set of fragmented spans.
type Iter struct {
	cmp   base.Compare
	spans []Span
	index int

	// lower and upper are indexes into spans, indicating the spans within the
	// current bounds set by SetBounds. lower is inclusive, upper exclusive.
	lower int
	upper int
}

// Iter implements the FragmentIterator interface.
var _ FragmentIterator = (*Iter)(nil)

// NewIter returns a new iterator over a set of fragmented spans.
func NewIter(cmp base.Compare, spans []Span) *Iter {
	return &Iter{
		cmp:   cmp,
		spans: spans,
		index: -1,
		lower: 0,
		upper: len(spans),
	}
}

// SeekGE implements FragmentIterator.SeekGE.
func (i *Iter) SeekGE(key []byte) Span {
	// NB: manually inlined sort.Search is ~5% faster.
	//
	// Define f(-1) == false and f(n) == true.
	// Invariant: f(index-1) == false, f(upper) == true.
	i.index = i.lower
	upper := i.upper
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
	if i.index >= i.upper {
		return Span{}
	}
	return i.spans[i.index]
}

// SeekLT implements FragmentIterator.SeekLT.
func (i *Iter) SeekLT(key []byte) Span {
	// NB: manually inlined sort.Search is ~5% faster.
	//
	// Define f(-1) == false and f(n) == true.
	// Invariant: f(index-1) == false, f(upper) == true.
	i.index = i.lower
	upper := i.upper
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
	if i.index < i.lower {
		return Span{}
	}
	return i.spans[i.index]
}

// First implements FragmentIterator.First.
func (i *Iter) First() Span {
	if i.upper <= i.lower {
		return Span{}
	}
	i.index = i.lower
	return i.spans[i.index]
}

// Last implements FragmentIterator.Last.
func (i *Iter) Last() Span {
	if i.upper <= i.lower {
		return Span{}
	}
	i.index = i.upper - 1
	return i.spans[i.index]
}

// Next implements FragmentIterator.Next.
func (i *Iter) Next() Span {
	if i.index >= i.upper {
		return Span{}
	}
	i.index++
	if i.index >= i.upper {
		return Span{}
	}
	return i.spans[i.index]
}

// Prev implements FragmentIterator.Prev.
func (i *Iter) Prev() Span {
	if i.index < i.lower {
		return Span{}
	}
	i.index--
	if i.index < i.lower {
		return Span{}
	}
	return i.spans[i.index]
}

// Error implements FragmentIterator.Error.
func (i *Iter) Error() error {
	return nil
}

// Close implements FragmentIterator.Close.
func (i *Iter) Close() error {
	return nil
}

// SetBounds implements FragmentIterator.SetBounds.
//
// Iter may still return spans that extend beyond the lower or upper bounds, as
// long as some portion of the span overlaps [lower, upper).
func (i *Iter) SetBounds(lower, upper []byte) {
	// SetBounds is never called for range deletion iterators. It may be called
	// for range key iterators.
	if lower == nil {
		i.lower = 0
	} else {
		i.lower = sort.Search(len(i.spans), func(j int) bool {
			return i.cmp(i.spans[j].End, lower) > 0
		})
	}
	if upper == nil {
		i.upper = len(i.spans)
	} else {
		i.upper = sort.Search(len(i.spans), func(j int) bool {
			return i.cmp(i.spans[j].Start, upper) >= 0
		})
	}
}

// Clone implements FragmentIterator.Clone.
func (i *Iter) Clone() FragmentIterator {
	cloneIter := &Iter{}
	*cloneIter = *i
	return cloneIter
}

func (i *Iter) String() string {
	return "fragmented-spans"
}
