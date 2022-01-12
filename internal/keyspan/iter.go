// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"sort"

	"github.com/cockroachdb/pebble/internal/base"
)

// TODO(jackson): Disentangle FragmentIterator from InternalIterator and return
// full Spans from the positioning methods.

// FragmentIterator defines an interator interface over keyspan fragments. The
// spans surfaced by a FragmentIterator must already be fragmented — that is if
// two spans overlap, they must have identical start and end bounds. In forward
// iteration, spans are returned ascending by their start keys:
//
//     (user key ASC, sequence number DESC, kind DESC)
//
// In reverse iteration, spans are returned in opposite order:
//
//     (user key DESC, sequence number ASC, kind ASC)
//
type FragmentIterator interface {
	base.InternalIterator

	// Valid returns true iff the iterator is currently positioned over a
	// fragment.
	Valid() bool

	// End returns the end user key for the fragment at the current iterator
	// position.
	End() []byte

	// Current returns the fragment at the current iterator position.
	Current() Span

	// Clone returns an unpositioned iterator containing the same spans.
	Clone() FragmentIterator
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

// SeekGE implements InternalIterator.SeekGE, as documented in the
// internal/base package.
func (i *Iter) SeekGE(key []byte, trySeekUsingNext bool) (*base.InternalKey, []byte) {
	// NB: manually inlined sort.Seach is ~5% faster.
	//
	// Define f(-1) == false and f(n) == true.
	// Invariant: f(index-1) == false, f(upper) == true.
	ikey := base.MakeSearchKey(key)
	i.index = i.lower
	upper := i.upper
	for i.index < upper {
		h := int(uint(i.index+upper) >> 1) // avoid overflow when computing h
		// i.index ≤ h < upper
		if base.InternalCompare(i.cmp, ikey, i.spans[h].Start) >= 0 {
			i.index = h + 1 // preserves f(i-1) == false
		} else {
			upper = h // preserves f(j) == true
		}
	}
	// i.index == upper, f(i.index-1) == false, and f(upper) (= f(i.index)) ==
	// true => answer is i.index.
	if i.index >= i.upper {
		return nil, nil
	}
	s := &i.spans[i.index]
	return &s.Start, s.End
}

// SeekPrefixGE implements InternalIterator.SeekPrefixGE, as documented in the
// internal/base package.
func (i *Iter) SeekPrefixGE(prefix, key []byte, trySeekUsingNext bool) (*base.InternalKey, []byte) {
	// This should never be called as prefix iteration is only done for point records.
	panic("pebble: SeekPrefixGE unimplemented")
}

// SeekLT implements InternalIterator.SeekLT, as documented in the
// internal/base package.
func (i *Iter) SeekLT(key []byte) (*base.InternalKey, []byte) {
	// NB: manually inlined sort.Search is ~5% faster.
	//
	// Define f(-1) == false and f(n) == true.
	// Invariant: f(index-1) == false, f(upper) == true.
	ikey := base.MakeSearchKey(key)
	i.index = i.lower
	upper := i.upper
	for i.index < upper {
		h := int(uint(i.index+upper) >> 1) // avoid overflow when computing h
		// i.index ≤ h < upper
		if base.InternalCompare(i.cmp, ikey, i.spans[h].Start) > 0 {
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
		return nil, nil
	}
	s := &i.spans[i.index]
	return &s.Start, s.End
}

// First implements InternalIterator.First, as documented in the internal/base
// package.
func (i *Iter) First() (*base.InternalKey, []byte) {
	if i.upper <= i.lower {
		return nil, nil
	}
	i.index = i.lower
	s := &i.spans[i.index]
	return &s.Start, s.End
}

// Last implements InternalIterator.Last, as documented in the internal/base
// package.
func (i *Iter) Last() (*base.InternalKey, []byte) {
	if i.upper <= i.lower {
		return nil, nil
	}
	i.index = i.upper - 1
	s := &i.spans[i.index]
	return &s.Start, s.End
}

// Next implements InternalIterator.Next, as documented in the internal/base
// package.
func (i *Iter) Next() (*base.InternalKey, []byte) {
	if i.index >= i.upper {
		return nil, nil
	}
	i.index++
	if i.index >= i.upper {
		return nil, nil
	}
	s := &i.spans[i.index]
	return &s.Start, s.End
}

// Prev implements InternalIterator.Prev, as documented in the internal/base
// package.
func (i *Iter) Prev() (*base.InternalKey, []byte) {
	if i.index < i.lower {
		return nil, nil
	}
	i.index--
	if i.index < i.lower {
		return nil, nil
	}
	s := &i.spans[i.index]
	return &s.Start, s.End
}

// Current returns the current span.
func (i *Iter) Current() Span {
	if i.Valid() {
		return i.spans[i.index]
	}
	return Span{}
}

// End returns the end user key of the fragment at the current iterator
// position, implementing FragmentIterator.End.
func (i *Iter) End() []byte {
	return i.spans[i.index].End
}

// Valid implements InternalIterator.Valid, as documented in the internal/base
// package.
func (i *Iter) Valid() bool {
	return i.index >= i.lower && i.index < i.upper
}

// Error implements InternalIterator.Error, as documented in the internal/base
// package.
func (i *Iter) Error() error {
	return nil
}

// Close implements InternalIterator.Close, as documented in the internal/base
// package.
func (i *Iter) Close() error {
	return nil
}

// SetBounds implements InternalIterator.SetBounds, as documented in the
// internal/base package.
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
			return i.cmp(i.spans[j].Start.UserKey, upper) >= 0
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
