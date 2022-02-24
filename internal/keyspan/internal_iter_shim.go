// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import "github.com/cockroachdb/pebble/internal/base"

// InternalIteratorShim is a temporary iterator type used as a shim between
// keyspan.MergingIter and base.InternalIterator. It's used temporarily for
// range deletions during compactions, allowing range deletions to be
// interleaved by a compaction input iterator.
//
// TODO(jackson): This type should be removed, and the usages converted to using
// an InterleavingIterator type that interleaves keyspan.Spans from a
// keyspan.FragmentIterator with point keys.
type InternalIteratorShim struct {
	miter   MergingIter
	frags   Fragments
	iterKey base.InternalKey
}

// Assert that InternalIteratorShim implements InternalIterator.
var _ base.InternalIterator = &InternalIteratorShim{}

// Init initializes the internal iterator shim to merge the provided fragment
// iterators.
func (i *InternalIteratorShim) Init(cmp base.Compare, iters ...FragmentIterator) {
	noopTransform := func(*Fragments) {}
	i.miter.Init(cmp, noopTransform, iters...)
}

// Fragments returns the full set of fragments at the current iterator position.
func (i *InternalIteratorShim) Fragments() Fragments {
	return i.frags
}

// SeekGE implements (base.InternalIterator).SeekGE.
func (i *InternalIteratorShim) SeekGE(
	key []byte, trySeekUsingNext bool,
) (*base.InternalKey, []byte) {
	panic("unimplemented")
}

// SeekPrefixGE implements (base.InternalIterator).SeekPrefixGE.
func (i *InternalIteratorShim) SeekPrefixGE(
	prefix, key []byte, trySeekUsingNext bool,
) (*base.InternalKey, []byte) {
	panic("unimplemented")
}

// SeekLT implements (base.InternalIterator).SeekLT.
func (i *InternalIteratorShim) SeekLT(key []byte) (*base.InternalKey, []byte) {
	panic("unimplemented")
}

// First implements (base.InternalIterator).First.
func (i *InternalIteratorShim) First() (*base.InternalKey, []byte) {
	i.frags = i.miter.First()
	if i.frags.Empty() {
		return nil, nil
	}
	s := i.frags.At(0)
	i.iterKey = s.Start
	return &i.iterKey, s.End
}

// Last implements (base.InternalIterator).Last.
func (i *InternalIteratorShim) Last() (*base.InternalKey, []byte) {
	panic("unimplemented")
}

// Next implements (base.InternalIterator).Next.
func (i *InternalIteratorShim) Next() (*base.InternalKey, []byte) {
	i.frags = i.miter.Next()
	if i.frags.Empty() {
		return nil, nil
	}
	s := i.frags.At(0)
	i.iterKey = s.Start
	return &i.iterKey, s.End
}

// Prev implements (base.InternalIterator).Prev.
func (i *InternalIteratorShim) Prev() (*base.InternalKey, []byte) {
	panic("unimplemented")
}

// Error implements (base.InternalIterator).Error.
func (i *InternalIteratorShim) Error() error {
	return i.miter.Error()
}

// Close implements (base.InternalIterator).Close.
func (i *InternalIteratorShim) Close() error {
	return i.miter.Close()
}

// SetBounds implements (base.InternalIterator).SetBounds.
func (i *InternalIteratorShim) SetBounds(lower, upper []byte) {
	i.miter.SetBounds(lower, upper)
}

// String implements fmt.Stringer.
func (i *InternalIteratorShim) String() string {
	return i.miter.String()
}
