// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import "github.com/cockroachdb/pebble/internal/base"

// VisibleIter wraps an Iter, filtering out any spans not visible at the
// snapshot sequence number SeqNum.
type VisibleIter struct {
	keyspanIter
	SeqNum uint64
}

// Init initializes the visible iter to determine visiblity at the provided
// snapshot seqNum and to iterate over the provided spans.
func (i *VisibleIter) Init(cmp base.Compare, seqNum uint64, spans []Span) {
	*i = VisibleIter{
		keyspanIter: keyspanIter{
			cmp:   cmp,
			spans: spans,
			index: -1,
		},
		SeqNum: seqNum,
	}
}

// VisibleIter implements the base.InternalIterator interface.
var _ base.InternalIterator = (*VisibleIter)(nil)

// SeekGE implements InternalIterator.SeekGE, as documented in the
// internal/base package.
func (i *VisibleIter) SeekGE(key []byte) (*base.InternalKey, []byte) {
	k, v := i.keyspanIter.SeekGE(key)
	if k != nil && !k.Visible(i.SeqNum) {
		return i.Next()
	}
	return k, v
}

// SeekPrefixGE implements InternalIterator.SeekPrefixGE, as documented in the
// internal/base package.
func (i *VisibleIter) SeekPrefixGE(prefix, key []byte, trySeekUsingNext bool) (*base.InternalKey, []byte) {
	// This should never be called as prefix iteration is only done for point records.
	panic("pebble: SeekPrefixGE unimplemented")
}

// SeekLT implements InternalIterator.SeekLT, as documented in the
// internal/base package.
func (i *VisibleIter) SeekLT(key []byte) (*base.InternalKey, []byte) {
	k, v := i.keyspanIter.SeekLT(key)
	if k != nil && !k.Visible(i.SeqNum) {
		return i.Prev()
	}
	return k, v
}

// First implements InternalIterator.First, as documented in the internal/base
// package.
func (i *VisibleIter) First() (*base.InternalKey, []byte) {
	k, v := i.keyspanIter.First()
	if k != nil && !k.Visible(i.SeqNum) {
		return i.Next()
	}
	return k, v
}

// Last implements InternalIterator.Last, as documented in the internal/base
// package.
func (i *VisibleIter) Last() (*base.InternalKey, []byte) {
	k, v := i.keyspanIter.Last()
	if k != nil && !k.Visible(i.SeqNum) {
		return i.Prev()
	}
	return k, v
}

// Next implements InternalIterator.Next, as documented in the internal/base
// package.
func (i *VisibleIter) Next() (*base.InternalKey, []byte) {
	k, v := i.keyspanIter.Next()
	for k != nil && !k.Visible(i.SeqNum) {
		k, v = i.keyspanIter.Next()
	}
	return k, v
}

// Prev implements InternalIterator.Prev, as documented in the internal/base
// package.
func (i *VisibleIter) Prev() (*base.InternalKey, []byte) {
	k, v := i.keyspanIter.Prev()
	for k != nil && !k.Visible(i.SeqNum) {
		k, v = i.keyspanIter.Prev()
	}
	return k, v
}

// Key implements InternalIterator.Key, as documented in the internal/base
// package.
func (i *VisibleIter) Key() *base.InternalKey {
	return i.keyspanIter.Key()
}

// Value implements InternalIterator.Value, as documented in the internal/base
// package.
func (i *VisibleIter) Value() []byte {
	return i.keyspanIter.Value()
}

// Valid implements InternalIterator.Valid, as documented in the internal/base
// package.
func (i *VisibleIter) Valid() bool {
	return i.keyspanIter.Valid()
}

// Error implements InternalIterator.Error, as documented in the internal/base
// package.
func (i *VisibleIter) Error() error {
	return nil
}

// Close implements InternalIterator.Close, as documented in the internal/base
// package.
func (i *VisibleIter) Close() error {
	return nil
}

// SetBounds implements InternalIterator.SetBounds, as documented in the
// internal/base package.
func (i *VisibleIter) SetBounds(lower, upper []byte) {
	// This should never be called as bounds are only used for point records.
	panic("pebble: SetBounds unimplemented")
}

func (i *VisibleIter) String() string {
	return i.keyspanIter.String()
}
