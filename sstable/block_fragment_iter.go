// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/rangedel"
	"github.com/cockroachdb/pebble/internal/rangekey"
)

// fragmentBlockIter wraps a blockIter, implementing the
// keyspan.FragmentIterator interface. It's used for reading range deletion and
// range key blocks.
//
// Range deletions and range keys are fragmented before they're persisted to the
// block. Overlapping fragments have identical bounds.  The fragmentBlockIter
// gathers all the fragments with identical bounds within a block and returns a
// single keyspan.Span describing all the keys defined over the span.
//
// # Memory lifetime
//
// A Span returned by fragmentBlockIter is only guaranteed to be stable until
// the next fragmentBlockIter iteration positioning method. A Span's Keys slice
// may be reused, so the user must not assume it's stable.
//
// Blocks holding range deletions and range keys are configured to use a restart
// interval of 1. This provides key stability. The caller may treat the various
// byte slices (start, end, suffix, value) as stable for the lifetime of the
// iterator.
type fragmentBlockIter struct {
	blockIter blockIter
	keyBuf    [2]keyspan.Key
	span      keyspan.Span
	dir       int8
	closeHook func(i keyspan.FragmentIterator) error

	// elideSameSeqnum, if true, returns only the first-occurring (in forward
	// order) Key for each sequence number.
	elideSameSeqnum bool
}

func (i *fragmentBlockIter) resetForReuse() fragmentBlockIter {
	return fragmentBlockIter{blockIter: i.blockIter.resetForReuse()}
}

func (i *fragmentBlockIter) decodeSpanKeys(k *InternalKey, internalValue []byte) error {
	// TODO(jackson): The use of i.span.Keys to accumulate keys across multiple
	// calls to Decode is too confusing and subtle. Refactor to make it
	// explicit.

	// decode the contents of the fragment's value. This always includes at
	// least the end key: RANGEDELs store the end key directly as the value,
	// whereas the various range key kinds store are more complicated.  The
	// details of the range key internal value format are documented within the
	// internal/rangekey package.
	var err error
	switch k.Kind() {
	case base.InternalKeyKindRangeDelete:
		i.span = rangedel.Decode(*k, internalValue, i.span.Keys)
	case base.InternalKeyKindRangeKeySet, base.InternalKeyKindRangeKeyUnset, base.InternalKeyKindRangeKeyDelete:
		i.span, err = rangekey.Decode(*k, internalValue, i.span.Keys)
	default:
		i.span = keyspan.Span{}
		err = base.CorruptionErrorf("pebble: corrupt keyspan fragment of kind %d", k.Kind())
	}
	return err
}

func (i *fragmentBlockIter) elideKeysOfSameSeqNum() {
	if invariants.Enabled {
		if !i.elideSameSeqnum || len(i.span.Keys) == 0 {
			panic("elideKeysOfSameSeqNum called when it should not be")
		}
	}
	lastSeqNum := i.span.Keys[0].SeqNum()
	k := 1
	for j := 1; j < len(i.span.Keys); j++ {
		if lastSeqNum != i.span.Keys[j].SeqNum() {
			lastSeqNum = i.span.Keys[j].SeqNum()
			i.span.Keys[k] = i.span.Keys[j]
			k++
		}
	}
	i.span.Keys = i.span.Keys[:k]
}

// gatherForward gathers internal keys with identical bounds. Keys defined over
// spans of the keyspace are fragmented such that any overlapping key spans have
// identical bounds. When these spans are persisted to a range deletion or range
// key block, they may be persisted as multiple internal keys in order to encode
// multiple sequence numbers or key kinds.
//
// gatherForward iterates forward, re-combining the fragmented internal keys to
// reconstruct a keyspan.Span that holds all the keys defined over the span.
func (i *fragmentBlockIter) gatherForward(
	k *InternalKey, lazyValue base.LazyValue,
) (*keyspan.Span, error) {
	i.span = keyspan.Span{}
	if k == nil || !i.blockIter.valid() {
		return nil, nil
	}
	// Use the i.keyBuf array to back the Keys slice to prevent an allocation
	// when a span contains few keys.
	i.span.Keys = i.keyBuf[:0]

	// Decode the span's end key and individual keys from the value.
	internalValue := lazyValue.InPlaceValue()
	if err := i.decodeSpanKeys(k, internalValue); err != nil {
		return nil, err
	}
	prevEnd := i.span.End

	// There might exist additional internal keys with identical bounds encoded
	// within the block. Iterate forward, accumulating all the keys with
	// identical bounds to s.
	k, lazyValue = i.blockIter.Next()
	internalValue = lazyValue.InPlaceValue()
	for k != nil && i.blockIter.cmp(k.UserKey, i.span.Start) == 0 {
		if err := i.decodeSpanKeys(k, internalValue); err != nil {
			return nil, err
		}

		// Since k indicates an equal start key, the encoded end key must
		// exactly equal the original end key from the first internal key.
		// Overlapping fragments are required to have exactly equal start and
		// end bounds.
		if i.blockIter.cmp(prevEnd, i.span.End) != 0 {
			i.span = keyspan.Span{}
			return nil, base.CorruptionErrorf("pebble: corrupt keyspan fragmentation")
		}
		k, lazyValue = i.blockIter.Next()
		internalValue = lazyValue.InPlaceValue()
	}
	if i.elideSameSeqnum && len(i.span.Keys) > 0 {
		i.elideKeysOfSameSeqNum()
	}
	// i.blockIter is positioned over the first internal key for the next span.
	return &i.span, nil
}

// gatherBackward gathers internal keys with identical bounds. Keys defined over
// spans of the keyspace are fragmented such that any overlapping key spans have
// identical bounds. When these spans are persisted to a range deletion or range
// key block, they may be persisted as multiple internal keys in order to encode
// multiple sequence numbers or key kinds.
//
// gatherBackward iterates backwards, re-combining the fragmented internal keys
// to reconstruct a keyspan.Span that holds all the keys defined over the span.
func (i *fragmentBlockIter) gatherBackward(
	k *InternalKey, lazyValue base.LazyValue,
) (*keyspan.Span, error) {
	i.span = keyspan.Span{}
	if k == nil || !i.blockIter.valid() {
		return nil, nil
	}
	// Use the i.keyBuf array to back the Keys slice to prevent an allocation
	// when a span contains few keys.
	i.span.Keys = i.keyBuf[:0]

	// Decode the span's end key and individual keys from the value.
	internalValue := lazyValue.InPlaceValue()
	if err := i.decodeSpanKeys(k, internalValue); err != nil {
		return nil, err
	}
	prevEnd := i.span.End

	// There might exist additional internal keys with identical bounds encoded
	// within the block. Iterate backward, accumulating all the keys with
	// identical bounds to s.
	k, lazyValue = i.blockIter.Prev()
	internalValue = lazyValue.InPlaceValue()
	for k != nil && i.blockIter.cmp(k.UserKey, i.span.Start) == 0 {
		if err := i.decodeSpanKeys(k, internalValue); err != nil {
			return nil, err
		}

		// Since k indicates an equal start key, the encoded end key must
		// exactly equal the original end key from the first internal key.
		// Overlapping fragments are required to have exactly equal start and
		// end bounds.
		if i.blockIter.cmp(prevEnd, i.span.End) != 0 {
			i.span = keyspan.Span{}
			return nil, base.CorruptionErrorf("pebble: corrupt keyspan fragmentation")
		}
		k, lazyValue = i.blockIter.Prev()
		internalValue = lazyValue.InPlaceValue()
	}
	// i.blockIter is positioned over the last internal key for the previous
	// span.

	// Backwards iteration encounters internal keys in the wrong order.
	keyspan.SortKeysByTrailer(&i.span.Keys)

	if i.elideSameSeqnum && len(i.span.Keys) > 0 {
		i.elideKeysOfSameSeqNum()
	}
	return &i.span, nil
}

// Close implements (keyspan.FragmentIterator).Close.
func (i *fragmentBlockIter) Close() error {
	var err error
	if i.closeHook != nil {
		err = i.closeHook(i)
	}
	err = firstError(err, i.blockIter.Close())
	return err
}

// First implements (keyspan.FragmentIterator).First
func (i *fragmentBlockIter) First() (*keyspan.Span, error) {
	i.dir = +1
	return i.gatherForward(i.blockIter.First())
}

// Last implements (keyspan.FragmentIterator).Last.
func (i *fragmentBlockIter) Last() (*keyspan.Span, error) {
	i.dir = -1
	return i.gatherBackward(i.blockIter.Last())
}

// Next implements (keyspan.FragmentIterator).Next.
func (i *fragmentBlockIter) Next() (*keyspan.Span, error) {
	switch {
	case i.dir == -1 && !i.span.Valid():
		// Switching directions.
		//
		// i.blockIter is exhausted, before the first key. Move onto the first.
		i.blockIter.First()
		i.dir = +1
	case i.dir == -1 && i.span.Valid():
		// Switching directions.
		//
		// i.blockIter is currently positioned over the last internal key for
		// the previous span. Next it once to move to the first internal key
		// that makes up the current span, and gatherForwaad to land on the
		// first internal key making up the next span.
		//
		// In the diagram below, if the last span returned to the user during
		// reverse iteration was [b,c), i.blockIter is currently positioned at
		// [a,b). The block iter must be positioned over [d,e) to gather the
		// next span's fragments.
		//
		//    ... [a,b) [b,c) [b,c) [b,c) [d,e) ...
		//          ^                       ^
		//     i.blockIter                 want
		if x, err := i.gatherForward(i.blockIter.Next()); err != nil {
			return nil, err
		} else if invariants.Enabled && !x.Valid() {
			panic("pebble: invariant violation: next entry unexpectedly invalid")
		}
		i.dir = +1
	}
	// We know that this blockIter has in-place values.
	return i.gatherForward(&i.blockIter.ikey, base.MakeInPlaceValue(i.blockIter.val))
}

// Prev implements (keyspan.FragmentIterator).Prev.
func (i *fragmentBlockIter) Prev() (*keyspan.Span, error) {
	switch {
	case i.dir == +1 && !i.span.Valid():
		// Switching directions.
		//
		// i.blockIter is exhausted, after the last key. Move onto the last.
		i.blockIter.Last()
		i.dir = -1
	case i.dir == +1 && i.span.Valid():
		// Switching directions.
		//
		// i.blockIter is currently positioned over the first internal key for
		// the next span. Prev it once to move to the last internal key that
		// makes up the current span, and gatherBackward to land on the last
		// internal key making up the previous span.
		//
		// In the diagram below, if the last span returned to the user during
		// forward iteration was [b,c), i.blockIter is currently positioned at
		// [d,e). The block iter must be positioned over [a,b) to gather the
		// previous span's fragments.
		//
		//    ... [a,b) [b,c) [b,c) [b,c) [d,e) ...
		//          ^                       ^
		//        want                  i.blockIter
		if x, err := i.gatherBackward(i.blockIter.Prev()); err != nil {
			return nil, err
		} else if invariants.Enabled && !x.Valid() {
			panic("pebble: invariant violation: previous entry unexpectedly invalid")
		}
		i.dir = -1
	}
	// We know that this blockIter has in-place values.
	return i.gatherBackward(&i.blockIter.ikey, base.MakeInPlaceValue(i.blockIter.val))
}

// SeekGE implements (keyspan.FragmentIterator).SeekGE.
func (i *fragmentBlockIter) SeekGE(k []byte) (*keyspan.Span, error) {
	if s, err := i.SeekLT(k); err != nil {
		return nil, err
	} else if s != nil && i.blockIter.cmp(k, s.End) < 0 {
		return s, nil
	}
	// TODO(jackson): If the above i.SeekLT(k) discovers a span but the span
	// doesn't meet the k < s.End comparison, then there's no need for the
	// SeekLT to gatherBackward.
	return i.Next()
}

// SeekLT implements (keyspan.FragmentIterator).SeekLT.
func (i *fragmentBlockIter) SeekLT(k []byte) (*keyspan.Span, error) {
	i.dir = -1
	return i.gatherBackward(i.blockIter.SeekLT(k, base.SeekLTFlagsNone))
}

// String implements fmt.Stringer.
func (i *fragmentBlockIter) String() string {
	return "fragment-block-iter"
}

// SetCloseHook implements sstable.FragmentIterator.
func (i *fragmentBlockIter) SetCloseHook(fn func(i keyspan.FragmentIterator) error) {
	i.closeHook = fn
}

// WrapChildren implements FragmentIterator.
func (i *fragmentBlockIter) WrapChildren(wrap keyspan.WrapFn) {}
