// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"fmt"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/rangekey"
)

var errCorruptRangeKey = base.CorruptionErrorf("pebble/table: corrupt range key entry")

// RangeKeyIter provides an iterator over a sstable's range keys, surfacing
// RangeKeySet, RangeKeyUnset and RangeKeyDelete keys. Although physically
// overlapping range keys may be coalesced into a single internal key,
// RangeKeyIter iterates over each as separate entries.
type RangeKeyIter struct {
	iter      base.InternalIterator
	iterKey   *base.InternalKey
	iterValue []byte
	valueRem  []byte
	curr      RangeKeyItem
	err       error
}

func newRangeKeyIter(iter base.InternalIterator) *RangeKeyIter {
	return &RangeKeyIter{iter: iter}
}

// RangeKeyItem represents a single RangeKeySet, RangeKeyUnset or RangeKeyDelete
// item. Overlapping range keys may be stored physically as a single internal
// key. A RangeKeyItem holds the state of just one of these items.
type RangeKeyItem struct {
	// Kind indicates which kind of internal range key is represented. It may be
	// one of:
	//
	// - InternalKeyKindRangeKeySet
	// - InternalKeyKindRangeKeyUnset
	// - InternalKeyKindRangeKeyDelete
	Kind InternalKeyKind
	// SeqNum holds the internal sequence number of the range key.
	SeqNum uint64
	// Start is a user key forming the inclusive start boundary of the range
	// key. All keys within the bounds [Start, End) are considered covered by
	// the range key.
	Start []byte
	// End is a user key forming the exclusive end boundary of the range
	// key. All keys within the bounds [Start, End) are considered covered by
	// the range key.
	End []byte
	// Suffix is the suffix, if any, associated with a RangeKeySet or
	// RangeKeyUnset. Suffix is always nil for RangeKeyDelete keys, and is
	// optionally nil for RangeKeySet or RangeKeyUnset keys.
	Suffix []byte
	// Value is the user-provided value associated with a RangeKeySet. Value is
	// nil for RangeKeyUnset and RangeKeyDelete keys.
	Value []byte
}

// Close closes the range key iterator, releasing all acquired resources.
func (i *RangeKeyIter) Close() error {
	return i.iter.Close()
}

// Error returns any error encountered during iteration. If any positioning
// method returns nil, the caller must check Error to distinguish between an
// exhausted iterator and an error.
func (i *RangeKeyIter) Error() error {
	return firstError(i.err, i.iter.Error())
}

// First seeks the iterator to the first range key and returns it.
func (i *RangeKeyIter) First() *RangeKeyItem {
	i.iterKey, i.iterValue = i.iter.First()
	return i.newInternalKey(i.iter.First())
}

// Next advances to the next range key and returns it.
func (i *RangeKeyIter) Next() *RangeKeyItem {
	if len(i.valueRem) > 0 {
		if i.err = i.decodeNextItem(); i.err != nil {
			return nil
		}
		return &i.curr
	}
	return i.newInternalKey(i.iter.Next())
}

func (i *RangeKeyIter) newInternalKey(ik *InternalKey, v []byte) *RangeKeyItem {
	i.curr = RangeKeyItem{}
	i.iterKey, i.iterValue = ik, v
	if i.iterKey == nil {
		return nil
	}
	endKey, value, ok := rangekey.DecodeEndKey(i.iterKey.Kind(), v)
	if !ok {
		panic("pebble: unable to decode rangekey end")
	}
	i.curr = RangeKeyItem{
		Kind:   i.iterKey.Kind(),
		SeqNum: i.iterKey.SeqNum(),
		Start:  i.iterKey.UserKey,
		End:    endKey,
	}

	// Decode the first suffix-value tuple (RANGEKEYSET) or suffix
	// (RANGEKEYUNSET).
	i.valueRem = value
	if i.err = i.decodeNextItem(); i.err != nil {
		return nil
	}
	return &i.curr
}

// decodeNextItem decodes the next suffix-value tuple or suffix, depending on
// iterKey's kind, from `i.valueRem`.
func (i *RangeKeyIter) decodeNextItem() error {
	switch kind := i.iterKey.Kind(); kind {
	case InternalKeyKindRangeKeySet:
		sv, rest, ok := rangekey.DecodeSuffixValue(i.valueRem)
		if !ok {
			return errCorruptRangeKey
		}
		i.curr.Suffix = sv.Suffix
		i.curr.Value = sv.Value
		i.valueRem = rest
	case InternalKeyKindRangeKeyUnset:
		suffix, rest, ok := rangekey.DecodeSuffix(i.valueRem)
		if !ok {
			return errCorruptRangeKey
		}
		i.curr.Suffix = suffix
		i.valueRem = rest
	case InternalKeyKindRangeKeyDelete:
	default:
		panic(fmt.Sprintf("unrecognized rangekey kind %q", kind))
	}
	return nil
}
