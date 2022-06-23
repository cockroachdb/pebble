// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
)

const (
	seqNumL5PointKey = 2
	seqNumL5RangeDel = 1
	seqNumL6All      = 0
)

type tableIterator struct {
	Iterator
	rangeDelIter keyspan.FragmentIterator
}

// NOTE: The physical layout of user keys follows the descending order of freshness
//       (e.g., newer versions precede older versions)

func (i *tableIterator) getReader() *Reader {
	var r *Reader
	switch i.Iterator.(type) {
	case *twoLevelIterator:
		r = i.Iterator.(*twoLevelIterator).reader
	case *singleLevelIterator:
		r = i.Iterator.(*singleLevelIterator).reader
	default:
		panic("tableIterator: i.Iterator is not singleLevelIterator or twoLevelIterator")
	}
	return r
}

func (i *tableIterator) getCmp() Compare {
	var cmp Compare
	switch i.Iterator.(type) {
	case *twoLevelIterator:
		cmp = i.Iterator.(*twoLevelIterator).cmp
	case *singleLevelIterator:
		cmp = i.Iterator.(*singleLevelIterator).cmp
	default:
		panic("tableIterator: i.Iterator is not singleLevelIterator or twoLevelIterator")
	}
	return cmp
}

func (i *tableIterator) isShared() bool {
	r := i.getReader()
	if r.meta != nil && r.meta.IsShared {
		return true
	}
	return false
}

// cmpSharedBound returns -1 if key < smallest, 1 if key > largest,
// or 0 otherwise
func (i *tableIterator) cmpSharedBound(key []byte) int {
	if key == nil {
		return 0
	}
	r, cmp := i.getReader(), i.getCmp()
	lower := r.meta.Smallest.UserKey
	upper := r.meta.Largest.UserKey
	if cmp(key, lower) < 0 {
		return -1
	} else if cmp(key, upper) > 0 {
		return 1
	}
	return 0
}

// Note: the current implementation decouples isLocallyCreated() and isShared() for testing
// purposes. If a table is locally created, the reader should follow the read path of a regular
// iterator despite it is shared or not. Similarly, if a table is not locally created, it is
// inherently shared. This is kinda redundant but in some tests the file metadata is not complete
// which exposes a default value (e.g. 0) and it might direct the code to a wrong path.
// So for now I will just leave these two flags as is..

func (i *tableIterator) isLocallyCreated() bool {
	var r *Reader
	switch i.Iterator.(type) {
	case *twoLevelIterator:
		r = i.Iterator.(*twoLevelIterator).reader
	case *singleLevelIterator:
		r = i.Iterator.(*singleLevelIterator).reader
	default:
		panic("tableIterator: i.Iterator is not singleLevelIterator or twoLevelIterator")
	}
	return i.isShared() && r.meta.CreatorUniqueID == DBUniqueID
}

func (i *tableIterator) setExhaustedBounds(e int8) {
	switch i.Iterator.(type) {
	case *twoLevelIterator:
		i.Iterator.(*twoLevelIterator).exhaustedBounds = e
	case *singleLevelIterator:
		i.Iterator.(*singleLevelIterator).exhaustedBounds = e
	default:
		panic("tableIterator: i.Iterator is not singleLevelIterator or twoLevelIterator")
	}
}

func (i *tableIterator) getCurrUserKey() InternalKey {
	var k InternalKey
	switch i.Iterator.(type) {
	case *twoLevelIterator:
		k = i.Iterator.(*twoLevelIterator).data.ikey
	case *singleLevelIterator:
		k = i.Iterator.(*singleLevelIterator).data.ikey
	default:
		panic("tableIterator: i.Iterator is not singleLevelIterator or twoLevelIterator")
	}
	return k
}

func (i *tableIterator) isKeyDeleted(k *InternalKey) bool {
	if k.Kind() == InternalKeyKindMerge {
		panic("tableIterator: found InternalKeyKindMerge when evaluating key deletion")
	}
	if k.Kind() == InternalKeyKindDelete {
		return true
	}
	// check rangeDel in the same sstable
	if i.rangeDelIter != nil {
		cmp := i.getCmp()
		span := keyspan.SeekGE(cmp, i.rangeDelIter, k.UserKey)
		if span != nil && span.Contains(cmp, k.UserKey) && span.Covers(k.SeqNum()) {
			return true
		}
	}
	return false
}

func setKeySeqNum(key *InternalKey, level int) {
	if level == 5 {
		key.SetSeqNum(seqNumL5PointKey)
	} else if level == 6 {
		key.SetSeqNum(seqNumL6All)
	} else {
		panic("sharedTableIterator: a table with shared flag must have its level at 5 or 6")
	}
}

func (i *tableIterator) seekGEShared(
	prefix, key []byte, trySeekUsingNext bool,
) (*InternalKey, []byte) {
	r := i.getReader()
	ib := i.cmpSharedBound(key)
	if ib > 0 {
		// The search key overflows
		i.setExhaustedBounds(+1)
		return nil, nil
	} else if ib < 0 {
		// The search key underflows, substitute it with the lower shared bound
		key = r.meta.Smallest.UserKey
	}
	var k *InternalKey
	var v []byte
	if prefix == nil {
		k, v = i.Iterator.SeekGE(key, trySeekUsingNext)
	} else {
		k, v = i.Iterator.SeekPrefixGE(prefix, key, trySeekUsingNext)
	}
	if k == nil {
		i.setExhaustedBounds(+1)
		return nil, nil
	}
	// If the table is not locally created (i.e., purely foreign table), update
	// the SeqNum accordingly. Note that we don't need to perform any extra movement
	// here because if k != nil then we are guaranteed to be positioned at the first
	// user key that satisfies the condition, which is the latest version.
	if !i.isLocallyCreated() {
		// if the latest key is a tombstone, omit the current key
		// Note that we don't need to set the SeqNum in this case because the key
		// returned from the last level is either nil or has its SeqNum set correctly
		if i.isKeyDeleted(k) {
			k, v = i.nextShared()
		} else {
			setKeySeqNum(k, i.GetLevel())
		}
	}
	// finally, check upper bound
	if k == nil || i.cmpSharedBound(k.UserKey) > 0 {
		i.setExhaustedBounds(+1)
		return nil, nil
	}
	return k, v
}

func (i *tableIterator) SeekGE(key []byte, trySeekUsingNext bool) (*InternalKey, []byte) {
	// shared path
	if i.isShared() {
		return i.seekGEShared(nil, key, trySeekUsingNext)
	}
	// non-shared path
	return i.Iterator.SeekGE(key, trySeekUsingNext)
}

func (i *tableIterator) SeekPrefixGE(
	prefix, key []byte, trySeekUsingNext bool,
) (*InternalKey, []byte) {
	if i.isShared() {
		return i.seekGEShared(prefix, key, trySeekUsingNext)
	}
	// non-shared path
	return i.Iterator.SeekPrefixGE(prefix, key, trySeekUsingNext)
}

func (i *tableIterator) seekLTShared(key []byte) (*InternalKey, []byte) {
	r, cmp := i.getReader(), i.getCmp()
	ib := i.cmpSharedBound(key)
	if ib < 0 {
		i.setExhaustedBounds(-1)
		return nil, nil
	} else if ib > 0 {
		key = r.meta.Largest.UserKey
	}
	k, v := i.Iterator.SeekLT(key)
	if k == nil {
		i.setExhaustedBounds(-1)
		return nil, nil
	}
	// SeekLT is different from SeekGE as we are at the oldest version for the user key
	// and we need to move to the newest version
	if !i.isLocallyCreated() {
		ik := i.getCurrUserKey()
		k, _ = i.Iterator.Prev()
		for k != nil && cmp(k.UserKey, ik.UserKey) == 0 {
			k, _ = i.Iterator.Prev()
		}
		// now, either k == nil or k < ik, so k is just one slot over
		k, v = i.Iterator.Next()
		// if the latest key is a tombstone, omit the current key
		if i.isKeyDeleted(k) {
			k, v = i.prevShared()
		} else {
			setKeySeqNum(k, i.GetLevel())
		}
	}
	// check lower bound
	if i.cmpSharedBound(k.UserKey) < 0 {
		i.setExhaustedBounds(-1)
		return nil, nil
	}
	return k, v
}

func (i *tableIterator) SeekLT(key []byte) (*InternalKey, []byte) {
	// shared path
	if i.isShared() {
		return i.seekLTShared(key)
	}
	return i.Iterator.SeekLT(key)
}

// First() and Last() are just two synonyms of SeekGE and SeekLT

func (i *tableIterator) First() (*InternalKey, []byte) {
	if i.isShared() {
		// in this case the table must have a smallest key
		return i.seekGEShared(nil, i.getReader().meta.Smallest.UserKey, false)
	}
	return i.Iterator.First()
}

func (i *tableIterator) Last() (*InternalKey, []byte) {
	if i.isShared() {
		// in this case the table must have a smallest key
		return i.seekLTShared(i.getReader().meta.Largest.UserKey)
	}
	return i.Iterator.Last()
}

func (i *tableIterator) nextShared() (*InternalKey, []byte) {
	cmp := i.getCmp()
	// Next() is not a simple case, as a valid position of an iterator
	// for a purely foreign table always points to the latest version of a user key,
	// and all the other versions are not exposed. Therefore, when we move forward,
	// it is highly possible that we encounter these history versions which we should omit,
	// and we can not easily determine when we crossed the key boundaries.
	// To this end, we let tmpIter go first.
	ik := i.getCurrUserKey()
	k, v := i.Iterator.Next()
	if k == nil {
		i.setExhaustedBounds(+1)
		return nil, nil
	}
	if !i.isLocallyCreated() {
		// k is not nil, so it might position to a different key or a invisible history version
		for k != nil && cmp(k.UserKey, ik.UserKey) == 0 {
			k, v = i.Iterator.Next()
		}
		// now one of the following conditions stands:
		//   k == nil, we just return nil, or
		//   k > ik, we found a new key and it is the newest version
		if k == nil {
			i.setExhaustedBounds(+1)
			return nil, nil
		}
		// if the latest key is a tombstone, omit the current key
		if i.isKeyDeleted(k) {
			k, v = i.nextShared()
		} else {
			setKeySeqNum(k, i.GetLevel())
		}
	}
	// check upper bound
	if k == nil || i.cmpSharedBound(k.UserKey) > 0 {
		i.setExhaustedBounds(+1)
		return nil, nil
	}
	return k, v
}

func (i *tableIterator) Next() (*InternalKey, []byte) {
	if i.isShared() {
		return i.nextShared()
	}
	return i.Iterator.Next()
}

func (i *tableIterator) prevShared() (*InternalKey, []byte) {
	cmp := i.getCmp()
	// First move to the previous position, as we must move at least once.
	// Note that if the iterator operates correctly, this Prev() must set the position
	// of the iterator to a different key, as we were exposing the latest point version
	// of a user key, i.e., the first slot.
	k, v := i.Iterator.Prev()
	if k == nil {
		i.setExhaustedBounds(-1)
		return nil, nil
	}
	// if the table is not locally created (i.e., purely foreign table), make sure exactly
	// one version (the latest) of a user key is exposed. The SeqNum needs to be updated accordingly.
	if !i.isLocallyCreated() {
		ik := i.getCurrUserKey()
		// find duplicated keys, or nil, whichever comes first
		for k != nil && cmp(k.UserKey, ik.UserKey) == 0 {
			k, _ = i.Iterator.Prev()
		}
		// At the current moment, either k < ik, or k == nil. So we rewind iter once.
		k, v = i.Iterator.Next()
		if i.isKeyDeleted(k) {
			k, v = i.prevShared()
		} else {
			setKeySeqNum(k, i.GetLevel())
		}
	}
	// check lower bound
	if k == nil || i.cmpSharedBound(k.UserKey) > 0 {
		i.setExhaustedBounds(-1)
		return nil, nil
	}
	return k, v
}

func (i *tableIterator) Prev() (*InternalKey, []byte) {
	if i.isShared() {
		return i.prevShared()
	}
	return i.Iterator.Prev()
}

func (i *tableIterator) Close() error {
	if i.rangeDelIter != nil {
		err := i.rangeDelIter.Close()
		if err != nil {
			panic("tableIterator: internal fragmentBlockIter close() error")
		}
	}
	return i.Iterator.Close()
}

func (i tableIterator) Stats() base.InternalIteratorStats {
	var stats base.InternalIteratorStats
	switch i.Iterator.(type) {
	case *twoLevelIterator:
		stats = i.Iterator.(*twoLevelIterator).stats
	case *singleLevelIterator:
		stats = i.Iterator.(*singleLevelIterator).stats
	default:
		panic("tableIterator: i.Iterator is not singleLevelIterator or twoLevelIterator")
	}
	return stats
}

// ResetStats implements InternalIteratorWithStats.
func (i *tableIterator) ResetStats() {
	switch i.Iterator.(type) {
	case *twoLevelIterator:
		i.Iterator.(*twoLevelIterator).stats = base.InternalIteratorStats{}
	case *singleLevelIterator:
		i.Iterator.(*singleLevelIterator).stats = base.InternalIteratorStats{}
	default:
		panic("tableIterator: i.Iterator is not singleLevelIterator or twoLevelIterator")
	}
}

// Implemented interfaces. All things goes to the internal Interator.
var _ base.InternalIterator = (*tableIterator)(nil)
var _ base.InternalIteratorWithStats = (*tableIterator)(nil)
var _ Iterator = (*tableIterator)(nil)

// This is a copy of the vanilla NewRawRangeDelIter function.
// This function is used when creating a tableIterator because it needs a
// fragmentBlockIter that exposes the real SeqNUms.
// The NewRawRangeDelIter will be wrapped by the rangeDelIter type below
// and the SeqNum for shared L5/L6 will be manipulated.
func (r *Reader) newInternalRangeDelIter() (keyspan.FragmentIterator, error) {
	if r.rangeDelBH.Length == 0 {
		return nil, nil
	}
	h, err := r.readRangeDel()
	if err != nil {
		return nil, err
	}
	i := &fragmentBlockIter{}
	if err := i.blockIter.initHandle(r.Compare, h, r.Properties.GlobalSeqNum); err != nil {
		return nil, err
	}
	return i, nil
}

type rangeDelIter struct {
	fragmentBlockIter
	reader   *Reader
	level    int
	levelSet bool
}

func (i *rangeDelIter) SetLevel(level int) {
	i.levelSet = true
	i.level = level
}

func (i *rangeDelIter) GetLevel() int {
	if !i.levelSet {
		return -1
	}
	return i.level
}

func (i *rangeDelIter) isShared() bool {
	r := i.reader
	if r.meta != nil && r.meta.IsShared {
		return true
	}
	return false
}

func (i *rangeDelIter) isLocallyCreated() bool {
	r := i.reader
	return i.isShared() && r.meta.CreatorUniqueID == DBUniqueID
}

func setSpanSeqNum(s *keyspan.Span, seqnum uint64) {
	for i := range s.Keys {
		trailer := (s.Keys[i].Trailer & 0xff) | (seqnum << 8)
		s.Keys[i].Trailer = trailer
	}
}

func (i *rangeDelIter) filterSpan(s *keyspan.Span) *keyspan.Span {
	if i.isShared() && !i.isLocallyCreated() {
		level := i.GetLevel()
		if level == 5 {
			setSpanSeqNum(s, seqNumL5RangeDel)
		} else if level == 6 {
			setSpanSeqNum(s, seqNumL6All)
		} else {
			panic("rangeDelIter: a table with shared flag must have its level at 5 or 6")
		}
	}
	return s
}

// The following functions only need to have a different behavior if
// the table is not locally created (so it is also shared), which is different
// from tableIterator.

func (i *rangeDelIter) SeekGE(key []byte) *keyspan.Span {
	s := i.fragmentBlockIter.SeekGE(key)
	return i.filterSpan(s)
}

func (i *rangeDelIter) SeekLT(key []byte) *keyspan.Span {
	s := i.fragmentBlockIter.SeekLT(key)
	return i.filterSpan(s)
}

func (i *rangeDelIter) First() *keyspan.Span {
	s := i.fragmentBlockIter.First()
	return i.filterSpan(s)
}

func (i *rangeDelIter) Last() *keyspan.Span {
	s := i.fragmentBlockIter.Last()
	return i.filterSpan(s)
}

func (i *rangeDelIter) Next() *keyspan.Span {
	s := i.fragmentBlockIter.Next()
	return i.filterSpan(s)
}

func (i *rangeDelIter) Prev() *keyspan.Span {
	s := i.fragmentBlockIter.Prev()
	return i.filterSpan(s)
}

var _ keyspan.FragmentIterator = (*rangeDelIter)(nil)

// RangeDelIter exposes the internal rangeDelIter for setting levels
type RangeDelIter = rangeDelIter
