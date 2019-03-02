// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"

	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/internal/rangedel"
)

// mergingIter provides a merged view of multiple iterators from different
// levels of the LSM.
//
// The core of a mergingIter is a heap of internalIterators (see
// mergingIterHeap). The heap can operate as either a min-heap, used during
// forward iteration (First, SeekGE, Next) or a max-heap, used during reverse
// iteration (Last, SeekLT, Prev). The heap is initialized in calls to First,
// Last, SeekGE, and SeekLT. A call to Next or Prev takes the current top
// element on the heap, advances its iterator, and then "fixes" the heap
// property. When one of the child iterators is exhausted during Next/Prev
// iteration, it is removed from the heap.
//
// Range Deletions
//
// A mergingIter can optionally be configured with a slice of range deletion
// iterators. The range deletion iterator slice must exactly parallel the point
// iterators and the range deletion iterator must correspond to the same level
// in the LSM as the point iterator. Note that each memtable and each table in
// L0 is a different "level" from the mergingIter perspective. So level 0 below
// does not correspond to L0 in the LSM.
//
// A range deletion iterator iterates over fragmented range tombstones. Range
// tombstones are fragmented by splitting them at any overlapping points. This
// fragmentation guarantees that within an sstable tombstones will either be
// distinct or will have identical start and end user keys. While range
// tombstones are fragmented within an sstable, the end keys are not truncated
// to sstable boundaries. This is necessary because the tombstone end key is
// exclusive and does not have a sequence number. Consider an sstable
// containing the range tombstone [a,c)#9 and the key "b#8". The tombstone must
// delete "b#8", yet older versions of "b" might spill over to the next
// sstable. So the boundary key for this sstable must be "b#8". Adjusting the
// end key of tombstones to be optionally inclusive or contain a sequence
// number would be possible solutions. The approach taken here performs an
// implicit truncation of the tombstone to the sstable boundaries.
//
// During initialization of a mergingIter, the range deletion iterators for
// batches, memtables, and L0 tables are populated up front. Note that Batches
// and memtables index unfragmented tombstones.  Batch.newRangeDelIter() and
// memTable.newRangeDelIter() fragment and cache the tombstones on demand. The
// L1-L6 range deletion iterators are populated by levelIter. When configured
// to load range deletion iterators, whenever a levelIter loads a table it
// loads both the point iterator and the range deletion
// iterator. levelIter.rangeDelIter is configured to point to the right entry
// in mergingIter.rangeDelIters. The effect of this setup is that
// mergingIter.rangeDelIters[i] always contains the fragmented range tombstone
// for the current table in level i that the levelIter has open.
//
// Another crucial mechanism of levelIter is that it materializes fake point
// entries for the table boundaries if the boundary is range deletion
// key. Consider a table that contains only a range tombstone [a-e)#10. The
// sstable boundaries for this table will be a#10,15 and
// e#72057594037927935,15. During forward iteration levelIter will return
// e#72057594037927935,15 as a key. During reverse iteration levelIter will
// return a#10,15 as a key. These sentinel keys act as bookends to point
// iteration and allow mergingIter to keep a table and its associated range
// tombstones loaded as long as their are keys at lower levels that are within
// the bounds of the table.
//
// The final piece to the range deletion puzzle is the LSM invariant that for a
// given key K newer versions of K can only exist earlier in the level, or at
// higher levels of the tree. For example, if K#4 exists in L3, k#5 can only
// exist earlier in the L3 or in L0, L1, L2 or a memtable. Get very explicitly
// uses this invariant to find the value for a key by walking the LSM level by
// level. For range deletions, this invariant means that a range deletion at
// level N will necessarily shadow any keys within its bounds in level Y where
// Y > N. One wrinkle to this statement is that it only applies to keys that
// lie within the sstable bounds as well, but we get that guarantee due to the
// way the range deletion iterator and point iterator are bound together by a
// levelIter.
//
// Tying the above all together, we get a picture where each level (index in
// mergingIter.{iters,rangeDelIters}) is composed of both point operations (pX)
// and range deletions (rX). The range deletions for level X shadow both the
// point operations and range deletions for level Y where Y > X allowing
// mergingIter to skip processing entries in that shadow. For example, consider
// the scenario:
//
//   r0: a---e
//   r1:    d---h
//   r2:       g---k
//   r3:          j---n
//   r4:             m---q
//
// This is showing 5 levels of range deletions. Consider what happens upon
// SeekGE("b"). We first seek the point iterator for level 0 (the point values
// are not shown above) and we then seek the range deletion iterator. That
// returns the tombstone [a,e). This tombstone tells us that all keys in the
// range [a,e) in lower levels are deleted so we can skip them. So we can
// adjust the seek key to "e", the tombstone end key. For level 1 we seek to
// "e" and find the range tombstone [d,h) and similar logic holds. By the time
// we get to level 4 we're seeking to "n".
//
// During actual iteration levels can contain both point operations and range
// deletions. Within a level, when a range deletion contains a point operation
// the sequence numbers must must be checked to determine if the point
// operation is newer or older than the range deletion tombstone. The
// mergingIter maintains the invariant that the range deletion iterators for
// all levels newer that the current iteration key (L < m.heap.items[0].index)
// are positioned at the next (or previous during reverse iteration) range
// deletion tombstone. We know those levels don't contain a range deletion
// tombstone that covers the current key because if they did the current key
// would be deleted. The range deletion iterator for the current key's level is
// positioned at a range tombstone covering or past the current key. The
// position of all of other range deletion iterators is unspecified. Whenever a
// key from those levels becomes the current key, their range deletion
// iterators need to be positioned. This lazy positioning avoids seeking the
// range deletion iterators for keys that are never considered. (A similar bit
// of lazy evaluation can be done for the point iterators, but is still TBD).
//
// For a full example, consider the following setup:
//
//   p0:               o
//   r0:             m---q
//
//   p1:              n p
//   r1:       g---k
//
//   p2:  b d    i
//   r2: a---e           q----v
//
//   p3:     e
//   r3:
//
// If we start iterating from the beginning, the first key we encounter is "b"
// in p2. When the mergingIter is pointing at a valid entry, the range deletion
// iterators for all of the levels < m.heap.items[0].index are positioned at
// the next range tombstone past the current key. So r0 will point at [m,q) and
// r1 at [g,k). When the key "b" is encountered, we check to see if the current
// tombstone for r0 or r1 contains it, and whether the tombstone for r2, [a,e),
// contains and is newer than "b".
//
// Advancing the iterator finds the next key at "d". This is in the same level
// as the previous key "b" so we don't have to reposition any of the range
// deletion iterators, but merely check whether "d" is now contained by any of
// the range tombstones at higher levels or has stepped past the range
// tombstone in its own level. In this case, there is nothing to be done.
//
// Advancing the iterator again finds "e". Since "e" comes from p3, we have to
// position the r3 range deletion iterator, which is empty. "e" is past the r2
// tombstone of [a,e) so we need to advance the r2 range deletion iterator to
// [q,v).
//
// The next key is "i". Because this key is on p2, a level above "e", we don't
// have to reposition any range deletion iterators and instead see that "i" is
// covered by the range tombstone [g,k). The iterator is immediately advanced
// to "n" which is covered by the range tombstone [m,q) causing the iterator to
// advance to "o" which is visible.
//
// TODO(peter,rangedel): For testing, advance the iterator through various
// scenarios and have each step display the current state (i.e. the current
// heap and range-del iterator positioning).
type mergingIter struct {
	dir           int
	snapshot      uint64
	iters         []internalIterator
	rangeDelIters []internalIterator
	heap          mergingIterHeap
	err           error
}

// mergingIter implements the internalIterator interface.
var _ internalIterator = (*mergingIter)(nil)

// newMergingIter returns an iterator that merges its input. Walking the
// resultant iterator will return all key/value pairs of all input iterators
// in strictly increasing key order, as defined by cmp.
//
// The input's key ranges may overlap, but there are assumed to be no duplicate
// keys: if iters[i] contains a key k then iters[j] will not contain that key k.
//
// None of the iters may be nil.
func newMergingIter(cmp *db.Comparer, iters ...internalIterator) *mergingIter {
	m := &mergingIter{}
	m.init(cmp, iters...)
	return m
}

func (m *mergingIter) init(cmp *db.Comparer, iters ...internalIterator) {
	m.snapshot = db.InternalKeySeqNumMax
	m.iters = iters
	m.heap.cmp = cmp
	m.heap.items = make([]mergingIterItem, 0, len(iters))
	m.initMinHeap()
}

func (m *mergingIter) initHeap() {
	m.heap.items = m.heap.items[:0]
	for i, t := range m.iters {
		if t.Valid() {
			m.heap.items = append(m.heap.items, mergingIterItem{
				index: i,
				key:   t.Key(),
				value: t.Value(),
				abbreviatedKey: m.heap.cmp.AbbreviatedKey(t.Key().UserKey),
			})
		}
	}
	m.heap.init()
}

func (m *mergingIter) initMinHeap() {
	m.dir = 1
	m.heap.reverse = false
	m.initHeap()
	m.initMinRangeDelIters(-1)
}

func (m *mergingIter) initMinRangeDelIters(oldTopLevel int) {
	if m.rangeDelIters == nil || m.heap.len() == 0 {
		return
	}

	// Position the range-del iterators at levels <= m.heap.items[0].index.
	item := &m.heap.items[0]
	for level := oldTopLevel + 1; level <= item.index; level++ {
		rangeDelIter := m.rangeDelIters[level]
		if rangeDelIter == nil {
			continue
		}
		_ = rangedel.SeekGE(m.heap.cmp.Compare, rangeDelIter, item.key.UserKey, m.snapshot)
	}
}

func (m *mergingIter) initMaxHeap() {
	m.dir = -1
	m.heap.reverse = true
	m.initHeap()
	m.initMaxRangeDelIters(-1)
}

func (m *mergingIter) initMaxRangeDelIters(oldTopLevel int) {
	if m.rangeDelIters == nil || m.heap.len() == 0 {
		return
	}
	// Position the range-del iterators at levels <= m.heap.items[0].index.
	item := &m.heap.items[0]
	for level := oldTopLevel + 1; level <= item.index; level++ {
		rangeDelIter := m.rangeDelIters[level]
		if rangeDelIter == nil {
			continue
		}
		_ = rangedel.SeekLE(m.heap.cmp.Compare, rangeDelIter, item.key.UserKey, m.snapshot)
	}
}

func (m *mergingIter) switchToMinHeap() {
	if m.heap.len() == 0 {
		m.First()
		return
	}

	// We're switching from using a max heap to a min heap. We need to advance
	// any iterator that is less than or equal to the current key. Consider the
	// scenario where we have 2 iterators being merged (user-key:seq-num):
	//
	// i1:     *a:2     b:2
	// i2: a:1      b:1
	//
	// The current key is a:2 and i2 is pointed at a:1. When we switch to forward
	// iteration, we want to return a key that is greater than a:2.

	key := m.heap.items[0].key
	cur := m.iters[m.heap.items[0].index]

	for _, i := range m.iters {
		if i == cur {
			continue
		}
		valid := i.Valid()
		if !valid {
			valid = i.Next()
		}
		for ; valid; valid = i.Next() {
			if db.InternalCompare(m.heap.cmp.Compare, key, i.Key()) < 0 {
				// key < iter-key
				break
			}
			// key >= iter-key
		}
	}

	// Special handling for the current iterator because we were using its key
	// above.
	cur.Next()
	m.initMinHeap()
}

func (m *mergingIter) switchToMaxHeap() {
	if m.heap.len() == 0 {
		m.Last()
		return
	}

	// We're switching from using a min heap to a max heap. We need to backup any
	// iterator that is greater than or equal to the current key. Consider the
	// scenario where we have 2 iterators being merged (user-key:seq-num):
	//
	// i1: a:2     *b:2
	// i2:     a:1      b:1
	//
	// The current key is b:2 and i2 is pointing at b:1. When we switch to
	// reverse iteration, we want to return a key that is less than b:2.
	key := m.heap.items[0].key
	cur := m.iters[m.heap.items[0].index]

	for _, i := range m.iters {
		if i == cur {
			continue
		}
		valid := i.Valid()
		if !valid {
			valid = i.Prev()
		}
		for ; valid; valid = i.Prev() {
			if db.InternalCompare(m.heap.cmp.Compare, key, i.Key()) > 0 {
				// key > iter-key
				break
			}
			// key <= iter-key
		}
	}

	// Special handling for the current iterator because we were using its key
	// above.
	cur.Prev()
	m.initMaxHeap()
}

func (m *mergingIter) nextEntry(item *mergingIterItem) {
	oldTopLevel := item.index
	iter := m.iters[item.index]
	if iter.Next() {
		item.key, item.value = iter.Key(), iter.Value()
		if m.heap.len() > 1 {
			m.heap.fix(0)
		}
	} else {
		m.err = iter.Error()
		if m.err == nil {
			m.heap.pop()
		}
	}
	m.initMinRangeDelIters(oldTopLevel)
}

func (m *mergingIter) isNextEntryDeleted(item *mergingIterItem) bool {
	// Look for a range deletion tombstone containing item.key at higher
	// levels (level < item.index). If we find such a range tombstone we know
	// it deletes the key in the current level. Also look for a range
	// deletion at the current level (level == item.index). If we find such a
	// range deletion we need to check whether it is newer than the current
	// entry.
	for level := 0; level <= item.index; level++ {
		rangeDelIter := m.rangeDelIters[level]
		if rangeDelIter == nil || !rangeDelIter.Valid() {
			continue
		}
		tombstone := rangedel.Tombstone{
			Start: rangeDelIter.Key(),
			End:   rangeDelIter.Value(),
		}
		if m.heap.cmp.Compare(tombstone.End, item.key.UserKey) <= 0 {
			// The current key is at or past the tombstone end key.
			tombstone = rangedel.SeekGE(m.heap.cmp.Compare, rangeDelIter, item.key.UserKey, m.snapshot)
		}
		if tombstone.Empty() {
			continue
		}
		if tombstone.Contains(m.heap.cmp.Compare, item.key.UserKey) {
			if level < item.index {
				m.seekGE(tombstone.End, item.index)
				return true
			}
			if tombstone.Deletes(item.key.SeqNum()) {
				m.nextEntry(item)
				return true
			}
		}
	}
	return false
}

func (m *mergingIter) findNextEntry() bool {
	for m.heap.len() > 0 && m.err == nil {
		item := &m.heap.items[0]
		if m.rangeDelIters != nil && m.isNextEntryDeleted(item) {
			continue
		}
		if item.key.Visible(m.snapshot) {
			return true
		}
		m.nextEntry(item)
	}
	return false
}

func (m *mergingIter) prevEntry(item *mergingIterItem) {
	oldTopLevel := item.index
	iter := m.iters[item.index]
	if iter.Prev() {
		item.key, item.value = iter.Key(), iter.Value()
		if m.heap.len() > 1 {
			m.heap.fix(0)
		}
	} else {
		m.err = iter.Error()
		if m.err == nil {
			m.heap.pop()
		}
	}
	m.initMaxRangeDelIters(oldTopLevel)
}

func (m *mergingIter) isPrevEntryDeleted(item *mergingIterItem) bool {
	// Look for a range deletion tombstone containing item.key at higher
	// levels (level < item.index). If we find such a range tombstone we know
	// it deletes the key in the current level. Also look for a range
	// deletion at the current level (level == item.index). If we find such a
	// range deletion we need to check whether it is newer than the current
	// entry.
	for level := 0; level <= item.index; level++ {
		rangeDelIter := m.rangeDelIters[level]
		if rangeDelIter == nil || !rangeDelIter.Valid() {
			continue
		}
		tombstone := rangedel.Tombstone{
			Start: rangeDelIter.Key(),
			End:   rangeDelIter.Value(),
		}
		if m.heap.cmp.Compare(item.key.UserKey, tombstone.Start.UserKey) < 0 {
			// The current key is before the tombstone start key8.
			tombstone = rangedel.SeekLE(m.heap.cmp.Compare, rangeDelIter, item.key.UserKey, m.snapshot)
		}
		if tombstone.Empty() {
			continue
		}
		if tombstone.Contains(m.heap.cmp.Compare, item.key.UserKey) {
			if level < item.index {
				m.seekLT(tombstone.Start.UserKey, item.index)
				return true
			}
			if tombstone.Deletes(item.key.SeqNum()) {
				m.prevEntry(item)
				return true
			}
		}
	}
	return false
}

func (m *mergingIter) findPrevEntry() bool {
	for m.heap.len() > 0 && m.err == nil {
		item := &m.heap.items[0]
		if m.rangeDelIters != nil && m.isPrevEntryDeleted(item) {
			continue
		}
		if item.key.Visible(m.snapshot) {
			return true
		}
		m.prevEntry(item)
	}
	return false
}

func (m *mergingIter) seekGE(key []byte, level int) {
	// When seeking, we can use tombstones to adjust the key we seek to on each
	// level. Consider the series of range tombstones:
	//
	//   1: a---e
	//   2:    d---h
	//   3:       g---k
	//   4:          j---n
	//   5:             m---q
	//
	// If we SeekGE("b") we also find the tombstone "b" resides within in the
	// first level which is [a,e). Regardless of whether this tombstone deletes
	// "b" in that level, we know it deletes "b" in all lower levels, so we
	// adjust the search key in the next level to the tombstone end key "e". We
	// then SeekGE("e") in the second level and find the corresponding tombstone
	// [d,h). This process continues and we end up seeking for "h" in the 3rd
	// level, "k" in the 4th level and "n" in the last level.
	//
	// TODO(peter,rangedel): In addition to the above we can delay seeking a
	// level (and any lower levels) when the current iterator position is
	// contained within a range tombstone at a higher level.

	for ; level < len(m.iters); level++ {
		iter := m.iters[level]
		iter.SeekGE(key)

		if m.rangeDelIters != nil {
			if rangeDelIter := m.rangeDelIters[level]; rangeDelIter != nil {
				// The level has a range-del iterator. Find the tombstone containing
				// the search key.
				tombstone := rangedel.SeekGE(m.heap.cmp.Compare, rangeDelIter, key, m.snapshot)
				if !tombstone.Empty() && tombstone.Contains(m.heap.cmp.Compare, key) {
					key = tombstone.End
				}
			}
		}
	}

	m.initMinHeap()
}

func (m *mergingIter) SeekGE(key []byte) bool {
	m.seekGE(key, 0 /* start level */)
	return m.findNextEntry()
}

func (m *mergingIter) seekLT(key []byte, level int) {
	// See the comment in seekLT regarding using tombstones to adjust the seek
	// target per level.

	for ; level < len(m.iters); level++ {
		m.iters[level].SeekLT(key)

		if m.rangeDelIters != nil {
			if rangeDelIter := m.rangeDelIters[level]; rangeDelIter != nil {
				// The level has a range-del iterator. Find the tombstone containing
				// the search key.
				tombstone := rangedel.SeekLE(m.heap.cmp.Compare, rangeDelIter, key, m.snapshot)
				if !tombstone.Empty() && tombstone.Contains(m.heap.cmp.Compare, key) {
					key = tombstone.Start.UserKey
				}
			}
		}
	}

	m.initMaxHeap()
}

func (m *mergingIter) SeekLT(key []byte) bool {
	m.seekLT(key, 0 /* start level */)
	return m.findPrevEntry()
}

func (m *mergingIter) First() bool {
	for _, t := range m.iters {
		t.First()
	}
	m.initMinHeap()
	return m.findNextEntry()
}

func (m *mergingIter) Last() bool {
	for _, t := range m.iters {
		t.Last()
	}
	m.initMaxHeap()
	return m.findPrevEntry()
}

func (m *mergingIter) Next() bool {
	if m.err != nil {
		return false
	}

	if m.dir != 1 {
		m.switchToMinHeap()
		return m.findNextEntry()
	}

	if m.heap.len() == 0 {
		return false
	}

	m.nextEntry(&m.heap.items[0])
	return m.findNextEntry()
}

func (m *mergingIter) Prev() bool {
	if m.err != nil {
		return false
	}

	if m.dir != -1 {
		m.switchToMaxHeap()
		return m.findPrevEntry()
	}

	if m.heap.len() == 0 {
		return false
	}

	m.prevEntry(&m.heap.items[0])
	return m.findPrevEntry()
}

func (m *mergingIter) Key() db.InternalKey {
	return m.heap.items[0].key
}

func (m *mergingIter) Value() []byte {
	return m.heap.items[0].value
}

func (m *mergingIter) Valid() bool {
	return m.heap.len() > 0 && m.err == nil
}

func (m *mergingIter) Error() error {
	if m.heap.len() == 0 || m.err != nil {
		return m.err
	}
	return m.iters[m.heap.items[0].index].Error()
}

func (m *mergingIter) Close() error {
	for _, iter := range m.iters {
		if err := iter.Close(); err != nil && m.err == nil {
			m.err = err
		}
	}
	for _, iter := range m.rangeDelIters {
		if iter != nil {
			if err := iter.Close(); err != nil && m.err == nil {
				m.err = err
			}
		}
	}
	m.iters = nil
	m.rangeDelIters = nil
	m.heap.items = nil
	return m.err
}

func (m *mergingIter) DebugString() string {
	var buf bytes.Buffer
	sep := ""
	for m.heap.len() > 0 {
		item := m.heap.pop()
		fmt.Fprintf(&buf, "%s%s:%d", sep, item.key.UserKey, item.key.SeqNum())
		sep = " "
	}
	if m.dir == 1 {
		m.initMinHeap()
	} else {
		m.initMaxHeap()
	}
	return buf.String()
}
