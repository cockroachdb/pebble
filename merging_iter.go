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

type mergingIterItem struct {
	index int
	key   db.InternalKey
}

type mergingIterHeap struct {
	cmp     db.Compare
	reverse bool
	items   []mergingIterItem
}

func (h *mergingIterHeap) len() int {
	return len(h.items)
}

func (h *mergingIterHeap) less(i, j int) bool {
	ikey, jkey := h.items[i].key, h.items[j].key
	if c := h.cmp(ikey.UserKey, jkey.UserKey); c != 0 {
		if h.reverse {
			return c > 0
		}
		return c < 0
	}
	if h.reverse {
		return ikey.Trailer < jkey.Trailer
	}
	return ikey.Trailer > jkey.Trailer
}

func (h *mergingIterHeap) swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

// init, fix, up and down are copied from the go stdlib.
func (h *mergingIterHeap) init() {
	// heapify
	n := h.len()
	for i := n/2 - 1; i >= 0; i-- {
		h.down(i, n)
	}
}

func (h *mergingIterHeap) fix(i int) {
	if !h.down(i, h.len()) {
		h.up(i)
	}
}

func (h *mergingIterHeap) pop() *mergingIterItem {
	n := h.len() - 1
	h.swap(0, n)
	h.down(0, n)
	item := &h.items[n]
	h.items = h.items[:n]
	return item
}

func (h *mergingIterHeap) up(j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !h.less(j, i) {
			break
		}
		h.swap(i, j)
		j = i
	}
}

func (h *mergingIterHeap) down(i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && h.less(j2, j1) {
			j = j2 // = 2*i + 2  // right child
		}
		if !h.less(j, i) {
			break
		}
		h.swap(i, j)
		i = j
	}
	return i > i0
}

// mergingIter provides a merged view of multiple iterators from different
// levels of the LSM.
//
// A mergingIter can optionally be configured with a slice of range deletion
// iterators which it will use to skip over keys covered by range
// tombstones. The range deletion iterator slice must exactly parallel the
// point iterators. This requirement allows mergingIter to only consider range
// tombstones from newer levels. Because range tombstones are fragmented with a
// level we know there can be no overlap within a level. When a level iter is
// backed by a levelIter, the levelIter takes care of initializing the range
// deletion iterator when switching tables. Note that levelIter also takes care
// of materializing fake "sentinel" keys at sstable boundaries to prevent
// iterating past a table when a range tombstone is the last (or first) entry
// in the table.
//
// Internally, mergingIter advances the iterators for each level by keeping
// them in a heap and advancing the iterator with the smallest key (or largest
// key during reverse iteration). Due to LSM invariants, the range deletion
// tombstones for each level are guaranteed to shadow lower levels. If we're
// seeking to a key X within a level and there is a tombstone within that level
// containing X, in lower levels we can seek to the boundary of the tombstone
// instead. See SeekGE for more details.
//
// Every level can have one or both of point operations (Px) and range
// deletions (Rx). The point operations and range deletions within a level can
// have overlapping sequence numbers, but between levels we are guaranteed
// independence. The range deletions for level N are guaranteed to have newer
// sequence numbers than any entry in level Y > n. Consider the example below
// which has 4 levels. R3 is empty indicating there are no range deletions.
//
//   P0:               o
//   R0:             m---q
//
//   P1:              n p
//   R1:       g---k
//
//   P2:  b d    i
//   R2: a---e           q----v
//
//   P3:     e
//   R3:
//
// If we start iterating from the beginning, the first key we encounter is "b"
// in P2. When the mergingIter is pointing at a valid entry, the range deletion
// iterators for all of the levels < m.heap.items[0].index are positioned at
// the next range tombstone past the current key. (Note that if those levels
// contained a range tombstone covering the current key then the current key
// would be deleted and thus the iterator would not be pointing at it). The
// position of the range deletion iterators for all other levels is unspecified
// (they could point anywhere).
//
// Advancing the iterator finds the next key at "d". This is in the same level
// as the previous entry so we don't have to reposition any of the range
// deletion iterators, but merely check whether "d" is now containe by any of
// the range tombstones at higher levels are has stepped past the range
// tombstone in its own level. In this case, there is nothing to be
// done.
//
// Advancing the iterator again brings the iterator to "e". Since "e" comes
// from P3, we have to position the R3 range deletion iterator, which is
// empty. "e" is past the R2 tombstone of [a,e) so we need to advance that
// range deletion iterator to [q,v).
//
// The next key is "i". Because this key is on a level above "e" we don't have
// to reposition any range deletion iterators and instead see that "i" is
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
func newMergingIter(cmp db.Compare, iters ...internalIterator) *mergingIter {
	m := &mergingIter{}
	m.init(cmp, iters...)
	return m
}

func (m *mergingIter) init(cmp db.Compare, iters ...internalIterator) {
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
			})
		}
	}
	m.heap.init()
}

func (m *mergingIter) initMinHeap() {
	m.dir = 1
	m.heap.reverse = false
	m.initHeap()

	if m.rangeDelIters != nil && m.heap.len() > 0 {
		// Position the range-del iterators at levels <= m.heap.items[0].index.
		item := &m.heap.items[0]
		for level := 0; level <= item.index; level++ {
			rangeDelIter := m.rangeDelIters[level]
			if rangeDelIter == nil {
				continue
			}
			_ = rangedel.SeekGE(m.heap.cmp, rangeDelIter, item.key.UserKey, m.snapshot)
		}
	}
}

func (m *mergingIter) initMaxHeap() {
	m.dir = -1
	m.heap.reverse = true
	m.initHeap()

	if m.rangeDelIters != nil && m.heap.len() > 0 {
		// Position the range-del iterators at levels <= m.heap.items[0].index.
		item := &m.heap.items[0]
		for level := 0; level <= item.index; level++ {
			rangeDelIter := m.rangeDelIters[level]
			if rangeDelIter == nil {
				continue
			}
			_ = rangedel.SeekLE(m.heap.cmp, rangeDelIter, item.key.UserKey, m.snapshot)
		}
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
		if !i.Valid() {
			i.Next()
		}
		for ; i.Valid(); i.Next() {
			if db.InternalCompare(m.heap.cmp, key, i.Key()) < 0 {
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
		if !i.Valid() {
			i.Prev()
		}
		for ; i.Valid(); i.Prev() {
			if db.InternalCompare(m.heap.cmp, key, i.Key()) > 0 {
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
	iter := m.iters[item.index]
	if iter.Next() {
		item.key = iter.Key()
		m.heap.fix(0)
		return
	}

	m.err = iter.Error()
	if m.err == nil {
		m.heap.pop()
	}
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
		if m.heap.cmp(tombstone.End, item.key.UserKey) <= 0 {
			// The current key is at or past the tombstone end key.
			tombstone = rangedel.SeekGE(m.heap.cmp, rangeDelIter, item.key.UserKey, m.snapshot)
		}
		if tombstone.Empty() {
			continue
		}
		if tombstone.Contains(m.heap.cmp, item.key.UserKey) {
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
	iter := m.iters[item.index]
	if iter.Prev() {
		item.key = iter.Key()
		m.heap.fix(0)
		return
	}

	m.err = iter.Error()
	if m.err == nil {
		m.heap.pop()
	}
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
		if m.heap.cmp(item.key.UserKey, tombstone.Start.UserKey) < 0 {
			// The current key is before the tombstone start key8.
			tombstone = rangedel.SeekLE(m.heap.cmp, rangeDelIter, item.key.UserKey, m.snapshot)
		}
		if tombstone.Empty() {
			continue
		}
		if tombstone.Contains(m.heap.cmp, item.key.UserKey) {
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
				tombstone := rangedel.SeekGE(m.heap.cmp, rangeDelIter, key, m.snapshot)
				if !tombstone.Empty() && tombstone.Contains(m.heap.cmp, key) {
					key = tombstone.End
				}
			}
		}
	}

	m.initMinHeap()
}

func (m *mergingIter) SeekGE(key []byte) {
	m.seekGE(key, 0 /* start level */)
	m.findNextEntry()
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
				tombstone := rangedel.SeekLE(m.heap.cmp, rangeDelIter, key, m.snapshot)
				if !tombstone.Empty() && tombstone.Contains(m.heap.cmp, key) {
					key = tombstone.Start.UserKey
				}
			}
		}
	}

	m.initMaxHeap()
}

func (m *mergingIter) SeekLT(key []byte) {
	m.seekLT(key, 0 /* start level */)
	m.findPrevEntry()
}

func (m *mergingIter) First() {
	for _, t := range m.iters {
		t.First()
	}
	m.initMinHeap()
	m.findNextEntry()
}

func (m *mergingIter) Last() {
	for _, t := range m.iters {
		t.Last()
	}
	m.initMaxHeap()
	m.findPrevEntry()
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
	if m.heap.len() == 0 || m.err != nil {
		return db.InvalidInternalKey
	}
	return m.heap.items[0].key
}

func (m *mergingIter) Value() []byte {
	if m.heap.len() == 0 || m.err != nil {
		return nil
	}
	return m.iters[m.heap.items[0].index].Value()
}

func (m *mergingIter) Valid() bool {
	if m.heap.len() == 0 || m.err != nil {
		return false
	}
	return true
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
