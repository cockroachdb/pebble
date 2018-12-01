// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"

	"github.com/petermattis/pebble/db"
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
// A mergingIter can optionally be configured with a slice of range-del
// iterators which it will use to skip over keys covered by range
// tombstones. The range-del iterator slice must exactly parallel the point
// iterators. This requirement allows mergingIter to only consider range
// tombstones from newer levels. Because range tombstones are fragmented with a
// level we know there can be no overlap within a level. When a level iter is
// backed by a levelIter, the levelIter takes care of initializing the
// range-del iterator when switching tables. Note that levelIter also takes
// care of materializing fake "sentinel" keys at sstable boundaries to prevent
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
}

func (m *mergingIter) initMaxHeap() {
	m.dir = -1
	m.heap.reverse = true
	m.initHeap()
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

func (m *mergingIter) findNextEntry() bool {
	for m.heap.len() > 0 && m.err == nil {
		item := &m.heap.items[0]

		if m.rangeDelIters != nil {
			// TODO(peter,rangedel): Avoid repeated work here. Once we find the range
			// tombstone for a level we can avoid seeking again until we reach its
			// boundary.

			// Look for a range deletion tombstone containing item.key at higher
			// levels of the tree (level < item.index). If we find such a range
			// tombstone we know it deletes the key in the current level.
			for level := 0; level < item.index; level++ {
				rangeDelIter := m.rangeDelIters[level]
				if rangeDelIter == nil {
					continue
				}
				tombstone := rangeDelIterGet(m.heap.cmp, rangeDelIter, item.key.UserKey, m.snapshot)
				if !tombstone.Empty() && tombstone.Contains(m.heap.cmp, item.key.UserKey) {
					m.seekGE(tombstone.End, item.index)
					continue
				}
			}

			// Look for a range deletion in the current level that deletes the
			// current key. Note that we can't seek because the range deletion is not
			// guaranteed to cover all of the keys in the current level.
			//
			// TODO(peter,rangedel): Avoid seeking the range-del iterator repeatedly
			// for a tombstone that lies underneath a set of keys within a
			// level. Possible this optimization can be added to rangeDelIterGet.
			if rangeDelIter := m.rangeDelIters[item.index]; rangeDelIter != nil {
				tombstone := rangeDelIterGet(m.heap.cmp, rangeDelIter, item.key.UserKey, m.snapshot)
				if tombstone.Deletes(item.key.SeqNum()) {
					m.nextEntry(item)
					continue
				}
			}
		}

		seqNum := item.key.SeqNum()
		if seqNum < m.snapshot || (seqNum&db.InternalKeySeqNumBatch) != 0 {
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

func (m *mergingIter) findPrevEntry() bool {
	for m.heap.len() > 0 && m.err == nil {
		item := &m.heap.items[0]

		if m.rangeDelIters != nil {
			// Look for a range deletion tombstone containing item.key at higher
			// levels of the tree (level < item.index). If we find such a range
			// tombstone we know it deletes the key in the current level.
			for level := 0; level < item.index; level++ {
				rangeDelIter := m.rangeDelIters[level]
				if rangeDelIter == nil {
					continue
				}
				tombstone := rangeDelIterGet(m.heap.cmp, rangeDelIter, item.key.UserKey, m.snapshot)
				if !tombstone.Empty() && tombstone.Contains(m.heap.cmp, item.key.UserKey) {
					m.seekLT(tombstone.Start.UserKey, item.index)
					continue
				}
			}

			// Look for a range deletion in the current level that deletes the
			// current key. Note that we can't seek because the range deletion is not
			// guaranteed to cover all of the keys in the current level.
			//
			// TODO(peter,rangedel): Avoid seeking the range-del iterator repeatedly
			// for a tombstone that lies underneath a set of keys within a
			// level. Possible this optimization can be added to rangeDelIterGet.
			if rangeDelIter := m.rangeDelIters[item.index]; rangeDelIter != nil {
				tombstone := rangeDelIterGet(m.heap.cmp, rangeDelIter, item.key.UserKey, m.snapshot)
				if tombstone.Deletes(item.key.SeqNum()) {
					m.prevEntry(item)
					continue
				}
			}
		}

		seqNum := item.key.SeqNum()
		if seqNum < m.snapshot || (seqNum&db.InternalKeySeqNumBatch) != 0 {
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
	// 1: a---e
	// 2:    d---h
	// 3:       g---k
	// 4:          j---n
	// 5:             m---q
	//
	// If we SeekGE("b") we also find the tombstone "b" resides within in the
	// first level which is [a,e). Regardless of whether this tombstone deletes
	// "b" in that level, we know it deletes "b" in all lower levels, so we
	// adjust the search key in the next level to the tombstone end key "e". We
	// then SeekGE("e") in the second level and find the corresponding tombstone
	// [d,h). This process continues and we end up seeking for "h" in the 3rd
	// level, "k" in the 4th level and "n" in the last level.

	for ; level < len(m.iters); level++ {
		iter := m.iters[level]
		iter.SeekGE(key)

		if m.rangeDelIters != nil {
			if rangeDelIter := m.rangeDelIters[level]; rangeDelIter != nil {
				// The level has a range-del iterator. Find the tombstone containing the
				// search key.
				tombstone := rangeDelIterGet(m.heap.cmp, rangeDelIter, key, m.snapshot)
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
				// The level has a range-del iterator. Find the tombstone containing the
				// search key.
				tombstone := rangeDelIterGet(m.heap.cmp, rangeDelIter, key, m.snapshot)
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
