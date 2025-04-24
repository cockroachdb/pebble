// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"context"
	"fmt"
	"runtime/debug"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/treeprinter"
)

type mergingIterLevel struct {
	index int
	iter  internalIterator
	// rangeDelIter is set to the range-deletion iterator for the level. When
	// configured with a levelIter, this pointer changes as sstable boundaries
	// are crossed. See levelIter.initRangeDel and the Range Deletions comment
	// below.
	rangeDelIter keyspan.FragmentIterator
	// rangeDelIterGeneration is incremented whenever rangeDelIter changes.
	rangeDelIterGeneration int
	// iterKV caches the current key-value pair iter points to.
	iterKV *base.InternalKV
	// levelIter is non-nil if this level's iter is ultimately backed by a
	// *levelIter. The handle in iter may have wrapped the levelIter with
	// intermediary internalIterator implementations.
	levelIter *levelIter

	// tombstone caches the tombstone rangeDelIter is currently pointed at. If
	// tombstone is nil, there are no further tombstones within the
	// current sstable in the current iterator direction. The cached tombstone is
	// only valid for the levels in the range [0,heap[0].index]. This avoids
	// positioning tombstones at lower levels which cannot possibly shadow the
	// current key.
	tombstone *keyspan.Span
}

// Assert that *mergingIterLevel implements rangeDelIterSetter.
var _ rangeDelIterSetter = (*mergingIterLevel)(nil)

func (ml *mergingIterLevel) setRangeDelIter(iter keyspan.FragmentIterator) {
	ml.tombstone = nil
	if ml.rangeDelIter != nil {
		ml.rangeDelIter.Close()
	}
	ml.rangeDelIter = iter
	ml.rangeDelIterGeneration++
}

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
// # Range Deletions
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
// tombstones are fragmented within an sstable, the start and end keys are not truncated
// to sstable boundaries. This is necessary because the tombstone end key is
// exclusive and does not have a sequence number. Consider an sstable
// containing the range tombstone [a,c)#9 and the key "b#8". The tombstone must
// delete "b#8", yet older versions of "b" might spill over to the next
// sstable. So the boundary key for this sstable must be "b#8". Adjusting the
// end key of tombstones to be optionally inclusive or contain a sequence
// number would be possible solutions (such solutions have potentially serious
// issues: tombstones have exclusive end keys since an inclusive deletion end can
// be converted to an exclusive one while the reverse transformation is not possible;
// the semantics of a sequence number for the end key of a range tombstone are murky).
//
// The approach taken here performs an
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
// in mergingIter.levels. The effect of this setup is that
// mergingIter.levels[i].rangeDelIter always contains the fragmented range
// tombstone for the current table in level i that the levelIter has open.
//
// Another crucial mechanism of levelIter is that it materializes fake point
// entries for the table boundaries if the boundary is range deletion
// key. Consider a table that contains only a range tombstone [a-e)#10. The
// sstable boundaries for this table will be a#10,15 and
// e#72057594037927935,15. During forward iteration levelIter will return
// e#72057594037927935,15 as a key. During reverse iteration levelIter will
// return a#10,15 as a key. These sentinel keys act as bookends to point
// iteration and allow mergingIter to keep a table and its associated range
// tombstones loaded as long as there are keys at lower levels that are within
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
// mergingIter.levels) is composed of both point operations (pX) and range
// deletions (rX). The range deletions for level X shadow both the point
// operations and range deletions for level Y where Y > X allowing mergingIter
// to skip processing entries in that shadow. For example, consider the
// scenario:
//
//	r0: a---e
//	r1:    d---h
//	r2:       g---k
//	r3:          j---n
//	r4:             m---q
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
// One consequence of not truncating tombstone end keys to sstable boundaries
// is the seeking process described above cannot always seek to the tombstone
// end key in the older level. For example, imagine in the above example r3 is
// a partitioned level (i.e., L1+ in our LSM), and the sstable containing [j,
// n) has "k" as its upper boundary. In this situation, compactions involving
// keys at or after "k" can output those keys to r4+, even if they're newer
// than our tombstone [j, n). So instead of seeking to "n" in r4 we can only
// seek to "k".  To achieve this, the instance variable `largestUserKey.`
// maintains the upper bounds of the current sstables in the partitioned
// levels. In this example, `levels[3].largestUserKey` holds "k", telling us to
// limit the seek triggered by a tombstone in r3 to "k".
//
// During actual iteration levels can contain both point operations and range
// deletions. Within a level, when a range deletion contains a point operation
// the sequence numbers must be checked to determine if the point operation is
// newer or older than the range deletion tombstone. The mergingIter maintains
// the invariant that the range deletion iterators for all levels newer that
// the current iteration key (L < m.heap.items[0].index) are positioned at the
// next (or previous during reverse iteration) range deletion tombstone. We
// know those levels don't contain a range deletion tombstone that covers the
// current key because if they did the current key would be deleted. The range
// deletion iterator for the current key's level is positioned at a range
// tombstone covering or past the current key. The position of all of other
// range deletion iterators is unspecified. Whenever a key from those levels
// becomes the current key, their range deletion iterators need to be
// positioned. This lazy positioning avoids seeking the range deletion
// iterators for keys that are never considered. (A similar bit of lazy
// evaluation can be done for the point iterators, but is still TBD).
//
// For a full example, consider the following setup:
//
//	p0:               o
//	r0:             m---q
//
//	p1:              n p
//	r1:       g---k
//
//	p2:  b d    i
//	r2: a---e           q----v
//
//	p3:     e
//	r3:
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
// tombstone in its own level or higher levels. In this case, there is nothing to be done.
//
// Advancing the iterator again finds "e". Since "e" comes from p3, we have to
// position the r3 range deletion iterator, which is empty. "e" is past the r2
// tombstone of [a,e) so we need to advance the r2 range deletion iterator to
// [q,v).
//
// The next key is "i". Because this key is in p2, a level above "e", we don't
// have to reposition any range deletion iterators and instead see that "i" is
// covered by the range tombstone [g,k). The iterator is immediately advanced
// to "n" which is covered by the range tombstone [m,q) causing the iterator to
// advance to "o" which is visible.
//
// # Error handling
//
// Any iterator operation may fail. The InternalIterator contract dictates that
// an iterator must return a nil internal key when an error occurs, and a
// subsequent call to Error() should return the error value. The exported
// merging iterator positioning methods must adhere to this contract by setting
// m.err to hold any error encountered by the individual level iterators and
// returning a nil internal key. Some internal helpers (eg,
// find[Next|Prev]Entry) also adhere to this contract, setting m.err directly).
// Other internal functions return an explicit error return value and DO NOT set
// m.err, relying on the caller to set m.err appropriately.
//
// TODO(jackson): Update the InternalIterator interface to return explicit error
// return values (and an *InternalKV pointer).
//
// TODO(peter,rangedel): For testing, advance the iterator through various
// scenarios and have each step display the current state (i.e. the current
// heap and range-del iterator positioning).
type mergingIter struct {
	logger        Logger
	split         Split
	dir           int
	snapshot      base.SeqNum
	batchSnapshot base.SeqNum
	levels        []mergingIterLevel
	heap          mergingIterHeap
	err           error
	prefix        []byte
	lower         []byte
	upper         []byte
	stats         *InternalIteratorStats
	seekKeyBuf    []byte

	// levelsPositioned, if non-nil, is a slice of the same length as levels.
	// It's used by NextPrefix to record which levels have already been
	// repositioned. It's created lazily by the first call to NextPrefix.
	levelsPositioned []bool

	combinedIterState *combinedIterState

	// Used in some tests to disable the random disabling of seek optimizations.
	forceEnableSeekOpt bool
}

// mergingIter implements the base.InternalIterator interface.
var _ base.InternalIterator = (*mergingIter)(nil)

// newMergingIter returns an iterator that merges its input. Walking the
// resultant iterator will return all key/value pairs of all input iterators
// in strictly increasing key order, as defined by cmp. It is permissible to
// pass a nil split parameter if the caller is never going to call
// SeekPrefixGE.
//
// The input's key ranges may overlap, but there are assumed to be no duplicate
// keys: if iters[i] contains a key k then iters[j] will not contain that key k.
//
// None of the iters may be nil.
func newMergingIter(
	logger Logger,
	stats *base.InternalIteratorStats,
	cmp Compare,
	split Split,
	iters ...internalIterator,
) *mergingIter {
	m := &mergingIter{}
	levels := make([]mergingIterLevel, len(iters))
	for i := range levels {
		levels[i].iter = iters[i]
	}
	m.init(&IterOptions{logger: logger}, stats, cmp, split, levels...)
	return m
}

func (m *mergingIter) init(
	opts *IterOptions,
	stats *base.InternalIteratorStats,
	cmp Compare,
	split Split,
	levels ...mergingIterLevel,
) {
	m.err = nil // clear cached iteration error
	m.logger = opts.getLogger()
	if opts != nil {
		m.lower = opts.LowerBound
		m.upper = opts.UpperBound
	}
	m.snapshot = base.SeqNumMax
	m.batchSnapshot = base.SeqNumMax
	m.levels = levels
	m.heap.cmp = cmp
	m.split = split
	m.stats = stats
	if cap(m.heap.items) < len(levels) {
		m.heap.items = make([]*mergingIterLevel, 0, len(levels))
	} else {
		m.heap.items = m.heap.items[:0]
	}
	for l := range m.levels {
		m.levels[l].index = l
	}
}

func (m *mergingIter) initHeap() {
	m.heap.items = m.heap.items[:0]
	for i := range m.levels {
		if l := &m.levels[i]; l.iterKV != nil {
			m.heap.items = append(m.heap.items, l)
		}
	}
	m.heap.init()
}

func (m *mergingIter) initMinHeap() error {
	m.dir = 1
	m.heap.reverse = false
	m.initHeap()
	return m.initMinRangeDelIters(-1)
}

// The level of the previous top element was oldTopLevel. Note that all range delete
// iterators < oldTopLevel are positioned past the key of the previous top element and
// the range delete iterator == oldTopLevel is positioned at or past the key of the
// previous top element. We need to position the range delete iterators from oldTopLevel + 1
// to the level of the current top element.
func (m *mergingIter) initMinRangeDelIters(oldTopLevel int) error {
	if m.heap.len() == 0 {
		return nil
	}

	// Position the range-del iterators at levels <= m.heap.items[0].index.
	item := m.heap.items[0]
	for level := oldTopLevel + 1; level <= item.index; level++ {
		l := &m.levels[level]
		if l.rangeDelIter == nil {
			continue
		}
		var err error
		l.tombstone, err = l.rangeDelIter.SeekGE(item.iterKV.K.UserKey)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *mergingIter) initMaxHeap() error {
	m.dir = -1
	m.heap.reverse = true
	m.initHeap()
	return m.initMaxRangeDelIters(-1)
}

// The level of the previous top element was oldTopLevel. Note that all range delete
// iterators < oldTopLevel are positioned before the key of the previous top element and
// the range delete iterator == oldTopLevel is positioned at or before the key of the
// previous top element. We need to position the range delete iterators from oldTopLevel + 1
// to the level of the current top element.
func (m *mergingIter) initMaxRangeDelIters(oldTopLevel int) error {
	if m.heap.len() == 0 {
		return nil
	}
	// Position the range-del iterators at levels <= m.heap.items[0].index.
	item := m.heap.items[0]
	for level := oldTopLevel + 1; level <= item.index; level++ {
		l := &m.levels[level]
		if l.rangeDelIter == nil {
			continue
		}
		tomb, err := keyspan.SeekLE(m.heap.cmp, l.rangeDelIter, item.iterKV.K.UserKey)
		if err != nil {
			return err
		}
		l.tombstone = tomb
	}
	return nil
}

func (m *mergingIter) switchToMinHeap() error {
	if m.heap.len() == 0 {
		if m.lower != nil {
			m.SeekGE(m.lower, base.SeekGEFlagsNone)
		} else {
			m.First()
		}
		return m.err
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

	key := m.heap.items[0].iterKV.K
	cur := m.heap.items[0]

	for i := range m.levels {
		l := &m.levels[i]
		if l == cur {
			continue
		}
		for l.iterKV = l.iter.Next(); l.iterKV != nil; l.iterKV = l.iter.Next() {
			if base.InternalCompare(m.heap.cmp, key, l.iterKV.K) < 0 {
				// key < iter-key
				break
			}
			// key >= iter-key
		}
		if l.iterKV == nil {
			if err := l.iter.Error(); err != nil {
				return err
			}
		}
	}

	// Special handling for the current iterator because we were using its key
	// above.
	cur.iterKV = cur.iter.Next()
	if cur.iterKV == nil {
		if err := cur.iter.Error(); err != nil {
			return err
		}
	}
	return m.initMinHeap()
}

func (m *mergingIter) switchToMaxHeap() error {
	if m.heap.len() == 0 {
		if m.upper != nil {
			m.SeekLT(m.upper, base.SeekLTFlagsNone)
		} else {
			m.Last()
		}
		return m.err
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
	key := m.heap.items[0].iterKV.K
	cur := m.heap.items[0]

	for i := range m.levels {
		l := &m.levels[i]
		if l == cur {
			continue
		}

		for l.iterKV = l.iter.Prev(); l.iterKV != nil; l.iterKV = l.iter.Prev() {
			if base.InternalCompare(m.heap.cmp, key, l.iterKV.K) > 0 {
				// key > iter-key
				break
			}
			// key <= iter-key
		}
		if l.iterKV == nil {
			if err := l.iter.Error(); err != nil {
				return err
			}
		}
	}

	// Special handling for the current iterator because we were using its key
	// above.
	cur.iterKV = cur.iter.Prev()
	if cur.iterKV == nil {
		if err := cur.iter.Error(); err != nil {
			return err
		}
	}
	return m.initMaxHeap()
}

// nextEntry unconditionally steps to the next entry. item is the current top
// item in the heap.
func (m *mergingIter) nextEntry(l *mergingIterLevel, succKey []byte) error {
	// INVARIANT: If in prefix iteration mode, item.iterKey must have a prefix equal
	// to m.prefix. This invariant is important for ensuring TrySeekUsingNext
	// optimizations behave correctly.
	//
	// During prefix iteration, the iterator does not have a full view of the
	// LSM. Some level iterators may omit keys that are known to fall outside
	// the seek prefix (eg, due to sstable bloom filter exclusion). It's
	// important that in such cases we don't position any iterators beyond
	// m.prefix, because doing so may interfere with future seeks.
	//
	// Let prefixes P1 < P2 < P3. Imagine a SeekPrefixGE to prefix P1, followed
	// by a SeekPrefixGE to prefix P2. Imagine there exist live keys at prefix
	// P2, but they're not visible to the SeekPrefixGE(P1) (because of
	// bloom-filter exclusion or a range tombstone that deletes prefix P1 but
	// not P2). If the SeekPrefixGE(P1) is allowed to move any level iterators
	// to P3, the SeekPrefixGE(P2, TrySeekUsingNext=true) may mistakenly think
	// the level contains no point keys or range tombstones within the prefix
	// P2. Care is taken to avoid ever advancing the iterator beyond the current
	// prefix. If nextEntry is ever invoked while we're already beyond the
	// current prefix, we're violating the invariant.
	if invariants.Enabled && m.prefix != nil {
		if p := m.split.Prefix(l.iterKV.K.UserKey); !bytes.Equal(m.prefix, p) {
			m.logger.Fatalf("mergingIter: prefix violation: nexting beyond prefix %q; existing heap root %q\n%s",
				m.prefix, l.iterKV, debug.Stack())
		}
	}

	oldTopLevel := l.index
	oldRangeDelIterGeneration := l.rangeDelIterGeneration

	if succKey == nil {
		l.iterKV = l.iter.Next()
	} else {
		l.iterKV = l.iter.NextPrefix(succKey)
	}

	if l.iterKV == nil {
		if err := l.iter.Error(); err != nil {
			return err
		}
		m.heap.pop()
	} else {
		if m.prefix != nil && !bytes.Equal(m.prefix, m.split.Prefix(l.iterKV.K.UserKey)) {
			// Set keys without a matching prefix to their zero values when in prefix
			// iteration mode and remove iterated level from heap.
			l.iterKV = nil
			m.heap.pop()
		} else if m.heap.len() > 1 {
			m.heap.fixTop()
		}
		if l.rangeDelIterGeneration != oldRangeDelIterGeneration {
			// The rangeDelIter changed which indicates that the l.iter moved to the
			// next sstable. We have to update the tombstone for oldTopLevel as well.
			oldTopLevel--
		}
	}

	// The cached tombstones are only valid for the levels
	// [0,oldTopLevel]. Updated the cached tombstones for any levels in the range
	// [oldTopLevel+1,heap[0].index].
	return m.initMinRangeDelIters(oldTopLevel)
}

// isNextEntryDeleted starts from the current entry (as the next entry) and if
// it is deleted, moves the iterators forward as needed and returns true, else
// it returns false. item is the top item in the heap. If any of the required
// iterator operations error, the error is returned without updating m.err.
//
// During prefix iteration mode, isNextEntryDeleted will exhaust the iterator by
// clearing the heap if the deleted key(s) extend beyond the iteration prefix
// during prefix-iteration mode.
func (m *mergingIter) isNextEntryDeleted(item *mergingIterLevel) (bool, error) {
	// Look for a range deletion tombstone containing item.iterKV at higher
	// levels (level < item.index). If we find such a range tombstone we know
	// it deletes the key in the current level. Also look for a range
	// deletion at the current level (level == item.index). If we find such a
	// range deletion we need to check whether it is newer than the current
	// entry.
	for level := 0; level <= item.index; level++ {
		l := &m.levels[level]
		if l.rangeDelIter == nil || l.tombstone == nil {
			// If l.tombstone is nil, there are no further tombstones
			// in the current sstable in the current (forward) iteration
			// direction.
			continue
		}
		if m.heap.cmp(l.tombstone.End, item.iterKV.K.UserKey) <= 0 {
			// The current key is at or past the tombstone end key.
			//
			// NB: for the case that this l.rangeDelIter is provided by a levelIter we know that
			// the levelIter must be positioned at a key >= item.iterKV. So it is sufficient to seek the
			// current l.rangeDelIter (since any range del iterators that will be provided by the
			// levelIter in the future cannot contain item.iterKV). Also, it is possible that we
			// will encounter parts of the range delete that should be ignored -- we handle that
			// below.
			var err error
			l.tombstone, err = l.rangeDelIter.SeekGE(item.iterKV.K.UserKey)
			if err != nil {
				return false, err
			}
		}
		if l.tombstone == nil {
			continue
		}

		if l.tombstone.VisibleAt(m.snapshot) && m.heap.cmp(l.tombstone.Start, item.iterKV.K.UserKey) <= 0 {
			if level < item.index {
				// We could also do m.seekGE(..., level + 1). The levels from
				// [level + 1, item.index) are already after item.iterKV so seeking them may be
				// wasteful.

				// We can seek up to tombstone.End.
				//
				// Progress argument: Since this file is at a higher level than item.iterKV we know
				// that the iterator in this file must be positioned within its bounds and at a key
				// X > item.iterKV (otherwise it would be the min of the heap). It is not
				// possible for X.UserKey == item.iterKV.UserKey, since it is incompatible with
				// X > item.iterKV (a lower version cannot be in a higher sstable), so it must be that
				// X.UserKey > item.iterKV.UserKey. Which means l.largestUserKey > item.key.UserKey.
				// We also know that l.tombstone.End > item.iterKV.UserKey. So the min of these,
				// seekKey, computed below, is > item.iterKV.UserKey, so the call to seekGE() will
				// make forward progress.
				m.seekKeyBuf = append(m.seekKeyBuf[:0], l.tombstone.End...)
				seekKey := m.seekKeyBuf
				// This seek is not directly due to a SeekGE call, so we don't know
				// enough about the underlying iterator positions, and so we keep the
				// try-seek-using-next optimization disabled. Additionally, if we're in
				// prefix-seek mode and a re-seek would have moved us past the original
				// prefix, we can remove all merging iter levels below the rangedel
				// tombstone's level and return immediately instead of re-seeking. This
				// is correct since those levels cannot provide a key that matches the
				// prefix, and is also visible. Additionally, this is important to make
				// subsequent `TrySeekUsingNext` work correctly, as a re-seek on a
				// different prefix could have resulted in this iterator skipping visible
				// keys at prefixes in between m.prefix and seekKey, that are currently
				// not in the heap due to a bloom filter mismatch.
				//
				// Additionally, we set the relative-seek flag. This is
				// important when iterating with lazy combined iteration. If
				// there's a range key between this level's current file and the
				// file the seek will land on, we need to detect it in order to
				// trigger construction of the combined iterator.
				if m.prefix != nil {
					if !bytes.Equal(m.prefix, m.split.Prefix(seekKey)) {
						for i := item.index; i < len(m.levels); i++ {
							// Remove this level from the heap. Setting iterKV
							// to nil should be sufficient for initMinHeap to
							// not re-initialize the heap with them in it. Other
							// fields in mergingIterLevel can remain as-is; the
							// iter/rangeDelIter needs to stay intact for future
							// trySeekUsingNexts to work, the level iter
							// boundary context is owned by the levelIter which
							// is not being repositioned, and any tombstones in
							// these levels will be irrelevant for us anyway.
							m.levels[i].iterKV = nil
						}
						// TODO(bilal): Consider a more efficient way of removing levels from
						// the heap without reinitializing all of it. This would likely
						// necessitate tracking the heap positions of each mergingIterHeap
						// item in the mergingIterLevel, and then swapping that item in the
						// heap with the last-positioned heap item, and shrinking the heap by
						// one.
						if err := m.initMinHeap(); err != nil {
							return false, err
						}
						return true, nil
					}
				}
				if err := m.seekGE(seekKey, item.index, base.SeekGEFlagsNone.EnableRelativeSeek()); err != nil {
					return false, err
				}
				return true, nil
			}
			if l.tombstone.CoversAt(m.snapshot, item.iterKV.SeqNum()) {
				if err := m.nextEntry(item, nil /* succKey */); err != nil {
					return false, err
				}
				return true, nil
			}
		}
	}
	return false, nil
}

// Starting from the current entry, finds the first (next) entry that can be returned.
//
// If an error occurs, m.err is updated to hold the error and findNextentry
// returns a nil internal key.
func (m *mergingIter) findNextEntry() *base.InternalKV {
	for m.heap.len() > 0 && m.err == nil {
		item := m.heap.items[0]

		// The levelIter internal iterator will interleave exclusive sentinel
		// keys to keep files open until their range deletions are no longer
		// necessary. Sometimes these are interleaved with the user key of a
		// file's largest key, in which case they may simply be stepped over to
		// move to the next file in the forward direction. Other times they're
		// interleaved at the user key of the user-iteration boundary, if that
		// falls within the bounds of a file. In the latter case, there are no
		// more keys < m.upper, and we can stop iterating.
		//
		// We perform a key comparison to differentiate between these two cases.
		// This key comparison is considered okay because it only happens for
		// sentinel keys. It may be eliminated after #2863.
		if m.levels[item.index].iterKV.K.IsExclusiveSentinel() {
			if m.upper != nil && m.heap.cmp(m.levels[item.index].iterKV.K.UserKey, m.upper) >= 0 {
				break
			}
			// This key is the largest boundary of a file and can be skipped now
			// that the file's range deletions are no longer relevant.
			m.err = m.nextEntry(item, nil /* succKey */)
			if m.err != nil {
				return nil
			}
			continue
		}

		m.addItemStats(item)

		// Check if the heap root key is deleted by a range tombstone in a
		// higher level. If it is, isNextEntryDeleted will advance the iterator
		// to a later key (through seeking or nexting).
		isDeleted, err := m.isNextEntryDeleted(item)
		if err != nil {
			m.err = err
			return nil
		} else if isDeleted {
			m.stats.PointsCoveredByRangeTombstones++
			continue
		}

		// Check if the key is visible at the iterator sequence numbers.
		if !item.iterKV.Visible(m.snapshot, m.batchSnapshot) {
			m.err = m.nextEntry(item, nil /* succKey */)
			if m.err != nil {
				return nil
			}
			continue
		}

		// The heap root is visible and not deleted by any range tombstones.
		// Return it.
		return item.iterKV
	}
	return nil
}

// Steps to the prev entry. item is the current top item in the heap.
func (m *mergingIter) prevEntry(l *mergingIterLevel) error {
	oldTopLevel := l.index
	oldRangeDelIterGeneration := l.rangeDelIterGeneration
	if l.iterKV = l.iter.Prev(); l.iterKV != nil {
		if m.heap.len() > 1 {
			m.heap.fixTop()
		}
		if l.rangeDelIterGeneration != oldRangeDelIterGeneration && l.rangeDelIter != nil {
			// The rangeDelIter changed which indicates that the l.iter moved to the
			// previous sstable. We have to update the tombstone for oldTopLevel as
			// well.
			oldTopLevel--
		}
	} else {
		if err := l.iter.Error(); err != nil {
			return err
		}
		m.heap.pop()
	}

	// The cached tombstones are only valid for the levels
	// [0,oldTopLevel]. Updated the cached tombstones for any levels in the range
	// [oldTopLevel+1,heap[0].index].
	return m.initMaxRangeDelIters(oldTopLevel)
}

// isPrevEntryDeleted() starts from the current entry (as the prev entry) and if it is deleted,
// moves the iterators backward as needed and returns true, else it returns false. item is the top
// item in the heap.
func (m *mergingIter) isPrevEntryDeleted(item *mergingIterLevel) (bool, error) {
	// Look for a range deletion tombstone containing item.iterKV at higher
	// levels (level < item.index). If we find such a range tombstone we know
	// it deletes the key in the current level. Also look for a range
	// deletion at the current level (level == item.index). If we find such a
	// range deletion we need to check whether it is newer than the current
	// entry.
	for level := 0; level <= item.index; level++ {
		l := &m.levels[level]
		if l.rangeDelIter == nil || l.tombstone == nil {
			// If l.tombstone is nil, there are no further tombstones
			// in the current sstable in the current (reverse) iteration
			// direction.
			continue
		}
		if m.heap.cmp(item.iterKV.K.UserKey, l.tombstone.Start) < 0 {
			// The current key is before the tombstone start key.
			//
			// NB: for the case that this l.rangeDelIter is provided by a levelIter we know that
			// the levelIter must be positioned at a key < item.iterKV. So it is sufficient to seek the
			// current l.rangeDelIter (since any range del iterators that will be provided by the
			// levelIter in the future cannot contain item.iterKV). Also, it is it is possible that we
			// will encounter parts of the range delete that should be ignored -- we handle that
			// below.

			tomb, err := keyspan.SeekLE(m.heap.cmp, l.rangeDelIter, item.iterKV.K.UserKey)
			if err != nil {
				return false, err
			}
			l.tombstone = tomb
		}
		if l.tombstone == nil {
			continue
		}
		if l.tombstone.VisibleAt(m.snapshot) && m.heap.cmp(l.tombstone.End, item.iterKV.K.UserKey) > 0 {
			if level < item.index {
				// We could also do m.seekLT(..., level + 1). The levels from
				// [level + 1, item.index) are already before item.iterKV so seeking them may be
				// wasteful.

				// We can seek up to tombstone.Start.UserKey.
				//
				// Progress argument: We know that the iterator in this file is positioned within
				// its bounds and at a key X < item.iterKV (otherwise it would be the max of the heap).
				// So smallestUserKey <= item.iterKV.UserKey and we already know that
				// l.tombstone.Start.UserKey <= item.iterKV.UserKey. So the seekKey computed below
				// is <= item.iterKV.UserKey, and since we do a seekLT() we will make backwards
				// progress.
				m.seekKeyBuf = append(m.seekKeyBuf[:0], l.tombstone.Start...)
				seekKey := m.seekKeyBuf
				// We set the relative-seek flag. This is important when
				// iterating with lazy combined iteration. If there's a range
				// key between this level's current file and the file the seek
				// will land on, we need to detect it in order to trigger
				// construction of the combined iterator.
				if err := m.seekLT(seekKey, item.index, base.SeekLTFlagsNone.EnableRelativeSeek()); err != nil {
					return false, err
				}
				return true, nil
			}
			if l.tombstone.CoversAt(m.snapshot, item.iterKV.SeqNum()) {
				if err := m.prevEntry(item); err != nil {
					return false, err
				}
				return true, nil
			}
		}
	}
	return false, nil
}

// Starting from the current entry, finds the first (prev) entry that can be returned.
//
// If an error occurs, m.err is updated to hold the error and findNextentry
// returns a nil internal key.
func (m *mergingIter) findPrevEntry() *base.InternalKV {
	for m.heap.len() > 0 && m.err == nil {
		item := m.heap.items[0]

		// The levelIter internal iterator will interleave exclusive sentinel
		// keys to keep files open until their range deletions are no longer
		// necessary. Sometimes these are interleaved with the user key of a
		// file's smallest key, in which case they may simply be stepped over to
		// move to the next file in the backward direction. Other times they're
		// interleaved at the user key of the user-iteration boundary, if that
		// falls within the bounds of a file. In the latter case, there are no
		// more keys ≥ m.lower, and we can stop iterating.
		//
		// We perform a key comparison to differentiate between these two cases.
		// This key comparison is considered okay because it only happens for
		// sentinel keys. It may be eliminated after #2863.
		if m.levels[item.index].iterKV.K.IsExclusiveSentinel() {
			if m.lower != nil && m.heap.cmp(m.levels[item.index].iterKV.K.UserKey, m.lower) <= 0 {
				break
			}
			// This key is the smallest boundary of a file and can be skipped
			// now that the file's range deletions are no longer relevant.
			m.err = m.prevEntry(item)
			if m.err != nil {
				return nil
			}
			continue
		}

		m.addItemStats(item)
		if isDeleted, err := m.isPrevEntryDeleted(item); err != nil {
			m.err = err
			return nil
		} else if isDeleted {
			m.stats.PointsCoveredByRangeTombstones++
			continue
		}
		if item.iterKV.Visible(m.snapshot, m.batchSnapshot) {
			return item.iterKV
		}
		m.err = m.prevEntry(item)
	}
	return nil
}

// Seeks levels >= level to >= key. Additionally uses range tombstones to extend the seeks.
//
// If an error occurs, seekGE returns the error without setting m.err.
func (m *mergingIter) seekGE(key []byte, level int, flags base.SeekGEFlags) error {
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

	// Deterministically disable the TrySeekUsingNext optimizations sometimes in
	// invariant builds to encourage the metamorphic tests to surface bugs. Note
	// that we cannot disable the optimization within individual levels. It must
	// be disabled for all levels or none. If one lower-level iterator performs
	// a fresh seek whereas another takes advantage of its current iterator
	// position, the heap can become inconsistent. Consider the following
	// example:
	//
	//     L5:  [ [b-c) ]  [ d ]*
	//     L6:  [  b ]           [e]*
	//
	// Imagine a SeekGE(a). The [b-c) range tombstone deletes the L6 point key
	// 'b', resulting in the iterator positioned at d with the heap:
	//
	//     {L5: d, L6: e}
	//
	// A subsequent SeekGE(b) is seeking to a larger key, so the caller may set
	// TrySeekUsingNext()=true. If the L5 iterator used the TrySeekUsingNext
	// optimization but the L6 iterator did not, the iterator would have the
	// heap:
	//
	//     {L6: b, L5: d}
	//
	// Because the L5 iterator has already advanced to the next sstable, the
	// merging iterator cannot observe the [b-c) range tombstone and will
	// mistakenly return L6's deleted point key 'b'.
	if testingDisableSeekOpt(key, uintptr(unsafe.Pointer(m))) && !m.forceEnableSeekOpt {
		flags = flags.DisableTrySeekUsingNext()
	}

	for ; level < len(m.levels); level++ {
		if invariants.Enabled && m.lower != nil && m.heap.cmp(key, m.lower) < 0 {
			m.logger.Fatalf("mergingIter: lower bound violation: %s < %s\n%s", key, m.lower, debug.Stack())
		}

		l := &m.levels[level]
		if m.prefix != nil {
			l.iterKV = l.iter.SeekPrefixGE(m.prefix, key, flags)
			if l.iterKV != nil {
				if !bytes.Equal(m.prefix, m.split.Prefix(l.iterKV.K.UserKey)) {
					// Prevent keys without a matching prefix from being added to the heap by setting
					// iterKey and iterValue to their zero values before calling initMinHeap.
					l.iterKV = nil
				}
			}
		} else {
			l.iterKV = l.iter.SeekGE(key, flags)
		}
		if l.iterKV == nil {
			if err := l.iter.Error(); err != nil {
				return err
			}
		}

		// If this level contains overlapping range tombstones, alter the seek
		// key accordingly. Caveat: If we're performing lazy-combined iteration,
		// we cannot alter the seek key: Range tombstones don't delete range
		// keys, and there might exist live range keys within the range
		// tombstone's span that need to be observed to trigger a switch to
		// combined iteration.
		if rangeDelIter := l.rangeDelIter; rangeDelIter != nil &&
			(m.combinedIterState == nil || m.combinedIterState.initialized) {
			// The level has a range-del iterator. Find the tombstone containing
			// the search key.
			var err error
			l.tombstone, err = rangeDelIter.SeekGE(key)
			if err != nil {
				return err
			}
			if l.tombstone != nil && l.tombstone.VisibleAt(m.snapshot) && m.heap.cmp(l.tombstone.Start, key) <= 0 {
				// Based on the containment condition tombstone.End > key, so
				// the assignment to key results in a monotonically
				// non-decreasing key across iterations of this loop.
				//
				// The adjustment of key here can only move it to a larger key.
				// Since the caller of seekGE guaranteed that the original key
				// was greater than or equal to m.lower, the new key will
				// continue to be greater than or equal to m.lower.
				key = l.tombstone.End
			}
		}
	}
	return m.initMinHeap()
}

func (m *mergingIter) String() string {
	return "merging"
}

// SeekGE implements base.InternalIterator.SeekGE. Note that SeekGE only checks
// the upper bound. It is up to the caller to ensure that key is greater than
// or equal to the lower bound.
func (m *mergingIter) SeekGE(key []byte, flags base.SeekGEFlags) *base.InternalKV {
	m.prefix = nil
	m.err = m.seekGE(key, 0 /* start level */, flags)
	if m.err != nil {
		return nil
	}
	return m.findNextEntry()
}

// SeekPrefixGE implements base.InternalIterator.SeekPrefixGE.
func (m *mergingIter) SeekPrefixGE(prefix, key []byte, flags base.SeekGEFlags) *base.InternalKV {
	return m.SeekPrefixGEStrict(prefix, key, flags)
}

// SeekPrefixGEStrict implements topLevelIterator.SeekPrefixGEStrict. Note that
// SeekPrefixGEStrict explicitly checks that the key has a matching prefix.
func (m *mergingIter) SeekPrefixGEStrict(
	prefix, key []byte, flags base.SeekGEFlags,
) *base.InternalKV {
	m.prefix = prefix
	m.err = m.seekGE(key, 0 /* start level */, flags)
	if m.err != nil {
		return nil
	}

	iterKV := m.findNextEntry()
	if invariants.Enabled && iterKV != nil {
		if !bytes.Equal(m.prefix, m.split.Prefix(iterKV.K.UserKey)) {
			m.logger.Fatalf("mergingIter: prefix violation: returning key %q without prefix %q\n", iterKV, m.prefix)
		}
	}
	return iterKV
}

// Seeks levels >= level to < key. Additionally uses range tombstones to extend the seeks.
func (m *mergingIter) seekLT(key []byte, level int, flags base.SeekLTFlags) error {
	// See the comment in seekGE regarding using tombstones to adjust the seek
	// target per level.
	m.prefix = nil
	for ; level < len(m.levels); level++ {
		if invariants.Enabled && m.upper != nil && m.heap.cmp(key, m.upper) > 0 {
			m.logger.Fatalf("mergingIter: upper bound violation: %s > %s\n%s", key, m.upper, debug.Stack())
		}

		l := &m.levels[level]
		l.iterKV = l.iter.SeekLT(key, flags)
		if l.iterKV == nil {
			if err := l.iter.Error(); err != nil {
				return err
			}
		}

		// If this level contains overlapping range tombstones, alter the seek
		// key accordingly. Caveat: If we're performing lazy-combined iteration,
		// we cannot alter the seek key: Range tombstones don't delete range
		// keys, and there might exist live range keys within the range
		// tombstone's span that need to be observed to trigger a switch to
		// combined iteration.
		if rangeDelIter := l.rangeDelIter; rangeDelIter != nil &&
			(m.combinedIterState == nil || m.combinedIterState.initialized) {
			// The level has a range-del iterator. Find the tombstone containing
			// the search key.
			tomb, err := keyspan.SeekLE(m.heap.cmp, rangeDelIter, key)
			if err != nil {
				return err
			}
			l.tombstone = tomb
			// Since SeekLT is exclusive on `key` and a tombstone's end key is
			// also exclusive, a seek key equal to a tombstone's end key still
			// enables the seek optimization (Note this is different than the
			// check performed by (*keyspan.Span).Contains).
			if l.tombstone != nil && l.tombstone.VisibleAt(m.snapshot) &&
				m.heap.cmp(key, l.tombstone.End) <= 0 {
				// NB: Based on the containment condition
				// tombstone.Start.UserKey <= key, so the assignment to key
				// results in a monotonically non-increasing key across
				// iterations of this loop.
				//
				// The adjustment of key here can only move it to a smaller key.
				// Since the caller of seekLT guaranteed that the original key
				// was less than or equal to m.upper, the new key will continue
				// to be less than or equal to m.upper.
				key = l.tombstone.Start
			}
		}
	}

	return m.initMaxHeap()
}

// SeekLT implements base.InternalIterator.SeekLT. Note that SeekLT only checks
// the lower bound. It is up to the caller to ensure that key is less than the
// upper bound.
func (m *mergingIter) SeekLT(key []byte, flags base.SeekLTFlags) *base.InternalKV {
	m.prefix = nil
	m.err = m.seekLT(key, 0 /* start level */, flags)
	if m.err != nil {
		return nil
	}
	return m.findPrevEntry()
}

// First implements base.InternalIterator.First. Note that First only checks
// the upper bound. It is up to the caller to ensure that key is greater than
// or equal to the lower bound (e.g. via a call to SeekGE(lower)).
func (m *mergingIter) First() *base.InternalKV {
	m.err = nil // clear cached iteration error
	m.prefix = nil
	m.heap.items = m.heap.items[:0]
	for i := range m.levels {
		l := &m.levels[i]
		l.iterKV = l.iter.First()
		if l.iterKV == nil {
			if m.err = l.iter.Error(); m.err != nil {
				return nil
			}
		}
	}
	if m.err = m.initMinHeap(); m.err != nil {
		return nil
	}
	return m.findNextEntry()
}

// Last implements base.InternalIterator.Last. Note that Last only checks the
// lower bound. It is up to the caller to ensure that key is less than the
// upper bound (e.g. via a call to SeekLT(upper))
func (m *mergingIter) Last() *base.InternalKV {
	m.err = nil // clear cached iteration error
	m.prefix = nil
	for i := range m.levels {
		l := &m.levels[i]
		l.iterKV = l.iter.Last()
		if l.iterKV == nil {
			if m.err = l.iter.Error(); m.err != nil {
				return nil
			}
		}
	}
	if m.err = m.initMaxHeap(); m.err != nil {
		return nil
	}
	return m.findPrevEntry()
}

func (m *mergingIter) Next() *base.InternalKV {
	if m.err != nil {
		return nil
	}

	if m.dir != 1 {
		if m.err = m.switchToMinHeap(); m.err != nil {
			return nil
		}
		return m.findNextEntry()
	}

	if m.heap.len() == 0 {
		return nil
	}

	// NB: It's okay to call nextEntry directly even during prefix iteration
	// mode. During prefix iteration mode, we rely on the caller to not call
	// Next if the iterator has already advanced beyond the iteration prefix.
	// See the comment above the base.InternalIterator interface.
	if m.err = m.nextEntry(m.heap.items[0], nil /* succKey */); m.err != nil {
		return nil
	}

	iterKV := m.findNextEntry()
	if invariants.Enabled && m.prefix != nil && iterKV != nil {
		if !bytes.Equal(m.prefix, m.split.Prefix(iterKV.K.UserKey)) {
			m.logger.Fatalf("mergingIter: prefix violation: returning key %q without prefix %q\n", iterKV, m.prefix)
		}
	}
	return iterKV
}

func (m *mergingIter) NextPrefix(succKey []byte) *base.InternalKV {
	if m.dir != 1 {
		panic("pebble: cannot switch directions with NextPrefix")
	}
	if m.err != nil || m.heap.len() == 0 {
		return nil
	}
	if m.levelsPositioned == nil {
		m.levelsPositioned = make([]bool, len(m.levels))
	} else {
		for i := range m.levelsPositioned {
			m.levelsPositioned[i] = false
		}
	}

	// The heap root necessarily must be positioned at a key < succKey, because
	// NextPrefix was invoked.
	root := m.heap.items[0]
	if invariants.Enabled && m.heap.cmp((*root).iterKV.K.UserKey, succKey) >= 0 {
		m.logger.Fatalf("pebble: invariant violation: NextPrefix(%q) called on merging iterator already positioned at %q",
			succKey, (*root).iterKV)
	}
	// NB: root is the heap root before we call nextEntry; nextEntry may change
	// the heap root, so we must not `root` to still be the root of the heap, or
	// even to be in the heap if the level's iterator becomes exhausted.
	if m.err = m.nextEntry(root, succKey); m.err != nil {
		return nil
	}
	// We only consider the level to be conclusively positioned at the next
	// prefix if our call to nextEntry did not advance the level onto a range
	// deletion's boundary. Range deletions may have bounds within the prefix
	// that are still surfaced by NextPrefix.
	m.levelsPositioned[root.index] = root.iterKV == nil || !root.iterKV.K.IsExclusiveSentinel()

	for m.heap.len() > 0 {
		root := m.heap.items[0]
		if m.levelsPositioned[root.index] {
			// A level we've previously positioned is at the top of the heap, so
			// there are no other levels positioned at keys < succKey. We've
			// advanced as far as we need to.
			break
		}
		// If the current heap root is a sentinel key, we need to skip it.
		// Calling NextPrefix while positioned at a sentinel key is not
		// supported.
		if root.iterKV.K.IsExclusiveSentinel() {
			if m.err = m.nextEntry(root, nil); m.err != nil {
				return nil
			}
			continue
		}

		// Since this level was not the original heap root when NextPrefix was
		// called, we don't know whether this level's current key has the
		// previous prefix or a new one.
		if m.heap.cmp(root.iterKV.K.UserKey, succKey) >= 0 {
			break
		}
		if m.err = m.nextEntry(root, succKey); m.err != nil {
			return nil
		}
		// We only consider the level to be conclusively positioned at the next
		// prefix if our call to nextEntry did not land onto a range deletion's
		// boundary. Range deletions may have bounds within the prefix that are
		// still surfaced by NextPrefix.
		m.levelsPositioned[root.index] = root.iterKV == nil || !root.iterKV.K.IsExclusiveSentinel()
	}
	return m.findNextEntry()
}

func (m *mergingIter) Prev() *base.InternalKV {
	if m.err != nil {
		return nil
	}

	if m.dir != -1 {
		if m.prefix != nil {
			m.err = errors.New("pebble: unsupported reverse prefix iteration")
			return nil
		}
		if m.err = m.switchToMaxHeap(); m.err != nil {
			return nil
		}
		return m.findPrevEntry()
	}

	if m.heap.len() == 0 {
		return nil
	}
	if m.err = m.prevEntry(m.heap.items[0]); m.err != nil {
		return nil
	}
	return m.findPrevEntry()
}

func (m *mergingIter) Error() error {
	if m.heap.len() == 0 || m.err != nil {
		return m.err
	}
	return m.levels[m.heap.items[0].index].iter.Error()
}

func (m *mergingIter) Close() error {
	for i := range m.levels {
		iter := m.levels[i].iter
		if err := iter.Close(); err != nil && m.err == nil {
			m.err = err
		}
		m.levels[i].setRangeDelIter(nil)
	}
	m.levels = nil
	m.heap.items = m.heap.items[:0]
	return m.err
}

func (m *mergingIter) SetBounds(lower, upper []byte) {
	m.prefix = nil
	m.lower = lower
	m.upper = upper
	for i := range m.levels {
		m.levels[i].iter.SetBounds(lower, upper)
	}
	m.heap.clear()
}

func (m *mergingIter) SetContext(ctx context.Context) {
	for i := range m.levels {
		m.levels[i].iter.SetContext(ctx)
	}
}

// DebugTree is part of the InternalIterator interface.
func (m *mergingIter) DebugTree(tp treeprinter.Node) {
	n := tp.Childf("%T(%p)", m, m)
	for i := range m.levels {
		if iter := m.levels[i].iter; iter != nil {
			iter.DebugTree(n)
		}
	}
}

func (m *mergingIter) DebugString() string {
	var buf bytes.Buffer
	sep := ""
	for m.heap.len() > 0 {
		item := m.heap.pop()
		fmt.Fprintf(&buf, "%s%s", sep, item.iterKV.K)
		sep = " "
	}
	var err error
	if m.dir == 1 {
		err = m.initMinHeap()
	} else {
		err = m.initMaxHeap()
	}
	if err != nil {
		fmt.Fprintf(&buf, "err=<%s>", err)
	}
	return buf.String()
}

func (m *mergingIter) ForEachLevelIter(fn func(li *levelIter) bool) {
	for _, ml := range m.levels {
		if ml.levelIter != nil {
			if done := fn(ml.levelIter); done {
				break
			}
		}
	}
}

func (m *mergingIter) addItemStats(l *mergingIterLevel) {
	m.stats.PointCount++
	m.stats.KeyBytes += uint64(len(l.iterKV.K.UserKey))
	m.stats.ValueBytes += uint64(l.iterKV.V.InternalLen())
}

var _ internalIterator = &mergingIter{}
