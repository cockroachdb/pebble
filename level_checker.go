// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"log"
	"sort"
	"sync/atomic"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/rangedel"
)

// This is a simplified version of mergingIter that only needs to step through points (analogous
// to only doing Next()) and does not do any seek optimizations (it minimally needs to seek the
// range del iterators to position them at or past the current point). These optimizations are
// avoided since we want to validate that sequence numbers are consistent across levels.
type simpleMergingIterLevel struct {
	iter            internalIterator
	rangeDelIter    internalIterator
	smallestUserKey []byte

	iterKey   *InternalKey
	iterValue []byte
	tombstone rangedel.Tombstone
}

type simpleMergingIter struct {
	levels   []simpleMergingIterLevel
	snapshot uint64
	heap     mergingIterHeap
	// The last point's user key and level. For validation.
	lastUserKey []byte
	lastLevel   int
	// The first error will cause step() to return false.
	err error
}

func (m *simpleMergingIter) init(cmp Compare, snapshot uint64, levels ...simpleMergingIterLevel) {
	m.levels = levels
	m.snapshot = snapshot
	m.lastLevel = -1
	m.heap.cmp = cmp
	m.heap.items = make([]mergingIterItem, 0, len(levels))
	for i := range m.levels {
		l := &m.levels[i]
		l.iterKey, l.iterValue = l.iter.First()
		if l.iterKey != nil {
			m.heap.items = append(m.heap.items, mergingIterItem{
				index: i,
				key:   *l.iterKey,
				value: l.iterValue,
			})
		}
	}
	m.heap.init()

	if m.heap.len() == 0 {
		return
	}
	m.positionRangeDels()
}

func (m *simpleMergingIter) positionRangeDels() {
	item := &m.heap.items[0]
	for i := range m.levels {
		l := &m.levels[i]
		if l.rangeDelIter == nil {
			continue
		}
		l.tombstone = rangedel.SeekGE(m.heap.cmp, l.rangeDelIter, item.key.UserKey, m.snapshot)
	}
}

// Returns true if not yet done.
func (m *simpleMergingIter) step() bool {
	if m.heap.len() == 0 || m.err != nil {
		return false
	}
	item := &m.heap.items[0]
	// Sentinels are not relevant for this point checking.
	if item.key.Trailer != InternalKeyRangeDeleteSentinel && item.key.Visible(m.snapshot) {
		if m.heap.cmp(item.key.UserKey, m.lastUserKey) == 0 {
			// At the same user key. We will see them in decreasing seqnum order so the lastLevel
			// must not be lower.
			if m.lastLevel > item.index {
				m.err = fmt.Errorf("found InternalKey %v at level %d that came after the same key from a lower level %d",
					item.key, item.index, m.lastLevel)
				return false
			}
			m.lastLevel = item.index
		} else {
			// The user key has changed.
			m.lastUserKey = append(m.lastUserKey[:0], item.key.UserKey...)
			m.lastLevel = item.index
		}
		// Is this point covered by a tombstone at a lower level? Note that all these iterators
		// must be positioned at a key > item.key. So the Largest key bound of the sstable containing
		// the tombstone >= item.key. So the upper limit of the tombstone cannot be
		// file-bounds-constrained to < item.key. But it is possible that item.key < smallest key
		// bound of the sstable, in which case this tombstone should be ignored.
		for level := item.index + 1; level < len(m.levels); level++ {
			l := &m.levels[level]
			if l.rangeDelIter == nil || l.tombstone.Empty() {
				continue
			}
			if (l.smallestUserKey == nil || m.heap.cmp(l.smallestUserKey, item.key.UserKey) <= 0) &&
				l.tombstone.Contains(m.heap.cmp, item.key.UserKey) {
				if l.tombstone.Deletes(item.key.SeqNum()) {
					m.err = fmt.Errorf("Tombstone %v at level %d deletes key %v at level %d",
						l.tombstone, level, item.key, item.index)
					return false
				}
			}
		}
	}

	// Step to the next point.
	l := &m.levels[item.index]
	if l.iterKey, l.iterValue = l.iter.Next(); l.iterKey != nil {
		item.key, item.value = *l.iterKey, l.iterValue
		if m.heap.len() > 1 {
			m.heap.fix(0)
		}
	} else {
		l.iter.Close()
		m.err = l.iter.Error()
		m.heap.pop()
	}
	if m.err != nil || m.heap.len() == 0 {
		return false
	}
	m.positionRangeDels()
	return true
}

type tombstoneWithLevel struct {
	rangedel.Tombstone
	level int
}

type tombstonesByStartKeyAndLevel struct {
	cmp Compare
	buf []tombstoneWithLevel
}

func (v *tombstonesByStartKeyAndLevel) Len() int { return len(v.buf) }
func (v *tombstonesByStartKeyAndLevel) Less(i, j int) bool {
	less := v.cmp(v.buf[i].Start.UserKey, v.buf[j].Start.UserKey)
	if less == 0 {
		if v.buf[i].level < v.buf[j].level {
			return true
		}
		if v.buf[i].level > v.buf[j].level {
			return false
		}
		// Same level. Order by decreasing seq num
		return v.buf[i].Start.SeqNum() > v.buf[j].Start.SeqNum()
	}
	return less < 0
}

func (v *tombstonesByStartKeyAndLevel) Swap(i, j int) {
	v.buf[i], v.buf[j] = v.buf[j], v.buf[i]
}

func iterateAndCheckTombstones(cmp Compare, tombstones []tombstoneWithLevel) error {
	sortBuf := tombstonesByStartKeyAndLevel{
		cmp: cmp,
		buf: tombstones,
	}
	sort.Sort(&sortBuf)

	// For a sequence of tombstones that share the same start key, we will encounter
	// them in non-decreasing level order. The lower numbered levels should have the
	// higher seq nums. We track the min seq num seen so far and ensure that each
	// step into the next level doesn't see a higher seq num.
	var lastUserKey []byte
	lastLevel := -1
	seqNumMin := InternalKeySeqNumMax
	for _, t := range tombstones {
		if cmp(lastUserKey, t.Start.UserKey) != 0 {
			lastUserKey = t.Start.UserKey
			lastLevel = t.level
			seqNumMin = InternalKeySeqNumMax
		}
		if lastLevel != t.level {
			if lastLevel > t.level {
				panic("incorrect sort")
			}
			if seqNumMin < t.Start.SeqNum() {
				return fmt.Errorf("Encountered tombstone %v at level %d that has higher seqnum than at level %d",
					t.Tombstone, t.level, lastLevel)
			}
			seqNumMin = t.Start.SeqNum()
			lastLevel = t.level
		} else if t.Start.SeqNum() < seqNumMin {
			seqNumMin = t.Start.SeqNum()
		}
	}
	return nil
}

type checkConfig struct {
	cmp       Compare
	readState *readState
	newIters  tableNewIters
	seqNum    uint64
}

// Checks that range tombstones are mutually consistent. This is non-trivial since
// one needs to handle untruncated tombstones, and one needs to detect inversions of the
// form [a, c)#8 at higher level and [b, c)#10 at a lower level. The start key of the former
// is not contained in the latter and we can't use the exclusive end key, c, for a
// containment check since it is the sentinel key. We observe that if these tombstones
// were fragmented wrt each other we would have [a, b)#8 and [b, c)#8 at the higher level
// and [b, c)#10 at the lower level and then it is is trivial to compare the two [b, c)
// tombstones.
//
// We do this check as follows:
// - For each level that can have untruncated tombstones, compute the atomic compaction
//   bounds and use them to truncate tombstones.
// - Now that we have a set of truncated tombstones for each level, put them into one
//   pool of tombstones along with their level information.
// - Collect the start and end user keys from all these tombstones and use them to fragment
//   all the tombstones.
// - Sort tombstones by start key and increasing level number -- all tombstones that have
//   the same start key will have the same end key because they have been fragmented.
// - Iterate and check -- see comment in iterateAndCheckTombstones().
func checkRangeTombstones(c *checkConfig) error {
	var level int
	var tombstones []tombstoneWithLevel
	var err error

	memtables := c.readState.memtables
	for i := len(memtables) - 1; i >= 0; i-- {
		iter := memtables[i].newRangeDelIter(nil)
		if iter == nil {
			continue
		}
		if tombstones, err = addTombstonesFromIter(iter, level, tombstones, c.seqNum); err != nil {
			return err
		}
		level++
	}

	current := c.readState.current
	for i := len(current.Files[0]) - 1; i >= 0; i-- {
		f := &current.Files[0][i]
		iterToClose, iter, err := c.newIters(f, nil, nil)
		if err != nil {
			return err
		}
		iterToClose.Close()
		if iter == nil {
			continue
		}
		if tombstones, err = addTombstonesFromIter(iter, level, tombstones, c.seqNum); err != nil {
			return err
		}
		level++
	}
	// Now the levels with untruncated tombsones.
	for i := 1; i < len(current.Files); i++ {
		if len(current.Files[i]) == 0 {
			continue
		}
		files := &current.Files[i]
		for j := 0; j < len(*files); j++ {
			lower, upper := getAtomicUnitBounds(c.cmp, *files, j)
			iterToClose, iter, err := c.newIters(&(*files)[j], nil, nil)
			if err != nil {
				return err
			}
			iterToClose.Close()
			if iter == nil {
				continue
			}
			rangeDelIter := rangedel.Truncate(c.cmp, iter, lower, upper)
			if tombstones, err = addTombstonesFromIter(rangeDelIter, level, tombstones, c.seqNum); err != nil {
				return err
			}
		}
		level++
	}
	// We now have truncated tombstones.
	log.Printf("truncated tombstones: %d", len(tombstones))
	// Fragment them all.
	var userKeys [][]byte
	userKeys = collectAllUserKeys(c.cmp, tombstones)
	tombstones = fragmentUsingUserKeys(c.cmp, tombstones, userKeys)
	log.Printf("fragmented tombstones: %d", len(tombstones))
	return iterateAndCheckTombstones(c.cmp, tombstones)
}

// TODO: mostly copied from compaction.go: refactor and reuse?
func getAtomicUnitBounds(cmp Compare, files []fileMetadata, index int) (lower, upper []byte) {
	lower = files[index].Smallest.UserKey
	upper = files[index].Largest.UserKey
	for i := index; i > 0; i-- {
		cur := &files[i]
		prev := &files[i-1]
		if cmp(prev.Largest.UserKey, cur.Smallest.UserKey) < 0 {
			break
		}
		if prev.Largest.Trailer == InternalKeyRangeDeleteSentinel {
			break
		}
		lower = prev.Smallest.UserKey
	}
	for i := index + 1; i < len(files); i++ {
		cur := &files[i-1]
		next := &files[i]
		if cmp(cur.Largest.UserKey, next.Smallest.UserKey) < 0 {
			break
		}
		if cur.Largest.Trailer == InternalKeyRangeDeleteSentinel {
			break
		}
		upper = next.Largest.UserKey
	}
	return
}

func addTombstonesFromIter(
	iter base.InternalIterator, level int, tombstones []tombstoneWithLevel, seqNum uint64,
) ([]tombstoneWithLevel, error) {
	var count int
	for key, value := iter.First(); key != nil; key, value = iter.Next() {
		if !key.Visible(seqNum) {
			continue
		}
		var t rangedel.Tombstone
		t.Start = key.Clone()
		t.End = append(t.End[:0], value...)
		tombstones = append(tombstones, tombstoneWithLevel{
			Tombstone: t,
			level:     level,
		})
		count++
	}
	if count > 0 {
		log.Printf("level: %d, tombstones: %d", level, count)
	}
	if err := iter.Close(); err != nil {
		return nil, iter.Error()
	}
	return tombstones, nil
}

type userKeysSort struct {
	cmp Compare
	buf [][]byte
}

func (v *userKeysSort) Len() int { return len(v.buf) }
func (v *userKeysSort) Less(i, j int) bool {
	return v.cmp(v.buf[i], v.buf[j]) < 0
}
func (v *userKeysSort) Swap(i, j int) {
	v.buf[i], v.buf[j] = v.buf[j], v.buf[i]
}
func collectAllUserKeys(cmp Compare, tombstones []tombstoneWithLevel) [][]byte {
	var keys [][]byte
	for _, t := range tombstones {
		keys = append(keys, t.Start.UserKey)
		keys = append(keys, t.End)
	}
	sorter := userKeysSort{
		cmp: cmp,
		buf: keys,
	}
	sort.Sort(&sorter)
	var last, curr int
	for last, curr = -1, 0; curr < len(keys); curr++ {
		if last < 0 || cmp(keys[last], keys[curr]) != 0 {
			last++
			keys[last] = keys[curr]
		}
	}
	keys = keys[:last+1]
	return keys
}

func fragmentUsingUserKeys(
	cmp Compare, tombstones []tombstoneWithLevel, userKeys [][]byte,
) []tombstoneWithLevel {
	var buf []tombstoneWithLevel
	for _, t := range tombstones {
		// Find the first position with tombstone start < user key
		i := sort.Search(len(userKeys), func(i int) bool {
			return cmp(t.Start.UserKey, userKeys[i]) < 0
		})
		for ; i < len(userKeys); i++ {
			if cmp(userKeys[i], t.End) >= 0 {
				break
			}
			tPartial := t
			tPartial.End = userKeys[i]
			buf = append(buf, tPartial)
			t.Start.UserKey = userKeys[i]
		}
		buf = append(buf, t)
	}
	return buf
}

func (d *DB) CheckLevels() error {
	// Grab and reference the current readState.
	readState := d.loadReadState()
	defer func() {
		readState.unref()
	}()

	// Determine the seqnum to read at after grabbing the read state (current and
	// memtables) above.
	seqNum := atomic.LoadUint64(&d.mu.versions.visibleSeqNum)

	checkConfig := &checkConfig{
		cmp:       DefaultComparer.Compare,
		readState: readState,
		newIters:  d.newIters,
		seqNum:    seqNum,
	}
	return CheckLevelInternal(checkConfig)
}

func CheckLevelInternal(c *checkConfig) error {
	// Phase 1: Use a simpleMergingIter to step through all the points and ensure that
	// points with the same user key at different levels are not inverted wrt sequence numbers
	// and the same holds for tombstones that cover points. To do this, one needs to construct
	// a simpleMergingIter which is similar to how one constructs a mergingIter.

	// Add mem tables from newest to oldest.
	mlevels := make([]simpleMergingIterLevel, 0)
	memtables := c.readState.memtables
	for i := len(memtables) - 1; i >= 0; i-- {
		mem := memtables[i]
		mlevels = append(mlevels, simpleMergingIterLevel{
			iter:         mem.newIter(nil),
			rangeDelIter: mem.newRangeDelIter(nil),
		})
	}

	// Add L0 files from newest to oldest.
	current := c.readState.current
	for i := len(current.Files[0]) - 1; i >= 0; i-- {
		f := &current.Files[0][i]
		iter, rangeDelIter, err := c.newIters(f, nil, nil)
		if err != nil {
			return err
		}
		mlevels = append(mlevels, simpleMergingIterLevel{
			iter:         iter,
			rangeDelIter: rangeDelIter,
		})
	}

	// Determine the final size for mlevels so that there are no more reallocations. levelIter
	// will hold a pointer to elements in mlevels.
	start := len(mlevels)
	for level := 1; level < len(current.Files); level++ {
		if len(current.Files[level]) == 0 {
			continue
		}
		mlevels = append(mlevels, simpleMergingIterLevel{})
	}
	finalMLevels := mlevels
	mlevels = mlevels[start:]
	for level := 1; level < len(current.Files); level++ {
		if len(current.Files[level]) == 0 {
			continue
		}
		li := &levelIter{}
		li.init(nil, c.cmp, c.newIters, current.Files[level], nil)
		li.initRangeDel(&mlevels[0].rangeDelIter)
		li.initSmallestLargestUserKey(&mlevels[0].smallestUserKey, nil, nil)
		mlevels[0].iter = li
		mlevels = mlevels[1:]
	}

	mergingIter := &simpleMergingIter{}
	mergingIter.init(c.cmp, c.seqNum, finalMLevels...)
	var count int
	for cont := mergingIter.step(); cont; cont = mergingIter.step() {
		count++
	}
	log.Printf("mergingIter steps: %d", count)
	if err := mergingIter.err; err != nil {
		return err
	}

	// Phase 2: Check that the tombstones are mutually consistent.
	return checkRangeTombstones(c)
}
