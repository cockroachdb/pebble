// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"sort"
	"sync/atomic"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/rangedel"
)

// This file implements DB.CheckLevels() which checks that every entry in the DB is
// consistent with respect to the level invariant: any point (or the infinite number of
// points in a range tombstone) has a seqnum such that a point with the same UserKey at a lower
// level has a lower seqnum. This is an expensive check since it involves iterating over all the
// entries in the DB, hence only intended for tests or tools.
//
// If we ignore range tombstones, the consistency checking of points can be done with a simplified
// version of mergingIter. simpleMergingIter is that simplified version of mergingIter that only
// needs to step through points (analogous to only doing Next()). It can also easily accommodate
// consistency checking of points relative to range tombstones. simpleMergingIter does not do any
// seek optimizations present in mergingIter (it minimally needs to seek the range delete iterators
// to position them at or past the current point) since it does not want to miss points for purposes
// of consistency checking.
//
// Mutual consistency of range tombstones is non-trivial to check. One needs to detect inversions
// of the form [a, c)#8 at higher level and [b, c)#10 at a lower level. The start key of the former
// is not contained in the latter and we can't use the exclusive end key, c, for a containment
// check since it is the sentinel key. We observe that if these tombstones were fragmented wrt each
// other we would have [a, b)#8 and [b, c)#8 at the higher level and [b, c)#10 at the lower level
// and then it is is trivial to compare the two [b, c) tombstones. Note that this fragmentation
// needs to take into account that tombstones in a file may be untruncated and need to act within
// the bounds of the file. This checking is performed by checkRangeTombstones() and its helper
// functions.

// The per-level structure used by simpleMergingIter.
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
	// The last point's key and level. For validation.
	lastKey   InternalKey
	lastLevel int
	// The first error will cause step() to return false.
	err       error
	numPoints int64
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

// Positions all the rangedel iterators at or past the current top of the heap, using SeekGE().
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
		m.numPoints++
		if m.heap.cmp(item.key.UserKey, m.lastKey.UserKey) == 0 {
			// At the same user key. We will see them in decreasing seqnum order so the lastLevel
			// must not be lower.
			if m.lastLevel > item.index {
				m.err = fmt.Errorf("found InternalKey %v at level index %d and InternalKey %v at level index %d",
					item.key, item.index, m.lastKey, m.lastLevel)
				return false
			}
			m.lastLevel = item.index
		} else {
			// The user key has changed.
			m.lastKey.Trailer = item.key.Trailer
			m.lastKey.UserKey = append(m.lastKey.UserKey[:0], item.key.UserKey...)
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
					m.err = fmt.Errorf("tombstone %v at level index %d deletes key %v at level index %d",
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
		m.err = l.iter.Close()
		l.iter = nil
		m.heap.pop()
	}
	if m.err != nil || m.heap.len() == 0 {
		return false
	}
	m.positionRangeDels()
	return true
}

// Checking that range tombstones are mutually consistent is performed by checkRangeTombstones().
// See the overview comment at the top of the file.
//
// We do this check as follows:
// - For each level that can have untruncated tombstones, compute the atomic compaction
//   bounds (getAtomicUnitBounds()) and use them to truncate tombstones.
// - Now that we have a set of truncated tombstones for each level, put them into one
//   pool of tombstones along with their level information (addTombstonesFromIter()).
// - Collect the start and end user keys from all these tombstones (collectAllUserKey()) and use
//   them to fragment all the tombstones (fragmentUsingUserKey()).
// - Sort tombstones by start key and decreasing seqnum (tombstonesByStartKeyAndSeqnum) -- all
//   tombstones that have the same start key will have the same end key because they have been
//   fragmented.
// - Iterate and check (iterateAndCheckTombstones()).
// Note that this simple approach requires holding all the tombstones across all levels in-memory.
// A more sophisticated incremental approach could be devised, if necessary.

// A tombstone and the corresponding level it was found in.
type tombstoneWithLevel struct {
	rangedel.Tombstone
	level int
}

// For sorting tombstoneWithLevels in increasing order of start UserKey and for the same start UserKey
// in decreasing order of seqnum.
type tombstonesByStartKeyAndSeqnum struct {
	cmp Compare
	buf []tombstoneWithLevel
}

func (v *tombstonesByStartKeyAndSeqnum) Len() int { return len(v.buf) }
func (v *tombstonesByStartKeyAndSeqnum) Less(i, j int) bool {
	less := v.cmp(v.buf[i].Start.UserKey, v.buf[j].Start.UserKey)
	if less == 0 {
		return v.buf[i].Start.SeqNum() > v.buf[j].Start.SeqNum()
	}
	return less < 0
}
func (v *tombstonesByStartKeyAndSeqnum) Swap(i, j int) {
	v.buf[i], v.buf[j] = v.buf[j], v.buf[i]
}

func iterateAndCheckTombstones(cmp Compare, tombstones []tombstoneWithLevel) error {
	sortBuf := tombstonesByStartKeyAndSeqnum{
		cmp: cmp,
		buf: tombstones,
	}
	sort.Sort(&sortBuf)

	// For a sequence of tombstones that share the same start UserKey, we will encounter
	// them in non-increasing seqnum order and so should encounter them in non-decreasing level
	// order.
	var lastUserKey []byte
	lastLevel := -1
	for _, t := range tombstones {
		if cmp(lastUserKey, t.Start.UserKey) != 0 {
			lastUserKey = t.Start.UserKey
			lastLevel = t.level
			continue
		}
		if lastLevel > t.level {
			return fmt.Errorf("encountered tombstone %v at level index %d that has a lower seqnum than the same tombstone at level index %d",
				t.Tombstone, t.level, lastLevel)
		}
		lastLevel = t.level
	}
	return nil
}

type checkConfig struct {
	logger    Logger
	cmp       Compare
	readState *readState
	newIters  tableNewIters
	seqNum    uint64
	stats     *CheckLevelsStats
}

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
	if c.stats != nil {
		c.stats.NumTombstones = len(tombstones)
	}
	// We now have truncated tombstones.
	// Fragment them all.
	var userKeys [][]byte
	userKeys = collectAllUserKeys(c.cmp, tombstones)
	tombstones = fragmentUsingUserKeys(c.cmp, tombstones, userKeys)
	return iterateAndCheckTombstones(c.cmp, tombstones)
}

// TODO(sbhola): mostly copied from compaction.go: refactor and reuse?
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
	}
	if err := iter.Close(); err != nil {
		return nil, err
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

// CheckLevelsStats provides basic stats on points and tombstones encountered.
type CheckLevelsStats struct {
	NumPoints     int64
	NumTombstones int
}

// CheckLevels checks that every entry in the DB is consistent with the level invariant. See the
// comment at the top of the file.
func (d *DB) CheckLevels(stats *CheckLevelsStats) error {
	// Grab and reference the current readState.
	readState := d.loadReadState()
	defer readState.unref()

	// Determine the seqnum to read at after grabbing the read state (current and
	// memtables) above.
	seqNum := atomic.LoadUint64(&d.mu.versions.visibleSeqNum)

	checkConfig := &checkConfig{
		logger:    d.opts.Logger,
		cmp:       d.cmp,
		readState: readState,
		newIters:  d.newIters,
		seqNum:    seqNum,
		stats:     stats,
	}
	return checkLevelsInternal(checkConfig)
}

func checkLevelsInternal(c *checkConfig) (err error) {
	// Phase 1: Use a simpleMergingIter to step through all the points and ensure that
	// points with the same user key at different levels are not inverted wrt sequence numbers
	// and the same holds for tombstones that cover points. To do this, one needs to construct
	// a simpleMergingIter which is similar to how one constructs a mergingIter.

	// Add mem tables from newest to oldest.
	mlevels := make([]simpleMergingIterLevel, 0)
	defer func() {
		for i := range mlevels {
			l := &mlevels[i]
			if l.iter != nil {
				err = firstError(err, l.iter.Close())
				l.iter = nil
			}
			if l.rangeDelIter != nil {
				err = firstError(err, l.rangeDelIter.Close())
				l.rangeDelIter = nil
			}
		}
	}()

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
	mlevelAlloc := mlevels[start:]
	for level := 1; level < len(current.Files); level++ {
		if len(current.Files[level]) == 0 {
			continue
		}
		iterOpts := IterOptions{logger: c.logger}
		li := &levelIter{}
		li.init(iterOpts, c.cmp, c.newIters, current.Files[level], nil)
		li.initRangeDel(&mlevelAlloc[0].rangeDelIter)
		li.initSmallestLargestUserKey(&mlevelAlloc[0].smallestUserKey, nil, nil)
		mlevelAlloc[0].iter = li
		mlevelAlloc = mlevelAlloc[1:]
	}

	mergingIter := &simpleMergingIter{}
	mergingIter.init(c.cmp, c.seqNum, mlevels...)
	for cont := mergingIter.step(); cont; cont = mergingIter.step() {
	}
	if err := mergingIter.err; err != nil {
		return err
	}
	if c.stats != nil {
		c.stats.NumPoints = mergingIter.numPoints
	}

	// Phase 2: Check that the tombstones are mutually consistent.
	return checkRangeTombstones(c)
}
