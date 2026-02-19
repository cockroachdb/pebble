// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	stdcmp "cmp"
	"context"
	"fmt"
	"io"
	"iter"
	"maps"
	"slices"
	"sort"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/rangedel"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/blob"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/colblk"
)

// This file implements DB.CheckLevels() which checks that every entry in the
// DB is consistent with respect to the level invariant: any point (or the
// infinite number of points in a range tombstone) has a seqnum such that a
// point with the same UserKey at a lower level has a lower seqnum. This is an
// expensive check since it involves iterating over all the entries in the DB,
// hence only intended for tests or tools.
//
// If we ignore range tombstones, the consistency checking of points can be
// done with a simplified version of mergingIter. simpleMergingIter is that
// simplified version of mergingIter that only needs to step through points
// (analogous to only doing Next()). It can also easily accommodate
// consistency checking of points relative to range tombstones.
// simpleMergingIter does not do any seek optimizations present in mergingIter
// (it minimally needs to seek the range delete iterators to position them at
// or past the current point) since it does not want to miss points for
// purposes of consistency checking.
//
// Mutual consistency of range tombstones is non-trivial to check. One needs
// to detect inversions of the form [a, c)#8 at higher level and [b, c)#10 at
// a lower level. The start key of the former is not contained in the latter
// and we can't use the exclusive end key, c, for a containment check since it
// is the sentinel key. We observe that if these tombstones were fragmented
// wrt each other we would have [a, b)#8 and [b, c)#8 at the higher level and
// [b, c)#10 at the lower level and then it is is trivial to compare the two
// [b, c) tombstones. Note that this fragmentation needs to take into account
// that tombstones in a file may be untruncated and need to act within the
// bounds of the file. This checking is performed by checkRangeTombstones()
// and its helper functions.

// The per-level structure used by simpleMergingIter.
type simpleMergingIterLevel struct {
	iter internalIterator
	// getTombstone returns the range deletion tombstone covering the current
	// iterator position. getTombstone must not be called after iter is closed.
	getTombstone keyspan.TombstoneSpanGetter
	iterKV       *base.InternalKV
}

type simpleMergingIter struct {
	levels   []simpleMergingIterLevel
	snapshot base.SeqNum
	heap     simpleMergingIterHeap
	// The last point's key and level. For validation.
	lastKey     InternalKey
	lastLevel   int
	lastIterMsg string
	// A non-nil valueMerger means MERGE record processing is ongoing.
	valueMerger base.ValueMerger
	// The first error will cause step() to return false.
	err       error
	numPoints int64
	merge     Merge
	formatKey base.FormatKey
}

func (m *simpleMergingIter) init(
	merge Merge,
	cmp Compare,
	snapshot base.SeqNum,
	formatKey base.FormatKey,
	levels ...simpleMergingIterLevel,
) {
	m.levels = levels
	m.formatKey = formatKey
	m.merge = merge
	m.snapshot = snapshot
	m.lastLevel = -1
	m.heap.cmp = cmp
	m.heap.items = make([]simpleMergingIterItem, 0, len(levels))
	for i := range m.levels {
		l := &m.levels[i]
		l.iterKV = l.iter.First()
		if l.iterKV != nil {
			item := simpleMergingIterItem{
				index: i,
				kv:    *l.iterKV,
			}
			item.kv.K = l.iterKV.K.Clone()
			m.heap.items = append(m.heap.items, item)
		}
	}
	m.heap.init()
}

// Returns true if not yet done.
func (m *simpleMergingIter) step() bool {
	if m.heap.len() == 0 || m.err != nil {
		return false
	}
	item := &m.heap.items[0]
	l := &m.levels[item.index]
	// Sentinels are not relevant for this point checking.
	if !item.kv.K.IsExclusiveSentinel() && item.kv.K.Visible(m.snapshot, base.SeqNumMax) {
		// This is a visible point key.
		if !m.handleVisiblePoint(item, l) {
			return false
		}
	}

	// The iterator for the current level may be closed in the following call to
	// Next(). We save its debug string for potential use after it is closed -
	// either in this current step() invocation or on the next invocation.
	m.lastIterMsg = l.iter.String()

	// Step to the next point.
	l.iterKV = l.iter.Next()
	if l.iterKV == nil {
		m.err = errors.CombineErrors(l.iter.Error(), l.iter.Close())
		l.iter = nil
		l.getTombstone = nil
		m.heap.pop()
	} else {
		// Check point keys in an sstable are ordered. Although not required, we check
		// for memtables as well. A subtle check here is that successive sstables of
		// L1 and higher levels are ordered. This happens when levelIter moves to the
		// next sstable in the level, in which case item.key is previous sstable's
		// last point key.
		if !l.iterKV.K.IsExclusiveSentinel() && base.InternalCompare(m.heap.cmp, item.kv.K, l.iterKV.K) >= 0 {
			m.err = errors.Errorf("out of order keys %s >= %s in %s",
				item.kv.K.Pretty(m.formatKey), l.iterKV.K.Pretty(m.formatKey), l.iter)
			return false
		}
		userKeyBuf := item.kv.K.UserKey[:0]
		item.kv = *l.iterKV
		item.kv.K.UserKey = append(userKeyBuf, l.iterKV.K.UserKey...)
		if m.heap.len() > 1 {
			m.heap.fix(0)
		}
	}
	if m.err != nil {
		return false
	}
	if m.heap.len() == 0 {
		// If m.valueMerger != nil, the last record was a MERGE record.
		if m.valueMerger != nil {
			var closer io.Closer
			var err error
			_, closer, err = m.valueMerger.Finish(true /* includesBase */)
			if closer != nil {
				err = errors.CombineErrors(err, closer.Close())
			}
			if err != nil {
				m.err = errors.CombineErrors(m.err,
					errors.Wrapf(err, "merge processing error on key %s in %s",
						item.kv.K.Pretty(m.formatKey), m.lastIterMsg))
			}
			m.valueMerger = nil
		}
		return false
	}
	return true
}

// handleVisiblePoint returns true if validation succeeded and level checking
// can continue.
func (m *simpleMergingIter) handleVisiblePoint(
	item *simpleMergingIterItem, l *simpleMergingIterLevel,
) (ok bool) {
	m.numPoints++
	keyChanged := m.heap.cmp(item.kv.K.UserKey, m.lastKey.UserKey) != 0
	if !keyChanged {
		// At the same user key. We will see them in decreasing seqnum
		// order so the lastLevel must not be lower.
		if m.lastLevel > item.index {
			m.err = errors.Errorf("found InternalKey %s in %s and InternalKey %s in %s",
				item.kv.K.Pretty(m.formatKey), l.iter, m.lastKey.Pretty(m.formatKey),
				m.lastIterMsg)
			return false
		}
		m.lastLevel = item.index
	} else {
		// The user key has changed.
		m.lastKey.Trailer = item.kv.K.Trailer
		m.lastKey.UserKey = append(m.lastKey.UserKey[:0], item.kv.K.UserKey...)
		m.lastLevel = item.index
	}
	// Ongoing series of MERGE records ends with a MERGE record.
	if keyChanged && m.valueMerger != nil {
		var closer io.Closer
		_, closer, m.err = m.valueMerger.Finish(true /* includesBase */)
		if m.err == nil && closer != nil {
			m.err = closer.Close()
		}
		m.valueMerger = nil
	}
	itemValue, _, err := item.kv.Value(nil)
	if err != nil {
		m.err = err
		return false
	}
	if m.valueMerger != nil {
		// Ongoing series of MERGE records.
		switch item.kv.K.Kind() {
		case InternalKeyKindSingleDelete, InternalKeyKindDelete, InternalKeyKindDeleteSized:
			var closer io.Closer
			_, closer, m.err = m.valueMerger.Finish(true /* includesBase */)
			if m.err == nil && closer != nil {
				m.err = closer.Close()
			}
			m.valueMerger = nil
		case InternalKeyKindSet, InternalKeyKindSetWithDelete:
			m.err = m.valueMerger.MergeOlder(itemValue)
			if m.err == nil {
				var closer io.Closer
				_, closer, m.err = m.valueMerger.Finish(true /* includesBase */)
				if m.err == nil && closer != nil {
					m.err = closer.Close()
				}
			}
			m.valueMerger = nil
		case InternalKeyKindMerge:
			m.err = m.valueMerger.MergeOlder(itemValue)
		default:
			m.err = errors.Errorf("pebble: invalid internal key kind %s in %s",
				item.kv.K.Pretty(m.formatKey),
				l.iter)
			return false
		}
	} else if item.kv.K.Kind() == InternalKeyKindMerge && m.err == nil {
		// New series of MERGE records.
		m.valueMerger, m.err = m.merge(item.kv.K.UserKey, itemValue)
	}
	if m.err != nil {
		m.err = errors.Wrapf(m.err, "merge processing error on key %s in %s",
			item.kv.K.Pretty(m.formatKey), l.iter)
		return false
	}
	// Is this point covered by a tombstone at a lower level? Note that all these
	// iterators must be positioned at a key > item.key.
	for level := item.index + 1; level < len(m.levels); level++ {
		lvl := &m.levels[level]
		if lvl.getTombstone == nil {
			continue
		}
		t := lvl.getTombstone.Span()
		if t.Empty() {
			continue
		}
		if t.Contains(m.heap.cmp, item.kv.K.UserKey) && t.CoversAt(m.snapshot, item.kv.K.SeqNum()) {
			m.err = errors.Errorf("tombstone %s in %s deletes key %s in %s",
				t.Pretty(m.formatKey), lvl.iter, item.kv.K.Pretty(m.formatKey),
				l.iter)
			return false
		}
	}
	return true
}

// Checking that range tombstones are mutually consistent is performed by
// checkRangeTombstones(). See the overview comment at the top of the file.
//
// We do this check as follows:
// - Collect the tombstones for each level, put them into one pool of tombstones
//   along with their level information (addTombstonesFromIter()).
// - Collect the start and end user keys from all these tombstones
//   (collectAllUserKey()) and use them to fragment all the tombstones
//   (fragmentUsingUserKey()).
// - Sort tombstones by start key and decreasing seqnum (all tombstones that
//   have the same start key will have the same end key because they have been
//   fragmented)
// - Iterate and check (iterateAndCheckTombstones()).
//
// Note that this simple approach requires holding all the tombstones across all
// levels in-memory. A more sophisticated incremental approach could be devised,
// if necessary.

// A tombstone and the corresponding level it was found in.
type tombstoneWithLevel struct {
	keyspan.Span
	level int
	// The level in LSM. A -1 means it's a memtable.
	lsmLevel int
	tableNum base.TableNum
}

func iterateAndCheckTombstones(
	cmp Compare, formatKey base.FormatKey, tombstones []tombstoneWithLevel,
) error {
	slices.SortFunc(tombstones, func(a, b tombstoneWithLevel) int {
		if v := cmp(a.Start, b.Start); v != 0 {
			return v
		}
		return stdcmp.Compare(b.LargestSeqNum(), a.LargestSeqNum())
	})

	// For a sequence of tombstones that share the same start UserKey, we will
	// encounter them in non-increasing seqnum order and so should encounter them
	// in non-decreasing level order.
	lastTombstone := tombstoneWithLevel{}
	for _, t := range tombstones {
		if cmp(lastTombstone.Start, t.Start) == 0 && lastTombstone.level > t.level {
			return errors.Errorf("encountered tombstone %s in %s"+
				" that has a lower seqnum than the same tombstone in %s",
				t.Span.Pretty(formatKey), levelOrMemtable(t.lsmLevel, t.tableNum),
				levelOrMemtable(lastTombstone.lsmLevel, lastTombstone.tableNum))
		}
		lastTombstone = t
	}
	return nil
}

type checkConfig struct {
	logger    Logger
	comparer  *Comparer
	readState *readState
	newIters  tableNewIters
	seqNum    base.SeqNum
	stats     *CheckLevelsStats
	merge     Merge
	formatKey base.FormatKey
	readEnv   block.ReadEnv
	// combinedBlobMapping chains the version's BlobFileSet with any blob files
	// from flushable ingests that haven't been flushed yet.
	combinedBlobMapping combinedBlobFileMapping
	// blobValueFetcher is the ValueFetcher to use when retrieving values stored
	// externally in blob files.
	blobValueFetcher blob.ValueFetcher
	fileCache        *fileCacheHandle
}

// cmp is shorthand for comparer.Compare.
func (c *checkConfig) cmp(a, b []byte) int { return c.comparer.Compare(a, b) }

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
		tombstones, err = addTombstonesFromIter(
			iter, level, -1, 0, tombstones, c.seqNum, c.cmp, c.formatKey,
		)
		iter.Close()
		if err != nil {
			return err
		}
		level++
	}

	current := c.readState.current
	addTombstonesFromLevel := func(files iter.Seq[*manifest.TableMetadata], lsmLevel int) error {
		for f := range files {
			iters, err := c.newIters(
				context.Background(), f, &IterOptions{layer: manifest.Level(lsmLevel)},
				internalIterOpts{}, iterRangeDeletions)
			if err != nil {
				return err
			}
			tombstones, err = addTombstonesFromIter(iters.RangeDeletion(), level, lsmLevel, f.TableNum,
				tombstones, c.seqNum, c.cmp, c.formatKey)
			_ = iters.CloseAll()

			if err != nil {
				return err
			}
		}
		return nil
	}
	// Now the levels with untruncated tombsones.
	for i := len(current.L0SublevelFiles) - 1; i >= 0; i-- {
		if current.L0SublevelFiles[i].Empty() {
			continue
		}
		err := addTombstonesFromLevel(current.L0SublevelFiles[i].All(), 0)
		if err != nil {
			return err
		}
		level++
	}
	for i := 1; i < len(current.Levels); i++ {
		if err := addTombstonesFromLevel(current.Levels[i].All(), i); err != nil {
			return err
		}
		level++
	}
	if c.stats != nil {
		c.stats.NumTombstones = len(tombstones)
	}
	// We now have truncated tombstones.
	// Fragment them all.
	userKeys := collectAllUserKeys(c.cmp, tombstones)
	tombstones = fragmentUsingUserKeys(c.cmp, tombstones, userKeys)
	return iterateAndCheckTombstones(c.cmp, c.formatKey, tombstones)
}

func levelOrMemtable(lsmLevel int, tableNum base.TableNum) string {
	if lsmLevel == -1 {
		return "memtable"
	}
	return fmt.Sprintf("L%d: fileNum=%s", lsmLevel, tableNum)
}

func addTombstonesFromIter(
	iter keyspan.FragmentIterator,
	level int,
	lsmLevel int,
	tableNum base.TableNum,
	tombstones []tombstoneWithLevel,
	seqNum base.SeqNum,
	cmp Compare,
	formatKey base.FormatKey,
) (_ []tombstoneWithLevel, err error) {
	var prevTombstone keyspan.Span
	tomb, err := iter.First()
	for ; tomb != nil; tomb, err = iter.Next() {
		t := tomb.Visible(seqNum)
		if t.Empty() {
			continue
		}
		t = t.Clone()
		// This is mainly a test for rangeDelV2 formatted blocks which are expected to
		// be ordered and fragmented on disk. But we anyways check for memtables,
		// rangeDelV1 as well.
		if cmp(prevTombstone.End, t.Start) > 0 {
			return nil, errors.Errorf("unordered or unfragmented range delete tombstones %s, %s in %s",
				prevTombstone.Pretty(formatKey), t.Pretty(formatKey), levelOrMemtable(lsmLevel, tableNum))
		}
		prevTombstone = t

		if !t.Empty() {
			tombstones = append(tombstones, tombstoneWithLevel{
				Span:     t,
				level:    level,
				lsmLevel: lsmLevel,
				tableNum: tableNum,
			})
		}
	}
	if err != nil {
		return nil, err
	}
	return tombstones, nil
}

func collectAllUserKeys(cmp Compare, tombstones []tombstoneWithLevel) [][]byte {
	keys := make([][]byte, 0, len(tombstones)*2)
	for _, t := range tombstones {
		keys = append(keys, t.Start, t.End)
	}
	slices.SortFunc(keys, cmp)
	return slices.CompactFunc(keys, func(a, b []byte) bool {
		return cmp(a, b) == 0
	})
}

func fragmentUsingUserKeys(
	cmp Compare, tombstones []tombstoneWithLevel, userKeys [][]byte,
) []tombstoneWithLevel {
	var buf []tombstoneWithLevel
	for _, t := range tombstones {
		// Find the first position with tombstone start < user key
		i := sort.Search(len(userKeys), func(i int) bool {
			return cmp(t.Start, userKeys[i]) < 0
		})
		for ; i < len(userKeys); i++ {
			if cmp(userKeys[i], t.End) >= 0 {
				break
			}
			tPartial := t
			tPartial.End = userKeys[i]
			buf = append(buf, tPartial)
			t.Start = userKeys[i]
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

// CheckLevels checks:
//   - Every entry in the DB is consistent with the level invariant. See the
//     comment at the top of the file.
//   - Point keys in sstables are ordered.
//   - Range delete tombstones in sstables are ordered and fragmented.
//   - Successful processing of all MERGE records.
//   - Each sstable's blob reference liveness block is valid.
func (d *DB) CheckLevels(stats *CheckLevelsStats) error {
	// Grab and reference the current readState.
	readState := d.loadReadState()
	defer readState.unref()

	// Determine the seqnum to read at after grabbing the read state (current and
	// memtables) above.
	seqNum := d.mu.versions.visibleSeqNum.Load()

	bufferPool := new(block.BufferPool)
	bufferPool.Init(10, block.ForLevelChecking)
	defer bufferPool.Release()

	checkConfig := &checkConfig{
		logger:    d.opts.Logger,
		comparer:  d.opts.Comparer,
		readState: readState,
		newIters:  d.newIters,
		seqNum:    seqNum,
		stats:     stats,
		merge:     d.merge,
		formatKey: d.opts.Comparer.FormatKey,
		readEnv: block.ReadEnv{
			BufferPool: bufferPool,
			// TODO(jackson): Add categorized stats.
		},
		fileCache: d.fileCache,
	}
	// Set up a combined blob file mapping that includes both the version's
	// BlobFileSet and blob files from flushable ingests.
	if checkConfig.combinedBlobMapping.Init(
		&readState.current.BlobFiles, readState.memtables,
	) {
		checkConfig.blobValueFetcher.Init(
			&checkConfig.combinedBlobMapping,
			checkConfig.fileCache,
			checkConfig.readEnv,
			blob.SuggestedCachedReaders(readState.current.MaxReadAmp()))
	}
	defer func() { _ = checkConfig.blobValueFetcher.Close() }()
	return checkLevelsInternal(checkConfig)
}

func checkLevelsInternal(c *checkConfig) (err error) {
	internalOpts := internalIterOpts{
		readEnv:          sstable.ReadEnv{Block: c.readEnv},
		blobValueFetcher: &c.blobValueFetcher,
	}

	// Phase 1: Use a simpleMergingIter to step through all the points and ensure
	// that points with the same user key at different levels are not inverted
	// wrt sequence numbers and the same holds for tombstones that cover points.
	// To do this, one needs to construct a simpleMergingIter which is similar to
	// how one constructs a mergingIter.

	// Add mem tables from newest to oldest.
	var mlevels []simpleMergingIterLevel
	defer func() {
		for i := range mlevels {
			l := &mlevels[i]
			if l.iter != nil {
				err = firstError(err, l.iter.Close())
				l.iter = nil
			}
		}
	}()

	memtables := c.readState.memtables
	for i := len(memtables) - 1; i >= 0; i-- {
		mem := memtables[i]
		var mil simpleMergingIterLevel
		// For ingestedFlushable, we need to pass the blob value fetcher to allow
		// reading values from blob files.
		if ingested, ok := mem.flushable.(*ingestedFlushable); ok {
			mil.iter = ingested.newIterInternal(nil, internalOpts)
		} else {
			mil.iter = mem.newIter(nil)
		}
		mil.iter, mil.getTombstone = rangedel.Interleave(c.comparer, mil.iter, mem.newRangeDelIter(nil))
		mlevels = append(mlevels, mil)
	}

	current := c.readState.current
	// Determine the final size for mlevels so that there are no more
	// reallocations. levelIter will hold a pointer to elements in mlevels.
	start := len(mlevels)
	for sublevel := len(current.L0SublevelFiles) - 1; sublevel >= 0; sublevel-- {
		if current.L0SublevelFiles[sublevel].Empty() {
			continue
		}
		mlevels = append(mlevels, simpleMergingIterLevel{})
	}
	for level := 1; level < len(current.Levels); level++ {
		if current.Levels[level].Empty() {
			continue
		}
		mlevels = append(mlevels, simpleMergingIterLevel{})
	}
	mlevelAlloc := mlevels[start:]
	var allTables []*manifest.TableMetadata

	// Add L0 files by sublevel.
	for sublevel := len(current.L0SublevelFiles) - 1; sublevel >= 0; sublevel-- {
		if current.L0SublevelFiles[sublevel].Empty() {
			continue
		}
		manifestIter := current.L0SublevelFiles[sublevel].Iter()
		iterOpts := IterOptions{logger: c.logger}
		li := &levelIter{}
		li.init(context.Background(), iterOpts, c.comparer, c.newIters, manifestIter,
			manifest.L0Sublevel(sublevel), internalOpts)
		li.interleaveRangeDels = true
		mlevelAlloc[0].iter = li
		mlevelAlloc[0].getTombstone = li
		mlevelAlloc = mlevelAlloc[1:]
		for f := range current.L0SublevelFiles[sublevel].All() {
			allTables = append(allTables, f)
		}
	}
	for level := 1; level < len(current.Levels); level++ {
		if current.Levels[level].Empty() {
			continue
		}
		iterOpts := IterOptions{logger: c.logger}
		li := &levelIter{}
		li.init(context.Background(), iterOpts, c.comparer, c.newIters,
			current.Levels[level].Iter(), manifest.Level(level), internalOpts)
		li.interleaveRangeDels = true
		mlevelAlloc[0].iter = li
		mlevelAlloc[0].getTombstone = li
		mlevelAlloc = mlevelAlloc[1:]
		for f := range current.Levels[level].All() {
			allTables = append(allTables, f)
		}
	}

	mergingIter := &simpleMergingIter{}
	mergingIter.init(c.merge, c.cmp, c.seqNum, c.formatKey, mlevels...)
	for cont := mergingIter.step(); cont; cont = mergingIter.step() {
	}
	if err := mergingIter.err; err != nil {
		return err
	}
	if c.stats != nil {
		c.stats.NumPoints = mergingIter.numPoints
	}

	// Phase 2: Check that the tombstones are mutually consistent.
	if err := checkRangeTombstones(c); err != nil {
		return err
	}

	// Phase 3: Validate blob value liveness block for all tables in the LSM.
	// TODO(annie): This is a very expensive operation. We should try to reduce
	// the amount of work performed. One possibility is to have the caller
	// pass in a prng seed and use that to choose which tables to validate.
	if err := validateBlobValueLiveness(allTables, c.fileCache, c.readEnv, &c.blobValueFetcher); err != nil {
		return err
	}

	return nil
}

type valuesInfo struct {
	valueIDs  []int
	totalSize int
}

// gatherBlobHandles gathers all the blob handles in an sstable, returning a
// slice of maps; indexing into the slice at `i` is equivalent to retrieving
// each blob.BlockID's referenced blob.BlockValueID for the `i`th blob reference.
func gatherBlobHandles(
	ctx context.Context,
	readEnv block.ReadEnv,
	r *sstable.Reader,
	blobRefs manifest.BlobReferences,
	valueFetcher base.ValueFetcher,
) ([]map[blob.BlockID]valuesInfo, error) {
	iter, err := r.NewPointIter(ctx, sstable.IterOptions{
		Env: sstable.ReadEnv{
			Block: readEnv,
		},
		BlobContext: sstable.TableBlobContext{
			ValueFetcher: valueFetcher,
			References:   &blobRefs,
		},
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()

	referenced := make([]map[blob.BlockID]valuesInfo, len(blobRefs))
	for i := range referenced {
		referenced[i] = make(map[blob.BlockID]valuesInfo)
	}
	for kv := iter.First(); kv != nil; kv = iter.Next() {
		if kv.V.IsBlobValueHandle() {
			lv := kv.V.LazyValue()
			handleSuffix := blob.DecodeHandleSuffix(lv.ValueOrHandle)
			refID, ok := blobRefs.IDByBlobFileID(lv.Fetcher.BlobFileID)
			if !ok {
				return nil, errors.Errorf("blob file ID %d not found in blob references", lv.Fetcher.BlobFileID)
			}
			blockID := handleSuffix.BlockID
			valueID := int(handleSuffix.ValueID)
			vi := referenced[refID][blockID]
			vi.valueIDs = append(vi.valueIDs, valueID)
			vi.totalSize += lv.Len()
			referenced[refID][blockID] = vi
		}
	}
	return referenced, nil
}

func performValidationForSSTable(
	decoder colblk.ReferenceLivenessBlockDecoder,
	tableNum base.TableNum,
	referenced []map[blob.BlockID]valuesInfo,
) error {
	if len(referenced) != decoder.BlockDecoder().Rows() {
		return errors.Errorf("mismatch in number of references in blob value "+
			"liveness block: expected=%d found=%d", len(referenced),
			decoder.BlockDecoder().Rows())
	}
	for refID, blockValues := range referenced {
		bitmapEncodings := slices.Clone(decoder.LivenessAtReference(refID))
		blocks, err := sstable.DecodeBlobRefLivenessEncoding(bitmapEncodings)
		if err != nil {
			return err
		}
		for _, blockEnc := range blocks {
			blockID := blockEnc.BlockID
			vi, ok := blockValues[blockID]
			if !ok {
				return errors.Errorf("dangling refID=%d blockID=%d in blob value "+
					"liveness encoding for sstable %d", refID, blockID, tableNum)
			}
			encodedVals := slices.Collect(sstable.IterSetBitsInRunLengthBitmap(blockEnc.Bitmap))
			if !slices.Equal(vi.valueIDs, encodedVals) {
				return errors.Errorf("bitmap mismatch for refID=%d blockID=%d: "+
					"expected=%v encoded=%v for sstable %d", refID, blockID, vi.valueIDs,
					encodedVals, tableNum)
			}
			if vi.totalSize != blockEnc.ValuesSize {
				return errors.Errorf("value size mismatch for refID=%d blockID=%d: "+
					"expected=%d encoded=%d for sstable %d", refID, blockID, vi.totalSize,
					blockEnc.ValuesSize, tableNum)
			}
			// Remove the processed blockID from the map so that later,
			// we can check if we processed everything. This is to
			// ensure that we do not have any missing references in the
			// blob reference liveness block for any of the references
			// in the sstable.
			delete(blockValues, blockID)
		}
		if len(blockValues) > 0 {
			return errors.Errorf("refID=%d blockIDs=%v referenced by sstable %d "+
				"is/are not present in blob reference liveness block", refID,
				slices.Collect(maps.Keys(blockValues)), tableNum)
		}
	}
	return nil
}

// validateBlobValueLiveness iterates through each table,
// gathering all the blob handles, and then compares the values encoded in the
// blob reference liveness block to the values referenced by the blob handles.
func validateBlobValueLiveness(
	tables []*manifest.TableMetadata,
	fc *fileCacheHandle,
	readEnv block.ReadEnv,
	valueFetcher base.ValueFetcher,
) error {
	ctx := context.TODO()
	var decoder colblk.ReferenceLivenessBlockDecoder
	for _, t := range tables {
		if len(t.BlobReferences) == 0 {
			continue
		}
		if err := fc.withReader(ctx, readEnv, t, func(r *sstable.Reader, readEnv sstable.ReadEnv) error {
			// For this sstable, gather all the blob handles -- tracking
			// each base.BlobReferenceID + blob.BlockID's referenced
			// blob.BlockValueIDs.
			referenced, err := gatherBlobHandles(ctx, readEnv.Block, r, t.BlobReferences, valueFetcher)
			if err != nil {
				return err
			}
			h, err := r.ReadBlobRefIndexBlock(ctx, readEnv.Block)
			if err != nil {
				return err
			}
			defer h.Release()
			decoder.Init(h.BlockData())
			return performValidationForSSTable(decoder, t.TableNum, referenced)
		}); err != nil {
			return err
		}
	}
	return nil
}

type simpleMergingIterItem struct {
	index int
	kv    base.InternalKV
}

type simpleMergingIterHeap struct {
	cmp     Compare
	reverse bool
	items   []simpleMergingIterItem
}

func (h *simpleMergingIterHeap) len() int {
	return len(h.items)
}

func (h *simpleMergingIterHeap) less(i, j int) bool {
	ikey, jkey := h.items[i].kv.K, h.items[j].kv.K
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

func (h *simpleMergingIterHeap) swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

// init, fix, up and down are copied from the go stdlib.
func (h *simpleMergingIterHeap) init() {
	// heapify
	n := h.len()
	for i := n/2 - 1; i >= 0; i-- {
		h.down(i, n)
	}
}

func (h *simpleMergingIterHeap) fix(i int) {
	if !h.down(i, h.len()) {
		h.up(i)
	}
}

func (h *simpleMergingIterHeap) pop() *simpleMergingIterItem {
	n := h.len() - 1
	h.swap(0, n)
	h.down(0, n)
	item := &h.items[n]
	h.items = h.items[:n]
	return item
}

func (h *simpleMergingIterHeap) up(j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !h.less(j, i) {
			break
		}
		h.swap(i, j)
		j = i
	}
}

func (h *simpleMergingIterHeap) down(i0, n int) bool {
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
