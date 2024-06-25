// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"fmt"
	"runtime/debug"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/sstable"
)

type internalIterOpts struct {
	// if compaction is set, sstable-level iterators will be created using
	// NewCompactionIter; these iterators have a more constrained interface
	// and are optimized for the sequential scan of a compaction.
	compaction         bool
	bufferPool         *sstable.BufferPool
	stats              *base.InternalIteratorStats
	boundLimitedFilter sstable.BoundLimitedBlockPropertyFilter
}

// levelIter provides a merged view of the sstables in a level.
//
// levelIter is used during compaction and as part of the Iterator
// implementation. When used as part of the Iterator implementation, level
// iteration needs to "pause" at range deletion boundaries if file contains
// range deletions. In this case, the levelIter uses a keyspan.InterleavingIter
// to materialize InternalKVs at start and end boundaries of range deletions.
// This prevents mergingIter from advancing past the sstable until the sstable
// contains the smallest (or largest for reverse iteration) key in the merged
// heap. Note that mergingIter treats a range deletion tombstone returned by the
// point iterator as a no-op.
type levelIter struct {
	// The context is stored here since (a) iterators are expected to be
	// short-lived (since they pin sstables), (b) plumbing a context into every
	// method is very painful, (c) they do not (yet) respect context
	// cancellation and are only used for tracing.
	ctx      context.Context
	logger   Logger
	comparer *Comparer
	cmp      Compare
	split    Split
	// The lower/upper bounds for iteration as specified at creation or the most
	// recent call to SetBounds.
	lower []byte
	upper []byte
	// prefix holds the iteration prefix when the most recent absolute
	// positioning method was a SeekPrefixGE.
	prefix []byte
	// The iterator options for the currently open table. If
	// tableOpts.{Lower,Upper}Bound are nil, the corresponding iteration boundary
	// does not lie within the table bounds.
	tableOpts IterOptions
	// The LSM level this levelIter is initialized for.
	level manifest.Level
	// combinedIterState may be set when a levelIter is used during user
	// iteration. Although levelIter only iterates over point keys, it's also
	// responsible for lazily constructing the combined range & point iterator
	// when it observes a file containing range keys. If the combined iter
	// state's initialized field is true, the iterator is already using combined
	// iterator, OR the iterator is not configured to use combined iteration. If
	// it's false, the levelIter must set the `triggered` and `key` fields when
	// the levelIter passes over a file containing range keys. See the
	// lazyCombinedIter for more details.
	combinedIterState *combinedIterState
	// The iter for the current file. It is nil under any of the following conditions:
	// - files.Current() == nil
	// - err != nil
	// - some other constraint, like the bounds in opts, caused the file at index to not
	//   be relevant to the iteration.
	iter internalIterator
	// iterFile holds the current file. It is always equal to l.files.Current().
	iterFile *fileMetadata
	newIters tableNewIters
	files    manifest.LevelIterator
	err      error

	// When rangeDelIterFn != nil, the caller requires that this function gets
	// called with a range deletion iterator whenever the current file changes.
	// The iterator is relinquished to the caller which is responsible for closing
	// it.
	// When rangeDelIterFn != nil, the levelIter will also interleave the
	// boundaries of range deletions among point keys.
	rangeDelIterFn func(rangeDelIter keyspan.FragmentIterator)

	// interleaving is used when rangeDelIterFn != nil to interleave the
	// boundaries of range deletions among point keys. When the leve iterator is
	// used by a merging iterator, this ensures that we don't advance to a new
	// file until the range deletions are no longer needed by other levels.
	interleaving keyspan.InterleavingIter

	// internalOpts holds the internal iterator options to pass to the table
	// cache when constructing new table iterators.
	internalOpts internalIterOpts

	// Scratch space for the obsolete keys filter, when there are no other block
	// property filters specified. See the performance note where
	// IterOptions.PointKeyFilters is declared.
	filtersBuf [1]BlockPropertyFilter

	// exhaustedDir is set to +1 or -1 when the levelIter has been exhausted in
	// the forward or backward direction respectively. It is set when the
	// underlying data is exhausted or when iteration has reached the upper or
	// lower boundary and interleaved a synthetic iterator bound key. When the
	// iterator is exhausted and Next or Prev is called, the levelIter uses
	// exhaustedDir to determine whether the iterator should step on to the
	// first or last key within iteration bounds.
	exhaustedDir int8

	// Disable invariant checks even if they are otherwise enabled. Used by tests
	// which construct "impossible" situations (e.g. seeking to a key before the
	// lower bound).
	disableInvariants bool
}

// levelIter implements the base.InternalIterator interface.
var _ base.InternalIterator = (*levelIter)(nil)

// newLevelIter returns a levelIter. It is permissible to pass a nil split
// parameter if the caller is never going to call SeekPrefixGE.
func newLevelIter(
	ctx context.Context,
	opts IterOptions,
	comparer *Comparer,
	newIters tableNewIters,
	files manifest.LevelIterator,
	level manifest.Level,
	internalOpts internalIterOpts,
) *levelIter {
	l := &levelIter{}
	l.init(ctx, opts, comparer, newIters, files, level, internalOpts)
	return l
}

func (l *levelIter) init(
	ctx context.Context,
	opts IterOptions,
	comparer *Comparer,
	newIters tableNewIters,
	files manifest.LevelIterator,
	level manifest.Level,
	internalOpts internalIterOpts,
) {
	l.ctx = ctx
	l.err = nil
	l.level = level
	l.logger = opts.getLogger()
	l.prefix = nil
	l.lower = opts.LowerBound
	l.upper = opts.UpperBound
	l.tableOpts.TableFilter = opts.TableFilter
	l.tableOpts.PointKeyFilters = opts.PointKeyFilters
	if len(opts.PointKeyFilters) == 0 {
		l.tableOpts.PointKeyFilters = l.filtersBuf[:0:1]
	}
	l.tableOpts.UseL6Filters = opts.UseL6Filters
	l.tableOpts.CategoryAndQoS = opts.CategoryAndQoS
	l.tableOpts.level = l.level
	l.tableOpts.snapshotForHideObsoletePoints = opts.snapshotForHideObsoletePoints
	l.comparer = comparer
	l.cmp = comparer.Compare
	l.split = comparer.Split
	l.iterFile = nil
	l.newIters = newIters
	l.files = files
	l.exhaustedDir = 0
	l.internalOpts = internalOpts
}

// initRangeDel puts the level iterator into a mode where it interleaves range
// deletion boundaries with point keys and provides a range deletion iterator
// (through rangeDelIterFn) whenever the current file changes.
//
// The range deletion iterator passed to rangeDelIterFn is relinquished to the
// implementor who is responsible for closing it.
func (l *levelIter) initRangeDel(rangeDelIterFn func(rangeDelIter keyspan.FragmentIterator)) {
	l.rangeDelIterFn = rangeDelIterFn
}

func (l *levelIter) initCombinedIterState(state *combinedIterState) {
	l.combinedIterState = state
}

func (l *levelIter) maybeTriggerCombinedIteration(file *fileMetadata, dir int) {
	// If we encounter a file that contains range keys, we may need to
	// trigger a switch to combined range-key and point-key iteration,
	// if the *pebble.Iterator is configured for it. This switch is done
	// lazily because range keys are intended to be rare, and
	// constructing the range-key iterator substantially adds to the
	// cost of iterator construction and seeking.
	//
	// If l.combinedIterState.initialized is already true, either the
	// iterator is already using combined iteration or the iterator is not
	// configured to observe range keys. Either way, there's nothing to do.
	// If false, trigger the switch to combined iteration, using the the
	// file's bounds to seek the range-key iterator appropriately.
	//
	// We only need to trigger combined iteration if the file contains
	// RangeKeySets: if there are only Unsets and Dels, the user will observe no
	// range keys regardless. If this file has table stats available, they'll
	// tell us whether the file has any RangeKeySets. Otherwise, we must
	// fallback to assuming it does if HasRangeKeys=true.
	if file != nil && file.HasRangeKeys && l.combinedIterState != nil && !l.combinedIterState.initialized &&
		(l.upper == nil || l.cmp(file.SmallestRangeKey.UserKey, l.upper) < 0) &&
		(l.lower == nil || l.cmp(file.LargestRangeKey.UserKey, l.lower) > 0) &&
		(!file.StatsValid() || file.Stats.NumRangeKeySets > 0) {
		// The file contains range keys, and we're not using combined iteration yet.
		// Trigger a switch to combined iteration. It's possible that a switch has
		// already been triggered if multiple levels encounter files containing
		// range keys while executing a single mergingIter operation. In this case,
		// we need to compare the existing key recorded to l.combinedIterState.key,
		// adjusting it if our key is smaller (forward iteration) or larger
		// (backward iteration) than the existing key.
		//
		// These key comparisons are only required during a single high-level
		// iterator operation. When the high-level iter op completes,
		// iinitialized will be true, and future calls to this function will be
		// no-ops.
		switch dir {
		case +1:
			if !l.combinedIterState.triggered {
				l.combinedIterState.triggered = true
				l.combinedIterState.key = file.SmallestRangeKey.UserKey
			} else if l.cmp(l.combinedIterState.key, file.SmallestRangeKey.UserKey) > 0 {
				l.combinedIterState.key = file.SmallestRangeKey.UserKey
			}
		case -1:
			if !l.combinedIterState.triggered {
				l.combinedIterState.triggered = true
				l.combinedIterState.key = file.LargestRangeKey.UserKey
			} else if l.cmp(l.combinedIterState.key, file.LargestRangeKey.UserKey) < 0 {
				l.combinedIterState.key = file.LargestRangeKey.UserKey
			}
		}
	}
}

func (l *levelIter) findFileGE(key []byte, flags base.SeekGEFlags) *fileMetadata {
	// Find the earliest file whose largest key is >= key.

	// NB: if flags.TrySeekUsingNext()=true, the levelIter must respect it. If
	// the levelIter is positioned at the key P, it must return a key ≥ P. If
	// used within a merging iterator, the merging iterator will depend on the
	// levelIter only moving forward to maintain heap invariants.

	// Ordinarily we seek the LevelIterator using SeekGE. In some instances, we
	// Next instead. In other instances, we try Next-ing first, falling back to
	// seek:
	//   a) flags.TrySeekUsingNext(): The top-level Iterator knows we're seeking
	//      to a key later than the current iterator position. We don't know how
	//      much later the seek key is, so it's possible there are many sstables
	//      between the current position and the seek key. However in most real-
	//      world use cases, the seek key is likely to be nearby. Rather than
	//      performing a log(N) seek through the file metadata, we next a few
	//      times from our existing location. If we don't find a file whose
	//      largest is >= key within a few nexts, we fall back to seeking.
	//
	//      Note that in this case, the file returned by findFileGE may be
	//      different than the file returned by a raw binary search (eg, when
	//      TrySeekUsingNext=false). This is possible because the most recent
	//      positioning operation may have already determined that previous
	//      files' keys that are ≥ key are all deleted. This information is
	//      encoded within the iterator's current iterator position and is
	//      unavailable to a fresh binary search.
	//
	//   b) flags.RelativeSeek(): The merging iterator decided to re-seek this
	//      level according to a range tombstone. When lazy combined iteration
	//      is enabled, the level iterator is responsible for watching for
	//      files containing range keys and triggering the switch to combined
	//      iteration when such a file is observed. If a range deletion was
	//      observed in a higher level causing the merging iterator to seek the
	//      level to the range deletion's end key, we need to check whether all
	//      of the files between the old position and the new position contain
	//      any range keys.
	//
	//      In this scenario, we don't seek the LevelIterator and instead we
	//      Next it, one file at a time, checking each for range keys. The
	//      merging iterator sets this flag to inform us that we're moving
	//      forward relative to the existing position and that we must examine
	//      each intermediate sstable's metadata for lazy-combined iteration.
	//      In this case, we only Next and never Seek. We set nextsUntilSeek=-1
	//      to signal this intention.
	//
	// NB: At most one of flags.RelativeSeek() and flags.TrySeekUsingNext() may
	// be set, because the merging iterator re-seeks relative seeks with
	// explicitly only the RelativeSeek flag set.
	var nextsUntilSeek int
	var nextInsteadOfSeek bool
	if flags.TrySeekUsingNext() {
		nextInsteadOfSeek = true
		nextsUntilSeek = 4 // arbitrary
	}
	if flags.RelativeSeek() && l.combinedIterState != nil && !l.combinedIterState.initialized {
		nextInsteadOfSeek = true
		nextsUntilSeek = -1
	}

	var m *fileMetadata
	if nextInsteadOfSeek {
		m = l.iterFile
	} else {
		m = l.files.SeekGE(l.cmp, key)
	}
	// The below loop has a bit of an unusual organization. There are several
	// conditions under which we need to Next to a later file. If none of those
	// conditions are met, the file in `m` is okay to return. The loop body is
	// structured with a series of if statements, each of which may continue the
	// loop to the next file. If none of the statements are met, the end of the
	// loop body is a break.
	for m != nil {
		if m.HasRangeKeys {
			l.maybeTriggerCombinedIteration(m, +1)

			// Some files may only contain range keys, which we can skip.
			// NB: HasPointKeys=true if the file contains any points or range
			// deletions (which delete points).
			if !m.HasPointKeys {
				m = l.files.Next()
				continue
			}
		}

		// This file has point keys.
		//
		// However, there are a couple reasons why `m` may not be positioned ≥
		// `key` yet:
		//
		// 1. If SeekGE(key) landed on a file containing range keys, the file
		//    may contain range keys ≥ `key` but no point keys ≥ `key`.
		// 2. When nexting instead of seeking, we must check to see whether
		//    we've nexted sufficiently far, or we need to next again.
		//
		// If the file does not contain point keys ≥ `key`, next to continue
		// looking for a file that does.
		if (m.HasRangeKeys || nextInsteadOfSeek) && l.cmp(m.LargestPointKey.UserKey, key) < 0 {
			// If nextInsteadOfSeek is set and nextsUntilSeek is non-negative,
			// the iterator has been nexting hoping to discover the relevant
			// file without seeking. It's exhausted the allotted nextsUntilSeek
			// and should seek to the sought key.
			if nextInsteadOfSeek && nextsUntilSeek == 0 {
				nextInsteadOfSeek = false
				m = l.files.SeekGE(l.cmp, key)
				continue
			} else if nextsUntilSeek > 0 {
				nextsUntilSeek--
			}
			m = l.files.Next()
			continue
		}

		// This file has a point key bound ≥ `key`. But the largest point key
		// bound may still be a range deletion sentinel, which is exclusive.  In
		// this case, the file doesn't actually contain any point keys equal to
		// `key`. We next to keep searching for a file that actually contains
		// point keys ≥ key.
		//
		// Additionally, this prevents loading untruncated range deletions from
		// a table which can't possibly contain the target key and is required
		// for correctness by mergingIter.SeekGE (see the comment in that
		// function).
		if m.LargestPointKey.IsExclusiveSentinel() && l.cmp(m.LargestPointKey.UserKey, key) == 0 {
			m = l.files.Next()
			continue
		}

		// This file contains point keys ≥ `key`. Break and return it.
		break
	}
	return m
}

func (l *levelIter) findFileLT(key []byte, flags base.SeekLTFlags) *fileMetadata {
	// Find the last file whose smallest key is < ikey.

	// Ordinarily we seek the LevelIterator using SeekLT.
	//
	// When lazy combined iteration is enabled, there's a complication. The
	// level iterator is responsible for watching for files containing range
	// keys and triggering the switch to combined iteration when such a file is
	// observed. If a range deletion was observed in a higher level causing the
	// merging iterator to seek the level to the range deletion's start key, we
	// need to check whether all of the files between the old position and the
	// new position contain any range keys.
	//
	// In this scenario, we don't seek the LevelIterator and instead we Prev it,
	// one file at a time, checking each for range keys.
	prevInsteadOfSeek := flags.RelativeSeek() && l.combinedIterState != nil && !l.combinedIterState.initialized

	var m *fileMetadata
	if prevInsteadOfSeek {
		m = l.iterFile
	} else {
		m = l.files.SeekLT(l.cmp, key)
	}
	// The below loop has a bit of an unusual organization. There are several
	// conditions under which we need to Prev to a previous file. If none of
	// those conditions are met, the file in `m` is okay to return. The loop
	// body is structured with a series of if statements, each of which may
	// continue the loop to the previous file. If none of the statements are
	// met, the end of the loop body is a break.
	for m != nil {
		if m.HasRangeKeys {
			l.maybeTriggerCombinedIteration(m, -1)

			// Some files may only contain range keys, which we can skip.
			// NB: HasPointKeys=true if the file contains any points or range
			// deletions (which delete points).
			if !m.HasPointKeys {
				m = l.files.Prev()
				continue
			}
		}

		// This file has point keys.
		//
		// However, there are a couple reasons why `m` may not be positioned <
		// `key` yet:
		//
		// 1. If SeekLT(key) landed on a file containing range keys, the file
		//    may contain range keys < `key` but no point keys < `key`.
		// 2. When preving instead of seeking, we must check to see whether
		//    we've preved sufficiently far, or we need to prev again.
		//
		// If the file does not contain point keys < `key`, prev to continue
		// looking for a file that does.
		if (m.HasRangeKeys || prevInsteadOfSeek) && l.cmp(m.SmallestPointKey.UserKey, key) >= 0 {
			m = l.files.Prev()
			continue
		}

		// This file contains point keys < `key`. Break and return it.
		break
	}
	return m
}

// Init the iteration bounds for the current table. Returns -1 if the table
// lies fully before the lower bound, +1 if the table lies fully after the
// upper bound, and 0 if the table overlaps the iteration bounds.
func (l *levelIter) initTableBounds(f *fileMetadata) int {
	l.tableOpts.LowerBound = l.lower
	if l.tableOpts.LowerBound != nil {
		if l.cmp(f.LargestPointKey.UserKey, l.tableOpts.LowerBound) < 0 {
			// The largest key in the sstable is smaller than the lower bound.
			return -1
		}
		if l.cmp(l.tableOpts.LowerBound, f.SmallestPointKey.UserKey) <= 0 {
			// The lower bound is smaller or equal to the smallest key in the
			// table. Iteration within the table does not need to check the lower
			// bound.
			l.tableOpts.LowerBound = nil
		}
	}
	l.tableOpts.UpperBound = l.upper
	if l.tableOpts.UpperBound != nil {
		if l.cmp(f.SmallestPointKey.UserKey, l.tableOpts.UpperBound) >= 0 {
			// The smallest key in the sstable is greater than or equal to the upper
			// bound.
			return 1
		}
		if l.cmp(l.tableOpts.UpperBound, f.LargestPointKey.UserKey) > 0 {
			// The upper bound is greater than the largest key in the
			// table. Iteration within the table does not need to check the upper
			// bound. NB: tableOpts.UpperBound is exclusive and f.LargestPointKey is
			// inclusive.
			l.tableOpts.UpperBound = nil
		}
	}
	return 0
}

type loadFileReturnIndicator int8

const (
	noFileLoaded loadFileReturnIndicator = iota
	fileAlreadyLoaded
	newFileLoaded
)

func (l *levelIter) loadFile(file *fileMetadata, dir int) loadFileReturnIndicator {
	if l.iterFile == file {
		if l.err != nil {
			return noFileLoaded
		}
		if l.iter != nil {
			// We don't bother comparing the file bounds with the iteration bounds when we have
			// an already open iterator. It is possible that the iter may not be relevant given the
			// current iteration bounds, but it knows those bounds, so it will enforce them.

			// There are a few reasons we might not have triggered combined
			// iteration yet, even though we already had `file` open.
			// 1. If the bounds changed, we might have previously avoided
			//    switching to combined iteration because the bounds excluded
			//    the range keys contained in this file.
			// 2. If an existing iterator was reconfigured to iterate over range
			//    keys (eg, using SetOptions), then we wouldn't have triggered
			//    the switch to combined iteration yet.
			l.maybeTriggerCombinedIteration(file, dir)
			return fileAlreadyLoaded
		}
		// We were already at file, but don't have an iterator, probably because the file was
		// beyond the iteration bounds. It may still be, but it is also possible that the bounds
		// have changed. We handle that below.
	}

	// Close iter and send a nil iterator through rangeDelIterFn.rangeDelIterFn.
	if err := l.Close(); err != nil {
		return noFileLoaded
	}

	for {
		l.iterFile = file
		if file == nil {
			return noFileLoaded
		}

		l.maybeTriggerCombinedIteration(file, dir)
		if !file.HasPointKeys {
			switch dir {
			case +1:
				file = l.files.Next()
				continue
			case -1:
				file = l.files.Prev()
				continue
			}
		}

		switch l.initTableBounds(file) {
		case -1:
			// The largest key in the sstable is smaller than the lower bound.
			if dir < 0 {
				return noFileLoaded
			}
			file = l.files.Next()
			continue
		case +1:
			// The smallest key in the sstable is greater than or equal to the upper
			// bound.
			if dir > 0 {
				return noFileLoaded
			}
			file = l.files.Prev()
			continue
		}
		// If we're in prefix iteration, it's possible this file's smallest
		// boundary is large enough to prove the file cannot possibly contain
		// any keys within the iteration prefix. Loading the next file is
		// unnecessary. This has been observed in practice on slow shared
		// storage. See #3575.
		if l.prefix != nil && l.cmp(l.split.Prefix(file.SmallestPointKey.UserKey), l.prefix) > 0 {
			// Note that because l.iter is nil, a subsequent call to
			// SeekPrefixGE with TrySeekUsingNext()=true will load the file
			// (returning newFileLoaded) and disable TrySeekUsingNext before
			// performing a seek in the file.
			return noFileLoaded
		}

		iterKinds := iterPointKeys
		if l.rangeDelIterFn != nil {
			iterKinds |= iterRangeDeletions
		}

		var iters iterSet
		iters, l.err = l.newIters(l.ctx, l.iterFile, &l.tableOpts, l.internalOpts, iterKinds)
		if l.err != nil {
			if l.rangeDelIterFn != nil {
				l.rangeDelIterFn(nil)
			}
			return noFileLoaded
		}
		l.iter = iters.Point()
		if l.rangeDelIterFn != nil && iters.rangeDeletion != nil {
			// If this file has range deletions, interleave the bounds of the
			// range deletions among the point keys. When used with a
			// mergingIter, this ensures we don't move beyond a file with range
			// deletions until its range deletions are no longer relevant.
			//
			// For now, we open a second range deletion iterator. Future work
			// will avoid the need to open a second range deletion iterator, and
			// avoid surfacing the file's range deletion iterator via rangeDelIterFn.
			itersForBounds, err := l.newIters(l.ctx, l.iterFile, &l.tableOpts, l.internalOpts, iterRangeDeletions)
			if err != nil {
				l.iter = nil
				l.err = errors.CombineErrors(err, iters.CloseAll())
				return noFileLoaded
			}
			l.interleaving.Init(l.comparer, l.iter, itersForBounds.RangeDeletion(), keyspan.InterleavingIterOpts{
				LowerBound:        l.tableOpts.LowerBound,
				UpperBound:        l.tableOpts.UpperBound,
				InterleaveEndKeys: true,
			})
			l.iter = &l.interleaving

			// Relinquish iters.rangeDeletion to the caller.
			l.rangeDelIterFn(iters.rangeDeletion)
		}
		return newFileLoaded
	}
}

// In race builds we verify that the keys returned by levelIter lie within
// [lower,upper).
func (l *levelIter) verify(kv *base.InternalKV) *base.InternalKV {
	// Note that invariants.Enabled is a compile time constant, which means the
	// block of code will be compiled out of normal builds making this method
	// eligible for inlining. Do not change this to use a variable.
	if invariants.Enabled && !l.disableInvariants && kv != nil {
		// We allow returning a boundary key that is outside of the lower/upper
		// bounds as such keys are always range tombstones which will be skipped
		// by the Iterator.
		if l.lower != nil && kv != nil && !kv.K.IsExclusiveSentinel() && l.cmp(kv.K.UserKey, l.lower) < 0 {
			l.logger.Fatalf("levelIter %s: lower bound violation: %s < %s\n%s", l.level, kv, l.lower, debug.Stack())
		}
		if l.upper != nil && kv != nil && !kv.K.IsExclusiveSentinel() && l.cmp(kv.K.UserKey, l.upper) > 0 {
			l.logger.Fatalf("levelIter %s: upper bound violation: %s > %s\n%s", l.level, kv, l.upper, debug.Stack())
		}
	}
	return kv
}

func (l *levelIter) SeekGE(key []byte, flags base.SeekGEFlags) *base.InternalKV {
	if invariants.Enabled && l.lower != nil && l.cmp(key, l.lower) < 0 {
		panic(errors.AssertionFailedf("levelIter SeekGE to key %q violates lower bound %q", key, l.lower))
	}

	l.err = nil // clear cached iteration error
	l.exhaustedDir = 0
	l.prefix = nil
	// NB: the top-level Iterator has already adjusted key based on
	// IterOptions.LowerBound.
	loadFileIndicator := l.loadFile(l.findFileGE(key, flags), +1)
	if loadFileIndicator == noFileLoaded {
		l.exhaustedForward()
		return nil
	}
	if loadFileIndicator == newFileLoaded {
		// File changed, so l.iter has changed, and that iterator is not
		// positioned appropriately.
		flags = flags.DisableTrySeekUsingNext()
	}
	if kv := l.iter.SeekGE(key, flags); kv != nil {
		return l.verify(kv)
	}
	return l.verify(l.skipEmptyFileForward())
}

func (l *levelIter) SeekPrefixGE(prefix, key []byte, flags base.SeekGEFlags) *base.InternalKV {
	if invariants.Enabled && l.lower != nil && l.cmp(key, l.lower) < 0 {
		panic(errors.AssertionFailedf("levelIter SeekGE to key %q violates lower bound %q", key, l.lower))
	}
	l.err = nil // clear cached iteration error
	l.exhaustedDir = 0
	l.prefix = prefix

	// NB: the top-level Iterator has already adjusted key based on
	// IterOptions.LowerBound.
	loadFileIndicator := l.loadFile(l.findFileGE(key, flags), +1)
	if loadFileIndicator == noFileLoaded {
		l.exhaustedForward()
		return nil
	}
	if loadFileIndicator == newFileLoaded {
		// File changed, so l.iter has changed, and that iterator is not
		// positioned appropriately.
		flags = flags.DisableTrySeekUsingNext()
	}
	if kv := l.iter.SeekPrefixGE(prefix, key, flags); kv != nil {
		return l.verify(kv)
	}
	if err := l.iter.Error(); err != nil {
		return nil
	}
	return l.verify(l.skipEmptyFileForward())
}

func (l *levelIter) SeekLT(key []byte, flags base.SeekLTFlags) *base.InternalKV {
	if invariants.Enabled && l.upper != nil && l.cmp(key, l.upper) > 0 {
		panic(errors.AssertionFailedf("levelIter SeekLT to key %q violates upper bound %q", key, l.upper))
	}

	l.err = nil // clear cached iteration error
	l.exhaustedDir = 0
	l.prefix = nil

	// NB: the top-level Iterator has already adjusted key based on
	// IterOptions.UpperBound.
	if l.loadFile(l.findFileLT(key, flags), -1) == noFileLoaded {
		l.exhaustedBackward()
		return nil
	}
	if kv := l.iter.SeekLT(key, flags); kv != nil {
		return l.verify(kv)
	}
	return l.verify(l.skipEmptyFileBackward())
}

func (l *levelIter) First() *base.InternalKV {
	if invariants.Enabled && l.lower != nil {
		panic(errors.AssertionFailedf("levelIter First called while lower bound %q is set", l.lower))
	}

	l.err = nil // clear cached iteration error
	l.exhaustedDir = 0
	l.prefix = nil

	// NB: the top-level Iterator will call SeekGE if IterOptions.LowerBound is
	// set.
	if l.loadFile(l.files.First(), +1) == noFileLoaded {
		l.exhaustedForward()
		return nil
	}
	if kv := l.iter.First(); kv != nil {
		return l.verify(kv)
	}
	return l.verify(l.skipEmptyFileForward())
}

func (l *levelIter) Last() *base.InternalKV {
	if invariants.Enabled && l.upper != nil {
		panic(errors.AssertionFailedf("levelIter Last called while upper bound %q is set", l.upper))
	}

	l.err = nil // clear cached iteration error
	l.exhaustedDir = 0
	l.prefix = nil

	// NB: the top-level Iterator will call SeekLT if IterOptions.UpperBound is
	// set.
	if l.loadFile(l.files.Last(), -1) == noFileLoaded {
		l.exhaustedBackward()
		return nil
	}
	if kv := l.iter.Last(); kv != nil {
		return l.verify(kv)
	}
	return l.verify(l.skipEmptyFileBackward())
}

func (l *levelIter) Next() *base.InternalKV {
	if l.exhaustedDir == -1 {
		if l.lower != nil {
			return l.SeekGE(l.lower, base.SeekGEFlagsNone)
		}
		return l.First()
	}
	if l.err != nil || l.iter == nil {
		return nil
	}
	if kv := l.iter.Next(); kv != nil {
		return l.verify(kv)
	}
	return l.verify(l.skipEmptyFileForward())
}

func (l *levelIter) NextPrefix(succKey []byte) *base.InternalKV {
	if l.err != nil || l.iter == nil {
		return nil
	}

	if kv := l.iter.NextPrefix(succKey); kv != nil {
		return l.verify(kv)
	}
	if l.iter.Error() != nil {
		return nil
	}
	if l.tableOpts.UpperBound != nil {
		// The UpperBound was within this file, so don't load the next file.
		l.exhaustedForward()
		return nil
	}

	// Seek the manifest level iterator using TrySeekUsingNext=true and
	// RelativeSeek=true so that we take advantage of the knowledge that
	// `succKey` can only be contained in later files.
	metadataSeekFlags := base.SeekGEFlagsNone.EnableTrySeekUsingNext().EnableRelativeSeek()
	if l.loadFile(l.findFileGE(succKey, metadataSeekFlags), +1) != noFileLoaded {
		// NB: The SeekGE on the file's iterator must not set TrySeekUsingNext,
		// because l.iter is unpositioned.
		if kv := l.iter.SeekGE(succKey, base.SeekGEFlagsNone); kv != nil {
			return l.verify(kv)
		}
		return l.verify(l.skipEmptyFileForward())
	}
	l.exhaustedForward()
	return nil
}

func (l *levelIter) Prev() *base.InternalKV {
	if l.exhaustedDir == +1 {
		if l.upper != nil {
			return l.SeekLT(l.upper, base.SeekLTFlagsNone)
		}
		return l.Last()
	}
	if l.err != nil || l.iter == nil {
		return nil
	}
	if kv := l.iter.Prev(); kv != nil {
		return l.verify(kv)
	}
	return l.verify(l.skipEmptyFileBackward())
}

func (l *levelIter) skipEmptyFileForward() *base.InternalKV {
	var kv *base.InternalKV
	// The first iteration of this loop starts with an already exhausted l.iter.
	// The reason for the exhaustion is either that we iterated to the end of
	// the sstable, or our iteration was terminated early due to the presence of
	// an upper-bound or the use of SeekPrefixGE.
	//
	// Subsequent iterations will examine consecutive files such that the first
	// file that does not have an exhausted iterator causes the code to return
	// that key.
	for ; kv == nil; kv = l.iter.First() {
		if l.iter.Error() != nil {
			return nil
		}
		// If an upper bound is present and the upper bound lies within the
		// current sstable, then we will have reached the upper bound rather
		// than the end of the sstable.
		if l.tableOpts.UpperBound != nil {
			l.exhaustedForward()
			return nil
		}

		// If the iterator is in prefix iteration mode, it's possible that we
		// are here because bloom filter matching failed. In that case it is
		// likely that all keys matching the prefix are wholly within the
		// current file and cannot be in a subsequent file. In that case we
		// don't want to go to the next file, since loading and seeking in there
		// has some cost.
		//
		// This is not just an optimization. We must not advance to the next
		// file if the current file might possibly contain keys relevant to any
		// prefix greater than our current iteration prefix. If we did, a
		// subsequent SeekPrefixGE with TrySeekUsingNext could mistakenly skip
		// the file's relevant keys.
		if l.prefix != nil {
			if l.cmp(l.split.Prefix(l.iterFile.LargestPointKey.UserKey), l.prefix) > 0 {
				l.exhaustedForward()
				return nil
			}
		}

		// Current file was exhausted. Move to the next file.
		if l.loadFile(l.files.Next(), +1) == noFileLoaded {
			l.exhaustedForward()
			return nil
		}
	}
	return kv
}

func (l *levelIter) skipEmptyFileBackward() *base.InternalKV {
	var kv *base.InternalKV
	// The first iteration of this loop starts with an already exhausted
	// l.iter. The reason for the exhaustion is either that we iterated to the
	// end of the sstable, or our iteration was terminated early due to the
	// presence of a lower-bound.
	//
	// Subsequent iterations will examine consecutive files such that the first
	// file that does not have an exhausted iterator causes the code to return
	// that key.
	for ; kv == nil; kv = l.iter.Last() {
		if l.iter.Error() != nil {
			return nil
		}
		// If a lower bound is present and the lower bound lies within the
		// current sstable, then we will have reached the lowerr bound rather
		// than the end of the sstable.
		if l.tableOpts.LowerBound != nil {
			l.exhaustedBackward()
			return nil
		}
		// Current file was exhausted. Move to the previous file.
		if l.loadFile(l.files.Prev(), -1) == noFileLoaded {
			l.exhaustedBackward()
			return nil
		}
	}
	return kv
}

func (l *levelIter) exhaustedForward() {
	l.exhaustedDir = +1
}

func (l *levelIter) exhaustedBackward() {
	l.exhaustedDir = -1
}

func (l *levelIter) Error() error {
	if l.err != nil || l.iter == nil {
		return l.err
	}
	return l.iter.Error()
}

func (l *levelIter) Close() error {
	if l.iter != nil {
		l.err = l.iter.Close()
		l.iter = nil
	}
	if l.rangeDelIterFn != nil {
		l.rangeDelIterFn(nil)
	}
	return l.err
}

func (l *levelIter) SetBounds(lower, upper []byte) {
	l.lower = lower
	l.upper = upper

	if l.iter == nil {
		return
	}

	// Update tableOpts.{Lower,Upper}Bound in case the new boundaries fall within
	// the boundaries of the current table.
	if l.initTableBounds(l.iterFile) != 0 {
		// The table does not overlap the bounds. Close() will set levelIter.err if
		// an error occurs.
		_ = l.Close()
		return
	}

	l.iter.SetBounds(l.tableOpts.LowerBound, l.tableOpts.UpperBound)
}

func (l *levelIter) SetContext(ctx context.Context) {
	l.ctx = ctx
	if l.iter != nil {
		// TODO(sumeer): this is losing the ctx = objiotracing.WithLevel(ctx,
		// manifest.LevelToInt(opts.level)) that happens in table_cache.go.
		l.iter.SetContext(ctx)
	}
}

func (l *levelIter) String() string {
	if l.iterFile != nil {
		return fmt.Sprintf("%s: fileNum=%s", l.level, l.iterFile.FileNum.String())
	}
	return fmt.Sprintf("%s: fileNum=<nil>", l.level)
}

var _ internalIterator = &levelIter{}
