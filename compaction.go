// Copyright 2013 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"runtime/pprof"
	"sort"
	"sync/atomic"
	"unsafe"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/private"
	"github.com/cockroachdb/pebble/internal/rangedel"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

var errEmptyTable = errors.New("pebble: empty table")
var errFlushInvariant = errors.New("pebble: flush next log number is unset")

var compactLabels = pprof.Labels("pebble", "compact")
var flushLabels = pprof.Labels("pebble", "flush")
var gcLabels = pprof.Labels("pebble", "gc")

// expandedCompactionByteSizeLimit is the maximum number of bytes in all
// compacted files. We avoid expanding the lower level file set of a compaction
// if it would make the total compaction cover more than this many bytes.
func expandedCompactionByteSizeLimit(opts *Options, level int) uint64 {
	return uint64(25 * opts.Level(level).TargetFileSize)
}

// maxGrandparentOverlapBytes is the maximum bytes of overlap with level+2
// before we stop building a single file in a level to level+1 compaction.
func maxGrandparentOverlapBytes(opts *Options, level int) uint64 {
	return uint64(10 * opts.Level(level).TargetFileSize)
}

// totalSize returns the total size of all the files in f.
func totalSize(f []fileMetadata) (size uint64) {
	for _, x := range f {
		size += x.Size
	}
	return size
}

type userKeyRange struct {
	start, end []byte
}

// compaction is a table compaction from one level to the next, starting from a
// given version.
type compaction struct {
	cmp     Compare
	format  base.Formatter
	logger  Logger
	version *version

	// startLevel is the level that is being compacted. Inputs from startLevel
	// and outputLevel will be merged to produce a set of outputLevel files.
	startLevel int
	// outputLevel is the level that files are being produced in. outputLevel is
	// equal to startLevel+1 except when startLevel is 0 in which case it is
	// equal to compactionPicker.baseLevel().
	outputLevel int

	// maxOutputFileSize is the maximum size of an individual table created
	// during compaction.
	maxOutputFileSize uint64
	// maxOverlapBytes is the maximum number of bytes of overlap allowed for a
	// single output table with the tables in the grandparent level.
	maxOverlapBytes uint64
	// maxExpandedBytes is the maximum size of an expanded compaction. If growing
	// a compaction results in a larger size, the original compaction is used
	// instead.
	maxExpandedBytes uint64
	// disableRangeTombstoneElision disables elision of range tombstones. Used by
	// tests to allow range tombstones to be added to tables where they would
	// otherwise be elided.
	disableRangeTombstoneElision bool

	// flushing contains the flushables (aka memtables) that are being flushed.
	flushing flushableList
	// bytesIterated contains the number of bytes that have been flushed/compacted.
	bytesIterated uint64
	// atomicBytesIterated points to the variable to increment during iteration.
	// atomicBytesIterated must be read/written atomically. Flushing will increment
	// the shared variable which compaction will read. This allows for the
	// compaction routine to know how many bytes have been flushed before the flush
	// is applied.
	atomicBytesIterated *uint64
	// inputs are the tables to be compacted.
	inputs [2][]fileMetadata
	// The boundaries of the input data.
	smallest InternalKey
	largest  InternalKey

	// The range deletion tombstone fragmenter. Adds range tombstones as they are
	// returned from `compactionIter` and fragments them for output to files.
	// Referenced by `compactionIter` which uses it to check whether keys are deleted.
	rangeDelFrag rangedel.Fragmenter

	// grandparents are the tables in level+2 that overlap with the files being
	// compacted. Used to determine output table boundaries. Do not assume that the actual files
	// in the grandparent when this compaction finishes will be the same.
	grandparents []fileMetadata

	// List of disjoint inuse key ranges the compaction overlaps with in
	// grandparent and lower levels. See setupInuseKeyRanges() for the
	// construction. Used by elideTombstone() and elideRangeTombstone() to
	// determine if keys affected by a tombstone possibly exist at a lower level.
	inuseKeyRanges      []userKeyRange
	elideTombstoneIndex int

	metrics map[int]*LevelMetrics
}

func newCompaction(
	opts *Options, cur *version, startLevel,
	baseLevel int, bytesCompacted *uint64,
) *compaction {
	if startLevel > 0 && startLevel < baseLevel {
		panic(fmt.Sprintf("invalid compaction: start level %d should be empty (base level %d)",
			startLevel, baseLevel))
	}

	outputLevel := startLevel + 1
	if startLevel == 0 {
		outputLevel = baseLevel
	}
	if outputLevel >= numLevels-1 {
		outputLevel = numLevels - 1
	}
	// Output level is in the range [baseLevel,numLevels]. For the purpose of
	// determining the target output file size, overlap bytes, and expanded
	// bytes, we want to adjust the range to [1,numLevels].
	adjustedOutputLevel := 1 + outputLevel - baseLevel

	return &compaction{
		cmp:                 opts.Comparer.Compare,
		format:              opts.Comparer.Format,
		logger:              opts.Logger,
		version:             cur,
		startLevel:          startLevel,
		outputLevel:         outputLevel,
		maxOutputFileSize:   uint64(opts.Level(adjustedOutputLevel).TargetFileSize),
		maxOverlapBytes:     maxGrandparentOverlapBytes(opts, adjustedOutputLevel),
		maxExpandedBytes:    expandedCompactionByteSizeLimit(opts, adjustedOutputLevel),
		atomicBytesIterated: bytesCompacted,
	}
}

func newFlush(
	opts *Options, cur *version, baseLevel int, flushing flushableList, bytesFlushed *uint64,
) *compaction {
	c := &compaction{
		cmp:                 opts.Comparer.Compare,
		format:              opts.Comparer.Format,
		logger:              opts.Logger,
		version:             cur,
		startLevel:          -1,
		outputLevel:         0,
		maxOutputFileSize:   math.MaxUint64,
		maxOverlapBytes:     math.MaxUint64,
		maxExpandedBytes:    math.MaxUint64,
		flushing:            flushing,
		atomicBytesIterated: bytesFlushed,
	}

	smallestSet, largestSet := false, false
	updatePointBounds := func(iter internalIterator) {
		if key, _ := iter.First(); key != nil {
			if !smallestSet ||
				base.InternalCompare(c.cmp, c.smallest, *key) > 0 {
				smallestSet = true
				c.smallest = key.Clone()
			}
		}
		if key, _ := iter.Last(); key != nil {
			if !largestSet ||
				base.InternalCompare(c.cmp, c.largest, *key) < 0 {
				largestSet = true
				c.largest = key.Clone()
			}
		}
	}

	updateRangeBounds := func(iter internalIterator) {
		if key, _ := iter.First(); key != nil {
			if !smallestSet ||
				base.InternalCompare(c.cmp, c.smallest, *key) > 0 {
				smallestSet = true
				c.smallest = key.Clone()
			}
		}
		if key, value := iter.Last(); key != nil {
			tmp := base.InternalKey{
				UserKey: value,
				Trailer: key.Trailer,
			}
			if !largestSet ||
				base.InternalCompare(c.cmp, c.largest, tmp) < 0 {
				largestSet = true
				c.largest = tmp.Clone()
			}
		}
	}

	for i := range flushing {
		f := flushing[i]
		updatePointBounds(f.newIter(nil))
		if rangeDelIter := f.newRangeDelIter(nil); rangeDelIter != nil {
			updateRangeBounds(rangeDelIter)
		}
	}

	// TODO(peter): When we allow flushing to create multiple tables we'll want
	// to choose sstable boundaries based on the grandparents. But for now we
	// want to create a single table during flushing so this is all commented
	// out.
	if false {
		c.maxOutputFileSize = uint64(opts.Level(0).TargetFileSize)
		c.maxOverlapBytes = maxGrandparentOverlapBytes(opts, 0)
		c.maxExpandedBytes = expandedCompactionByteSizeLimit(opts, 0)
		c.grandparents = c.version.Overlaps(baseLevel, c.cmp, c.smallest.UserKey, c.largest.UserKey)
	}

	c.setupInuseKeyRanges()
	return c
}

var _ compactionInfo = &compaction{}

func (c *compaction) startLevelNum() int       { return c.startLevel }
func (c *compaction) outputLevelNum() int      { return c.outputLevel }
func (c *compaction) smallestKey() InternalKey { return c.smallest }
func (c *compaction) largestKey() InternalKey  { return c.largest }

// setupInputs fills in the rest of the compaction inputs, regardless of
// whether the compaction was automatically scheduled or user initiated.
func (c *compaction) setupInputs() {
	// Expand the initial inputs to a clean cut.
	c.inputs[0] = c.expandInputs(c.startLevel, c.inputs[0])
	c.smallest, c.largest = manifest.KeyRange(c.cmp, c.inputs[0], nil)

	// Determine the sstables in the output level which overlap with the input
	// sstables, and then expand those tables to a clean cut.
	c.inputs[1] = c.version.Overlaps(c.outputLevel, c.cmp, c.smallest.UserKey, c.largest.UserKey)
	c.inputs[1] = c.expandInputs(c.outputLevel, c.inputs[1])
	c.smallest, c.largest = manifest.KeyRange(c.cmp, c.inputs[0], c.inputs[1])

	// Grow the sstables in c.startLevel as long as it doesn't affect the number
	// of sstables included from c.outputLevel.
	if c.grow(c.smallest, c.largest) {
		c.smallest, c.largest = manifest.KeyRange(c.cmp, c.inputs[0], c.inputs[1])
	}

	// Compute the set of outputLevel+1 files that overlap this compaction (these
	// are the grandparent sstables).
	if c.outputLevel+1 < numLevels {
		c.grandparents = c.version.Overlaps(c.outputLevel+1, c.cmp, c.smallest.UserKey, c.largest.UserKey)
	}

	c.setupInuseKeyRanges()
}

// expandInputs expands the files in inputs in order to maintain the invariant
// that the versions of keys at level+1 are older than the versions of keys at
// level. This is achieved by adding tables to the right of the current input
// tables such that the rightmost table has a "clean cut". A clean cut is
// either a change in user keys, or when the largest key in the left sstable is
// a range tombstone sentinel key (InternalKeyRangeDeleteSentinel).
//
// In addition to maintaining the seqnum invariant, expandInputs is used to
// provide clean boundaries for range tombstone truncation during
// compaction. In order to achieve these clean boundaries, expandInputs needs
// to find a "clean cut" on the left edge of the compaction as well. This is
// necessary in order for "atomic compaction units" to always be compacted as a
// unit. Failure to do this leads to a subtle bug with truncation of range
// tombstones to atomic compaction unit boundaries. Consider the scenario:
//
//   L3:
//     12:[a#2,15-b#1,1]
//     13:[b#0,15-d#72057594037927935,15]
//
// These sstables contain a range tombstone [a-d)#2 which spans the two
// sstables. The two sstables need to always be kept together. Compacting
// sstable 13 independently of sstable 12 would result in:
//
//   L3:
//     12:[a#2,15-b#1,1]
//   L4:
//     14:[b#0,15-d#72057594037927935,15]
//
// This state is still ok, but when sstable 12 is next compacted, its range
// tombstones will be truncated at "b" (the largest key in its atomic
// compaction unit). In the scenario here, that could result in b#1 becoming
// visible when it should be deleted.
func (c *compaction) expandInputs(level int, inputs []fileMetadata) []fileMetadata {
	if level == 0 {
		// We already called version.Overlaps for L0 and that call guarantees that
		// we get a "clean cut".
		return inputs
	}
	if len(inputs) == 0 {
		// Nothing to expand.
		return inputs
	}
	files := c.version.Files[level]
	// Pointer arithmetic to figure out the index if inputs[0] with
	// files[0]. This requires that the inputs slice is a sub-slice of
	// files. This is true for non-L0 files returned from version.overlaps.
	if uintptr(unsafe.Pointer(&inputs[0])) < uintptr(unsafe.Pointer(&files[0])) {
		panic("pebble: invalid input slice")
	}
	start := int((uintptr(unsafe.Pointer(&inputs[0])) -
		uintptr(unsafe.Pointer(&files[0]))) / unsafe.Sizeof(inputs[0]))
	if start >= len(files) {
		panic("pebble: invalid input slice")
	}
	end := start + len(inputs)

	for ; start > 0; start-- {
		cur := &files[start]
		prev := &files[start-1]
		if c.cmp(prev.Largest.UserKey, cur.Smallest.UserKey) < 0 {
			break
		}
		if prev.Largest.Trailer == InternalKeyRangeDeleteSentinel {
			// The range deletion sentinel key is set for the largest key in a
			// table when a range deletion tombstone straddles a table. It
			// isn't necessary to include the prev table in the atomic
			// compaction unit as prev.largest.UserKey does not actually exist
			// in the prev table.
			break
		}
		// prev.Largest.UserKey == cur.Smallest.UserKey, so we need to include prev
		// in the compaction.
	}

	for ; end < len(files); end++ {
		cur := &files[end-1]
		next := &files[end]
		if c.cmp(cur.Largest.UserKey, next.Smallest.UserKey) < 0 {
			break
		}
		if cur.Largest.Trailer == InternalKeyRangeDeleteSentinel {
			// The range deletion sentinel key is set for the largest key in a table
			// when a range deletion tombstone straddles a table. It isn't necessary
			// to include the next table in the compaction as cur.largest.UserKey
			// does not actually exist in the table.
			break
		}
		// cur.Largest.UserKey == next.Smallest.UserKey, so we need to include next
		// in the compaction.
	}
	return files[start:end]
}

// grow grows the number of inputs at c.level without changing the number of
// c.level+1 files in the compaction, and returns whether the inputs grew. sm
// and la are the smallest and largest InternalKeys in all of the inputs.
func (c *compaction) grow(sm, la InternalKey) bool {
	if len(c.inputs[1]) == 0 {
		return false
	}
	grow0 := c.version.Overlaps(c.startLevel, c.cmp, sm.UserKey, la.UserKey)
	grow0 = c.expandInputs(c.startLevel, grow0)
	if len(grow0) <= len(c.inputs[0]) {
		return false
	}
	if totalSize(grow0)+totalSize(c.inputs[1]) >= c.maxExpandedBytes {
		return false
	}
	sm1, la1 := manifest.KeyRange(c.cmp, grow0, nil)
	grow1 := c.version.Overlaps(c.outputLevel, c.cmp, sm1.UserKey, la1.UserKey)
	grow1 = c.expandInputs(c.outputLevel, grow1)
	if len(grow1) != len(c.inputs[1]) {
		return false
	}
	c.inputs[0] = grow0
	c.inputs[1] = grow1
	return true
}

func (c *compaction) setupInuseKeyRanges() {
	level := c.outputLevel + 1
	if c.outputLevel == 0 {
		// Level 0 can contain overlapping sstables so we need to check it for
		// overlaps.
		level = 0
	}

	// Gather up the raw list of key ranges from overlapping tables in lower
	// levels.
	var input []userKeyRange
	for ; level < numLevels; level++ {
		overlaps := c.version.Overlaps(level, c.cmp, c.smallest.UserKey, c.largest.UserKey)
		for i := range overlaps {
			m := &overlaps[i]
			input = append(input, userKeyRange{m.Smallest.UserKey, m.Largest.UserKey})
		}
	}

	if len(input) == 0 {
		// Nothing more to do.
		return
	}

	// Sort the raw list of key ranges by start key.
	sort.Slice(input, func(i, j int) bool {
		return c.cmp(input[i].start, input[j].start) < 0
	})

	// Take the first input as the first output. This key range is guaranteed to
	// have the smallest start key (or share the smallest start key) with another
	// range. Loop over the remaining input key ranges and either add a new
	// output, or merge with the last output.
	c.inuseKeyRanges = input[:1]
	for _, v := range input[1:] {
		last := &c.inuseKeyRanges[len(c.inuseKeyRanges)-1]
		switch {
		case c.cmp(last.end, v.start) < 0:
			c.inuseKeyRanges = append(c.inuseKeyRanges, v)
		case c.cmp(last.end, v.end) < 0:
			last.end = v.end
		}
	}
}

func (c *compaction) trivialMove() bool {
	if len(c.flushing) != 0 {
		return false
	}
	// Check for a trivial move of one table from one level to the next. We avoid
	// such a move if there is lots of overlapping grandparent data. Otherwise,
	// the move could create a parent file that will require a very expensive
	// merge later on.
	if len(c.inputs[0]) == 1 && len(c.inputs[1]) == 0 &&
		totalSize(c.grandparents) <= c.maxOverlapBytes {
		return true
	}
	return false
}

// findGrandparentLimit takes the start user key for a table and returns the
// user key to which that table can extend without excessively overlapping
// the grandparent level. If no limit is needed considering the grandparent
// files, this function returns nil. This is done in order to prevent a table
// at level N from overlapping too much data at level N+1. We want to avoid
// such large overlaps because they translate into large compactions. The
// current heuristic stops output of a table if the addition of another key
// would cause the table to overlap more than 10x the target file size at
// level N. See maxGrandparentOverlapBytes.
//
// TODO(peter): Stopping compaction output in the middle of a user-key creates
// 2 sstables that need to be compacted together as an "atomic compaction
// unit". This is unfortunate as it removes the benefit of stopping output to
// an sstable in order to prevent a large compaction with the next level. Seems
// better to adjust findGrandparentLimit to not stop output in the middle of a
// user-key. Perhaps this isn't a problem if the compaction picking heuristics
// always pick the right (older) sibling for compaction first.
func (c *compaction) findGrandparentLimit(start []byte) []byte {
	lower := sort.Search(len(c.grandparents), func(i int) bool {
		return c.cmp(start, c.grandparents[i].Largest.UserKey) <= 0
	})
	var overlappedBytes uint64
	for upper := lower; upper < len(c.grandparents); upper++ {
		overlappedBytes += c.grandparents[upper].Size
		// To ensure forward progress we always return a larger user
		// key than where we started. See comments above clients of
		// this function for how this is used.
		if overlappedBytes > c.maxOverlapBytes && c.cmp(start, c.grandparents[upper].Largest.UserKey) < 0 {
			return c.grandparents[upper].Largest.UserKey
		}
	}
	return nil
}

// allowZeroSeqNum returns true if seqnum's can be zeroed if there are no
// snapshots requiring them to be kept. It performs this determination by
// looking for an sstable which overlaps the bounds of the compaction at a
// lower level in the LSM.
func (c *compaction) allowZeroSeqNum(iter internalIterator) bool {
	if len(c.flushing) != 0 {
		// TODO(peter): we disable zeroing of seqnums during flushing to match
		// RocksDB behavior and to avoid generating overlapping sstables during
		// DB.replayWAL. When replaying WAL files at startup, we flush after each
		// WAL is replayed building up a single version edit that is
		// applied. Because we don't apply the version edit after each flush, this
		// code doesn't know that L0 contains files and zeroing of seqnums should
		// be disabled. That is fixable, but it seems safer to just match the
		// RocksDB behavior for now.
		return false
	}

	return c.elideRangeTombstone(c.smallest.UserKey, c.largest.UserKey)
}

// elideTombstone returns true if it is ok to elide a tombstone for the
// specified key. A return value of true guarantees that there are no key/value
// pairs at c.level+2 or higher that possibly contain the specified user
// key. The keys in multiple invocations to elideTombstone must be supplied in
// order.
func (c *compaction) elideTombstone(key []byte) bool {
	if len(c.flushing) != 0 {
		return false
	}

	for ; c.elideTombstoneIndex < len(c.inuseKeyRanges); c.elideTombstoneIndex++ {
		r := &c.inuseKeyRanges[c.elideTombstoneIndex]
		if c.cmp(key, r.end) <= 0 {
			if c.cmp(key, r.start) >= 0 {
				return false
			}
			break
		}
	}
	return true
}

// elideRangeTombstone returns true if it is ok to elide the specified range
// tombstone. A return value of true guarantees that there are no key/value
// pairs at c.outputLevel+1 or higher that possibly overlap the specified
// tombstone.
func (c *compaction) elideRangeTombstone(start, end []byte) bool {
	if c.disableRangeTombstoneElision {
		return false
	}

	lower := sort.Search(len(c.inuseKeyRanges), func(i int) bool {
		return c.cmp(c.inuseKeyRanges[i].end, start) >= 0
	})
	upper := sort.Search(len(c.inuseKeyRanges), func(i int) bool {
		return c.cmp(c.inuseKeyRanges[i].start, end) > 0
	})
	return lower >= upper
}

// atomicUnitBounds returns the bounds of the atomic compaction unit containing
// the specified sstable (identified by a pointer to its fileMetadata).
func (c *compaction) atomicUnitBounds(f *fileMetadata) (lower, upper []byte) {
	for i := range c.inputs {
		files := c.inputs[i]
		for j := range files {
			if f == &files[j] {
				// Note that if this file is in a multi-file atomic compaction unit, this file
				// may not be the first file in that unit. An example in Pebble would be a
				// preceding file with Largest c#12,1 and this file with Smallest c#9,1 and
				// containing a range tombstone [c, g)#11,15. The start of the range tombstone
				// is already truncated to this file's Smallest.UserKey, due to the code in
				// rangedel.Fragmenter.FlushTo(), so this walking back should not be necessary
				// (also see range_deletions.md for more details).
				//
				// We do this walking back to be extra cautious, in case it helps with RocksDB
				// compatibility.
				lowerBound := f.Smallest.UserKey
				for k := j; k > 0; k-- {
					cur := &files[k]
					prev := &files[k-1]
					if c.cmp(prev.Largest.UserKey, cur.Smallest.UserKey) < 0 {
						break
					}
					if prev.Largest.Trailer == InternalKeyRangeDeleteSentinel {
						// The range deletion sentinel key is set for the largest key in a
						// table when a range deletion tombstone straddles a table. It
						// isn't necessary to include the prev table in the atomic
						// compaction unit as prev.largest.UserKey does not actually exist
						// in the prev table.
						break
					}
					lowerBound = prev.Smallest.UserKey
				}

				upperBound := f.Largest.UserKey
				for k := j + 1; k < len(files); k++ {
					cur := &files[k-1]
					next := &files[k]
					if c.cmp(cur.Largest.UserKey, next.Smallest.UserKey) < 0 {
						break
					}
					if cur.Largest.Trailer == InternalKeyRangeDeleteSentinel {
						// The range deletion sentinel key is set for the largest key in a
						// table when a range deletion tombstone straddles a table. It
						// isn't necessary to include the next table in the atomic
						// compaction unit as cur.largest.UserKey does not actually exist
						// in the current table.
						break
					}
					// cur.largest.UserKey == next.largest.UserKey, so next is part of
					// the atomic compaction unit.
					upperBound = next.Largest.UserKey
				}
				return lowerBound, upperBound
			}
		}
	}
	return nil, nil
}

// newInputIter returns an iterator over all the input tables in a compaction.
func (c *compaction) newInputIter(newIters tableNewIters) (_ internalIterator, retErr error) {
	if len(c.flushing) != 0 {
		if len(c.flushing) == 1 {
			f := c.flushing[0]
			iter := f.newFlushIter(nil, &c.bytesIterated)
			if rangeDelIter := f.newRangeDelIter(nil); rangeDelIter != nil {
				return newMergingIter(c.logger, c.cmp, iter, rangeDelIter), nil
			}
			return iter, nil
		}
		iters := make([]internalIterator, 0, 2*len(c.flushing))
		for i := range c.flushing {
			f := c.flushing[i]
			iters = append(iters, f.newFlushIter(nil, &c.bytesIterated))
			rangeDelIter := f.newRangeDelIter(nil)
			if rangeDelIter != nil {
				iters = append(iters, rangeDelIter)
			}
		}
		return newMergingIter(c.logger, c.cmp, iters...), nil
	}

	// Check that the LSM ordering invariants are ok in order to prevent
	// generating corrupted sstables due to a violation of those invariants.
	if err := manifest.CheckOrdering(c.cmp, c.format, c.startLevel, c.inputs[0]); err != nil {
		c.logger.Fatalf("%s", err)
	}
	if err := manifest.CheckOrdering(c.cmp, c.format, c.outputLevel, c.inputs[1]); err != nil {
		c.logger.Fatalf("%s", err)
	}

	iters := make([]internalIterator, 0, 2*len(c.inputs[0])+1)
	defer func() {
		if retErr != nil {
			for _, iter := range iters {
				if iter != nil {
					iter.Close()
				}
			}
		}
	}()

	// In normal operation, levelIter iterates over the point operations in a
	// level, and initializes a rangeDelIter pointer for the range deletions in
	// each table. During compaction, we want to iterate over the merged view of
	// point operations and range deletions. In order to do this we create two
	// levelIters per level, one which iterates over the point operations, and
	// one which iterates over the range deletions. These two iterators are
	// combined with a mergingIter.
	newRangeDelIter := func(
		f *fileMetadata, _ *IterOptions, bytesIterated *uint64,
	) (internalIterator, internalIterator, error) {
		iter, rangeDelIter, err := newIters(f, nil /* iter options */, &c.bytesIterated)
		if err == nil {
			// TODO(peter): It is mildly wasteful to open the point iterator only to
			// immediately close it. One way to solve this would be to add new
			// methods to tableCache for creating point and range-deletion iterators
			// independently. We'd only want to use those methods here,
			// though. Doesn't seem worth the hassle in the near term.
			if err = iter.Close(); err != nil {
				rangeDelIter.Close()
				rangeDelIter = nil
			}
		}
		if rangeDelIter != nil {
			// Truncate the range tombstones returned by the iterator to the upper
			// bound of the atomic compaction unit. Note that we need do this
			// truncation at read time in order to handle RocksDB generated sstables
			// which do not truncate range tombstones to atomic compaction unit
			// boundaries at write time. Because we're doing the truncation at read
			// time, we follow RocksDB's lead and do not truncate tombstones to
			// atomic unit boundaries at compaction time.
			lowerBound, upperBound := c.atomicUnitBounds(f)
			if lowerBound != nil || upperBound != nil {
				rangeDelIter = rangedel.Truncate(c.cmp, rangeDelIter, lowerBound, upperBound)
			}
		}
		if rangeDelIter == nil {
			rangeDelIter = emptyIter
		}
		return rangeDelIter, nil, err
	}

	iterOpts := IterOptions{logger: c.logger}
	if c.startLevel != 0 {
		iters = append(iters, newLevelIter(iterOpts, c.cmp, newIters, c.inputs[0], &c.bytesIterated))
		iters = append(iters, newLevelIter(iterOpts, c.cmp, newRangeDelIter, c.inputs[0], &c.bytesIterated))
	} else {
		for i := range c.inputs[0] {
			f := &c.inputs[0][i]
			iter, rangeDelIter, err := newIters(f, nil /* iter options */, &c.bytesIterated)
			if err != nil {
				return nil, fmt.Errorf("pebble: could not open table %d: %v", f.FileNum, err)
			}
			iters = append(iters, iter)
			if rangeDelIter != nil {
				iters = append(iters, rangeDelIter)
			}
		}
	}

	iters = append(iters, newLevelIter(iterOpts, c.cmp, newIters, c.inputs[1], &c.bytesIterated))
	iters = append(iters, newLevelIter(iterOpts, c.cmp, newRangeDelIter, c.inputs[1], &c.bytesIterated))
	return newMergingIter(c.logger, c.cmp, iters...), nil
}

func (c *compaction) String() string {
	if len(c.flushing) != 0 {
		return "flush\n"
	}

	var buf bytes.Buffer
	for i := range c.inputs {
		level := c.startLevel
		if i == 1 {
			level = c.outputLevel
		}
		fmt.Fprintf(&buf, "%d:", level)
		for _, f := range c.inputs[i] {
			fmt.Fprintf(&buf, " %d:%s-%s", f.FileNum, f.Smallest, f.Largest)
		}
		fmt.Fprintf(&buf, "\n")
	}
	return buf.String()
}

type manualCompaction struct {
	// Count of the retries either due to too many concurrent compactions, or a
	// concurrent compaction to overlapping levels.
	retries     int
	level       int
	outputLevel int
	done        chan error
	start       InternalKey
	end         InternalKey
}

func (d *DB) getCompactionPacerInfo() compactionPacerInfo {
	bytesFlushed := atomic.LoadUint64(&d.bytesFlushed)

	d.mu.Lock()
	estimatedMaxWAmp := d.mu.versions.picker.getEstimatedMaxWAmp()
	pacerInfo := compactionPacerInfo{
		slowdownThreshold:   uint64(estimatedMaxWAmp * float64(d.opts.MemTableSize)),
		totalCompactionDebt: d.mu.versions.picker.estimatedCompactionDebt(bytesFlushed),
	}
	for _, m := range d.mu.mem.queue {
		pacerInfo.totalDirtyBytes += m.inuseBytes()
	}
	d.mu.Unlock()

	return pacerInfo
}

func (d *DB) getFlushPacerInfo() flushPacerInfo {
	var pacerInfo flushPacerInfo
	d.mu.Lock()
	for _, m := range d.mu.mem.queue {
		pacerInfo.inuseBytes += m.inuseBytes()
	}
	d.mu.Unlock()
	return pacerInfo
}

// maybeScheduleFlush schedules a flush if necessary.
//
// d.mu must be held when calling this.
func (d *DB) maybeScheduleFlush() {
	if d.mu.compact.flushing || atomic.LoadInt32(&d.closed) != 0 || d.opts.ReadOnly {
		return
	}
	if len(d.mu.mem.queue) <= 1 {
		return
	}

	var n int
	var size uint64
	for ; n < len(d.mu.mem.queue)-1; n++ {
		if !d.mu.mem.queue[n].readyForFlush() {
			break
		}
		if d.mu.mem.queue[n].flushForced {
			// A flush was forced. Pretend the memtable size is the configured
			// size. See minFlushSize below.
			size += uint64(d.opts.MemTableSize)
		} else {
			size += d.mu.mem.queue[n].totalBytes()
		}
	}
	if n == 0 {
		// None of the immutable memtables are ready for flushing.
		return
	}

	// Only flush once the sum of the queued memtable sizes exceeds half the
	// configured memtable size. This prevents flushing of memtables at startup
	// while we're undergoing the ramp period on the memtable size. See
	// DB.newMemTable().
	minFlushSize := uint64(d.opts.MemTableSize) / 2
	if size < minFlushSize {
		return
	}

	d.mu.compact.flushing = true
	go d.flush()
}

func (d *DB) flush() {
	pprof.Do(context.Background(), flushLabels, func(context.Context) {
		d.mu.Lock()
		defer d.mu.Unlock()
		if err := d.flush1(); err != nil {
			// TODO(peter): count consecutive flush errors and backoff.
			d.opts.EventListener.BackgroundError(err)
		}
		d.mu.compact.flushing = false
		// More flush work may have arrived while we were flushing, so schedule
		// another flush if needed.
		d.maybeScheduleFlush()
		// The flush may have produced too many files in a level, so schedule a
		// compaction if needed.
		d.maybeScheduleCompaction()
		d.mu.compact.cond.Broadcast()
	})
}

// flush runs a compaction that copies the immutable memtables from memory to
// disk.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) flush1() error {
	var n int
	for ; n < len(d.mu.mem.queue)-1; n++ {
		if !d.mu.mem.queue[n].readyForFlush() {
			break
		}
	}
	if n == 0 {
		// None of the immutable memtables are ready for flushing.
		return nil
	}

	// Require that every memtable being flushed has a log number less than the
	// new minimum unflushed log number.
	minUnflushedLogNum := d.mu.mem.queue[n].logNum
	if !d.opts.DisableWAL {
		for i := 0; i < n; i++ {
			logNum := d.mu.mem.queue[i].logNum
			if logNum >= minUnflushedLogNum {
				return errFlushInvariant
			}
		}
	}

	c := newFlush(d.opts, d.mu.versions.currentVersion(),
		d.mu.versions.picker.getBaseLevel(), d.mu.mem.queue[:n], &d.bytesFlushed)
	d.mu.compact.inProgress[c] = struct{}{}

	jobID := d.mu.nextJobID
	d.mu.nextJobID++
	d.opts.EventListener.FlushBegin(FlushInfo{
		JobID: jobID,
	})

	flushPacer := (pacer)(nilPacer)
	if d.opts.enablePacing {
		// TODO(peter): Flush pacing is disabled until we figure out why it impacts
		// throughput.
		flushPacer = newFlushPacer(flushPacerEnv{
			limiter:      d.flushLimiter,
			memTableSize: uint64(d.opts.MemTableSize),
			getInfo:      d.getFlushPacerInfo,
		})
	}
	ve, pendingOutputs, err := d.runCompaction(jobID, c, flushPacer)

	info := FlushInfo{
		JobID: jobID,
		Done:  true,
		Err:   err,
	}
	if err == nil {
		for i := range ve.NewFiles {
			e := &ve.NewFiles[i]
			info.Output = append(info.Output, e.Meta.TableInfo())
		}
		if len(ve.NewFiles) == 0 {
			info.Err = errEmptyTable
		}

		// The flush succeeded or it produced an empty sstable. In either case we
		// want to bump the minimum unflushed log number to the log number of the
		// oldest unflushed memtable.
		ve.MinUnflushedLogNum = minUnflushedLogNum
		metrics := c.metrics[0]
		for i := 0; i < n; i++ {
			metrics.BytesIn += d.mu.mem.queue[i].logSize
		}

		d.mu.versions.logLock()
		err = d.mu.versions.logAndApply(jobID, ve, c.metrics, d.dataDir,
			func() []compactionInfo { return d.getInProgressCompactionInfoLocked(c) })
		if err != nil {
			// TODO(peter): untested.
			d.mu.versions.obsoleteTables = append(d.mu.versions.obsoleteTables, pendingOutputs...)
		}
	}

	delete(d.mu.compact.inProgress, c)
	d.mu.versions.incrementFlushes()
	d.opts.EventListener.FlushEnd(info)

	// Refresh bytes flushed count.
	atomic.StoreUint64(&d.bytesFlushed, 0)

	var flushed flushableList
	if err == nil {
		flushed = d.mu.mem.queue[:n]
		d.mu.mem.queue = d.mu.mem.queue[n:]
		var checker func() error
		if d.opts.DebugCheck {
			checker = func() error { return d.CheckLevels(nil) }
		}
		d.updateReadStateLocked(checker)
	}

	d.deleteObsoleteFiles(jobID)

	// Mark all the memtables we flushed as flushed. Note that we do this last so
	// that a synchronous call to DB.Flush() will not return until the deletion
	// of obsolete files from this job have completed. This makes testing easier
	// and provides similar behavior to manual compactions where the compaction
	// is not marked as completed until the deletion of obsolete files job has
	// completed.
	for i := range flushed {
		// The order of these operations matters here for ease of testing. Removing
		// the reader reference first allows tests to be guaranteed that the
		// memtable reservation has been released by the time a synchronous flush
		// returns.
		flushed[i].readerUnref()
		close(flushed[i].flushed)
	}
	return err
}

// maybeScheduleCompaction schedules a compaction if necessary.
//
// d.mu must be held when calling this.
func (d *DB) maybeScheduleCompaction() {
	if atomic.LoadInt32(&d.closed) != 0 || d.opts.ReadOnly {
		return
	}
	if d.mu.compact.compactingCount >= d.opts.MaxConcurrentCompactions {
		if len(d.mu.compact.manual) > 0 {
			// Inability to run head blocks later manual compactions.
			d.mu.compact.manual[0].retries++
		}
		return
	}

	// Compacting picking needs a coherent view a Version. In particular, we need
	// to exlude concurrent ingestions from making a decision on which level to
	// ingest into that conflicts with our compaction
	// decision. versionSet.logLock provides the necessary mutual exclusion.
	d.mu.versions.logLock()
	defer d.mu.versions.logUnlock()

	for len(d.mu.compact.manual) > 0 && d.mu.compact.compactingCount < d.opts.MaxConcurrentCompactions {
		manual := d.mu.compact.manual[0]
		c, retryLater := d.mu.versions.picker.pickManual(d.opts, manual, &d.bytesCompacted, d.getInProgressCompactionInfoLocked(nil))
		if c != nil {
			d.mu.compact.manual = d.mu.compact.manual[1:]
			d.mu.compact.inProgress[c] = struct{}{}
			d.mu.compact.compactingCount++
			go d.compact(c, manual.done)
		} else if !retryLater {
			// Noop
			d.mu.compact.manual = d.mu.compact.manual[1:]
			manual.done <- nil
		} else {
			// Inability to run head blocks later manual compactions.
			manual.retries++
			break
		}
	}

	for d.mu.compact.compactingCount < d.opts.MaxConcurrentCompactions {
		c := d.mu.versions.picker.pickAuto(d.opts, &d.bytesCompacted, d.getInProgressCompactionInfoLocked(nil))
		if c == nil {
			break
		}
		d.mu.compact.compactingCount++
		d.mu.compact.inProgress[c] = struct{}{}
		go d.compact(c, nil)
	}
}

// compact runs one compaction and maybe schedules another call to compact.
func (d *DB) compact(c *compaction, errChannel chan error) {
	pprof.Do(context.Background(), compactLabels, func(context.Context) {
		d.mu.Lock()
		defer d.mu.Unlock()
		if err := d.compact1(c, errChannel); err != nil {
			// TODO(peter): count consecutive compaction errors and backoff.
			d.opts.EventListener.BackgroundError(err)
		}
		d.mu.compact.compactingCount--
		// The previous compaction may have produced too many files in a
		// level, so reschedule another compaction if needed.
		d.maybeScheduleCompaction()
		d.mu.compact.cond.Broadcast()
	})
}

// compact1 runs one compaction.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) compact1(c *compaction, errChannel chan error) (err error) {
	if errChannel != nil {
		defer func() {
			errChannel <- err
		}()
	}

	jobID := d.mu.nextJobID
	d.mu.nextJobID++
	info := CompactionInfo{
		JobID: jobID,
	}
	info.Input.Level = c.startLevel
	info.Output.Level = c.outputLevel
	for i := range c.inputs {
		for j := range c.inputs[i] {
			m := &c.inputs[i][j]
			info.Input.Tables[i] = append(info.Input.Tables[i], m.TableInfo())
		}
	}
	d.opts.EventListener.CompactionBegin(info)
	startTime := d.timeNow()

	compactionPacer := (pacer)(nilPacer)
	if d.opts.enablePacing {
		// TODO(peter): Compaction pacing is disabled until we figure out why it
		// impacts throughput.
		compactionPacer = newCompactionPacer(compactionPacerEnv{
			limiter:      d.compactionLimiter,
			memTableSize: uint64(d.opts.MemTableSize),
			getInfo:      d.getCompactionPacerInfo,
		})
	}
	ve, pendingOutputs, err := d.runCompaction(jobID, c, compactionPacer)

	info.Duration = d.timeNow().Sub(startTime)
	if err == nil {
		d.mu.versions.logLock()
		err = d.mu.versions.logAndApply(jobID, ve, c.metrics, d.dataDir, func() []compactionInfo {
			return d.getInProgressCompactionInfoLocked(c)
		})
		if err != nil {
			// TODO(peter): untested.
			d.mu.versions.obsoleteTables = append(d.mu.versions.obsoleteTables, pendingOutputs...)
		}
	}

	info.Done = true
	info.Err = err
	if err == nil {
		for i := range ve.NewFiles {
			e := &ve.NewFiles[i]
			info.Output.Tables = append(info.Output.Tables, e.Meta.TableInfo())
		}
	}

	delete(d.mu.compact.inProgress, c)
	d.mu.versions.incrementCompactions()
	d.opts.EventListener.CompactionEnd(info)

	// Update the read state before deleting obsolete files because the
	// read-state update will cause the previous version to be unref'd and if
	// there are no references obsolete tables will be added to the obsolete
	// table list.
	if err == nil {
		var checker func() error
		if d.opts.DebugCheck {
			checker = func() error { return d.CheckLevels(nil) }
		}
		d.updateReadStateLocked(checker)
	}
	d.deleteObsoleteFiles(jobID)

	return err
}

// runCompactions runs a compaction that produces new on-disk tables from
// memtables or old on-disk tables.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) runCompaction(
	jobID int, c *compaction, pacer pacer,
) (ve *versionEdit, pendingOutputs []uint64, retErr error) {
	// Check for a trivial move of one table from one level to the next. We avoid
	// such a move if there is lots of overlapping grandparent data. Otherwise,
	// the move could create a parent file that will require a very expensive
	// merge later on.
	if c.trivialMove() {
		meta := &c.inputs[0][0]
		c.metrics = map[int]*LevelMetrics{
			c.outputLevel: &LevelMetrics{
				BytesMoved:  meta.Size,
				TablesMoved: 1,
			},
		}
		ve := &versionEdit{
			DeletedFiles: map[deletedFileEntry]bool{
				deletedFileEntry{Level: c.startLevel, FileNum: meta.FileNum}: true,
			},
			NewFiles: []newFileEntry{
				{Level: c.outputLevel, Meta: *meta},
			},
		}
		return ve, nil, nil
	}

	defer func() {
		if retErr != nil {
			pendingOutputs = nil
		}
	}()

	snapshots := d.mu.snapshots.toSlice()

	// Release the d.mu lock while doing I/O.
	// Note the unusual order: Unlock and then Lock.
	d.mu.Unlock()
	defer d.mu.Lock()

	iiter, err := c.newInputIter(d.newIters)
	if err != nil {
		return nil, pendingOutputs, err
	}
	allowZeroSeqNum := c.allowZeroSeqNum(iiter)
	iter := newCompactionIter(c.cmp, d.merge, iiter, snapshots, &c.rangeDelFrag,
		allowZeroSeqNum, c.elideTombstone, c.elideRangeTombstone)

	var (
		filenames []string
		tw        *sstable.Writer
	)
	defer func() {
		if iter != nil {
			retErr = firstError(retErr, iter.Close())
		}
		if tw != nil {
			retErr = firstError(retErr, tw.Close())
		}
		if retErr != nil {
			for _, filename := range filenames {
				d.opts.FS.Remove(filename)
			}
		}
	}()

	ve = &versionEdit{
		DeletedFiles: map[deletedFileEntry]bool{},
	}

	metrics := &LevelMetrics{
		BytesIn:   totalSize(c.inputs[0]),
		BytesRead: totalSize(c.inputs[1]),
	}
	metrics.BytesRead += metrics.BytesIn
	c.metrics = map[int]*LevelMetrics{
		c.outputLevel: metrics,
	}

	writerOpts := d.opts.MakeWriterOptions(c.outputLevel)

	newOutput := func() error {
		d.mu.Lock()
		fileNum := d.mu.versions.getNextFileNum()
		pendingOutputs = append(pendingOutputs, fileNum)
		d.mu.Unlock()

		filename := base.MakeFilename(d.opts.FS, d.dirname, fileTypeTable, fileNum)
		file, err := d.opts.FS.Create(filename)
		if err != nil {
			return err
		}
		reason := "flushing"
		if c.flushing == nil {
			reason = "compacting"
		}
		d.opts.EventListener.TableCreated(TableCreateInfo{
			JobID:   jobID,
			Reason:  reason,
			Path:    filename,
			FileNum: fileNum,
		})
		file = vfs.NewSyncingFile(file, vfs.SyncingFileOptions{
			BytesPerSync: d.opts.BytesPerSync,
		})
		filenames = append(filenames, filename)
		cacheOpts := private.SSTableCacheOpts(d.cacheID, fileNum).(sstable.WriterOption)
		internalTableOpt := private.SSTableInternalTableOpt.(sstable.WriterOption)
		tw = sstable.NewWriter(file, writerOpts, cacheOpts, internalTableOpt)

		ve.NewFiles = append(ve.NewFiles, newFileEntry{
			Level: c.outputLevel,
			Meta: fileMetadata{
				FileNum: fileNum,
			},
		})
		return nil
	}

	// finishOutput is called for an sstable with the first key of the next sstable, and for the
	// last sstable with an empty key.
	finishOutput := func(key []byte) error {
		// NB: clone the key because the data can be held on to by the call to
		// compactionIter.Tombstones via rangedel.Fragmenter.FlushTo.
		key = append([]byte(nil), key...)
		for _, v := range iter.Tombstones(key) {
			if tw == nil {
				if err := newOutput(); err != nil {
					return err
				}
			}
			if err := tw.Add(v.Start, v.End); err != nil {
				return err
			}
		}

		if tw == nil {
			return nil
		}

		if err := tw.Close(); err != nil {
			tw = nil
			return err
		}
		writerMeta, err := tw.Metadata()
		if err != nil {
			tw = nil
			return err
		}
		tw = nil
		meta := &ve.NewFiles[len(ve.NewFiles)-1].Meta
		meta.Size = writerMeta.Size
		meta.SmallestSeqNum = writerMeta.SmallestSeqNum
		meta.LargestSeqNum = writerMeta.LargestSeqNum

		metrics.BytesWritten += meta.Size
		if c.flushing == nil {
			metrics.TablesCompacted++
		} else {
			metrics.TablesFlushed++
		}

		// The handling of range boundaries is a bit complicated.
		if n := len(ve.NewFiles); n > 1 {
			// This is not the first output file. Bound the smallest range key by the
			// previous tables largest key.
			prevMeta := &ve.NewFiles[n-2].Meta
			if writerMeta.SmallestRange.UserKey != nil {
				c := d.cmp(writerMeta.SmallestRange.UserKey, prevMeta.Largest.UserKey)
				if c < 0 {
					return fmt.Errorf(
						"pebble: smallest range tombstone start key is less than previous sstable largest key: %s < %s",
						writerMeta.SmallestRange.Pretty(d.opts.Comparer.Format),
						prevMeta.Largest.Pretty(d.opts.Comparer.Format))
				}
				if c == 0 && prevMeta.Largest.SeqNum() <= writerMeta.SmallestRange.SeqNum() {
					// The user key portion of the range boundary start key is equal to
					// the previous table's largest key. We need the tables to be
					// key-space partitioned, so force the boundary to a key that we know
					// is larger than the previous table's largest key.
					if prevMeta.Largest.SeqNum() == 0 {
						// If the seqnum of the previous table's largest key is 0, we can't
						// decrement it. This should never happen as we take care in the
						// main compaction loop to avoid generating an sstable with a
						// largest key containing a zero seqnum.
						return fmt.Errorf(
							"pebble: previous sstable largest key unexpectedly has 0 seqnum: %s",
							prevMeta.Largest.Pretty(d.opts.Comparer.Format))
					}
					// TODO(peter): Technically, this produces a small gap with the
					// previous sstable. The largest key of the previous table may be
					// b#5.SET. The smallest key of the new sstable may then become
					// b#4.RANGEDEL even though the tombstone is [b,z)#6. If iteration
					// ever precisely truncates sstable boundaries, the key b#5.DEL at
					// a lower level could slip through. Note that this can't ever
					// actually happen, though, because the only way for two records to
					// have the same seqnum is via ingestion. And even if it did happen
					// revealing a deletion tombstone is not problematic. Slightly more
					// worrisome is the combination of b#5.MERGE, b#5.SET and
					// b#4.RANGEDEL, but we can't ever see b#5.MERGE and b#5.SET at
					// different levels in the tree due to the ingestion argument and
					// atomic compaction units.
					//
					// TODO(sumeer): Incorporate the the comment in
					// https://github.com/cockroachdb/pebble/pull/479#pullrequestreview-340600654
					// into docs/range_deletions.md and reference the correctness
					// argument here. Note that that comment might be slightly incorrect.
					writerMeta.SmallestRange.SetSeqNum(prevMeta.Largest.SeqNum() - 1)
				}
			}
		}

		if key != nil && writerMeta.LargestRange.UserKey != nil {
			// The current file is not the last output file and there is a range tombstone in it.
			// If the tombstone extends into the next file, then truncate it for the purposes of
			// computing meta.Largest. For example, say the next file's first key is c#7,1 and the
			// current file's last key is c#10,1 and the current file has a range tombstone
			// [b, d)#12,15. For purposes of the bounds we pretend that the range tombstone ends at
			// c#inf where inf is the InternalKeyRangeDeleteSentinel. Note that this is just for
			// purposes of bounds computation -- the current sstable will end up with a Largest key
			// of c#7,1 so the range tombstone in the current file will be able to delete c#7.
			if d.cmp(writerMeta.LargestRange.UserKey, key) >= 0 {
				writerMeta.LargestRange.UserKey = key
				writerMeta.LargestRange.Trailer = InternalKeyRangeDeleteSentinel
			}
		}

		meta.Smallest = writerMeta.Smallest(d.cmp)
		meta.Largest = writerMeta.Largest(d.cmp)

		// Verify that the sstable bounds fall within the compaction input
		// bounds. This is a sanity check that we don't have a logic error
		// elsewhere that causes the sstable bounds to accidentally expand past the
		// compaction input bounds as doing so could lead to various badness such
		// as keys being deleted by a range tombstone incorrectly.
		if c.smallest.UserKey != nil {
			switch v := d.cmp(meta.Smallest.UserKey, c.smallest.UserKey); {
			case v >= 0:
				// Nothing to do.
			case v < 0:
				return fmt.Errorf("pebble: compaction output grew beyond bounds of input: %s < %s",
					meta.Smallest.Pretty(d.opts.Comparer.Format),
					c.smallest.Pretty(d.opts.Comparer.Format))
			}
		}
		if c.largest.UserKey != nil {
			switch v := d.cmp(meta.Largest.UserKey, c.largest.UserKey); {
			case v <= 0:
				// Nothing to do.
			case v == 0:
				if meta.Largest.Trailer >= c.largest.Trailer {
					break
				}
				if allowZeroSeqNum && meta.Largest.SeqNum() == 0 {
					break
				}
				fallthrough
			case v > 0:
				return fmt.Errorf("pebble: compaction output grew beyond bounds of input: %s > %s",
					meta.Largest.Pretty(d.opts.Comparer.Format),
					c.largest.Pretty(d.opts.Comparer.Format))
			}
		}
		return nil
	}

	// Each outer loop iteration produces one output file. An iteration that
	// produces a file containing point keys (and optionally range tombstones)
	// guarantees that the input iterator advanced. An iteration that produces
	// a file containing only range tombstones guarantees the limit passed to
	// `finishOutput()` advanced to a strictly greater user key corresponding
	// to a grandparent file largest key, or nil. Taken together, these
	// progress guarantees ensure that eventually the input iterator will be
	// exhausted and the range tombstone fragments will all be flushed.
	for key, val := iter.First(); key != nil || !c.rangeDelFrag.Empty(); {
		var limit []byte
		if c.rangeDelFrag.Empty() {
			// In this case, `limit` will be a larger user key than `key.UserKey`, or
			// nil. In either case, the inner loop will execute at least once to
			// process `key`, and the input iterator will be advanced.
			limit = c.findGrandparentLimit(key.UserKey)
		} else {
			// There is a range tombstone spanning from the last file into the
			// current one. Therefore this file's smallest boundary will overlap the
			// last file's largest boundary.
			//
			// In this case, `limit` will be a larger user key than the previous
			// file's largest key and correspond to a grandparent file's largest user
			// key, or nil. Then, it is possible the inner loop executes zero times,
			// and the output file contains only range tombstones. That is fine as
			// long as the number of times we execute this case is bounded. Since
			// `findGrandparentLimit()` returns a strictly larger user key each time
			// and it corresponds to a grandparent file largest key, the number of
			// times this case can execute is bounded by the number of grandparent
			// files (plus one for the final time it returns nil).
			//
			// n > 0 since we cannot have seen range tombstones at the
			// beginning of the first file.
			n := len(ve.NewFiles)
			limit = c.findGrandparentLimit(ve.NewFiles[n-1].Meta.Largest.UserKey)
		}

		// Each inner loop iteration processes one key from the input iterator.
		passedGrandparentLimit := false
		prevPointSeqNum := InternalKeySeqNumMax
		for ; key != nil; key, val = iter.Next() {
			// Break out of this loop and switch to a new sstable if we've reached
			// the grandparent limit. There is a complication here: we can't create
			// an sstable where the largest key has a seqnum of zero. This limitation
			// exists because a range tombstone which extends into the next sstable
			// will cause the smallest key for the next sstable to have the same user
			// key, but we need the two tables to be disjoint in key space. Consider
			// the scenario:
			//
			//    a#RANGEDEL-c,3 b#SET,0
			//
			// If b#SET,0 is the last key added to an sstable, the range tombstone
			// [b-c)#3 will extend into the next sstable. The boundary generation
			// code in finishOutput() will compute the smallest key for that sstable
			// as b#RANGEDEL,3 which sorts before b#SET,0. Normally we just adjust
			// the seqnum of this key, but that isn't possible for seqnum 0.
			if passedGrandparentLimit || (limit != nil && c.cmp(key.UserKey, limit) > 0) {
				if prevPointSeqNum != 0 || c.rangeDelFrag.Empty() {
					if passedGrandparentLimit {
						limit = key.UserKey
					}
					break
				}
				// If we can't cut the sstable at limit, we need to ensure that the
				// limit is at least the last key added to the table. We can't actually
				// use key.UserKey here because it will be invalidated by the next call
				// to iter.Next(). Instead, we set a flag indicating that we've passed
				// the grandparent limit and clear the limit. If we can stop output at
				// a subsequent key, we'll use that key as the new limit.
				passedGrandparentLimit = true
				limit = nil
			}

			atomic.StoreUint64(c.atomicBytesIterated, c.bytesIterated)
			if err := pacer.maybeThrottle(c.bytesIterated); err != nil {
				return nil, pendingOutputs, err
			}
			if key.Kind() == InternalKeyKindRangeDelete {
				// Range tombstones are handled specially. They are fragmented and
				// written later during `finishOutput()`. We add them to the
				// `Fragmenter` now to make them visible to `compactionIter` so covered
				// keys in the same snapshot stripe can be elided.
				c.rangeDelFrag.Add(iter.cloneKey(*key), val)
				continue
			}
			if tw != nil && tw.EstimatedSize() >= c.maxOutputFileSize {
				// Use the next key as the sstable boundary. Note that we already
				// checked this key against the grandparent limit above.
				limit = key.UserKey
				break
			}
			if tw == nil {
				if err := newOutput(); err != nil {
					return nil, pendingOutputs, err
				}
			}
			if err := tw.Add(*key, val); err != nil {
				return nil, pendingOutputs, err
			}
			prevPointSeqNum = key.SeqNum()
		}

		switch {
		case key == nil && prevPointSeqNum == 0 && !c.rangeDelFrag.Empty():
			// We ran out of keys and the last key added to the sstable has a zero
			// seqnum and there are buffered range tombstones, so we're unable to use
			// the grandparent limit for the sstable boundary. See the example in the
			// in the loop above with range tombstones straddling sstables.
			limit = nil
		case key == nil /* && (prevPointSeqNum != 0 || c.rangeDelFrag.Empty()) */ :
			// We ran out of keys. Because of the previous case, either rangeDelFrag
			// is empty or the last record added to the sstable has a non-zero
			// seqnum. If the rangeDelFragmenter is empty we have no concerns as
			// there won't be another sstable generated by this compaction and the
			// current limit is fine (it won't apply). Otherwise, if the last key
			// added to the sstable had a non-zero seqnum we're also in the clear as
			// we can decrement that seqnum to create a boundary key for the next
			// sstable (if we end up generating a next sstable).
		case key != nil:
			// We either hit the size limit or the grandparent limit for the sstable.
		default:
			return nil, nil, fmt.Errorf("pebble: not reached")
		}

		if err := finishOutput(limit); err != nil {
			return nil, pendingOutputs, err
		}
	}

	for i := range c.inputs {
		level := c.startLevel
		if i == 1 {
			level = c.outputLevel
		}
		for _, f := range c.inputs[i] {
			ve.DeletedFiles[deletedFileEntry{
				Level:   level,
				FileNum: f.FileNum,
			}] = true
		}
	}

	if err := d.dataDir.Sync(); err != nil {
		return nil, pendingOutputs, err
	}
	return ve, pendingOutputs, nil
}

// scanObsoleteFiles scans the filesystem for files that are no longer needed
// and adds those to the internal lists of obsolete files. Note that the files
// are not actually deleted by this method. A subsequent call to
// deleteObsoleteFiles must be performed. Must be not be called concurrently
// with compactions and flushes.
func (d *DB) scanObsoleteFiles(list []string) {
	if d.mu.compact.compactingCount > 0 || d.mu.compact.flushing {
		panic("pebble: cannot scan obsolete files concurrently with compaction/flushing")
	}

	liveFileNums := make(map[uint64]struct{})
	d.mu.versions.addLiveFileNums(liveFileNums)
	minUnflushedLogNum := d.mu.versions.minUnflushedLogNum
	manifestFileNum := d.mu.versions.manifestFileNum

	var obsoleteLogs []uint64
	var obsoleteTables []uint64
	var obsoleteManifests []uint64
	var obsoleteOptions []uint64

	for _, filename := range list {
		fileType, fileNum, ok := base.ParseFilename(d.opts.FS, filename)
		if !ok {
			continue
		}
		switch fileType {
		case fileTypeLog:
			if fileNum >= minUnflushedLogNum {
				continue
			}
			obsoleteLogs = append(obsoleteLogs, fileNum)
		case fileTypeManifest:
			if fileNum >= manifestFileNum {
				continue
			}
			obsoleteManifests = append(obsoleteManifests, fileNum)
		case fileTypeOptions:
			if fileNum >= d.optionsFileNum {
				continue
			}
			obsoleteOptions = append(obsoleteOptions, fileNum)
		case fileTypeTable:
			if _, ok := liveFileNums[fileNum]; ok {
				continue
			}
			obsoleteTables = append(obsoleteTables, fileNum)
		default:
			// Don't delete files we don't know about.
			continue
		}
	}

	d.mu.log.queue = merge(d.mu.log.queue, obsoleteLogs)
	d.mu.versions.metrics.WAL.Files += int64(len(obsoleteLogs))
	d.mu.versions.obsoleteTables = merge(d.mu.versions.obsoleteTables, obsoleteTables)
	d.mu.versions.obsoleteManifests = merge(d.mu.versions.obsoleteManifests, obsoleteManifests)
	d.mu.versions.obsoleteOptions = merge(d.mu.versions.obsoleteOptions, obsoleteOptions)
}

// disableFileDeletions disables file deletions and then waits for any
// in-progress deletion to finish. The caller is required to call
// enableFileDeletions in order to enable file deletions again. It is ok for
// multiple callers to disable file deletions simultaneously, though they must
// all invoke enableFileDeletions in order for file deletions to be re-enabled
// (there is an internal reference count on file deletion disablement).
//
// d.mu must be held when calling this method.
func (d *DB) disableFileDeletions() {
	d.mu.cleaner.disabled++
	for d.mu.cleaner.cleaning {
		d.mu.cleaner.cond.Wait()
	}
	d.mu.cleaner.cond.Signal()
}

// enableFileDeletions enables previously disabled file deletions. Note that if
// file deletions have been re-enabled, the current goroutine will be used to
// perform the queued up deletions.
//
// d.mu must be held when calling this method.
func (d *DB) enableFileDeletions() {
	if d.mu.cleaner.disabled <= 0 || d.mu.cleaner.cleaning {
		panic("pebble: file deletion disablement invariant violated")
	}
	d.mu.cleaner.disabled--
	if d.mu.cleaner.disabled > 0 {
		return
	}
	jobID := d.mu.nextJobID
	d.mu.nextJobID++
	d.deleteObsoleteFiles(jobID)
}

// d.mu must be held when calling this.
func (d *DB) acquireCleaningTurn(waitForOngoing bool) bool {
	// Only allow a single delete obsolete files job to run at a time.
	for d.mu.cleaner.cleaning && d.mu.cleaner.disabled == 0 && waitForOngoing {
		d.mu.cleaner.cond.Wait()
	}
	if d.mu.cleaner.cleaning {
		return false
	}
	if d.mu.cleaner.disabled > 0 {
		// File deletions are currently disabled. When they are re-enabled a new
		// job will be created to catch up on file deletions.
		return false
	}
	d.mu.cleaner.cleaning = true
	return true
}

// d.mu must be held when calling this.
func (d *DB) releaseCleaningTurn() {
	d.mu.cleaner.cleaning = false
	d.mu.cleaner.cond.Signal()
}

// deleteObsoleteFiles deletes those files that are no longer needed.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) deleteObsoleteFiles(jobID int) {
	if !d.acquireCleaningTurn(true) {
		return
	}
	d.doDeleteObsoleteFiles(jobID)
	d.releaseCleaningTurn()
}

// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) doDeleteObsoleteFiles(jobID int) {
	var obsoleteTables []uint64

	defer func() {
		for _, fileNum := range obsoleteTables {
			delete(d.mu.versions.zombieTables, fileNum)
		}
	}()

	var obsoleteLogs []uint64
	for i := range d.mu.log.queue {
		// NB: d.mu.versions.minUnflushedLogNum is the log number of the earliest
		// log that has not had its contents flushed to an sstable. We can recycle
		// the prefix of d.mu.log.queue with log numbers less than
		// minUnflushedLogNum.
		if d.mu.log.queue[i] >= d.mu.versions.minUnflushedLogNum {
			obsoleteLogs = d.mu.log.queue[:i]
			d.mu.log.queue = d.mu.log.queue[i:]
			d.mu.versions.metrics.WAL.Files -= int64(len(obsoleteLogs))
			break
		}
	}

	obsoleteTables = d.mu.versions.obsoleteTables
	d.mu.versions.obsoleteTables = nil

	obsoleteManifests := d.mu.versions.obsoleteManifests
	d.mu.versions.obsoleteManifests = nil

	obsoleteOptions := d.mu.versions.obsoleteOptions
	d.mu.versions.obsoleteOptions = nil

	// Release d.mu while doing I/O
	// Note the unusual order: Unlock and then Lock.
	d.mu.Unlock()
	defer d.mu.Lock()

	files := [4]struct {
		fileType fileType
		obsolete []uint64
	}{
		{fileTypeLog, obsoleteLogs},
		{fileTypeTable, obsoleteTables},
		{fileTypeManifest, obsoleteManifests},
		{fileTypeOptions, obsoleteOptions},
	}
	_, noRecycle := d.opts.Cleaner.(base.NeedsFileContents)
	for _, f := range files {
		// We sort to make the order of deletions deterministic, which is nice for
		// tests.
		sort.Slice(f.obsolete, func(i, j int) bool {
			return f.obsolete[i] < f.obsolete[j]
		})
		for _, fileNum := range f.obsolete {
			dir := d.dirname
			switch f.fileType {
			case fileTypeLog:
				if !noRecycle && d.logRecycler.add(fileNum) {
					continue
				}
				dir = d.walDirname
			case fileTypeTable:
				d.tableCache.evict(fileNum)
			}

			path := base.MakeFilename(d.opts.FS, dir, f.fileType, fileNum)
			d.deleteObsoleteFile(f.fileType, jobID, path, fileNum)
		}
	}
}

func (d *DB) maybeScheduleObsoleteTableDeletion() {
	d.mu.Lock()
	defer d.mu.Unlock()

	if len(d.mu.versions.obsoleteTables) == 0 {
		return
	}
	if !d.acquireCleaningTurn(false) {
		return
	}

	go func() {
		pprof.Do(context.Background(), gcLabels, func(context.Context) {
			d.mu.Lock()
			defer d.mu.Unlock()

			jobID := d.mu.nextJobID
			d.mu.nextJobID++
			d.doDeleteObsoleteFiles(jobID)
			d.releaseCleaningTurn()
		})
	}()
}

// deleteObsoleteFile deletes file that is no longer needed.
func (d *DB) deleteObsoleteFile(fileType fileType, jobID int, path string, fileNum uint64) {
	// TODO(peter): need to handle this error, probably by re-adding the
	// file that couldn't be deleted to one of the obsolete slices map.
	err := d.opts.Cleaner.Clean(d.opts.FS, fileType, path)
	if err == os.ErrNotExist {
		return
	}

	switch fileType {
	case fileTypeLog:
		d.opts.EventListener.WALDeleted(WALDeleteInfo{
			JobID:   jobID,
			Path:    path,
			FileNum: fileNum,
			Err:     err,
		})
	case fileTypeManifest:
		d.opts.EventListener.ManifestDeleted(ManifestDeleteInfo{
			JobID:   jobID,
			Path:    path,
			FileNum: fileNum,
			Err:     err,
		})
	case fileTypeTable:
		d.opts.EventListener.TableDeleted(TableDeleteInfo{
			JobID:   jobID,
			Path:    path,
			FileNum: fileNum,
			Err:     err,
		})
	}
}

func merge(a, b []uint64) []uint64 {
	if len(b) == 0 {
		return a
	}

	a = append(a, b...)
	sort.Slice(a, func(i, j int) bool {
		return a[i] < a[j]
	})

	n := 0
	for i := 0; i < len(a); i++ {
		if n == 0 || a[i] != a[n-1] {
			a[n] = a[i]
			n++
		}
	}
	return a[:n]
}
