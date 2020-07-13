// Copyright 2013 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"runtime/pprof"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
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

// maxGrandparentOverlapBytes is the maximum bytes of overlap with level+1
// before we stop building a single file in a level-1 to level compaction.
func maxGrandparentOverlapBytes(opts *Options, level int) uint64 {
	return uint64(10 * opts.Level(level).TargetFileSize)
}

// totalSize returns the total size of all the files in f.
func totalSize(f []*fileMetadata) (size uint64) {
	for _, x := range f {
		size += x.Size
	}
	return size
}

// noCloseIter wraps around an internal iterator, intercepting and eliding
// calls to Close. It is used during compaction to ensure that rangeDelIters
// are not closed prematurely.
type noCloseIter struct {
	base.InternalIterator
}

func (i noCloseIter) Close() error {
	return nil
}

type userKeyRange struct {
	start, end []byte
}

type compactionLevel struct {
	level int
	files []*fileMetadata
}

type compactionKind string

const (
	compactionKindDefault    compactionKind = "default"
	compactionKindFlush                     = "flush"
	compactionKindMove                      = "move"
	compactionKindDeleteOnly                = "delete-only"
)

// compaction is a table compaction from one level to the next, starting from a
// given version.
type compaction struct {
	kind      compactionKind
	cmp       Compare
	formatKey base.FormatKey
	logger    Logger
	version   *version

	score float64

	// startLevel is the level that is being compacted. Inputs from startLevel
	// and outputLevel will be merged to produce a set of outputLevel files.
	startLevel *compactionLevel
	// outputLevel is the level that files are being produced in. outputLevel is
	// equal to startLevel+1 except when startLevel is 0 in which case it is
	// equal to compactionPicker.baseLevel().
	outputLevel *compactionLevel

	inputs []compactionLevel

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

	// The boundaries of the input data.
	smallest InternalKey
	largest  InternalKey

	// The range deletion tombstone fragmenter. Adds range tombstones as they are
	// returned from `compactionIter` and fragments them for output to files.
	// Referenced by `compactionIter` which uses it to check whether keys are deleted.
	rangeDelFrag rangedel.Fragmenter

	// A list of objects to close when the compaction finishes. Used by input
	// iteration to keep rangeDelIters open for the lifetime of the compaction,
	// and only close them when the compaction finishes.
	closers []io.Closer

	// grandparents are the tables in level+2 that overlap with the files being
	// compacted. Used to determine output table boundaries. Do not assume that the actual files
	// in the grandparent when this compaction finishes will be the same.
	grandparents manifest.LevelIterator

	// Boundaries at which flushes to L0 should be split. Determined by
	// L0Sublevels. If nil, flushes aren't split.
	l0Limits [][]byte

	// List of disjoint inuse key ranges the compaction overlaps with in
	// grandparent and lower levels. See setupInuseKeyRanges() for the
	// construction. Used by elideTombstone() and elideRangeTombstone() to
	// determine if keys affected by a tombstone possibly exist at a lower level.
	inuseKeyRanges      []userKeyRange
	elideTombstoneIndex int

	// allowedZeroSeqNum is true if seqnums can be zeroed if there are no
	// snapshots requiring them to be kept. This determination is made by
	// looking for an sstable which overlaps the bounds of the compaction at a
	// lower level in the LSM during runCompaction.
	allowedZeroSeqNum bool

	metrics map[int]*LevelMetrics
}

func (c *compaction) makeInfo(jobID int) CompactionInfo {
	info := CompactionInfo{
		JobID: jobID,
		Input: make([]LevelInfo, 0, len(c.inputs)),
	}
	for _, cl := range c.inputs {
		inputInfo := LevelInfo{Level: cl.level, Tables: make([]TableInfo, 0, len(cl.files))}
		for _, m := range cl.files {
			inputInfo.Tables = append(inputInfo.Tables, m.TableInfo())
		}
		info.Input = append(info.Input, inputInfo)
	}
	if c.outputLevel != nil {
		info.Output.Level = c.outputLevel.level
		// If there are no inputs from the output level (eg, a move
		// compaction), add an empty LevelInfo to info.Input.
		if len(c.inputs) > 0 && c.inputs[len(c.inputs)-1].level != c.outputLevel.level {
			info.Input = append(info.Input, LevelInfo{Level: c.outputLevel.level})
		}
	} else {
		// For a delete-only compaction, set the output level to L6. The
		// output level is not meaningful here, but complicating the
		// info.Output interface with a pointer doesn't seem worth the
		// semantic distinction.
		info.Output.Level = numLevels - 1
	}
	return info
}

func newCompaction(
	pc *pickedCompaction, opts *Options, bytesCompacted *uint64,
) *compaction {
	c := &compaction{
		kind:                compactionKindDefault,
		cmp:                 pc.cmp,
		formatKey:           opts.Comparer.FormatKey,
		score:               pc.score,
		smallest:            pc.smallest,
		largest:             pc.largest,
		logger:              opts.Logger,
		version:             pc.version,
		inputs:              pc.inputs,
		maxOutputFileSize:   pc.maxOutputFileSize,
		maxOverlapBytes:     pc.maxOverlapBytes,
		maxExpandedBytes:    pc.maxOverlapBytes,
		atomicBytesIterated: bytesCompacted,
	}
	c.startLevel = &c.inputs[0]
	c.outputLevel = &c.inputs[1]

	// Compute the set of outputLevel+1 files that overlap this compaction (these
	// are the grandparent sstables).
	if c.outputLevel.level+1 < numLevels {
		c.grandparents = c.version.Overlaps(c.outputLevel.level+1, c.cmp, c.smallest.UserKey, c.largest.UserKey)
	}
	c.setupInuseKeyRanges()

	// Check if this compaction can be converted into a trivial move from one
	// level to the next. We avoid such a move if there is lots of overlapping
	// grandparent data. Otherwise, the move could create a parent file that
	// will require a very expensive merge later on.
	if len(c.startLevel.files) == 1 && len(c.outputLevel.files) == 0 &&
		c.grandparents.SizeSum() <= c.maxOverlapBytes {
		c.kind = compactionKindMove
	}
	return c
}

func newDeleteOnlyCompaction(
	opts *Options, cur *version, inputs []compactionLevel,
) *compaction {
	c := &compaction{
		kind:      compactionKindDeleteOnly,
		cmp:       opts.Comparer.Compare,
		formatKey: opts.Comparer.FormatKey,
		logger:    opts.Logger,
		version:   cur,
		inputs:    inputs,
	}

	// Set c.smallest, c.largest.
	levelFiles := make([][]*fileMetadata, 0, len(inputs))
	for _, in := range inputs {
		levelFiles = append(levelFiles, in.files)
	}
	c.smallest, c.largest = manifest.KeyRange(opts.Comparer.Compare, levelFiles...)
	return c
}

func newFlush(
	opts *Options, cur *version, baseLevel int, flushing flushableList, bytesFlushed *uint64,
) *compaction {
	c := &compaction{
		kind:                compactionKindFlush,
		cmp:                 opts.Comparer.Compare,
		formatKey:           opts.Comparer.FormatKey,
		logger:              opts.Logger,
		version:             cur,
		inputs:              []compactionLevel{{level: -1}, {level: 0}},
		maxOutputFileSize:   math.MaxUint64,
		maxOverlapBytes:     math.MaxUint64,
		maxExpandedBytes:    math.MaxUint64,
		flushing:            flushing,
		atomicBytesIterated: bytesFlushed,
	}
	c.startLevel = &c.inputs[0]
	c.outputLevel = &c.inputs[1]
	if cur.L0Sublevels != nil {
		c.l0Limits = cur.L0Sublevels.FlushSplitKeys()
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

	if opts.Experimental.FlushSplitBytes > 0 {
		c.maxOutputFileSize = uint64(opts.Level(0).TargetFileSize)
		c.maxOverlapBytes = maxGrandparentOverlapBytes(opts, 0)
		c.maxExpandedBytes = expandedCompactionByteSizeLimit(opts, 0)
		c.grandparents = c.version.Overlaps(baseLevel, c.cmp, c.smallest.UserKey, c.largest.UserKey)
	}

	c.setupInuseKeyRanges()
	return c
}

func (c *compaction) setupInuseKeyRanges() {
	level := c.outputLevel.level + 1
	if c.outputLevel.level == 0 {
		// Level 0 can contain overlapping sstables so we need to check it for
		// overlaps.
		level = 0
	}

	// Gather up the raw list of key ranges from overlapping tables in lower
	// levels.
	var input []userKeyRange
	for ; level < numLevels; level++ {
		iter := c.version.Overlaps(level, c.cmp, c.smallest.UserKey, c.largest.UserKey)
		for m := iter.First(); m != nil; m = iter.Next() {
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
	iter := c.grandparents
	var overlappedBytes uint64
	for f := iter.SeekGE(c.cmp, start); f != nil; f = iter.Next() {
		overlappedBytes += f.Size
		// To ensure forward progress we always return a larger user
		// key than where we started. See comments above clients of
		// this function for how this is used.
		if overlappedBytes > c.maxOverlapBytes && c.cmp(start, f.Largest.UserKey) < 0 {
			return f.Largest.UserKey
		}
	}
	return nil
}

// findL0Limit takes the start key for a table and returns the user key to which
// that table can be extended without excessively overlapping the grandparent
// level, or hitting the next l0Limit, whichever happens sooner. For non-flushes
// this function passes through to findGrandparentLimit.
func (c *compaction) findL0Limit(start []byte) []byte {
	grandparentLimit := c.findGrandparentLimit(start)
	if c.startLevel.level > -1 || c.outputLevel.level != 0 || len(c.l0Limits) == 0 {
		return grandparentLimit
	}
	index := sort.Search(len(c.l0Limits), func(i int) bool {
		return c.cmp(c.l0Limits[i], start) > 0
	})
	if index < len(c.l0Limits) && (grandparentLimit == nil || c.cmp(c.l0Limits[index], grandparentLimit) < 0) {
		return c.l0Limits[index]
	}
	return grandparentLimit
}

// allowZeroSeqNum returns true if seqnum's can be zeroed if there are no
// snapshots requiring them to be kept. It performs this determination by
// looking for an sstable which overlaps the bounds of the compaction at a
// lower level in the LSM.
func (c *compaction) allowZeroSeqNum(iter internalIterator) bool {
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
// pairs at c.outputLevel.level+1 or higher that possibly overlap the specified
// tombstone.
func (c *compaction) elideRangeTombstone(start, end []byte) bool {
	// Disable range tombstone elision if the testing knob for that is enabled,
	// or if we are flushing memtables. The latter requirement is due to
	// inuseKeyRanges not accounting for key ranges in other memtables that are
	// being flushed in the same compaction. It's possible for a range tombstone
	// in one memtable to overlap keys in a preceding memtable in c.flushing.
	//
	// This function is also used in setting allowZeroSeqNum, so disabling
	// elision of range tombstones also disables zeroing of SeqNums.
	//
	// TODO(peter): we disable zeroing of seqnums during flushing to match
	// RocksDB behavior and to avoid generating overlapping sstables during
	// DB.replayWAL. When replaying WAL files at startup, we flush after each
	// WAL is replayed building up a single version edit that is
	// applied. Because we don't apply the version edit after each flush, this
	// code doesn't know that L0 contains files and zeroing of seqnums should
	// be disabled. That is fixable, but it seems safer to just match the
	// RocksDB behavior for now.
	if c.disableRangeTombstoneElision || len(c.flushing) != 0 {
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
		files := c.inputs[i].files
		for j := range files {
			if f == files[j] {
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
					cur := files[k]
					prev := files[k-1]
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
					cur := files[k-1]
					next := files[k]
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
	if c.startLevel.level >= 0 {
		if err := manifest.CheckOrdering(c.cmp, c.formatKey, manifest.Level(c.startLevel.level), c.startLevel.files); err != nil {
			c.logger.Fatalf("%s", err)
		}
	}
	if err := manifest.CheckOrdering(c.cmp, c.formatKey, manifest.Level(c.outputLevel.level), c.outputLevel.files); err != nil {
		c.logger.Fatalf("%s", err)
	}

	iters := make([]internalIterator, 0, 2*len(c.startLevel.files)+1)
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
			// Ensure that rangeDelIter is not closed until the compaction is
			// finished. This is necessary because range tombstone processing
			// requires the range tombstones to be held in memory for up to the
			// lifetime of the compaction.
			c.closers = append(c.closers, rangeDelIter)
			rangeDelIter = noCloseIter{rangeDelIter}

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
	if c.startLevel.level != 0 {
		iters = append(iters, newLevelIter(iterOpts, c.cmp, newIters, c.startLevel.files,
			manifest.Level(c.startLevel.level), &c.bytesIterated))
		iters = append(iters, newLevelIter(iterOpts, c.cmp, newRangeDelIter, c.startLevel.files,
			manifest.Level(c.startLevel.level), &c.bytesIterated))
	} else {
		for i := range c.startLevel.files {
			f := c.startLevel.files[i]
			iter, rangeDelIter, err := newIters(f, nil /* iter options */, &c.bytesIterated)
			if err != nil {
				return nil, errors.Wrapf(err, "pebble: could not open table %s", errors.Safe(f.FileNum))
			}
			iters = append(iters, iter)
			if rangeDelIter != nil {
				iters = append(iters, rangeDelIter)
			}
		}
	}

	iters = append(iters, newLevelIter(iterOpts, c.cmp, newIters, c.outputLevel.files,
		manifest.Level(c.outputLevel.level), &c.bytesIterated))
	iters = append(iters, newLevelIter(iterOpts, c.cmp, newRangeDelIter, c.outputLevel.files,
		manifest.Level(c.outputLevel.level), &c.bytesIterated))
	return newMergingIter(c.logger, c.cmp, iters...), nil
}

func (c *compaction) String() string {
	if len(c.flushing) != 0 {
		return "flush\n"
	}

	var buf bytes.Buffer
	for i := range c.inputs {
		level := c.startLevel.level
		if i == 1 {
			level = c.outputLevel.level
		}
		fmt.Fprintf(&buf, "%d:", level)
		for _, f := range c.inputs[i].files {
			fmt.Fprintf(&buf, " %s:%s-%s", f.FileNum, f.Smallest, f.Largest)
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

func (d *DB) addInProgressCompaction(c *compaction) {
	d.mu.compact.inProgress[c] = struct{}{}
	var isBase, isIntraL0 bool
	for _, cl := range c.inputs {
		for _, f := range cl.files {
			if f.Compacting {
				d.opts.Logger.Fatalf("L%d->L%d: %s already being compacted", c.startLevel.level, c.outputLevel.level, f.FileNum)
			}
			f.Compacting = true
			if c.startLevel != nil && c.outputLevel != nil && c.startLevel.level == 0 {
				if c.outputLevel.level == 0 {
					f.IsIntraL0Compacting = true
					isIntraL0 = true
				} else {
					isBase = true
				}
			}
		}
	}

	if (isIntraL0 || isBase) && c.version.L0Sublevels != nil {
		l0Inputs := [][]*fileMetadata{c.startLevel.files}
		if isIntraL0 {
			l0Inputs = append(l0Inputs, c.outputLevel.files)
		}
		if err := c.version.L0Sublevels.UpdateStateForStartedCompaction(l0Inputs, isBase); err != nil {
			d.opts.Logger.Fatalf("could not update state for compaction: %s", err)
		}
	}

	if false {
		// TODO(peter): Do we want to keep this? It is useful for seeing the
		// concurrent compactions/flushes that are taking place. Right now, this
		// spams the logs and output to tests. Figure out a way to useful expose
		// it.
		strs := make([]string, 0, len(d.mu.compact.inProgress))
		for c := range d.mu.compact.inProgress {
			var s string
			if c.startLevel.level == -1 {
				s = fmt.Sprintf("mem->L%d", c.outputLevel.level)
			} else {
				s = fmt.Sprintf("L%d->L%d:%.1f", c.startLevel.level, c.outputLevel.level, c.score)
			}
			strs = append(strs, s)
		}
		// This odd sorting function is intended to sort "mem" before "L*".
		sort.Slice(strs, func(i, j int) bool {
			if strs[i][0] == strs[j][0] {
				return strs[i] < strs[j]
			}
			return strs[i] > strs[j]
		})
		d.opts.Logger.Infof("compactions: %s", strings.Join(strs, " "))
	}
}

// Removes compaction markers from files in a compaction.
//
// DB.mu must be held when calling this method. All writes to the manifest
// for this compaction should have completed by this point.
func (d *DB) removeInProgressCompaction(c *compaction) {
	for _, cl := range c.inputs {
		for _, f := range cl.files {
			if !f.Compacting {
				d.opts.Logger.Fatalf("L%d->L%d: %s already being compacted", c.startLevel.level, c.outputLevel.level, f.FileNum)
			}
			f.Compacting = false
			f.IsIntraL0Compacting = false
		}
	}
	delete(d.mu.compact.inProgress, c)
	d.mu.versions.currentVersion().L0Sublevels.InitCompactingFileInfo()
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

func (d *DB) maybeScheduleDelayedFlush(tbl *memTable) {
	var mem *flushableEntry
	for _, m := range d.mu.mem.queue {
		if m.flushable == tbl {
			mem = m
			break
		}
	}
	if mem == nil || mem.flushForced || mem.delayedFlushForced {
		return
	}
	mem.delayedFlushForced = true
	go func() {
		timer := time.NewTimer(d.opts.Experimental.DeleteRangeFlushDelay)
		defer timer.Stop()

		select {
		case <-d.closedCh:
			return
		case <-mem.flushed:
			return
		case <-timer.C:
			d.commit.mu.Lock()
			defer d.commit.mu.Unlock()
			d.mu.Lock()
			defer d.mu.Unlock()

			// NB: The timer may fire concurrently with a call to Close.  If a
			// Close call beat us to acquiring d.mu, d.closed is 1, and it's
			// too late to flush anything. Otherwise, the Close call will
			// block on locking d.mu until we've finished scheduling the flush
			// and set `d.mu.compact.flushing` to true. Close will wait for
			// the current flush to complete.
			if atomic.LoadInt32(&d.closed) != 0 {
				return
			}

			if d.mu.mem.mutable == tbl {
				d.makeRoomForWrite(nil)
			} else {
				mem.flushForced = true
				d.maybeScheduleFlush()
			}
		}
	}()
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
	d.addInProgressCompaction(c)

	jobID := d.mu.nextJobID
	d.mu.nextJobID++
	d.opts.EventListener.FlushBegin(FlushInfo{
		JobID: jobID,
		Input: n,
	})
	startTime := d.timeNow()

	flushPacer := (pacer)(nilPacer)
	if d.opts.private.enablePacing {
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
		JobID:    jobID,
		Input:    n,
		Duration: d.timeNow().Sub(startTime),
		Done:     true,
		Err:      err,
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

	d.maybeUpdateDeleteCompactionHints(c)
	d.removeInProgressCompaction(c)
	d.mu.versions.incrementFlushes()
	d.opts.EventListener.FlushEnd(info)

	// Refresh bytes flushed count.
	atomic.StoreUint64(&d.bytesFlushed, 0)

	var flushed flushableList
	if err == nil {
		flushed = d.mu.mem.queue[:n]
		d.mu.mem.queue = d.mu.mem.queue[n:]
		d.updateReadStateLocked(d.opts.DebugCheck)
		d.updateTableStatsLocked(ve.NewFiles)
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

	// Compaction picking needs a coherent view of a Version. In particular, we
	// need to exlude concurrent ingestions from making a decision on which level
	// to ingest into that conflicts with our compaction
	// decision. versionSet.logLock provides the necessary mutual exclusion.
	d.mu.versions.logLock()
	defer d.mu.versions.logUnlock()

	// Check for the closed flag again, in case the DB was closed while we were
	// waiting for logLock().
	if atomic.LoadInt32(&d.closed) != 0 {
		return
	}

	env := compactionEnv{
		bytesCompacted:          &d.bytesCompacted,
		earliestUnflushedSeqNum: d.getEarliestUnflushedSeqNumLocked(),
	}

	// Check for delete-only compactions first, because they're expected to be
	// cheap and reduce future compaction work.
	if len(d.mu.compact.deletionHints) > 0 &&
		d.mu.compact.compactingCount < d.opts.MaxConcurrentCompactions &&
		!d.opts.private.disableAutomaticCompactions {
		v := d.mu.versions.currentVersion()
		snapshots := d.mu.snapshots.toSlice()
		inputs, unresolvedHints := checkDeleteCompactionHints(d.cmp, v, d.mu.compact.deletionHints, snapshots)
		d.mu.compact.deletionHints = unresolvedHints

		if len(inputs) > 0 {
			c := newDeleteOnlyCompaction(d.opts, v, inputs)
			d.mu.compact.compactingCount++
			d.addInProgressCompaction(c)
			go d.compact(c, nil)
		}
	}

	for len(d.mu.compact.manual) > 0 && d.mu.compact.compactingCount < d.opts.MaxConcurrentCompactions {
		manual := d.mu.compact.manual[0]
		env.inProgressCompactions = d.getInProgressCompactionInfoLocked(nil)
		pc, retryLater := d.mu.versions.picker.pickManual(env, manual)
		if pc != nil {
			c := newCompaction(pc, d.opts, env.bytesCompacted)
			d.mu.compact.manual = d.mu.compact.manual[1:]
			d.mu.compact.compactingCount++
			d.addInProgressCompaction(c)
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

	for !d.opts.private.disableAutomaticCompactions && d.mu.compact.compactingCount < d.opts.MaxConcurrentCompactions {
		env.inProgressCompactions = d.getInProgressCompactionInfoLocked(nil)
		pc := d.mu.versions.picker.pickAuto(env)
		if pc == nil {
			break
		}
		c := newCompaction(pc, d.opts, env.bytesCompacted)
		d.mu.compact.compactingCount++
		d.addInProgressCompaction(c)
		go d.compact(c, nil)
	}
}

// A deleteCompactionHint records a user key and sequence number span that has been
// deleted by a range tombstone. A hint is recorded if at least one sstable
// falls completely within both the user key and sequence number spans.
// Once the tombstones and the observed completely-contained sstables fall
// into the same snapshot stripe, a delete-only compaction may delete any
// sstables within the range.
type deleteCompactionHint struct {
	start []byte
	end   []byte
	// The level of the file containing the range tombstone(s) when the hint
	// was created. Only lower levels need to be searched for files that may
	// be deleted.
	tombstoneLevel int
	// The file containing the range tombstone(s) that created the hint.
	tombstoneFile *fileMetadata
	// The smallest and largest sequence numbers of the abutting tombstones
	// merged to form this hint. All of a tables' keys must be less than the
	// tombstone smallest sequence number to be deleted. All of a tables'
	// sequence numbers must fall into the same snapshot stripe as the
	// tombstone largest sequence number to be deleted.
	tombstoneLargestSeqNum  uint64
	tombstoneSmallestSeqNum uint64
	// The smallest sequence number of a sstable that was found to be covered
	// by this hint. The hint cannot be resolved until this sequence number is
	// in the same snapshot stripe as the largest tombstone sequence number.
	// This is set when a hint is created, so the LSM may look different and
	// notably no longer contian the sstable that contained the key at this
	// sequence number.
	fileSmallestSeqNum uint64
}

func (h deleteCompactionHint) String() string {
	return fmt.Sprintf("L%d.%s %s-%s seqnums(tombstone=%d-%d, file-smallest=%d)",
		h.tombstoneLevel, h.tombstoneFile.FileNum, h.start, h.end,
		h.tombstoneSmallestSeqNum, h.tombstoneLargestSeqNum, h.fileSmallestSeqNum)
}

func (h *deleteCompactionHint) canDelete(cmp Compare, m *fileMetadata, snapshots []uint64) bool {
	// The file can only be deleted if all of its keys are older than the
	// earliest tombstone aggregated into the hint.
	if m.LargestSeqNum >= h.tombstoneSmallestSeqNum || m.SmallestSeqNum < h.fileSmallestSeqNum {
		return false
	}

	// The file's oldest key must  be in the same snapshot stripe as the
	// newest tombstone. NB: We already checked the hint's sequence numbers,
	// but this file's oldest sequence number might be lower than the hint's
	// smallest sequence number despite the file falling within the key range
	// if this file was constructed after the hint by a compaction.
	ti, _ := snapshotIndex(h.tombstoneLargestSeqNum, snapshots)
	fi, _ := snapshotIndex(m.SmallestSeqNum, snapshots)
	if ti != fi {
		return false
	}

	// The file's keys must be completely contianed within the hint range.
	return cmp(h.start, m.Smallest.UserKey) <= 0 && cmp(m.Largest.UserKey, h.end) < 0
}

func (d *DB) maybeUpdateDeleteCompactionHints(c *compaction) {
	// Compactions that zero sequence numbers can interfere with compaction
	// deletion hints. Deletion hints apply to tables containing keys older
	// than a threshold. If a key more recent than the threshold is zeroed in
	// a compaction, a delete-only compaction may mistake it as meeting the
	// threshold and drop a table containing live data.
	//
	// To avoid this scenario, compactions that zero sequence numbers remove
	// any conflicting deletion hints. A deletion hint is conflicting if both
	// of the following conditions apply:
	// * its key space overlaps with the compaction
	// * at least one of its inputs contains a key as recent as one of the
	//   hint's tombstones.
	//
	if !c.allowedZeroSeqNum {
		return
	}

	updatedHints := d.mu.compact.deletionHints[:0]
	for _, h := range d.mu.compact.deletionHints {
		// If the compaction's key space is disjoint from the hint's key
		// space, the zeroing of sequence numbers won't affect the hint. Keep
		// the hint.
		keysDisjoint := d.cmp(h.end, c.smallest.UserKey) < 0 || d.cmp(h.start, c.largest.UserKey) > 0
		if keysDisjoint {
			updatedHints = append(updatedHints, h)
			continue
		}

		// All of the compaction's inputs must be older than the hint's
		// tombstones.
		inputsOlder := true
		for _, in := range c.inputs {
			for _, f := range in.files {
				inputsOlder = inputsOlder && f.LargestSeqNum < h.tombstoneSmallestSeqNum
			}
		}
		if inputsOlder {
			updatedHints = append(updatedHints, h)
			continue
		}

		// Drop h, because the compaction c may have zeroed sequence numbers
		// of keys more recent than some of h's tombstones.
	}
	d.mu.compact.deletionHints = updatedHints
}

func checkDeleteCompactionHints(
	cmp Compare, v *version, hints []deleteCompactionHint, snapshots []uint64,
) ([]compactionLevel, []deleteCompactionHint) {
	var files map[*fileMetadata]bool
	var byLevel [numLevels][]*fileMetadata

	unresolvedHints := hints[:0]
	for _, h := range hints {
		// Check each compaction hint to see if it's resolvable. Resolvable
		// hints are removed and trigger a delete-only compaction if any files
		// in the current LSM still meet their criteria. Unresolvable hints
		// are saved and don't trigger a delete-only compaction.
		//
		// When a compaction hint is created, the sequence numbers of the
		// range tombstones and the covered file with the oldest key are
		// recorded. The largest tombstone sequence number and the smallest
		// file sequence number must be in the same snapshot stripe for the
		// hint to be resolved. The below graphic models a compaction hint
		// covering the keyspace [b, r). The hint completely contains two
		// files, 000002 and 000003. The file 000003 contains the lowest
		// covered sequence number at #90. The tombstone b.RANGEDEL.230:h has
		// the highest tombstone sequence number incorporated into the hint.
		// The hint may be resolved only once the snapshots at #100, #180 and
		// #210 are all closed. File 000001 is not included within the hint
		// because it extends beyond the range tombstones in user key space.
		//
		// 250
		//
		//       |-b...230:h-|
		// _____________________________________________________ snapshot #210
		// 200               |--h.RANGEDEL.200:r--|
		//
		// _____________________________________________________ snapshot #180
		//
		// 150                     +--------+
		//           +---------+   | 000003 |
		//           | 000002  |   |        |
		//           +_________+   |        |
		// 100_____________________|________|___________________ snapshot #100
		//                         +--------+
		// _____________________________________________________ snapshot #70
		//                             +---------------+
		//  50                         | 000001        |
		//                             |               |
		//                             +---------------+
		// ______________________________________________________________
		//     a b c d e f g h i j k l m n o p q r s t u v w x y z

		ti, _ := snapshotIndex(h.tombstoneLargestSeqNum, snapshots)
		fi, _ := snapshotIndex(h.fileSmallestSeqNum, snapshots)
		if ti != fi {
			// Cannot resolve yet.
			unresolvedHints = append(unresolvedHints, h)
			continue
		}

		// The hint h will be resolved and dropped, regardless of whether
		// there are any tables that can be deleted.
		for l := h.tombstoneLevel + 1; l < numLevels; l++ {
			overlaps := v.Overlaps(l, cmp, h.start, h.end)
			for m := overlaps.First(); m != nil; m = overlaps.Next() {
				if m.Compacting || !h.canDelete(cmp, m, snapshots) || files[m] {
					continue
				}

				if files == nil {
					// Construct files lazily, assuming most calls will not
					// produce delete-only compactions.
					files = make(map[*fileMetadata]bool)
				}
				files[m] = true
				byLevel[l] = append(byLevel[l], m)
			}
		}
	}

	var compactLevels []compactionLevel
	for l, files := range byLevel {
		if len(files) == 0 {
			continue
		}
		compactLevels = append(compactLevels, compactionLevel{
			level: l,
			files: files,
		})
	}
	return compactLevels, unresolvedHints
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
	info := c.makeInfo(jobID)
	d.opts.EventListener.CompactionBegin(info)
	startTime := d.timeNow()

	compactionPacer := (pacer)(nilPacer)
	if d.opts.private.enablePacing {
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

	d.maybeUpdateDeleteCompactionHints(c)
	d.removeInProgressCompaction(c)
	d.mu.versions.incrementCompactions()
	d.opts.EventListener.CompactionEnd(info)

	// Update the read state before deleting obsolete files because the
	// read-state update will cause the previous version to be unref'd and if
	// there are no references obsolete tables will be added to the obsolete
	// table list.
	if err == nil {
		d.updateReadStateLocked(d.opts.DebugCheck)
		d.updateTableStatsLocked(ve.NewFiles)
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
) (ve *versionEdit, pendingOutputs []FileNum, retErr error) {
	// Check for a delete-only compaction. This can occur when wide range
	// tombstones completely contain sstables.
	if c.kind == compactionKindDeleteOnly {
		c.metrics = make(map[int]*LevelMetrics, len(c.inputs))
		ve := &versionEdit{
			DeletedFiles: map[deletedFileEntry]bool{},
		}
		for _, cl := range c.inputs {
			c.metrics[cl.level] = &LevelMetrics{}
			for _, f := range cl.files {
				ve.DeletedFiles[deletedFileEntry{
					Level:   cl.level,
					FileNum: f.FileNum,
				}] = true
			}
		}
		return ve, nil, nil
	}

	// Check for a trivial move of one table from one level to the next. We avoid
	// such a move if there is lots of overlapping grandparent data. Otherwise,
	// the move could create a parent file that will require a very expensive
	// merge later on.
	if c.kind == compactionKindMove {
		meta := c.startLevel.files[0]
		c.metrics = map[int]*LevelMetrics{
			c.outputLevel.level: &LevelMetrics{
				BytesMoved:  meta.Size,
				TablesMoved: 1,
			},
		}
		ve := &versionEdit{
			DeletedFiles: map[deletedFileEntry]bool{
				deletedFileEntry{Level: c.startLevel.level, FileNum: meta.FileNum}: true,
			},
			NewFiles: []newFileEntry{
				{Level: c.outputLevel.level, Meta: meta},
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
	c.allowedZeroSeqNum = c.allowZeroSeqNum(iiter)
	iter := newCompactionIter(c.cmp, d.merge, iiter, snapshots, &c.rangeDelFrag,
		c.allowedZeroSeqNum, c.elideTombstone, c.elideRangeTombstone)

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
		for _, closer := range c.closers {
			retErr = firstError(retErr, closer.Close())
		}
	}()

	ve = &versionEdit{
		DeletedFiles: map[deletedFileEntry]bool{},
	}

	metrics := &LevelMetrics{
		BytesIn:   totalSize(c.startLevel.files),
		BytesRead: totalSize(c.outputLevel.files),
	}
	metrics.BytesRead += metrics.BytesIn
	c.metrics = map[int]*LevelMetrics{
		c.outputLevel.level: metrics,
	}

	writerOpts := d.opts.MakeWriterOptions(c.outputLevel.level)

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
			Level: c.outputLevel.level,
			Meta: &fileMetadata{
				FileNum:      fileNum,
				CreationTime: time.Now().Unix(),
			},
		})
		return nil
	}

	splittingFlush := c.startLevel.level < 0 && c.outputLevel.level == 0 && d.opts.Experimental.FlushSplitBytes > 0

	// finishOutput is called for an sstable with the first key of the next sstable, and for the
	// last sstable with an empty key.
	finishOutput := func(key []byte) error {
		// NB: clone the key because the data can be held on to by the call to
		// compactionIter.Tombstones via rangedel.Fragmenter.FlushTo.
		key = append([]byte(nil), key...)
		for _, v := range iter.Tombstones(key, splittingFlush) {
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
		meta := ve.NewFiles[len(ve.NewFiles)-1].Meta
		meta.Size = writerMeta.Size
		meta.SmallestSeqNum = writerMeta.SmallestSeqNum
		meta.LargestSeqNum = writerMeta.LargestSeqNum
		meta.MarkedForCompaction = writerMeta.MarkedForCompaction
		// If the file didn't contain any range deletions, we can fill its
		// table stats now, avoiding unnecessarily loading the table later.
		if writerMeta.Properties.NumRangeDeletions == 0 {
			meta.Stats = manifest.TableStats{
				Valid:                       true,
				RangeDeletionsBytesEstimate: 0,
			}
		}

		if c.flushing == nil {
			metrics.TablesCompacted++
			metrics.BytesCompacted += meta.Size
		} else {
			metrics.TablesFlushed++
			metrics.BytesFlushed += meta.Size
		}

		// The handling of range boundaries is a bit complicated.
		if n := len(ve.NewFiles); n > 1 {
			// This is not the first output file. Bound the smallest range key by the
			// previous tables largest key.
			prevMeta := ve.NewFiles[n-2].Meta
			if writerMeta.SmallestRange.UserKey != nil {
				c := d.cmp(writerMeta.SmallestRange.UserKey, prevMeta.Largest.UserKey)
				if c < 0 {
					return errors.Errorf(
						"pebble: smallest range tombstone start key is less than previous sstable largest key: %s < %s",
						writerMeta.SmallestRange.Pretty(d.opts.Comparer.FormatKey),
						prevMeta.Largest.Pretty(d.opts.Comparer.FormatKey))
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
						return errors.Errorf(
							"pebble: previous sstable largest key unexpectedly has 0 seqnum: %s",
							prevMeta.Largest.Pretty(d.opts.Comparer.FormatKey))
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
				return errors.Errorf("pebble: compaction output grew beyond bounds of input: %s < %s",
					meta.Smallest.Pretty(d.opts.Comparer.FormatKey),
					c.smallest.Pretty(d.opts.Comparer.FormatKey))
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
				if c.allowedZeroSeqNum && meta.Largest.SeqNum() == 0 {
					break
				}
				fallthrough
			case v > 0:
				return errors.Errorf("pebble: compaction output grew beyond bounds of input: %s > %s",
					meta.Largest.Pretty(d.opts.Comparer.FormatKey),
					c.largest.Pretty(d.opts.Comparer.FormatKey))
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
		if splittingFlush {
			// For flushes being split across multiple sstables, call
			// findL0Limit to find the next L0 limit.
			if key != nil {
				limit = c.findL0Limit(key.UserKey)
			} else {
				// Use the start key of the first pending tombstone to find the
				// next limit. All pending tombstones have the same start key.
				// We use this as opposed to the end key of the
				// last written sstable to effectively handle cases like these:
				//
				// a.SET.3
				// (L0 limit at b)
				// d.RANGEDEL.4:f
				//
				// In this case, the partition after b has only range deletions,
				// so if we were to find the L0 limit after the last written
				// key at the split point (key a), we'd get the limit b again,
				// and finishOutput() would not advance any further because
				// the next range tombstone to write does not start until after
				// the L0 split point.
				startKey := c.rangeDelFrag.Start()
				if startKey != nil {
					limit = c.findL0Limit(startKey)
				}
			}
		} else if c.rangeDelFrag.Empty() {
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
				// The flush split exception exists because, when flushing
				// to L0, we are able to guarantee that the end key of
				// tombstones will also be truncated (through the
				// TruncateAndFlushTo call), and no user keys will
				// be split between sstables.
				if prevPointSeqNum != 0 || c.rangeDelFrag.Empty() || splittingFlush {
					if passedGrandparentLimit || splittingFlush {
						limit = key.UserKey
					}
					if splittingFlush {
						// Flush all tombstones up until key.UserKey, and
						// truncate them at that key.
						//
						// The fragmenter could save the passed-in key. As this
						// key could live beyond the write into the current
						// sstable output file, make a copy.
						c.rangeDelFrag.TruncateAndFlushTo(key.Clone().UserKey)
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
				if !splittingFlush {
					limit = key.UserKey
					break
				}
				// We do not split user keys across different sstables when
				// splitting flushes. This includes range tombstones; a range
				// tombstone covering a key in a given flush is guaranteed to
				// end up in the same sstable as that key. Wide range tombstones
				// spanning split boundaries are split to keep this invariant
				// true. This invariant exists because L0 compaction picking and
				// sublevel construction does not account for atomic compaction
				// units, unlike regular compactions. Atomic compaction units
				// resulting from user keys being split across sstables
				// are an unhelpful side-effects of regular compactions; and the
				// only reason it's done for regular compactions is to maintain
				// the same behaviour as RocksDB.
				//
				// Set the limit to a copy of the current key, as the current
				// key is backed by compactionIter. The inner loop will break
				// and switch outputs when it iterates past this user key.
				limit = key.Clone().UserKey
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
			// the grandparent/flush limit for the sstable boundary. See the example in the
			// in the loop above with range tombstones straddling sstables.
			limit = nil
		case key == nil && splittingFlush && !c.rangeDelFrag.Empty():
			// We ran out of keys with flush splits enabled, and have remaining
			// buffered range tombstones. Set limit to nil so all range
			// tombstones get flushed in the current sstable. Consider this
			// example:
			//
			// a.SET.4
			// d.MERGE.5
			// d.RANGEDEL.3:f
			// (no more keys remaining)
			//
			// Where d is a flush split key (i.e. limit = 'd'). Since d.MERGE.5
			// has already been written to this output by this point (as it's
			// <= limit), and flushes cannot have user keys split across
			// multiple sstables, we have to set limit to a key greater than
			// 'd' to ensure the range deletion also gets flushed. Setting
			// the limit to nil is the simplest way to ensure that.
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
			// We either hit the size, grandparent, or L0 limit for the sstable.
		default:
			return nil, nil, errors.New("pebble: not reached")
		}

		if err := finishOutput(limit); err != nil {
			return nil, pendingOutputs, err
		}
	}

	for _, cl := range c.inputs {
		for _, f := range cl.files {
			ve.DeletedFiles[deletedFileEntry{
				Level:   cl.level,
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

	liveFileNums := make(map[FileNum]struct{})
	d.mu.versions.addLiveFileNums(liveFileNums)
	minUnflushedLogNum := d.mu.versions.minUnflushedLogNum
	manifestFileNum := d.mu.versions.manifestFileNum

	var obsoleteLogs []FileNum
	var obsoleteTables []FileNum
	var obsoleteManifests []FileNum
	var obsoleteOptions []FileNum

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
	d.mu.cleaner.cond.Broadcast()
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
	d.mu.cleaner.cond.Broadcast()
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
	var obsoleteTables []FileNum

	defer func() {
		for _, fileNum := range obsoleteTables {
			delete(d.mu.versions.zombieTables, fileNum)
		}
	}()

	var obsoleteLogs []FileNum
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
		obsolete []FileNum
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
func (d *DB) deleteObsoleteFile(fileType fileType, jobID int, path string, fileNum FileNum) {
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

func merge(a, b []FileNum) []FileNum {
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
