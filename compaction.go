// Copyright 2013 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"sync/atomic"
	"unsafe"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/rangedel"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

var errEmptyTable = errors.New("pebble: empty table")
var errFlushInvariant = errors.New("pebble: flush next log number is unset")

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

// compaction is a table compaction from one level to the next, starting from a
// given version.
type compaction struct {
	cmp     Compare
	format  base.Formatter
	logger  base.Logger
	version *version

	// startLevel is the level that is being compacted. Inputs from startLevel
	// and outputLevel will be merged to produce a set of outputLevel files.
	startLevel int
	// outputLevel is the level that files are being produced in. outputLevel is
	// equal to startLevel+1 except when startLevel is 0 in which case it is
	// equal to compactionPicker.baseLevel.
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
	flushing []flushable
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

	// grandparents are the tables in level+2 that overlap with the files being
	// compacted. Used to determine output table boundaries.
	grandparents    []fileMetadata
	overlappedBytes uint64 // bytes of overlap with grandparent tables
	seenKey         bool   // some output key has been seen

	metrics map[int]*LevelMetrics
}

func newCompaction(
	opts *Options,
	cur *version,
	startLevel,
	baseLevel int,
	bytesCompacted *uint64,
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
	opts *Options,
	cur *version,
	baseLevel int,
	flushing []flushable,
	bytesFlushed *uint64,
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

	// TODO(peter): When we allow flushing to create multiple tables we'll want
	// to choose sstable boundaries based on the grandparents. But for now we
	// want to create a single table during flushing so this is all commented
	// out.
	if false {
		c.maxOutputFileSize = uint64(opts.Level(0).TargetFileSize)
		c.maxOverlapBytes = maxGrandparentOverlapBytes(opts, 0)
		c.maxExpandedBytes = expandedCompactionByteSizeLimit(opts, 0)

		var smallest InternalKey
		var largest InternalKey
		smallestSet, largestSet := false, false

		updatePointBounds := func(iter internalIterator) {
			if key, _ := iter.First(); key != nil {
				if !smallestSet ||
					base.InternalCompare(c.cmp, smallest, *key) > 0 {
					smallestSet = true
					smallest = key.Clone()
				}
			}
			if key, _ := iter.Last(); key != nil {
				if !largestSet ||
					base.InternalCompare(c.cmp, largest, *key) < 0 {
					largestSet = true
					largest = key.Clone()
				}
			}
		}

		updateRangeBounds := func(iter internalIterator) {
			if key, _ := iter.First(); key != nil {
				if !smallestSet ||
					base.InternalCompare(c.cmp, smallest, *key) > 0 {
					smallestSet = true
					smallest = key.Clone()
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

		c.grandparents = c.version.Overlaps(baseLevel, c.cmp, smallest.UserKey, largest.UserKey)
	}
	return c
}

// setupOtherInputs fills in the rest of the compaction inputs, regardless of
// whether the compaction was automatically scheduled or user initiated.
func (c *compaction) setupOtherInputs() {
	c.inputs[0] = c.expandInputs(c.inputs[0])
	smallest0, largest0 := manifest.KeyRange(c.cmp, c.inputs[0], nil)
	c.inputs[1] = c.version.Overlaps(c.outputLevel, c.cmp, smallest0.UserKey, largest0.UserKey)
	smallest01, largest01 := manifest.KeyRange(c.cmp, c.inputs[0], c.inputs[1])

	// Grow the inputs if it doesn't affect the number of level+1 files.
	if c.grow(smallest01, largest01) {
		smallest01, largest01 = manifest.KeyRange(c.cmp, c.inputs[0], c.inputs[1])
	}

	// Compute the set of outputLevel+1 files that overlap this compaction.
	if c.outputLevel+1 < numLevels {
		c.grandparents = c.version.Overlaps(c.outputLevel+1, c.cmp, smallest01.UserKey, largest01.UserKey)
	}
}

// expandInputs expands the files in inputs[0] in order to maintain the
// invariant that the versions of keys at level+1 are older than the versions
// of keys at level. This is achieved by adding tables to the right of the
// current input tables such that the rightmost table has a "clean cut". A
// clean cut is either a change in user keys, or when the largest key in the
// left sstable is a range tombstone sentinel key
// (InternalKeyRangeDeleteSentinel).
func (c *compaction) expandInputs(inputs []fileMetadata) []fileMetadata {
	if c.startLevel == 0 {
		// We already call version.overlaps for L0 and that call guarantees that we
		// get a "clean cut".
		return inputs
	}
	files := c.version.Files[c.startLevel]
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
		// cur.largest.UserKey == next.largest.UserKey, so we need to include next
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
	grow0 = c.expandInputs(grow0)
	if len(grow0) <= len(c.inputs[0]) {
		return false
	}
	if totalSize(grow0)+totalSize(c.inputs[1]) >= c.maxExpandedBytes {
		return false
	}
	sm1, la1 := manifest.KeyRange(c.cmp, grow0, nil)
	grow1 := c.version.Overlaps(c.outputLevel, c.cmp, sm1.UserKey, la1.UserKey)
	if len(grow1) != len(c.inputs[1]) {
		return false
	}
	c.inputs[0] = grow0
	c.inputs[1] = grow1
	return true
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

// shouldStopBefore returns true if the output to the current table should be
// finished and a new table started before adding the specified key. This is
// done in order to prevent a table at level N from overlapping too much data
// at level N+1. We want to avoid such large overlaps because they translate
// into large compactions. The current heuristic stops output of a table if the
// addition of another key would cause the table to overlap more than 10x the
// target file size at level N. See maxGrandparentOverlapBytes.
//
// TODO(peter): Stopping compaction output in the middle of a user-key creates
// 2 sstables that need to be compacted together as an "atomic compaction
// unit". This is unfortunate as it removes the benefit of stopping output to
// an sstable in order to prevent a large compaction with the next level. Seems
// better to adjust shouldStopBefore to not stop output in the middle of a
// user-key. Perhaps this isn't a problem if the compaction picking heuristics
// always pick the right (older) sibling for compaction first.
func (c *compaction) shouldStopBefore(key InternalKey) bool {
	for len(c.grandparents) > 0 {
		g := &c.grandparents[0]
		if base.InternalCompare(c.cmp, key, g.Largest) <= 0 {
			break
		}
		if c.seenKey {
			c.overlappedBytes += g.Size
		}
		c.grandparents = c.grandparents[1:]
	}
	c.seenKey = true
	if c.overlappedBytes > c.maxOverlapBytes {
		c.overlappedBytes = 0
		return true
	}
	return false
}

// allowZeroSeqNum returns true if seqnum's can be zeroed if there are no
// snapshots requiring them to be kept. It performs this determination by
// looking for an sstable which overlaps the bounds of the compaction at a
// lower level in the LSM.
func (c *compaction) allowZeroSeqNum(iter internalIterator) bool {
	if len(c.flushing) != 0 {
		if len(c.version.Files[0]) > 0 {
			// We can only allow zeroing of seqnum for L0 tables if no other L0 tables
			// exist. Otherwise we may violate the invariant that L0 tables are ordered
			// by increasing seqnum. This could be relaxed with a bit more intelligence
			// in how a new L0 table is merged into the existing set of L0 tables.
			return false
		}
		lower, _ := iter.First()
		upper, _ := iter.Last()
		if lower == nil || upper == nil {
			return false
		}
		return c.elideRangeTombstone(lower.UserKey, upper.UserKey)
	}

	var lower, upper []byte
	for i := range c.inputs {
		files := c.inputs[i]
		for j := range files {
			f := &files[j]
			if lower == nil || c.cmp(lower, f.Smallest.UserKey) > 0 {
				lower = f.Smallest.UserKey
			}
			if upper == nil || c.cmp(upper, f.Largest.UserKey) < 0 {
				upper = f.Largest.UserKey
			}
		}
	}
	// [lower,upper] now cover the bounds of the compaction inputs. Check to see
	// if those bounds overlap an sstable at a lower level.
	return c.elideRangeTombstone(lower, upper)
}

// elideTombstone returns true if it is ok to elide a tombstone for the
// specified key. A return value of true guarantees that there are no key/value
// pairs at c.level+2 or higher that possibly contain the specified user key.
func (c *compaction) elideTombstone(key []byte) bool {
	if len(c.flushing) != 0 {
		return false
	}

	level := c.outputLevel + 1
	if c.outputLevel == 0 {
		// Level 0 can contain overlapping sstables so we need to check it for
		// overlaps.
		level = 0
	}

	// TODO(peter): this can be faster if key is always increasing between
	// successive elideTombstones calls and we can keep some state in between
	// calls.
	for ; level < numLevels; level++ {
		for _, f := range c.version.Files[level] {
			if c.cmp(key, f.Largest.UserKey) <= 0 {
				if c.cmp(key, f.Smallest.UserKey) >= 0 {
					return false
				}
				// For levels below level 0, the files within a level are in
				// increasing ikey order, so we can break early.
				break
			}
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

	level := c.outputLevel + 1
	if c.outputLevel == 0 {
		// Level 0 can contain overlapping sstables so we need to check it for
		// overlaps.
		level = 0
	}

	for ; level < numLevels; level++ {
		overlaps := c.version.Overlaps(level, c.cmp, start, end)
		if len(overlaps) > 0 {
			return false
		}
	}
	return true
}

// atomicUnitBounds returns the bounds of the atomic compaction unit containing
// the specified sstable (identified by a pointer to its fileMetadata).
func (c *compaction) atomicUnitBounds(f *fileMetadata) (lower, upper []byte) {
	for i := range c.inputs {
		files := c.inputs[i]
		for j := range files {
			if f == &files[j] {
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
						// isn't necessary to include the next table in the atomic
						// compaction unit as cur.largest.UserKey does not actually exist
						// in the table.
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
						// in the table.
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
func (c *compaction) newInputIter(
	newIters tableNewIters,
) (_ internalIterator, retErr error) {
	if len(c.flushing) != 0 {
		if len(c.flushing) == 1 {
			f := c.flushing[0]
			iter := f.newFlushIter(nil, &c.bytesIterated)
			if rangeDelIter := f.newRangeDelIter(nil); rangeDelIter != nil {
				return newMergingIter(c.cmp, iter, rangeDelIter), nil
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
		return newMergingIter(c.cmp, iters...), nil
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
			// bound of the atomic compaction unit.
			lowerBound, upperBound := c.atomicUnitBounds(f)
			if lowerBound != nil || upperBound != nil {
				rangeDelIter = rangedel.Truncate(c.cmp, rangeDelIter, lowerBound, upperBound)
			}
		}
		return rangeDelIter, nil, err
	}

	if c.startLevel != 0 {
		iters = append(iters, newLevelIter(nil, c.cmp, newIters, c.inputs[0], &c.bytesIterated))
		iters = append(iters, newLevelIter(nil, c.cmp, newRangeDelIter, c.inputs[0], &c.bytesIterated))
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

	iters = append(iters, newLevelIter(nil, c.cmp, newIters, c.inputs[1], &c.bytesIterated))
	iters = append(iters, newLevelIter(nil, c.cmp, newRangeDelIter, c.inputs[1], &c.bytesIterated))
	return newMergingIter(c.cmp, iters...), nil
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
	level       int
	outputLevel int
	done        chan error
	start       InternalKey
	end         InternalKey
}

func (d *DB) getCompactionPacerInfo() compactionPacerInfo {
	bytesFlushed := atomic.LoadUint64(&d.bytesFlushed)

	d.mu.Lock()
	estimatedMaxWAmp := d.mu.versions.picker.estimatedMaxWAmp
	pacerInfo := compactionPacerInfo{
		slowdownThreshold:   uint64(estimatedMaxWAmp * float64(d.opts.MemTableSize)),
		totalCompactionDebt: d.mu.versions.picker.estimatedCompactionDebt(bytesFlushed),
	}
	for _, m := range d.mu.mem.queue {
		pacerInfo.totalDirtyBytes += m.totalBytes()
	}
	d.mu.Unlock()

	return pacerInfo
}

func (d *DB) getFlushPacerInfo() flushPacerInfo {
	var pacerInfo flushPacerInfo
	d.mu.Lock()
	for _, m := range d.mu.mem.queue {
		pacerInfo.totalBytes += m.totalBytes()
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
	if !d.mu.mem.queue[0].readyForFlush() {
		return
	}

	d.mu.compact.flushing = true
	go d.flush()
}

func (d *DB) flush() {
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
	minUnflushedLogNum, _ := d.mu.mem.queue[n].logInfo()
	if !d.opts.DisableWAL {
		for i := 0; i < n; i++ {
			logNum, _ := d.mu.mem.queue[i].logInfo()
			if logNum >= minUnflushedLogNum {
				return errFlushInvariant
			}
		}
	}

	c := newFlush(d.opts, d.mu.versions.currentVersion(),
		d.mu.versions.picker.baseLevel, d.mu.mem.queue[:n], &d.bytesFlushed)

	jobID := d.mu.nextJobID
	d.mu.nextJobID++
	d.opts.EventListener.FlushBegin(FlushInfo{
		JobID: jobID,
	})

	flushPacer := newFlushPacer(flushPacerEnv{
		limiter:      d.flushLimiter,
		memTableSize: uint64(d.opts.MemTableSize),
		getInfo:      d.getFlushPacerInfo,
	})
	ve, pendingOutputs, err := d.runCompaction(jobID, c, flushPacer)

	info := FlushInfo{
		JobID: jobID,
		Done:  true,
		Err:   err,
	}
	if err == nil {
		for i := range ve.NewFiles {
			e := &ve.NewFiles[i]
			info.Output = append(info.Output, e.Meta.TableInfo(d.opts.FS, d.dirname))
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
			_, size := d.mu.mem.queue[i].logInfo()
			metrics.BytesIn += size
		}

		err = d.mu.versions.logAndApply(jobID, ve, c.metrics, d.dataDir)
		for _, fileNum := range pendingOutputs {
			if _, ok := d.mu.compact.pendingOutputs[fileNum]; !ok {
				panic("pebble: expected pending output not present")
			}
			delete(d.mu.compact.pendingOutputs, fileNum)
			if err != nil {
				// TODO(peter): untested.
				d.mu.versions.obsoleteTables = append(d.mu.versions.obsoleteTables, fileNum)
			}
		}
	}

	d.opts.EventListener.FlushEnd(info)

	// Refresh bytes flushed count.
	atomic.StoreUint64(&d.bytesFlushed, 0)

	var flushed []flushable
	if err == nil {
		flushed = d.mu.mem.queue[:n]
		d.mu.mem.queue = d.mu.mem.queue[n:]
		d.updateReadStateLocked()
	}

	d.deleteObsoleteFiles(jobID)

	// Mark all the memtables we flushed as flushed. Note that we do this last so
	// that a synchronous call to DB.Flush() will not return until the deletion
	// of obsolete files from this job have completed. This makes testing easier
	// and provides similar behavior to manual compactions where the compaction
	// is not marked as completed until the deletion of obsolete files job has
	// completed.
	for i := range flushed {
		close(flushed[i].flushed())
	}
	return err
}

// maybeScheduleCompaction schedules a compaction if necessary.
//
// d.mu must be held when calling this.
func (d *DB) maybeScheduleCompaction() {
	if d.mu.compact.compacting || atomic.LoadInt32(&d.closed) != 0 || d.opts.ReadOnly {
		return
	}

	if len(d.mu.compact.manual) > 0 {
		d.mu.compact.compacting = true
		go d.compact()
		return
	}

	if !d.mu.versions.picker.compactionNeeded() {
		// There is no work to be done.
		return
	}

	d.mu.compact.compacting = true
	go d.compact()
}

// compact runs one compaction and maybe schedules another call to compact.
func (d *DB) compact() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.compact1(); err != nil {
		// TODO(peter): count consecutive compaction errors and backoff.
		d.opts.EventListener.BackgroundError(err)
	}
	d.mu.compact.compacting = false
	// The previous compaction may have produced too many files in a
	// level, so reschedule another compaction if needed.
	d.maybeScheduleCompaction()
	d.mu.compact.cond.Broadcast()
}

// compact1 runs one compaction.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) compact1() (err error) {
	var c *compaction
	if len(d.mu.compact.manual) > 0 {
		manual := d.mu.compact.manual[0]
		d.mu.compact.manual = d.mu.compact.manual[1:]
		c = d.mu.versions.picker.pickManual(d.opts, manual, &d.bytesCompacted)
		defer func() {
			manual.done <- err
		}()
	} else {
		c = d.mu.versions.picker.pickAuto(d.opts, &d.bytesCompacted)
	}
	if c == nil {
		return nil
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
			info.Input.Tables[i] = append(info.Input.Tables[i], m.TableInfo(d.opts.FS, d.dirname))
		}
	}
	d.opts.EventListener.CompactionBegin(info)

	compactionPacer := newCompactionPacer(compactionPacerEnv{
		limiter:      d.compactionLimiter,
		memTableSize: uint64(d.opts.MemTableSize),
		getInfo:      d.getCompactionPacerInfo,
	})
	ve, pendingOutputs, err := d.runCompaction(jobID, c, compactionPacer)

	if err == nil {
		err = d.mu.versions.logAndApply(jobID, ve, c.metrics, d.dataDir)
		for _, fileNum := range pendingOutputs {
			if _, ok := d.mu.compact.pendingOutputs[fileNum]; !ok {
				panic("pebble: expected pending output not present")
			}
			delete(d.mu.compact.pendingOutputs, fileNum)
			if err != nil {
				// TODO(peter): untested.
				d.mu.versions.obsoleteTables = append(d.mu.versions.obsoleteTables, fileNum)
			}
		}
	}

	info.Done = true
	info.Err = err
	if err == nil {
		for i := range ve.NewFiles {
			e := &ve.NewFiles[i]
			info.Output.Tables = append(info.Output.Tables, e.Meta.TableInfo(d.opts.FS, d.dirname))
		}
	}
	d.opts.EventListener.CompactionEnd(info)

	// Update the read state before deleting obsolete files because the
	// read-state update will cause the previous version to be unref'd and if
	// there are no references obsolete tables will be added to the obsolete
	// table list.
	if err == nil {
		d.updateReadStateLocked()
	}
	d.deleteObsoleteFiles(jobID)

	return err
}

// runCompactions runs a compaction that produces new on-disk tables from
// memtables or old on-disk tables.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) runCompaction(jobID int, c *compaction, pacer pacer) (
	ve *versionEdit, pendingOutputs []uint64, retErr error,
) {
	// Check for a trivial move of one table from one level to the next. We avoid
	// such a move if there is lots of overlapping grandparent data. Otherwise,
	// the move could create a parent file that will require a very expensive
	// merge later on.
	if c.trivialMove() {
		meta := &c.inputs[0][0]
		c.metrics = map[int]*LevelMetrics{
			c.outputLevel: &LevelMetrics{
				BytesMoved: meta.Size,
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
			for _, fileNum := range pendingOutputs {
				delete(d.mu.compact.pendingOutputs, fileNum)
			}
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
	iter := newCompactionIter(c.cmp, d.merge, iiter, snapshots,
		c.allowZeroSeqNum(iiter), c.elideTombstone, c.elideRangeTombstone)

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

	newOutput := func() error {
		d.mu.Lock()
		fileNum := d.mu.versions.getNextFileNum()
		d.mu.compact.pendingOutputs[fileNum] = struct{}{}
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
		tw = sstable.NewWriter(file, d.opts, d.opts.Level(c.outputLevel))

		ve.NewFiles = append(ve.NewFiles, newFileEntry{
			Level: c.outputLevel,
			Meta: fileMetadata{
				FileNum: fileNum,
			},
		})
		return nil
	}

	finishOutput := func(key InternalKey) error {
		// NB: clone the key because the data can be held on to by the call to
		// compactionIter.Tombstones via rangedel.Fragmenter.FlushTo.
		key = key.Clone()
		for _, v := range iter.Tombstones(key.UserKey) {
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

		// The handling of range boundaries is a bit complicated.
		if n := len(ve.NewFiles); n > 1 {
			// This is not the first output. Bound the smallest range key by the
			// previous tables largest key.
			prevMeta := &ve.NewFiles[n-2].Meta
			if writerMeta.SmallestRange.UserKey != nil &&
				d.cmp(writerMeta.SmallestRange.UserKey, prevMeta.Largest.UserKey) <= 0 {
				// The range boundary user key is less than or equal to the previous
				// table's largest key. We need the tables to be key-space partitioned,
				// so force the boundary to a key that we know is larger than the
				// previous key.
				//
				// We use seqnum zero since seqnums are in descending order, and our
				// goal is to ensure this forged key does not overlap with the previous
				// file. `InternalKeyRangeDeleteSentinel` is actually the first key
				// kind as key kinds are also in descending order. But, this is OK
				// because choosing seqnum zero is already enough to prevent overlap
				// (the previous file could not end with a key at seqnum zero if this
				// file had a tombstone extending into it).
				writerMeta.SmallestRange = base.MakeInternalKey(
					prevMeta.Largest.UserKey, 0, InternalKeyKindRangeDelete)
			}
		}

		if key.UserKey != nil && writerMeta.LargestRange.UserKey != nil {
			if d.cmp(writerMeta.LargestRange.UserKey, key.UserKey) >= 0 {
				writerMeta.LargestRange = key
				writerMeta.LargestRange.Trailer = InternalKeyRangeDeleteSentinel
			}
		}

		meta.Smallest = writerMeta.Smallest(d.cmp)
		meta.Largest = writerMeta.Largest(d.cmp)
		return nil
	}

	for key, val := iter.First(); key != nil; key, val = iter.Next() {
		atomic.StoreUint64(c.atomicBytesIterated, c.bytesIterated)

		if err := pacer.maybeThrottle(c.bytesIterated); err != nil {
			return nil, pendingOutputs, err
		}

		// TODO(peter,rangedel): Need to incorporate the range tombstones in the
		// shouldStopBefore decision.
		if tw != nil && (tw.EstimatedSize() >= c.maxOutputFileSize || c.shouldStopBefore(*key)) {
			if err := finishOutput(*key); err != nil {
				return nil, pendingOutputs, err
			}
		}

		if tw == nil {
			if err := newOutput(); err != nil {
				return nil, pendingOutputs, err
			}
		}

		if err := tw.Add(*key, val); err != nil {
			return nil, pendingOutputs, err
		}
	}

	if err := finishOutput(InternalKey{}); err != nil {
		return nil, pendingOutputs, err
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
// and adds those to the internal lists of obsolete files. Note that he files
// are not actually deleted by this method. A subsequent call to
// deleteObsoleteFiles must be performed.
func (d *DB) scanObsoleteFiles(list []string) {
	liveFileNums := make(map[uint64]struct{}, len(d.mu.compact.pendingOutputs))
	for fileNum := range d.mu.compact.pendingOutputs {
		liveFileNums[fileNum] = struct{}{}
	}
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

// deleteObsoleteFiles deletes those files that are no longer needed.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) deleteObsoleteFiles(jobID int) {
	// Only allow a single delete obsolete files job to run at a time.
	for d.mu.cleaner.cleaning && d.mu.cleaner.disabled == 0 {
		d.mu.cleaner.cond.Wait()
	}
	if d.mu.cleaner.disabled > 0 {
		// File deletions are currently disabled. When they are re-enabled a new
		// job will be allocated to catch
		return
	}

	d.mu.cleaner.cleaning = true
	defer func() {
		d.mu.cleaner.cleaning = false
		d.mu.cleaner.cond.Signal()
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

	obsoleteTables := d.mu.versions.obsoleteTables
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
	for _, f := range files {
		// We sort to make the order of deletions deterministic, which is nice for
		// tests.
		sort.Slice(f.obsolete, func(i, j int) bool {
			return f.obsolete[i] < f.obsolete[j]
		})
		for _, fileNum := range f.obsolete {
			switch f.fileType {
			case fileTypeLog:
				if d.logRecycler.add(fileNum) {
					continue
				}
			case fileTypeTable:
				d.tableCache.evict(fileNum)
			}

			path := base.MakeFilename(d.opts.FS, d.dirname, f.fileType, fileNum)
			err := d.opts.FS.Remove(path)
			if err == os.ErrNotExist {
				continue
			}

			// TODO(peter): need to handle this errror, probably by re-adding the
			// file that couldn't be deleted to one of the obsolete slices map.

			switch f.fileType {
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
