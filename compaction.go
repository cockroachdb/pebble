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
	"sort"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/petermattis/pebble/internal/base"
	"github.com/petermattis/pebble/internal/rangedel"
	"github.com/petermattis/pebble/internal/rate"
	"github.com/petermattis/pebble/sstable"
	"github.com/petermattis/pebble/vfs"
)

var errEmptyTable = errors.New("pebble: empty table")

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

// compaction is a table compaction from one level to the next, starting from a
// given version.
type compaction struct {
	cmp     Compare
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
	// inputs are the tables to be compacted.
	inputs [2][]fileMetadata

	// grandparents are the tables in level+2 that overlap with the files being
	// compacted. Used to determine output table boundaries.
	grandparents    []fileMetadata
	overlappedBytes uint64 // bytes of overlap with grandparent tables
	seenKey         bool   // some output key has been seen
}

func newCompaction(opts *Options, cur *version, startLevel, baseLevel int) *compaction {
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
		cmp:               opts.Comparer.Compare,
		version:           cur,
		startLevel:        startLevel,
		outputLevel:       outputLevel,
		maxOutputFileSize: uint64(opts.Level(adjustedOutputLevel).TargetFileSize),
		maxOverlapBytes:   maxGrandparentOverlapBytes(opts, adjustedOutputLevel),
		maxExpandedBytes:  expandedCompactionByteSizeLimit(opts, adjustedOutputLevel),
	}
}

func newFlush(opts *Options, cur *version, baseLevel int, flushing []flushable) *compaction {
	c := &compaction{
		cmp:               opts.Comparer.Compare,
		version:           cur,
		startLevel:        -1,
		outputLevel:       0,
		maxOutputFileSize: math.MaxUint64,
		maxOverlapBytes:   math.MaxUint64,
		maxExpandedBytes:  math.MaxUint64,
		flushing:          flushing,
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

		c.grandparents = c.version.overlaps(baseLevel, c.cmp, smallest.UserKey, largest.UserKey)
	}
	return c
}

// setupOtherInputs fills in the rest of the compaction inputs, regardless of
// whether the compaction was automatically scheduled or user initiated.
func (c *compaction) setupOtherInputs() {
	c.inputs[0] = c.expandInputs(c.inputs[0])
	smallest0, largest0 := ikeyRange(c.cmp, c.inputs[0], nil)
	c.inputs[1] = c.version.overlaps(c.outputLevel, c.cmp, smallest0.UserKey, largest0.UserKey)
	smallest01, largest01 := ikeyRange(c.cmp, c.inputs[0], c.inputs[1])

	// Grow the inputs if it doesn't affect the number of level+1 files.
	if c.grow(smallest01, largest01) {
		smallest01, largest01 = ikeyRange(c.cmp, c.inputs[0], c.inputs[1])
	}

	// Compute the set of outputLevel+1 files that overlap this compaction.
	if c.outputLevel+1 < numLevels {
		c.grandparents = c.version.overlaps(c.outputLevel+1, c.cmp, smallest01.UserKey, largest01.UserKey)
	}
}

// expandInputs expands the files in inputs[0] in order to maintain the
// invariant that the versions of keys at level+1 are older than the versions
// of keys at level. This is achieved by adding tables to the right of the
// current input tables such that the rightmost table has a "clean cut". A
// clean cut is either a change in user keys, or
func (c *compaction) expandInputs(inputs []fileMetadata) []fileMetadata {
	if c.startLevel == 0 {
		// We already call version.overlaps for L0 and that call guarantees that we
		// get a "clean cut".
		return inputs
	}
	files := c.version.files[c.startLevel]
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
		if c.cmp(cur.largest.UserKey, next.smallest.UserKey) < 0 {
			break
		}
		if cur.largest.Trailer == InternalKeyRangeDeleteSentinel {
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
	grow0 := c.version.overlaps(c.startLevel, c.cmp, sm.UserKey, la.UserKey)
	grow0 = c.expandInputs(grow0)
	if len(grow0) <= len(c.inputs[0]) {
		return false
	}
	if totalSize(grow0)+totalSize(c.inputs[1]) >= c.maxExpandedBytes {
		return false
	}
	sm1, la1 := ikeyRange(c.cmp, grow0, nil)
	grow1 := c.version.overlaps(c.outputLevel, c.cmp, sm1.UserKey, la1.UserKey)
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
		if base.InternalCompare(c.cmp, key, g.largest) <= 0 {
			break
		}
		if c.seenKey {
			c.overlappedBytes += g.size
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
		if len(c.version.files[0]) > 0 {
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
			if lower == nil || c.cmp(lower, f.smallest.UserKey) > 0 {
				lower = f.smallest.UserKey
			}
			if upper == nil || c.cmp(upper, f.largest.UserKey) < 0 {
				upper = f.largest.UserKey
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
		for _, f := range c.version.files[level] {
			if c.cmp(key, f.largest.UserKey) <= 0 {
				if c.cmp(key, f.smallest.UserKey) >= 0 {
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
		overlaps := c.version.overlaps(level, c.cmp, start, end)
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
				lowerBound := f.smallest.UserKey
				for k := j; k > 0; k-- {
					cur := &files[k]
					prev := &files[k-1]
					if c.cmp(prev.largest.UserKey, cur.smallest.UserKey) < 0 {
						break
					}
					if prev.largest.Trailer == InternalKeyRangeDeleteSentinel {
						// The range deletion sentinel key is set for the largest key in a
						// table when a range deletion tombstone straddles a table. It
						// isn't necessary to include the next table in the atomic
						// compaction unit as cur.largest.UserKey does not actually exist
						// in the table.
						break
					}
					lowerBound = prev.smallest.UserKey
				}

				upperBound := f.largest.UserKey
				for k := j + 1; k < len(files); k++ {
					cur := &files[k-1]
					next := &files[k]
					if c.cmp(cur.largest.UserKey, next.smallest.UserKey) < 0 {
						break
					}
					if cur.largest.Trailer == InternalKeyRangeDeleteSentinel {
						// The range deletion sentinel key is set for the largest key in a
						// table when a range deletion tombstone straddles a table. It
						// isn't necessary to include the next table in the atomic
						// compaction unit as cur.largest.UserKey does not actually exist
						// in the table.
						break
					}
					// cur.largest.UserKey == next.largest.UserKey, so next is part of
					// the atomic compaction unit.
					upperBound = next.largest.UserKey
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
				return nil, fmt.Errorf("pebble: could not open table %d: %v", f.fileNum, err)
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
			fmt.Fprintf(&buf, " %d:%s-%s", f.fileNum, f.smallest, f.largest)
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

// maybeScheduleFlush schedules a flush if necessary.
//
// d.mu must be held when calling this.
func (d *DB) maybeScheduleFlush() {
	if d.mu.compact.flushing || atomic.LoadInt32(&d.closed) != 0 {
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
		if d.opts.EventListener.BackgroundError != nil {
			d.opts.EventListener.BackgroundError(err)
		}
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

	c := newFlush(d.opts, d.mu.versions.currentVersion(),
		d.mu.versions.picker.baseLevel, d.mu.mem.queue[:n])

	jobID := d.mu.nextJobID
	d.mu.nextJobID++
	if d.opts.EventListener.FlushBegin != nil {
		d.opts.EventListener.FlushBegin(FlushInfo{
			JobID: jobID,
		})
	}

	ve, pendingOutputs, err := d.runCompaction(c)

	if d.opts.EventListener.FlushEnd != nil {
		info := FlushInfo{
			JobID: jobID,
			Err:   err,
		}
		if err == nil {
			for i := range ve.newFiles {
				e := &ve.newFiles[i]
				info.Output = append(info.Output, e.meta.tableInfo(d.dirname))
			}
			if len(ve.newFiles) == 0 {
				info.Err = errEmptyTable
			}
		}
		d.opts.EventListener.FlushEnd(info)
	}

	if err != nil {
		return err
	}

	// The flush succeeded or it produced an empty sstable. In either case we
	// want to bump the log number.
	ve.logNumber, _ = d.mu.mem.queue[n].logInfo()
	metrics := ve.metrics[0]
	for i := 0; i < n; i++ {
		_, size := d.mu.mem.queue[i].logInfo()
		metrics.BytesIn += size
	}

	err = d.mu.versions.logAndApply(jobID, ve, d.dataDir)
	for _, fileNum := range pendingOutputs {
		if _, ok := d.mu.compact.pendingOutputs[fileNum]; !ok {
			panic("pebble: expected pending output not present")
		}
		delete(d.mu.compact.pendingOutputs, fileNum)
	}
	if err != nil {
		return err
	}

	// Refresh bytes flushed count.
	atomic.StoreUint64(&d.bytesFlushed, 0)

	flushed := d.mu.mem.queue[:n]
	d.mu.mem.queue = d.mu.mem.queue[n:]
	d.updateReadStateLocked()
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
	return nil
}

// maybeScheduleCompaction schedules a compaction if necessary.
//
// d.mu must be held when calling this.
func (d *DB) maybeScheduleCompaction() {
	if d.mu.compact.compacting || atomic.LoadInt32(&d.closed) != 0 {
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
		if d.opts.EventListener.BackgroundError != nil {
			d.opts.EventListener.BackgroundError(err)
		}
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
		c = d.mu.versions.picker.pickManual(d.opts, manual)
		defer func() {
			manual.done <- err
		}()
	} else {
		c = d.mu.versions.picker.pickAuto(d.opts)
	}
	if c == nil {
		return nil
	}

	jobID := d.mu.nextJobID
	d.mu.nextJobID++
	info := CompactionInfo{
		JobID: jobID,
	}
	if d.opts.EventListener.CompactionBegin != nil || d.opts.EventListener.CompactionEnd != nil {
		info.Input.Level = c.startLevel
		info.Output.Level = c.outputLevel
		for i := range c.inputs {
			for j := range c.inputs[i] {
				m := &c.inputs[i][j]
				info.Input.Tables[i] = append(info.Input.Tables[i], m.tableInfo(d.dirname))
			}
		}
	}
	if d.opts.EventListener.CompactionBegin != nil {
		d.opts.EventListener.CompactionBegin(info)
	}

	ve, pendingOutputs, err := d.runCompaction(c)

	if d.opts.EventListener.CompactionEnd != nil {
		info.Err = err
		if err == nil {
			for i := range ve.newFiles {
				e := &ve.newFiles[i]
				info.Output.Tables = append(info.Output.Tables, e.meta.tableInfo(d.dirname))
			}
		}
		d.opts.EventListener.CompactionEnd(info)
	}

	if err != nil {
		return err
	}
	err = d.mu.versions.logAndApply(jobID, ve, d.dataDir)
	for _, fileNum := range pendingOutputs {
		if _, ok := d.mu.compact.pendingOutputs[fileNum]; !ok {
			panic("pebble: expected pending output not present")
		}
		delete(d.mu.compact.pendingOutputs, fileNum)
	}
	if err != nil {
		return err
	}

	d.updateReadStateLocked()
	d.deleteObsoleteFiles(jobID)
	return nil
}

// runCompactions runs a compaction that produces new on-disk tables from
// memtables or old on-disk tables.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) runCompaction(c *compaction) (
	ve *versionEdit, pendingOutputs []uint64, retErr error,
) {
	// Check for a trivial move of one table from one level to the next. We avoid
	// such a move if there is lots of overlapping grandparent data. Otherwise,
	// the move could create a parent file that will require a very expensive
	// merge later on.
	if c.trivialMove() {
		meta := &c.inputs[0][0]
		return &versionEdit{
			deletedFiles: map[deletedFileEntry]bool{
				deletedFileEntry{level: c.startLevel, fileNum: meta.fileNum}: true,
			},
			newFiles: []newFileEntry{
				{level: c.outputLevel, meta: *meta},
			},
			metrics: map[int]*LevelMetrics{
				c.outputLevel: &LevelMetrics{
					BytesMoved: meta.size,
				},
			},
		}, nil, nil
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

	metrics := &LevelMetrics{
		BytesIn:   totalSize(c.inputs[0]),
		BytesRead: totalSize(c.inputs[1]),
	}
	metrics.BytesRead += metrics.BytesIn

	ve = &versionEdit{
		deletedFiles: map[deletedFileEntry]bool{},
		metrics: map[int]*LevelMetrics{
			c.outputLevel: metrics,
		},
	}

	newOutput := func() error {
		d.mu.Lock()
		fileNum := d.mu.versions.nextFileNum()
		d.mu.compact.pendingOutputs[fileNum] = struct{}{}
		pendingOutputs = append(pendingOutputs, fileNum)
		d.mu.Unlock()

		filename := dbFilename(d.dirname, fileTypeTable, fileNum)
		file, err := d.opts.FS.Create(filename)
		if err != nil {
			return err
		}
		file = vfs.NewSyncingFile(file, vfs.SyncingFileOptions{
			BytesPerSync: d.opts.BytesPerSync,
		})
		filenames = append(filenames, filename)
		tw = sstable.NewWriter(file, d.opts, d.opts.Level(c.outputLevel))

		ve.newFiles = append(ve.newFiles, newFileEntry{
			level: c.outputLevel,
			meta: fileMetadata{
				fileNum: fileNum,
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
		meta := &ve.newFiles[len(ve.newFiles)-1].meta
		meta.size = writerMeta.Size
		meta.smallestSeqNum = writerMeta.SmallestSeqNum
		meta.largestSeqNum = writerMeta.LargestSeqNum

		metrics.BytesWritten += meta.size

		// The handling of range boundaries is a bit complicated.
		if n := len(ve.newFiles); n > 1 {
			// This is not the first output. Bound the smallest range key by the
			// previous tables largest key.
			prevMeta := &ve.newFiles[n-2].meta
			if writerMeta.SmallestRange.UserKey != nil &&
				d.cmp(writerMeta.SmallestRange.UserKey, prevMeta.largest.UserKey) <= 0 {
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
					prevMeta.largest.UserKey, 0, InternalKeyKindRangeDelete)
			}
		}

		if key.UserKey != nil && writerMeta.LargestRange.UserKey != nil {
			if d.cmp(writerMeta.LargestRange.UserKey, key.UserKey) >= 0 {
				writerMeta.LargestRange = key
				writerMeta.LargestRange.Trailer = InternalKeyRangeDeleteSentinel
			}
		}

		meta.smallest = writerMeta.Smallest(d.cmp)
		meta.largest = writerMeta.Largest(d.cmp)
		return nil
	}

	var prevBytesIterated uint64
	var iterCount int
	totalBytes := d.memTableTotalBytes()
	refreshDirtyBytesThreshold := uint64(d.opts.MemTableSize * 5 / 100)

	var compactionSlowdownThreshold uint64
	var totalCompactionDebt uint64
	var estimatedMaxWAmp float64

	for key, val := iter.First(); key != nil; key, val = iter.Next() {
		// Slow down memtable flushing to match fill rate.
		if c.flushing != nil {
			// Recalculate total memtable bytes only once every 1000 iterations or
			// when the refresh threshold is hit since getting the total memtable
			// byte count requires grabbing DB.mu which is expensive.
			if iterCount >= 1000 || c.bytesIterated > refreshDirtyBytesThreshold {
				totalBytes = d.memTableTotalBytes()
				refreshDirtyBytesThreshold = c.bytesIterated + uint64(d.opts.MemTableSize*5/100)
				iterCount = 0
			}
			iterCount++

			// dirtyBytes is the total number of bytes in the memtables minus the number of
			// bytes flushed. It represents unflushed bytes in all the memtables, even the
			// ones which aren't being flushed such as the mutable memtable.
			dirtyBytes := totalBytes - c.bytesIterated
			flushAmount := c.bytesIterated - prevBytesIterated
			prevBytesIterated = c.bytesIterated

			atomic.StoreUint64(&d.bytesFlushed, c.bytesIterated)

			// We slow down memtable flushing when the dirty bytes indicator falls
			// below the low watermark, which is 105% memtable size. This will only
			// occur if memtable flushing can keep up with the pace of incoming
			// writes. If writes come in faster than how fast the memtable can flush,
			// flushing proceeds at maximum (unthrottled) speed.
			if dirtyBytes <= uint64(d.opts.MemTableSize*105/100) {
				burst := d.flushLimiter.Burst()
				for flushAmount > uint64(burst) {
					err := d.flushLimiter.WaitN(context.Background(), burst)
					if err != nil {
						return nil, pendingOutputs, err
					}
					flushAmount -= uint64(burst)
				}
				err := d.flushLimiter.WaitN(context.Background(), int(flushAmount))
				if err != nil {
					return nil, pendingOutputs, err
				}
			} else {
				burst := d.flushLimiter.Burst()
				for flushAmount > uint64(burst) {
					d.flushLimiter.AllowN(time.Now(), burst)
					flushAmount -= uint64(burst)
				}
				d.flushLimiter.AllowN(time.Now(), int(flushAmount))
			}
		} else {
			bytesFlushed := atomic.LoadUint64(&d.bytesFlushed)

			if iterCount >= 1000 || c.bytesIterated > refreshDirtyBytesThreshold {
				d.mu.Lock()
				estimatedMaxWAmp = d.mu.versions.picker.estimatedMaxWAmp()
				// compactionSlowdownThreshold is the low watermark for compaction debt. If compaction
				// debt is below this threshold, we slow down compactions. If compaction debt is above
				// this threshold, we let compactions continue as fast as possible. We want to keep
				// compaction debt as low as possible to match the speed of flushes. This threshold
				// is set so that a single flush cannot contribute enough compaction debt to overshoot
				// the threshold.
				compactionSlowdownThreshold = uint64(estimatedMaxWAmp * float64(d.opts.MemTableSize))
				totalCompactionDebt = d.mu.versions.picker.estimatedCompactionDebt(bytesFlushed)
				d.mu.Unlock()
				refreshDirtyBytesThreshold = c.bytesIterated + uint64(d.opts.MemTableSize*5/100)
				iterCount = 0
			}
			iterCount++

			var curCompactionDebt uint64
			if totalCompactionDebt > c.bytesIterated {
				curCompactionDebt = totalCompactionDebt - c.bytesIterated
			}

			// Set the minimum compaction rate to match the minimum flush rate.
			d.compactionLimiter.SetLimit(rate.Limit(float64(d.opts.MinFlushRate) * estimatedMaxWAmp))

			compactAmount := c.bytesIterated - prevBytesIterated
			// We slow down compactions when the compaction debt falls below the slowdown
			// threshold, which is set dynamically based on the number of non-empty levels.
			// This will only occur if compactions can keep up with the pace of flushes. If
			// bytes are flushed faster than how fast compactions can occur, compactions
			// proceed at maximum (unthrottled) speed.
			if curCompactionDebt <= compactionSlowdownThreshold {
				burst := d.compactionLimiter.Burst()
				for compactAmount > uint64(burst) {
					err := d.compactionLimiter.WaitN(context.Background(), burst)
					if err != nil {
						return nil, pendingOutputs, err
					}
					compactAmount -= uint64(burst)
				}
				err := d.compactionLimiter.WaitN(context.Background(), int(compactAmount))
				if err != nil {
					return nil, pendingOutputs, err
				}
			} else {
				burst := d.compactionLimiter.Burst()
				for compactAmount > uint64(burst) {
					d.compactionLimiter.AllowN(time.Now(), burst)
					compactAmount -= uint64(burst)
				}
				d.compactionLimiter.AllowN(time.Now(), int(compactAmount))
			}

			prevBytesIterated = c.bytesIterated
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
			ve.deletedFiles[deletedFileEntry{
				level:   level,
				fileNum: f.fileNum,
			}] = true
		}
	}

	if err := d.dataDir.Sync(); err != nil {
		return nil, pendingOutputs, err
	}
	return ve, pendingOutputs, nil
}

// memTableTotalBytes returns the total number of bytes in the memtables. Note
// that this includes the mutable memtable as well.
func (d *DB) memTableTotalBytes() (totalBytes uint64) {
	d.mu.Lock()
	for _, m := range d.mu.mem.queue {
		totalBytes += m.totalBytes()
	}
	d.mu.Unlock()
	return totalBytes
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
	logNumber := d.mu.versions.logNumber
	manifestFileNumber := d.mu.versions.manifestFileNumber

	var obsoleteLogs []uint64
	var obsoleteTables []uint64
	var obsoleteManifests []uint64
	var obsoleteOptions []uint64

	for _, filename := range list {
		fileType, fileNum, ok := parseDBFilename(filename)
		if !ok {
			continue
		}
		switch fileType {
		case fileTypeLog:
			// TODO(peter): also look at prevLogNumber?
			if fileNum >= logNumber {
				continue
			}
			obsoleteLogs = append(obsoleteLogs, fileNum)
		case fileTypeManifest:
			if fileNum >= manifestFileNumber {
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

// deleteObsoleteFiles deletes those files that are no longer needed.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) deleteObsoleteFiles(jobID int) {
	// Only allow a single delete obsolete files job to run at a time.
	for d.mu.cleaner.cleaning {
		d.mu.cleaner.cond.Wait()
	}
	d.mu.cleaner.cleaning = true
	defer func() {
		d.mu.cleaner.cleaning = false
		d.mu.cleaner.cond.Signal()
	}()

	var obsoleteLogs []uint64
	for i := range d.mu.log.queue {
		// NB: d.mu.versions.logNumber is the file number of the latest log that
		// has had its contents persisted to the LSM.
		if d.mu.log.queue[i] >= d.mu.versions.logNumber {
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

			path := dbFilename(d.dirname, f.fileType, fileNum)
			err := d.opts.FS.Remove(path)
			if err == os.ErrNotExist {
				continue
			}

			// TODO(peter): need to handle this errror, probably by re-adding the
			// file that couldn't be deleted to one of the obsolete slices map.

			switch f.fileType {
			case fileTypeLog:
				if d.opts.EventListener.WALDeleted != nil {
					d.opts.EventListener.WALDeleted(WALDeleteInfo{
						JobID:   jobID,
						Path:    path,
						FileNum: fileNum,
						Err:     err,
					})
				}
			case fileTypeManifest:
				if d.opts.EventListener.ManifestDeleted != nil {
					d.opts.EventListener.ManifestDeleted(ManifestDeleteInfo{
						JobID:   jobID,
						Path:    path,
						FileNum: fileNum,
						Err:     err,
					})
				}
			case fileTypeTable:
				if d.opts.EventListener.TableDeleted != nil {
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
