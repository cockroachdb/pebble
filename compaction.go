// Copyright 2013 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"unsafe"

	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/sstable"
	"github.com/petermattis/pebble/storage"
)

var errEmptyTable = errors.New("pebble: empty table")

// expandedCompactionByteSizeLimit is the maximum number of bytes in all
// compacted files. We avoid expanding the lower level file set of a compaction
// if it would make the total compaction cover more than this many bytes.
func expandedCompactionByteSizeLimit(opts *db.Options, level int) uint64 {
	return uint64(25 * opts.Level(level).TargetFileSize)
}

// maxGrandparentOverlapBytes is the maximum bytes of overlap with level+2
// before we stop building a single file in a level to level+1 compaction.
func maxGrandparentOverlapBytes(opts *db.Options, level int) uint64 {
	return uint64(10 * opts.Level(level).TargetFileSize)
}

// compaction is a table compaction from one level to the next, starting from a
// given version.
type compaction struct {
	cmp     *db.Comparer
	version *version

	// level is the level that is being compacted. Inputs from level and
	// level+1 will be merged to produce a set of level+1 files.
	level int

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

	// inputs are the tables to be compacted.
	inputs [2][]fileMetadata

	// grandparents are the tables in level+2 that overlap with the files being
	// compacted. Used to determine output table boundaries.
	grandparents    []fileMetadata
	overlappedBytes uint64 // bytes of overlap with grandparent tables
	seenKey         bool   // some output key has been seen
}

func newCompaction(opts *db.Options, cur *version, level int) *compaction {
	c := &compaction{
		cmp:               opts.Comparer,
		version:           cur,
		level:             level,
		maxOutputFileSize: uint64(opts.Level(level + 1).TargetFileSize),
		maxOverlapBytes:   maxGrandparentOverlapBytes(opts, level+1),
		maxExpandedBytes:  expandedCompactionByteSizeLimit(opts, level+1),
	}
	return c
}

// setupOtherInputs fills in the rest of the compaction inputs, regardless of
// whether the compaction was automatically scheduled or user initiated.
func (c *compaction) setupOtherInputs() {
	c.inputs[0] = c.expandInputs(c.inputs[0])
	smallest0, largest0 := ikeyRange(c.cmp.Compare, c.inputs[0], nil)
	c.inputs[1] = c.version.overlaps(c.level+1, c.cmp.Compare, smallest0.UserKey, largest0.UserKey)
	smallest01, largest01 := ikeyRange(c.cmp.Compare, c.inputs[0], c.inputs[1])

	// Grow the inputs if it doesn't affect the number of level+1 files.
	if c.grow(smallest01, largest01) {
		smallest01, largest01 = ikeyRange(c.cmp.Compare, c.inputs[0], c.inputs[1])
	}

	// Compute the set of level+2 files that overlap this compaction.
	if c.level+2 < numLevels {
		c.grandparents = c.version.overlaps(c.level+2, c.cmp.Compare, smallest01.UserKey, largest01.UserKey)
	}
}

// expandInputs expands the files in inputs[0] in order to maintain the
// invariant that the versions of keys at level+1 are older than the versions
// of keys at level. This is achieved by adding tables to the right of the
// current input tables such that the rightmost table has a "clean cut". A
// clean cut is either a change in user keys, or
func (c *compaction) expandInputs(inputs []fileMetadata) []fileMetadata {
	if c.level == 0 {
		// We already call version.overlaps for L0 and that call guarantees that we
		// get a "clean cut".
		return inputs
	}
	files := c.version.files[c.level]
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
		next := files[end]
		if c.cmp.Compare(cur.largest.UserKey, next.smallest.UserKey) < 0 {
			break
		}
		if cur.largest.Trailer == db.InternalKeyRangeDeleteSentinel {
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
func (c *compaction) grow(sm, la db.InternalKey) bool {
	if len(c.inputs[1]) == 0 {
		return false
	}
	grow0 := c.version.overlaps(c.level, c.cmp.Compare, sm.UserKey, la.UserKey)
	grow0 = c.expandInputs(grow0)
	if len(grow0) <= len(c.inputs[0]) {
		return false
	}
	if totalSize(grow0)+totalSize(c.inputs[1]) >= c.maxExpandedBytes {
		return false
	}
	sm1, la1 := ikeyRange(c.cmp.Compare, grow0, nil)
	grow1 := c.version.overlaps(c.level+1, c.cmp.Compare, sm1.UserKey, la1.UserKey)
	if len(grow1) != len(c.inputs[1]) {
		return false
	}
	c.inputs[0] = grow0
	c.inputs[1] = grow1
	return true
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
func (c *compaction) shouldStopBefore(key db.InternalKey) bool {
	for len(c.grandparents) > 0 {
		g := &c.grandparents[0]
		if db.InternalCompare(c.cmp.Compare, key, g.largest) <= 0 {
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

// elideTombstone returns true if it is ok to elide a tombstone for the
// specified key. A return value of true guarantees that there are no key/value
// pairs at c.level+2 or higher that possibly contain the specified user key.
func (c *compaction) elideTombstone(key []byte) bool {
	// TODO(peter): this can be faster if ukey is always increasing between
	// successive elideTombstones calls and we can keep some state in between
	// calls.
	for level := c.level + 2; level < numLevels; level++ {
		for _, f := range c.version.files[level] {
			if c.cmp.Compare(key, f.largest.UserKey) <= 0 {
				if c.cmp.Compare(key, f.smallest.UserKey) >= 0 {
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
// pairs at c.level+2 or higher that possibly overlap the specified tombstone.
func (c *compaction) elideRangeTombstone(start, end []byte) bool {
	for level := c.level + 2; level < numLevels; level++ {
		overlaps := c.version.overlaps(level, c.cmp.Compare, start, end)
		if len(overlaps) > 0 {
			return false
		}
	}
	return true
}

// newInputIter returns an iterator over all the input tables in a compaction.
func (c *compaction) newInputIter(
	newIters tableNewIters,
) (_ internalIterator, retErr error) {
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
	newRangeDelIter := func(f *fileMetadata) (internalIterator, internalIterator, error) {
		iter, rangeDelIter, err := newIters(f)
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
		return rangeDelIter, nil, err
	}

	// TODO(peter,rangedel): test that range tombstones are properly included in
	// the output sstable.
	if c.level != 0 {
		iters = append(iters, newLevelIter(nil, c.cmp.Compare, newIters, c.inputs[0]))
		iters = append(iters, newLevelIter(nil, c.cmp.Compare, newRangeDelIter, c.inputs[0]))
	} else {
		for i := range c.inputs[0] {
			f := &c.inputs[0][i]
			iter, rangeDelIter, err := newIters(f)
			if err != nil {
				return nil, fmt.Errorf("pebble: could not open table %d: %v", f.fileNum, err)
			}
			iters = append(iters, iter)
			if rangeDelIter != nil {
				iters = append(iters, rangeDelIter)
			}
		}
	}

	iters = append(iters, newLevelIter(nil, c.cmp.Compare, newIters, c.inputs[1]))
	iters = append(iters, newLevelIter(nil, c.cmp.Compare, newRangeDelIter, c.inputs[1]))
	return newMergingIter(c.cmp, iters...), nil
}

func (c *compaction) String() string {
	var buf bytes.Buffer
	for i := range c.inputs {
		fmt.Fprintf(&buf, "%d:", i+c.level)
		for _, f := range c.inputs[i] {
			fmt.Fprintf(&buf, " %d:%s-%s", f.fileNum, f.smallest, f.largest)
		}
		fmt.Fprintf(&buf, "\n")
	}
	return buf.String()
}

type manualCompaction struct {
	level int
	done  chan error
	start db.InternalKey
	end   db.InternalKey
}

// maybeScheduleFlush schedules a flush if necessary.
//
// d.mu must be held when calling this.
func (d *DB) maybeScheduleFlush() {
	if d.mu.compact.flushing || d.mu.closed {
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
		// TODO(peter): count consecutive compaction errors and backoff.
		_ = err
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
	// var dirty int
	// for _, mem := range d.mu.mem.queue {
	// 	dirty += mem.ApproximateMemoryUsage()
	// }

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

	// TODO(peter,rangedel): test that range tombstones are properly included in
	// the output sstable. Should propbably pull out the code below into a method
	// that can be separately tested.
	var iter internalIterator
	if n == 1 {
		mem := d.mu.mem.queue[0]
		iter = mem.newIter(nil)
		if rangeDelIter := mem.newRangeDelIter(nil); rangeDelIter != nil {
			iter = newMergingIter(d.cmp, iter, rangeDelIter)
		}
	} else {
		iters := make([]internalIterator, 0, 2*n)
		for i := 0; i < n; i++ {
			mem := d.mu.mem.queue[i]
			iters = append(iters, mem.newIter(nil))
			rangeDelIter := mem.newRangeDelIter(nil)
			if rangeDelIter != nil {
				iters = append(iters, rangeDelIter)
			}
		}
		iter = newMergingIter(d.cmp, iters...)
	}

	jobID := d.mu.nextJobID
	d.mu.nextJobID++
	if d.opts.EventListener != nil && d.opts.EventListener.FlushBegin != nil {
		d.opts.EventListener.FlushBegin(db.FlushInfo{
			JobID: jobID,
		})
	}

	meta, err := d.writeLevel0Table(d.opts.Storage, iter,
		true /* allowRangeTombstoneElision */)

	if d.opts.EventListener != nil && d.opts.EventListener.FlushEnd != nil {
		info := db.FlushInfo{
			JobID: jobID,
			Err:   err,
		}
		if err == nil {
			info.Output = meta.tableInfo(d.dirname)
		}
		d.opts.EventListener.FlushEnd(info)
	}

	if err == errEmptyTable {
		// The flush succeed, but produced an empty sstable. Mark all the
		// memtables we flushed as flushed.
		for i := 0; i < n; i++ {
			close(d.mu.mem.queue[i].flushed())
		}
		d.mu.mem.queue = d.mu.mem.queue[n:]
		return nil
	}

	if err != nil {
		return err
	}

	err = d.mu.versions.logAndApply(&versionEdit{
		logNumber: d.mu.log.number,
		newFiles: []newFileEntry{
			{level: 0, meta: meta},
		},
	})
	if _, ok := d.mu.compact.pendingOutputs[meta.fileNum]; !ok {
		panic("pebble: expected pending output not present")
	}
	delete(d.mu.compact.pendingOutputs, meta.fileNum)
	if err != nil {
		return err
	}

	// Mark all the memtables we flushed as flushed.
	for i := 0; i < n; i++ {
		close(d.mu.mem.queue[i].flushed())
	}
	d.mu.mem.queue = d.mu.mem.queue[n:]

	// var newDirty int
	// for _, mem := range d.mu.mem.queue {
	// 	newDirty += mem.ApproximateMemoryUsage()
	// }
	// fmt.Printf("flushed %d: %.1f MB -> %.1f MB\n",
	// 	n, float64(dirty)/(1<<20), float64(newDirty)/(1<<20))

	d.deleteObsoleteFiles(jobID)
	return nil
}

// writeLevel0Table writes a memtable to a level-0 on-disk table.
//
// If no error is returned, it adds the file number of that on-disk table to
// d.pendingOutputs. It is the caller's responsibility to remove that fileNum
// from that set when it has been applied to d.mu.versions.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) writeLevel0Table(
	fs storage.Storage, iiter internalIterator, allowRangeTombstoneElision bool,
) (meta fileMetadata, err error) {
	meta.fileNum = d.mu.versions.nextFileNum()
	filename := dbFilename(d.dirname, fileTypeTable, meta.fileNum)
	d.mu.compact.pendingOutputs[meta.fileNum] = struct{}{}
	defer func(fileNum uint64) {
		if err != nil {
			delete(d.mu.compact.pendingOutputs, fileNum)
		}
	}(meta.fileNum)

	snapshots := d.mu.snapshots.toSlice()
	version := d.mu.versions.currentVersion()

	// Release the d.mu lock while doing I/O.
	// Note the unusual order: Unlock and then Lock.
	d.mu.Unlock()
	defer d.mu.Lock()

	elideRangeTombstone := func(start, end []byte) bool {
		if !allowRangeTombstoneElision {
			return false
		}
		for level := 0; level < numLevels; level++ {
			overlaps := version.overlaps(level, d.cmp.Compare, start, end)
			if len(overlaps) > 0 {
				return false
			}
		}
		return true
	}

	iter := newCompactionIter(
		d.cmp.Compare, d.merge, iiter, snapshots,
		func([]byte) bool { return false },
		elideRangeTombstone,
	)
	var (
		file storage.File
		tw   *sstable.Writer
	)
	defer func() {
		if iter != nil {
			err = firstError(err, iter.Close())
		}
		if tw != nil {
			err = firstError(err, tw.Close())
		}
		if err != nil {
			fs.Remove(filename)
			meta = fileMetadata{}
		}
	}()

	file, err = fs.Create(filename)
	if err != nil {
		return fileMetadata{}, err
	}
	file = newRateLimitedFile(file, d.flushController)
	tw = sstable.NewWriter(file, d.opts, d.opts.Level(0))

	var count int
	for valid := iter.First(); valid; valid = iter.Next() {
		if err1 := tw.Add(iter.Key(), iter.Value()); err1 != nil {
			return fileMetadata{}, err1
		}
		count++
	}

	for _, v := range iter.Tombstones(nil) {
		if err1 := tw.Add(v.Start, v.End); err1 != nil {
			return fileMetadata{}, err1
		}
		count++
	}

	if err1 := iter.Close(); err1 != nil {
		iter = nil
		return fileMetadata{}, err1
	}
	iter = nil

	if err1 := tw.Close(); err1 != nil {
		tw = nil
		return fileMetadata{}, err1
	}

	if count == 0 {
		// The flush may have produced an empty table if a range tombstone deleted
		// all the entries in the table and the range tombstone could be elided.
		return fileMetadata{}, errEmptyTable
	}

	writerMeta, err := tw.Metadata()
	if err != nil {
		return fileMetadata{}, err
	}
	meta.size = writerMeta.Size
	meta.smallest = writerMeta.Smallest(d.cmp.Compare)
	meta.largest = writerMeta.Largest(d.cmp.Compare)
	meta.smallestSeqNum = writerMeta.SmallestSeqNum
	meta.largestSeqNum = writerMeta.LargestSeqNum
	tw = nil

	// TODO(peter): After a flush we set the commit rate to 110% of the flush
	// rate. The rationale behind the 110% is to account for slack. Investigate a
	// more principled way of setting this.
	// d.commitController.limiter.SetLimit(rate.Limit(d.flushController.sensor.Rate()))
	// if false {
	// 	fmt.Printf("flush: %.1f MB/s\n", d.flushController.sensor.Rate()/float64(1<<20))
	// }

	// TODO(peter): compaction stats.

	return meta, nil
}

// maybeScheduleCompaction schedules a compaction if necessary.
//
// d.mu must be held when calling this.
func (d *DB) maybeScheduleCompaction() {
	if d.mu.compact.compacting || d.mu.closed {
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
		_ = err
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
	if d.opts.EventListener != nil && d.opts.EventListener.CompactionBegin != nil {
		info := db.CompactionInfo{
			JobID: jobID,
		}
		d.opts.EventListener.CompactionBegin(info)
	}

	ve, pendingOutputs, err := d.compactDiskTables(c)

	if d.opts.EventListener != nil && d.opts.EventListener.CompactionEnd != nil {
		info := db.CompactionInfo{
			JobID: jobID,
			Err:   err,
		}
		if err != nil {
			info.Input.Level = c.level
			info.Output.Level = c.level + 1
			for i := range c.inputs {
				for j := range c.inputs[i] {
					m := &c.inputs[i][j]
					info.Input.Tables[i] = append(info.Input.Tables[i], m.tableInfo(d.dirname))
				}
			}
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
	err = d.mu.versions.logAndApply(ve)
	for _, fileNum := range pendingOutputs {
		if _, ok := d.mu.compact.pendingOutputs[fileNum]; !ok {
			panic("pebble: expected pending output not present")
		}
		delete(d.mu.compact.pendingOutputs, fileNum)
	}
	if err != nil {
		return err
	}
	d.deleteObsoleteFiles(jobID)
	return nil
}

// compactDiskTables runs a compaction that produces new on-disk tables from
// old on-disk tables.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) compactDiskTables(c *compaction) (ve *versionEdit, pendingOutputs []uint64, retErr error) {
	// Check for a trivial move of one table from one level to the next. We avoid
	// such a move if there is lots of overlapping grandparent data. Otherwise,
	// the move could create a parent file that will require a very expensive
	// merge later on.
	if len(c.inputs[0]) == 1 && len(c.inputs[1]) == 0 &&
		totalSize(c.grandparents) <= maxGrandparentOverlapBytes(d.opts, c.level+1) {
		meta := &c.inputs[0][0]
		return &versionEdit{
			deletedFiles: map[deletedFileEntry]bool{
				deletedFileEntry{level: c.level, fileNum: meta.fileNum}: true,
			},
			newFiles: []newFileEntry{
				{level: c.level + 1, meta: *meta},
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

	c.cmp = d.cmp
	iiter, err := c.newInputIter(d.newIters)
	if err != nil {
		return nil, pendingOutputs, err
	}
	iter := newCompactionIter(d.cmp.Compare, d.merge, iiter, snapshots,
		c.elideTombstone, c.elideRangeTombstone)

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
				d.opts.Storage.Remove(filename)
			}
		}
	}()

	ve = &versionEdit{
		deletedFiles: map[deletedFileEntry]bool{},
	}

	newOutput := func() error {
		d.mu.Lock()
		fileNum := d.mu.versions.nextFileNum()
		d.mu.compact.pendingOutputs[fileNum] = struct{}{}
		pendingOutputs = append(pendingOutputs, fileNum)
		d.mu.Unlock()

		filename := dbFilename(d.dirname, fileTypeTable, fileNum)
		file, err := d.opts.Storage.Create(filename)
		if err != nil {
			return err
		}
		filenames = append(filenames, filename)
		tw = sstable.NewWriter(file, d.opts, d.opts.Level(c.level+1))

		ve.newFiles = append(ve.newFiles, newFileEntry{
			level: c.level + 1,
			meta: fileMetadata{
				fileNum: fileNum,
			},
		})
		return nil
	}

	finishOutput := func(key db.InternalKey) error {
		if tw == nil {
			return nil
		}

		// NB: clone the key because the data can be held on to by the call to
		// compactionIter.Tombstones via rangedel.Fragmenter.FlushTo.
		key = key.Clone()
		for _, v := range iter.Tombstones(key.UserKey) {
			if err := tw.Add(v.Start, v.End); err != nil {
				return err
			}
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

		// The handling of range boundaries is a bit complicated.
		if n := len(ve.newFiles); n > 1 {
			// This is not the first output. Bound the smallest range key by the
			// previous tables largest key.
			prevMeta := &ve.newFiles[n-2].meta
			if d.cmp.Compare(writerMeta.SmallestRange.UserKey, prevMeta.largest.UserKey) <= 0 {
				// The range boundary user key is less than or equal to the previous
				// table's largest key. We need the tables to be key-space partitioned,
				// so force the boundary to a key that we know is larger than the
				// previous key.
				writerMeta.SmallestRange = db.MakeInternalKey(
					prevMeta.largest.UserKey, 0, db.InternalKeyKindRangeDelete)
			}
		}

		if key.UserKey != nil {
			if d.cmp.Compare(writerMeta.LargestRange.UserKey, key.UserKey) >= 0 {
				writerMeta.LargestRange = key
				writerMeta.LargestRange.Trailer = db.InternalKeyRangeDeleteSentinel
			}
		}

		meta.smallest = writerMeta.Smallest(d.cmp.Compare)
		meta.largest = writerMeta.Largest(d.cmp.Compare)

		return nil
	}

	for valid := iter.First(); valid; valid = iter.Next() {
		key := iter.Key()
		// TODO(peter,rangedel): Need to incorporate the range tombstones in the
		// shouldStopBefore decision.
		if tw != nil && (tw.EstimatedSize() >= c.maxOutputFileSize || c.shouldStopBefore(key)) {
			if err := finishOutput(key); err != nil {
				return nil, pendingOutputs, err
			}
		}

		if tw == nil {
			if err := newOutput(); err != nil {
				return nil, pendingOutputs, err
			}
		}

		if err := tw.Add(key, iter.Value()); err != nil {
			return nil, pendingOutputs, err
		}
	}

	if err := finishOutput(db.InternalKey{}); err != nil {
		return nil, pendingOutputs, nil
	}

	for i := range c.inputs {
		for _, f := range c.inputs[i] {
			ve.deletedFiles[deletedFileEntry{
				level:   c.level + i,
				fileNum: f.fileNum,
			}] = true
		}
	}
	return ve, pendingOutputs, nil
}

// deleteObsoleteFiles deletes those files that are no longer needed.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) deleteObsoleteFiles(jobID int) {
	// Release d.mu while doing I/O
	// Note the unusual order: Unlock and then Lock.
	d.mu.Unlock()
	defer d.mu.Lock()

	fs := d.opts.Storage
	list, err := fs.List(d.dirname)
	if err != nil {
		// Ignore any filesystem errors.
		return
	}
	// We sort to make the order of deletions deterministic, which is nice for
	// tests.
	sort.Strings(list)

	// Grab d.mu again in order to get a snapshot of the live state. Note that we
	// need to this after the directory list because after releasing the lock
	// again new files can be created.
	d.mu.Lock()
	liveFileNums := make(map[uint64]struct{}, len(d.mu.compact.pendingOutputs))
	for fileNum := range d.mu.compact.pendingOutputs {
		liveFileNums[fileNum] = struct{}{}
	}
	d.mu.versions.addLiveFileNums(liveFileNums)
	logNumber := d.mu.versions.logNumber
	manifestFileNumber := d.mu.versions.manifestFileNumber
	d.mu.Unlock()

	for _, filename := range list {
		fileType, fileNum, ok := parseDBFilename(filename)
		if !ok {
			continue
		}
		keep := true
		switch fileType {
		case fileTypeLog:
			// TODO(peter): also look at prevLogNumber?
			keep = fileNum >= logNumber
		case fileTypeManifest:
			keep = fileNum >= manifestFileNumber
		case fileTypeOptions:
			keep = fileNum >= d.optionsFileNum
		case fileTypeTable:
			_, keep = liveFileNums[fileNum]
		default:
			// Don't delete files we don't know about.
			continue
		}
		if keep {
			continue
		}
		if fileType == fileTypeTable {
			d.tableCache.evict(fileNum)
		}
		path := filepath.Join(d.dirname, filename)
		err := fs.Remove(path)

		if err != os.ErrNotExist && fileType == fileTypeTable {
			if d.opts.EventListener != nil && d.opts.EventListener.TableDeleted != nil {
				d.opts.EventListener.TableDeleted(db.TableDeleteInfo{
					JobID:   jobID,
					Path:    path,
					FileNum: fileNum,
					Err:     err,
				})
			}
		}
	}
}
