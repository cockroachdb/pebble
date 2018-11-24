// Copyright 2013 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sort"

	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/sstable"
	"github.com/petermattis/pebble/storage"
)

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
	cmp     db.Compare
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
		cmp:               opts.Comparer.Compare,
		version:           cur,
		level:             level,
		maxOutputFileSize: uint64(opts.Level(level + 1).TargetFileSize),
		maxOverlapBytes:   maxGrandparentOverlapBytes(opts, level+1),
	}
	return c
}

// setupOtherInputs fills in the rest of the compaction inputs, regardless of
// whether the compaction was automatically scheduled or user initiated.
func (c *compaction) setupOtherInputs(opts *db.Options) {
	cmp := opts.Comparer.Compare
	smallest0, largest0 := ikeyRange(cmp, c.inputs[0], nil)
	c.inputs[1] = c.version.overlaps(c.level+1, cmp, smallest0.UserKey, largest0.UserKey)
	smallest01, largest01 := ikeyRange(cmp, c.inputs[0], c.inputs[1])

	// Grow the inputs if it doesn't affect the number of level+1 files.
	if c.grow(opts, smallest01, largest01) {
		smallest01, largest01 = ikeyRange(cmp, c.inputs[0], c.inputs[1])
	}

	// Compute the set of level+2 files that overlap this compaction.
	if c.level+2 < numLevels {
		c.grandparents = c.version.overlaps(c.level+2, cmp, smallest01.UserKey, largest01.UserKey)
	}
}

// grow grows the number of inputs at c.level without changing the number of
// c.level+1 files in the compaction, and returns whether the inputs grew. sm
// and la are the smallest and largest InternalKeys in all of the inputs.
func (c *compaction) grow(opts *db.Options, sm, la db.InternalKey) bool {
	if len(c.inputs[1]) == 0 {
		return false
	}
	cmp := opts.Comparer.Compare
	grow0 := c.version.overlaps(c.level, cmp, sm.UserKey, la.UserKey)
	if len(grow0) <= len(c.inputs[0]) {
		return false
	}
	if totalSize(grow0)+totalSize(c.inputs[1]) >=
		expandedCompactionByteSizeLimit(opts, c.level+1) {
		return false
	}
	sm1, la1 := ikeyRange(cmp, grow0, nil)
	grow1 := c.version.overlaps(c.level+1, cmp, sm1.UserKey, la1.UserKey)
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
func (c *compaction) shouldStopBefore(key db.InternalKey) bool {
	for len(c.grandparents) > 0 {
		g := &c.grandparents[0]
		if db.InternalCompare(c.cmp, key, g.largest) <= 0 {
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
			if c.cmp(key, f.largest.UserKey) <= 0 {
				if c.cmp(key, f.smallest.UserKey) >= 0 {
					return false
				}
				// For levels above level 0, the files within a level are in
				// increasing ikey order, so we can break early.
				break
			}
		}
	}
	return true
}

// newInputIter returns an iterator over all the input tables in a compaction.
func (c *compaction) newInputIter(
	newIter, newRangeDelIter tableNewIter,
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

	// TODO(peter): test that range tombstones are properly included in the
	// output sstable.
	if c.level != 0 {
		iters = append(iters, newLevelIter(nil, c.cmp, newIter, c.inputs[0]))
		iters = append(iters, newLevelIter(nil, c.cmp, newRangeDelIter, c.inputs[0]))
	} else {
		for i := range c.inputs[0] {
			f := &c.inputs[0][i]

			iter, err := newIter(f)
			if err != nil {
				return nil, fmt.Errorf("pebble: could not open table %d: %v", f.fileNum, err)
			}
			iters = append(iters, iter)

			iter, err = newRangeDelIter(f)
			if err != nil {
				return nil, fmt.Errorf("pebble: could not open table %d: %v", f.fileNum, err)
			}
			if iter != nil {
				iters = append(iters, iter)
			}
		}
	}

	iters = append(iters, newLevelIter(nil, c.cmp, newIter, c.inputs[1]))
	iters = append(iters, newLevelIter(nil, c.cmp, newRangeDelIter, c.inputs[1]))
	return newMergingIter(c.cmp, iters...), nil
}

func (c *compaction) String() string {
	var buf bytes.Buffer
	for i := 0; i < 2; i++ {
		fmt.Fprintf(&buf, "%d:", i+c.level)
		for _, f := range c.inputs[0] {
			fmt.Fprintf(&buf, " %s-%s", f.smallest.UserKey, f.largest.UserKey)
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

	// TODO(peter): test that range tombstones are properly included in the
	// output sstable. Should propbably pull out the code below into a method
	// that can be separately tested.
	var iter internalIterator
	if n == 1 {
		mem := d.mu.mem.queue[0]
		iter = mem.newIter(nil)
		if riter := mem.newRangeDelIter(nil); riter != nil {
			iter = newMergingIter(d.cmp, iter, riter)
		}
	} else {
		iters := make([]internalIterator, 0, 2*n)
		for i := 0; i < n; i++ {
			mem := d.mu.mem.queue[i]
			iters = append(iters, mem.newIter(nil))
			riter := mem.newRangeDelIter(nil)
			if riter != nil {
				iters = append(iters, riter)
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

	meta, err := d.writeLevel0Table(d.opts.Storage, iter)

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

	if err != nil {
		return err
	}

	err = d.mu.versions.logAndApply(&versionEdit{
		logNumber: d.mu.log.number,
		newFiles: []newFileEntry{
			{level: 0, meta: meta},
		},
	})
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
	fs storage.Storage, iiter internalIterator,
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

	// Release the d.mu lock while doing I/O.
	// Note the unusual order: Unlock and then Lock.
	d.mu.Unlock()
	defer d.mu.Lock()

	iter := newCompactionIter(
		d.cmp, d.merge, iiter, snapshots,
		func([]byte) bool { return false })
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

	iter.First()
	if !iter.Valid() {
		return fileMetadata{}, fmt.Errorf("pebble: memtable empty")
	}

	file, err = fs.Create(filename)
	if err != nil {
		return fileMetadata{}, err
	}
	file = newRateLimitedFile(file, d.flushController)
	tw = sstable.NewWriter(file, d.opts, d.opts.Level(0))

	for ; iter.Valid(); iter.Next() {
		if err1 := tw.Add(iter.Key(), iter.Value()); err1 != nil {
			return fileMetadata{}, err1
		}
	}

	for _, v := range iter.Tombstones(nil) {
		if err1 := tw.Add(v.Start, v.End); err1 != nil {
			return fileMetadata{}, err1
		}
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

	writerMeta, err := tw.Metadata()
	if err != nil {
		return fileMetadata{}, err
	}
	meta.size = writerMeta.Size
	meta.smallest = writerMeta.Smallest
	meta.largest = writerMeta.Largest
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
		c = d.mu.versions.picker.pick(d.opts)
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
	iiter, err := c.newInputIter(d.newIter, d.newRangeDelIter)
	if err != nil {
		return nil, pendingOutputs, err
	}
	iter := newCompactionIter(d.cmp, d.merge, iiter, snapshots, c.elideTombstone)

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

	finishOutput := func(userKey []byte) error {
		if tw == nil {
			return nil
		}

		for _, v := range iter.Tombstones(userKey) {
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
		meta.smallest = writerMeta.Smallest
		meta.largest = writerMeta.Largest
		meta.smallestSeqNum = writerMeta.SmallestSeqNum
		meta.largestSeqNum = writerMeta.LargestSeqNum
		return nil
	}

	for iter.First(); iter.Valid(); iter.Next() {
		ikey := iter.Key()
		// TODO(peter): Need to incorporate the range tombstones in the
		// shouldStopBefore decision.
		if tw != nil && (tw.EstimatedSize() >= c.maxOutputFileSize || c.shouldStopBefore(ikey)) {
			if err := finishOutput(ikey.UserKey); err != nil {
				return nil, pendingOutputs, err
			}
		}

		if tw == nil {
			if err := newOutput(); err != nil {
				return nil, pendingOutputs, err
			}
		}

		if err := tw.Add(ikey, iter.Value()); err != nil {
			return nil, pendingOutputs, err
		}
	}

	if err := finishOutput(nil); err != nil {
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
	liveFileNums := map[uint64]struct{}{}
	for fileNum := range d.mu.compact.pendingOutputs {
		liveFileNums[fileNum] = struct{}{}
	}
	d.mu.versions.addLiveFileNums(liveFileNums)
	logNumber := d.mu.versions.logNumber
	manifestFileNumber := d.mu.versions.manifestFileNumber

	// Release the d.mu lock while doing I/O.
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
		}
		if keep {
			continue
		}
		if fileType == fileTypeTable {
			d.tableCache.evict(fileNum)
		}
		path := filepath.Join(d.dirname, filename)
		err := fs.Remove(path)

		if fileType == fileTypeTable {
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
