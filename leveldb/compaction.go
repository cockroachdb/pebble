// Copyright 2013 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package leveldb

import (
	"fmt"

	"code.google.com/p/leveldb-go/leveldb/db"
	"code.google.com/p/leveldb-go/leveldb/table"
)

const (
	targetFileSize = 2 * 1024 * 1024

	// maxGrandparentOverlapBytes is the maximum bytes of overlap with
	// level+2 before we stop building a single file in a level to level+1
	// compaction.
	maxGrandparentOverlapBytes = 10 * targetFileSize

	// expandedCompactionByteSizeLimit is the maximum number of bytes in
	// all compacted files. We avoid expanding the lower level file set of
	// a compaction if it would make the total compaction cover more than
	// this many bytes.
	expandedCompactionByteSizeLimit = 25 * targetFileSize
)

// compaction is a table compaction from one level to the next, starting from a
// given version.
type compaction struct {
	version *version

	// level is the level that is being compacted. Inputs from level and
	// level+1 will be merged to produce a set of level+1 files.
	level int

	// inputs are the tables to be compacted.
	inputs [3][]fileMetadata
}

// pickCompaction picks the best compaction, if any, for vs' current version.
func pickCompaction(vs *versionSet) (c *compaction) {
	cur := vs.currentVersion()

	// Pick a compaction based on size. If none exist, pick one based on seeks.
	if cur.compactionScore >= 1 {
		c = &compaction{
			version: cur,
			level:   cur.compactionLevel,
		}
		// TODO: Pick the first file that comes after the compaction pointer for c.level.
		c.inputs[0] = []fileMetadata{cur.files[c.level][0]}

	} else if false {
		// TODO: look for a compaction triggered by seeks.

	} else {
		return nil
	}

	// Files in level 0 may overlap each other, so pick up all overlapping ones.
	if c.level == 0 {
		smallest, largest := ikeyRange(vs.icmp, c.inputs[0], nil)
		c.inputs[0] = cur.overlaps(0, vs.ucmp, smallest.ukey(), largest.ukey())
		if len(c.inputs) == 0 {
			panic("leveldb: empty compaction")
		}
	}

	c.setupOtherInputs(vs)
	return c
}

// TODO: user initiated compactions.

// setupOtherInputs fills in the rest of the compaction inputs, regardless of
// whether the compaction was automatically scheduled or user initiated.
func (c *compaction) setupOtherInputs(vs *versionSet) {
	smallest0, largest0 := ikeyRange(vs.icmp, c.inputs[0], nil)
	c.inputs[1] = c.version.overlaps(c.level+1, vs.ucmp, smallest0.ukey(), largest0.ukey())
	smallest01, largest01 := ikeyRange(vs.icmp, c.inputs[0], c.inputs[1])

	// Grow the inputs if it doesn't affect the number of level+1 files.
	if c.grow(vs, smallest01, largest01) {
		smallest01, largest01 = ikeyRange(vs.icmp, c.inputs[0], c.inputs[1])
	}

	// Compute the set of level+2 files that overlap this compaction.
	if c.level+2 < numLevels {
		c.inputs[2] = c.version.overlaps(c.level+2, vs.ucmp, smallest01.ukey(), largest01.ukey())
	}

	// TODO: update the compaction pointer for c.level.
}

// grow grows the number of inputs at c.level without changing the number of
// c.level+1 files in the compaction, and returns whether the inputs grew. sm
// and la are the smallest and largest internalKeys in all of the inputs.
func (c *compaction) grow(vs *versionSet, sm, la internalKey) bool {
	if len(c.inputs[1]) == 0 {
		return false
	}
	grow0 := c.version.overlaps(c.level, vs.ucmp, sm.ukey(), la.ukey())
	if len(grow0) <= len(c.inputs[0]) {
		return false
	}
	if totalSize(grow0)+totalSize(c.inputs[1]) >= expandedCompactionByteSizeLimit {
		return false
	}
	sm1, la1 := ikeyRange(vs.icmp, grow0, nil)
	grow1 := c.version.overlaps(c.level+1, vs.ucmp, sm1, la1)
	if len(grow1) != len(c.inputs[1]) {
		return false
	}
	c.inputs[0] = grow0
	c.inputs[1] = grow1
	return true
}

// isBaseLevelForUkey reports whether it is guaranteed that there are no
// key/value pairs at c.level+2 or higher that have the user key ukey.
func (c *compaction) isBaseLevelForUkey(userCmp db.Comparer, ukey []byte) bool {
	// TODO: this can be faster if ukey is always increasing between successive
	// isBaseLevelForUkey calls and we can keep some state in between calls.
	for level := c.level + 2; level < numLevels; level++ {
		for _, f := range c.version.files[level] {
			if userCmp.Compare(ukey, f.largest.ukey()) <= 0 {
				if userCmp.Compare(ukey, f.smallest.ukey()) >= 0 {
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

// maybeScheduleCompaction schedules a compaction if necessary.
//
// d.mu must be held when calling this.
func (d *DB) maybeScheduleCompaction() {
	if d.compacting || d.closed {
		return
	}
	// TODO: check for manual compactions.
	if d.imm == nil {
		v := d.versions.currentVersion()
		// TODO: check v.fileToCompact.
		if v.compactionScore < 1 {
			// There is no work to be done.
			return
		}
	}
	d.compacting = true
	go d.compact()
}

// compact runs one compaction and maybe schedules another call to compact.
func (d *DB) compact() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.compact1(); err != nil {
		// TODO: count consecutive compaction errors and backoff.
	}
	d.compacting = false
	// The previous compaction may have produced too many files in a
	// level, so reschedule another compaction if needed.
	d.maybeScheduleCompaction()
	d.compactionCond.Broadcast()
}

// compact1 runs one compaction.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) compact1() error {
	if d.imm != nil {
		return d.compactMemTable()
	}

	// TODO: support manual compactions.

	c := pickCompaction(&d.versions)
	if c == nil {
		return nil
	}

	// Check for a trivial move of one table from one level to the next.
	// We avoid such a move if there is lots of overlapping grandparent data.
	// Otherwise, the move could create a parent file that will require
	// a very expensive merge later on.
	if len(c.inputs[0]) == 1 && len(c.inputs[1]) == 0 &&
		totalSize(c.inputs[2]) <= maxGrandparentOverlapBytes {

		meta := &c.inputs[0][0]
		return d.versions.logAndApply(d.dirname, &versionEdit{
			deletedFiles: map[deletedFileEntry]bool{
				deletedFileEntry{level: c.level, fileNum: meta.fileNum}: true,
			},
			newFiles: []newFileEntry{
				{level: c.level + 1, meta: *meta},
			},
		})
	}

	ve, pendingOutputs, err := d.compactDiskTables(c)
	if err != nil {
		return err
	}
	err = d.versions.logAndApply(d.dirname, ve)
	for _, fileNum := range pendingOutputs {
		delete(d.pendingOutputs, fileNum)
	}
	if err != nil {
		return err
	}
	d.deleteObsoleteFiles()
	return nil
}

// compactMemTable runs a compaction that copies d.imm from memory to disk.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) compactMemTable() error {
	meta, err := d.writeLevel0Table(d.opts.GetFileSystem(), d.imm)
	if err != nil {
		return err
	}
	err = d.versions.logAndApply(d.dirname, &versionEdit{
		logNumber: d.logNumber,
		newFiles: []newFileEntry{
			{level: 0, meta: meta},
		},
	})
	delete(d.pendingOutputs, meta.fileNum)
	if err != nil {
		return err
	}
	d.imm = nil
	d.deleteObsoleteFiles()
	return nil
}

// compactDiskTables runs a compaction that produces new on-disk tables from
// old on-disk tables.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) compactDiskTables(c *compaction) (ve *versionEdit, pendingOutputs []uint64, retErr error) {
	defer func() {
		if retErr != nil {
			for _, fileNum := range pendingOutputs {
				delete(d.pendingOutputs, fileNum)
			}
			pendingOutputs = nil
		}
	}()

	// TODO: track snapshots.
	smallestSnapshot := d.versions.lastSequence

	// Release the d.mu lock while doing I/O.
	// Note the unusual order: Unlock and then Lock.
	d.mu.Unlock()
	defer d.mu.Lock()

	iter, err := compactionIterator(&d.tableCache, d.icmp, c)
	if err != nil {
		return nil, pendingOutputs, err
	}

	// TODO: output to more than one table, if it would otherwise be too large.
	var (
		fileNum  uint64
		filename string
		tw       *table.Writer
	)
	defer func() {
		if iter != nil {
			retErr = firstError(retErr, iter.Close())
		}
		if tw != nil {
			retErr = firstError(retErr, tw.Close())
		}
		if retErr != nil {
			d.opts.GetFileSystem().Remove(filename)
		}
	}()

	currentUkey := make([]byte, 0, 4096)
	hasCurrentUkey := false
	lastSeqNumForKey := internalKeySeqNumMax
	smallest, largest := internalKey(nil), internalKey(nil)
	for iter.Next() {
		// TODO: prioritize compacting d.imm.

		// TODO: support c.shouldStopBefore.

		ikey := internalKey(iter.Key())
		if !ikey.valid() {
			// Do not hide invalid keys.
			currentUkey = currentUkey[:0]
			hasCurrentUkey = false
			lastSeqNumForKey = internalKeySeqNumMax

		} else {
			ukey := ikey.ukey()
			if !hasCurrentUkey || d.icmp.userCmp.Compare(currentUkey, ukey) != 0 {
				// This is the first occurrence of this user key.
				currentUkey = append(currentUkey[:0], ukey...)
				hasCurrentUkey = true
				lastSeqNumForKey = internalKeySeqNumMax
			}

			drop, ikeySeqNum := false, ikey.seqNum()
			if lastSeqNumForKey <= smallestSnapshot {
				drop = true // Rule (A) referenced below.

			} else if ikey.kind() == internalKeyKindDelete &&
				ikeySeqNum <= smallestSnapshot &&
				c.isBaseLevelForUkey(d.icmp.userCmp, ukey) {

				// For this user key:
				// (1) there is no data in higher levels
				// (2) data in lower levels will have larger sequence numbers
				// (3) data in layers that are being compacted here and have
				//     smaller sequence numbers will be dropped in the next
				//     few iterations of this loop (by rule (A) above).
				// Therefore this deletion marker is obsolete and can be dropped.
				drop = true
			}

			lastSeqNumForKey = ikeySeqNum
			if drop {
				continue
			}
		}

		if tw == nil {
			d.mu.Lock()
			fileNum = d.versions.nextFileNum()
			d.pendingOutputs[fileNum] = struct{}{}
			pendingOutputs = append(pendingOutputs, fileNum)
			d.mu.Unlock()

			filename = dbFilename(d.dirname, fileTypeTable, fileNum)
			file, err := d.opts.GetFileSystem().Create(filename)
			if err != nil {
				return nil, pendingOutputs, err
			}
			tw = table.NewWriter(file, &d.icmpOpts)

			smallest = make(internalKey, len(ikey))
			copy(smallest, ikey)
			largest = make(internalKey, 0, 2*len(ikey))
		}
		largest = append(largest[:0], ikey...)
		if err := tw.Set(ikey, iter.Value(), nil); err != nil {
			return nil, pendingOutputs, err
		}
	}

	ve = &versionEdit{
		deletedFiles: map[deletedFileEntry]bool{},
		newFiles: []newFileEntry{
			{
				level: c.level + 1,
				meta: fileMetadata{
					fileNum:  fileNum,
					size:     1,
					smallest: smallest,
					largest:  largest,
				},
			},
		},
	}
	for i := 0; i < 2; i++ {
		for _, f := range c.inputs[i] {
			ve.deletedFiles[deletedFileEntry{
				level:   c.level + i,
				fileNum: f.fileNum,
			}] = true
		}
	}
	return ve, pendingOutputs, nil
}

// compactionIterator returns an iterator over all the tables in a compaction.
func compactionIterator(tc *tableCache, icmp db.Comparer, c *compaction) (cIter db.Iterator, retErr error) {
	iters := make([]db.Iterator, 0, len(c.inputs[0])+1)
	defer func() {
		if retErr != nil {
			for _, iter := range iters {
				if iter != nil {
					iter.Close()
				}
			}
		}
	}()

	if c.level != 0 {
		iter, err := newConcatenatingIterator(tc, c.inputs[0])
		if err != nil {
			return nil, err
		}
		iters = append(iters, iter)
	} else {
		for _, f := range c.inputs[0] {
			iter, err := tc.find(f.fileNum, nil)
			if err != nil {
				return nil, fmt.Errorf("leveldb: could not open table %d: %v", f.fileNum, err)
			}
			iters = append(iters, iter)
		}
	}

	iter, err := newConcatenatingIterator(tc, c.inputs[1])
	if err != nil {
		return nil, err
	}
	iters = append(iters, iter)
	return db.NewMergingIterator(icmp, iters...), nil
}

// newConcatenatingIterator returns a concatenating iterator over all of the
// input tables.
func newConcatenatingIterator(tc *tableCache, inputs []fileMetadata) (cIter db.Iterator, retErr error) {
	iters := make([]db.Iterator, len(inputs))
	defer func() {
		if retErr != nil {
			for _, iter := range iters {
				if iter != nil {
					iter.Close()
				}
			}
		}
	}()

	for i, f := range inputs {
		iter, err := tc.find(f.fileNum, nil)
		if err != nil {
			return nil, fmt.Errorf("leveldb: could not open table %d: %v", f.fileNum, err)
		}
		iters[i] = iter
	}
	return db.NewConcatenatingIterator(iters...), nil
}
