// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"sort"

	"github.com/petermattis/pebble/internal/base"
	"github.com/petermattis/pebble/sstable"
	"github.com/petermattis/pebble/vfs"
)

func sstableKeyCompare(userCmp Compare, a, b InternalKey) int {
	c := userCmp(a.UserKey, b.UserKey)
	if c != 0 {
		return c
	}
	if a.Trailer == InternalKeyRangeDeleteSentinel {
		if b.Trailer != InternalKeyRangeDeleteSentinel {
			return -1
		}
	} else if b.Trailer == InternalKeyRangeDeleteSentinel {
		return 1
	}
	return 0
}

func ingestLoad1(opts *Options, path string, dbNum, fileNum uint64) (*fileMetadata, error) {
	stat, err := opts.FS.Stat(path)
	if err != nil {
		return nil, err
	}

	f, err := opts.FS.Open(path)
	if err != nil {
		return nil, err
	}

	r, err := sstable.NewReader(f, dbNum, fileNum, opts)
	defer r.Close()
	if err != nil {
		return nil, err
	}

	meta := &fileMetadata{}
	meta.fileNum = fileNum
	meta.size = uint64(stat.Size())
	meta.smallest = InternalKey{}
	meta.largest = InternalKey{}
	smallestSet, largestSet := false, false

	{
		iter := r.NewIter(nil /* lower */, nil /* upper */)
		defer iter.Close()
		if key, _ := iter.First(); key != nil {
			meta.smallest = key.Clone()
			smallestSet = true
		}
		if key, _ := iter.Last(); key != nil {
			meta.largest = key.Clone()
			largestSet = true
		}
		if err := iter.Error(); err != nil {
			return nil, err
		}
	}

	if iter := r.NewRangeDelIter(); iter != nil {
		defer iter.Close()
		if key, _ := iter.First(); key != nil {
			if !smallestSet ||
				base.InternalCompare(opts.Comparer.Compare, meta.smallest, *key) > 0 {
				meta.smallest = key.Clone()
			}
		}
		if key, val := iter.Last(); key != nil {
			end := base.MakeRangeDeleteSentinelKey(val)
			if !largestSet ||
				base.InternalCompare(opts.Comparer.Compare, meta.largest, end) < 0 {
				meta.largest = end.Clone()
			}
		}
	}

	return meta, nil
}

func ingestLoad(
	opts *Options, paths []string, dbNum uint64, pending []uint64,
) ([]*fileMetadata, error) {
	meta := make([]*fileMetadata, len(paths))
	for i := range paths {
		var err error
		meta[i], err = ingestLoad1(opts, paths[i], dbNum, pending[i])
		if err != nil {
			return nil, err
		}
	}
	return meta, nil
}

func ingestSortAndVerify(cmp Compare, meta []*fileMetadata) error {
	if len(meta) <= 1 {
		return nil
	}

	sort.Slice(meta, func(i, j int) bool {
		return cmp(meta[i].smallest.UserKey, meta[j].smallest.UserKey) < 0
	})

	for i := 1; i < len(meta); i++ {
		if sstableKeyCompare(cmp, meta[i-1].largest, meta[i].smallest) >= 0 {
			return fmt.Errorf("files have overlapping ranges")
		}
	}
	return nil
}

func ingestCleanup(fs vfs.FS, dirname string, meta []*fileMetadata) error {
	var firstErr error
	for i := range meta {
		target := dbFilename(dirname, fileTypeTable, meta[i].fileNum)
		if err := fs.Remove(target); err != nil {
			if firstErr != nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func ingestLink(opts *Options, dirname string, paths []string, meta []*fileMetadata) error {
	for i := range paths {
		target := dbFilename(dirname, fileTypeTable, meta[i].fileNum)
		err := opts.FS.Link(paths[i], target)
		if err != nil {
			if err2 := ingestCleanup(opts.FS, dirname, meta[:i]); err2 != nil {
				opts.Logger.Infof("ingest cleanup failed: %v", err2)
			}
			return err
		}
	}

	return nil
}

func ingestMemtableOverlaps(cmp Compare, mem flushable, meta []*fileMetadata) bool {
	{
		// Check overlap with point operations.
		iter := mem.newIter(nil)
		defer iter.Close()

		for _, m := range meta {
			key, _ := iter.SeekGE(m.smallest.UserKey)
			if key == nil {
				continue
			}
			if cmp(key.UserKey, m.largest.UserKey) <= 0 {
				return true
			}
		}
	}

	// Check overlap with range deletions.
	if iter := mem.newRangeDelIter(nil); iter != nil {
		defer iter.Close()
		for _, m := range meta {
			key, val := iter.SeekLT(m.smallest.UserKey)
			if key == nil {
				key, val = iter.Next()
			}
			for ; key != nil; key, val = iter.Next() {
				if cmp(key.UserKey, m.largest.UserKey) > 0 {
					// The start of the tombstone is after the largest key in the
					// ingested table.
					break
				}
				if cmp(val, m.smallest.UserKey) > 0 {
					// The end of the tombstone is greater than the smallest in the
					// table. Note that the tombstone end key is exclusive, thus ">0"
					// instead of ">=0".
					return true
				}
			}
		}
	}

	return false
}

func ingestUpdateSeqNum(opts *Options, dirname string, seqNum uint64, meta []*fileMetadata) error {
	for _, m := range meta {
		m.smallest = base.MakeInternalKey(m.smallest.UserKey, seqNum, m.smallest.Kind())
		m.largest = base.MakeInternalKey(m.largest.UserKey, seqNum, m.largest.Kind())
		// Setting smallestSeqNum == largestSeqNum triggers the setting of
		// Properties.GlobalSeqNum when an sstable is loaded.
		m.smallestSeqNum = seqNum
		m.largestSeqNum = seqNum

		// TODO(peter): Update the global sequence number property. This is only
		// necessary for compatibility with RocksDB.
	}
	return nil
}

func ingestTargetLevel(cmp Compare, v *version, meta *fileMetadata) int {
	// Find the lowest level which does not have any files which overlap meta.
	if len(v.overlaps(0, cmp, meta.smallest.UserKey, meta.largest.UserKey)) != 0 {
		return 0
	}

	level := 1
	for ; level < numLevels; level++ {
		if len(v.overlaps(level, cmp, meta.smallest.UserKey, meta.largest.UserKey)) != 0 {
			break
		}
	}
	return level - 1
}

// Ingest ingests a set of sstables into the DB. Ingestion of the files is
// atomic and semantically equivalent to creating a single batch containing all
// of the mutations in the sstables. Ingestion may require the memtable to be
// flushed. The ingested sstable files are moved into the DB and must reside on
// the same filesystem as the DB. Sstables can be created for ingestion using
// sstable.Writer.
//
// Ingestion loads each sstable into the lowest level of the LSM which it
// doesn't overlap (see ingestTargetLevel). If an sstable overlaps a memtable,
// ingestion forces the memtable to flush, and then waits for the flush to
// occur.
//
// The steps for ingestion are:
//
//   1. Allocate file numbers for every sstable beign ingested.
//   2. Load the metadata for all sstables being ingest.
//   3. Sort the sstables by smallest key, verifying non overlap.
//   4. Hard link the sstables into the DB directory.
//   5. Allocate a sequence number to use for all of the entries in the
//      sstables. This is the step where overlap with memtables is
//      determined. If there is overlap, we remember the most recent memtable
//      that overlaps.
//   6. Update the sequence number in the ingested sstables.
//   7. Wait for the most recent memtable that overlaps to flush (if any).
//   8. Add the ingested sstables to the version (DB.ingestApply).
//   9. Publish the ingestion sequence number.
//
// Note that if the mutable memtable overlaps with ingestion, a flush of the
// memtable is forced equivalent to DB.Flush. Additionally, subsequent
// mutations that get sequence numbers larger than the ingestion sequence
// number get queued up behind the ingestion waiting for it to complete. This
// can produce a noticeable hiccup in performance. See
// https://github.com/petermattis/pebble/issues/25 for an idea for how to fix
// this hiccup.
func (d *DB) Ingest(paths []string) error {
	// Allocate file numbers for all of the files being ingested and mark them as
	// pending in order to prevent them from being deleted. Note that this causes
	// the file number ordering to be out of alignment with sequence number
	// ordering. The sorting of L0 tables by sequence number avoids relying on
	// that (busted) invariant.
	d.mu.Lock()
	pendingOutputs := make([]uint64, len(paths))
	for i := range paths {
		pendingOutputs[i] = d.mu.versions.nextFileNum()
	}
	for _, fileNum := range pendingOutputs {
		d.mu.compact.pendingOutputs[fileNum] = struct{}{}
	}
	jobID := d.mu.nextJobID
	d.mu.nextJobID++
	d.mu.Unlock()

	defer func() {
		d.mu.Lock()
		for _, fileNum := range pendingOutputs {
			delete(d.mu.compact.pendingOutputs, fileNum)
		}
		d.mu.Unlock()
	}()

	// Load the metadata for all of the files being ingested.
	meta, err := ingestLoad(d.opts, paths, d.dbNum, pendingOutputs)
	if err != nil {
		return err
	}

	// Verify the sstables do not overlap.
	if err := ingestSortAndVerify(d.cmp, meta); err != nil {
		return err
	}

	// Hard link the sstables into the DB directory. Since the sstables aren't
	// referenced by a version, they won't be used. If the hard linking fails
	// (e.g. because the files reside on a different filesystem) we undo our work
	// and return an error.
	if err := ingestLink(d.opts, d.dirname, paths, meta); err != nil {
		return err
	}
	// Fsync the directory we added the tables to. We need to do this at some
	// point before we update the MANIFEST (via logAndApply), otherwise a crash
	// can have the tables referenced in the MANIFEST, but not present in the
	// directory.
	if err := d.dataDir.Sync(); err != nil {
		return err
	}

	var mem flushable
	prepare := func() {
		d.mu.Lock()
		defer d.mu.Unlock()

		// If the mutable memtable contains keys which overlap any of the sstables
		// then flush the memtable. Note that apply will wait for the flushing to
		// finish.
		if ingestMemtableOverlaps(d.cmp, d.mu.mem.mutable, meta) {
			mem = d.mu.mem.mutable
			err = d.makeRoomForWrite(nil)
			return
		}

		// Check to see if any files overlap with any of the immutable
		// memtables. The queue is ordered from oldest to newest. We want to wait
		// for the newest table that overlaps.
		for i := len(d.mu.mem.queue) - 1; i >= 0; i-- {
			m := d.mu.mem.queue[i]
			if ingestMemtableOverlaps(d.cmp, m, meta) {
				mem = m
				return
			}
		}
	}

	var ve *versionEdit
	apply := func(seqNum uint64) {
		if err != nil {
			// An error occurred during prepare.
			return
		}

		// Update the sequence number for all of the sstables, both in the metadata
		// and the global sequence number property on disk.
		if err = ingestUpdateSeqNum(d.opts, d.dirname, seqNum, meta); err != nil {
			return
		}

		// If we flushed the mutable memtable in prepare wait for the flush to
		// finish.
		if mem != nil {
			<-mem.flushed()
		}

		// Assign the sstables to the correct level in the LSM and apply the
		// version edit.
		ve, err = d.ingestApply(jobID, meta)
	}

	d.commit.AllocateSeqNum(prepare, apply)

	if err != nil {
		if err2 := ingestCleanup(d.opts.FS, d.dirname, meta); err2 != nil {
			d.opts.Logger.Infof("ingest cleanup failed: %v", err2)
		}
	}

	if d.opts.EventListener.TableIngested != nil {
		info := TableIngestInfo{
			JobID:        jobID,
			GlobalSeqNum: meta[0].smallestSeqNum,
			Err:          err,
		}
		if ve != nil {
			info.Tables = make([]struct {
				TableInfo
				Level int
			}, len(ve.newFiles))
			for i := range ve.newFiles {
				e := &ve.newFiles[i]
				info.Tables[i].Level = e.level
				info.Tables[i].TableInfo = e.meta.tableInfo(d.dirname)
			}
		}
		d.opts.EventListener.TableIngested(info)
	}

	return err
}

func (d *DB) ingestApply(jobID int, meta []*fileMetadata) (*versionEdit, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	ve := &versionEdit{
		newFiles: make([]newFileEntry, len(meta)),
		metrics:  make(map[int]*LevelMetrics),
	}
	current := d.mu.versions.currentVersion()
	for i := range meta {
		// Determine the lowest level in the LSM for which the sstable doesn't
		// overlap any existing files in the level.
		m := meta[i]
		f := &ve.newFiles[i]
		f.level = ingestTargetLevel(d.cmp, current, m)
		f.meta = *m
		metrics := ve.metrics[f.level]
		if metrics == nil {
			metrics = &LevelMetrics{}
			ve.metrics[f.level] = metrics
		}
		metrics.BytesIngested += m.size
	}
	if err := d.mu.versions.logAndApply(jobID, ve, d.dataDir); err != nil {
		return nil, err
	}
	d.updateReadStateLocked()
	return ve, nil
}
