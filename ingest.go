// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"sort"

	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/sstable"
	"github.com/petermattis/pebble/storage"
)

func ingestLoad1(opts *db.Options, path string, fileNum uint64) (*fileMetadata, error) {
	stat, err := opts.Storage.Stat(path)
	if err != nil {
		return nil, err
	}

	f, err := opts.Storage.Open(path)
	if err != nil {
		return nil, err
	}

	r := sstable.NewReader(f, fileNum, opts)
	defer r.Close()

	meta := &fileMetadata{}
	meta.fileNum = fileNum
	meta.size = uint64(stat.Size())
	meta.smallest = db.InternalKey{}
	meta.largest = db.InternalKey{}
	smallestSet, largestSet := false, false

	{
		iter := r.NewIter(nil)
		defer iter.Close()
		if iter.First() {
			meta.smallest = iter.Key().Clone()
			smallestSet = true
		}
		if iter.Last() {
			meta.largest = iter.Key().Clone()
			largestSet = true
		}
		if err := iter.Error(); err != nil {
			return nil, err
		}
	}

	if iter := r.NewRangeDelIter(nil); iter != nil {
		defer iter.Close()
		if iter.First() {
			key := iter.Key()
			if !smallestSet ||
				db.InternalCompare(opts.Comparer.Compare, meta.smallest, key) > 0 {
				meta.smallest = key.Clone()
			}
		}
		if iter.Last() {
			key := db.MakeRangeDeleteSentinelKey(iter.Value())
			if !largestSet ||
				db.InternalCompare(opts.Comparer.Compare, meta.largest, key) < 0 {
				meta.largest = key.Clone()
			}
		}
	}

	return meta, nil
}

func ingestLoad(opts *db.Options, paths []string, pending []uint64) ([]*fileMetadata, error) {
	meta := make([]*fileMetadata, len(paths))
	for i := range paths {
		var err error
		meta[i], err = ingestLoad1(opts, paths[i], pending[i])
		if err != nil {
			return nil, err
		}
	}
	return meta, nil
}

func ingestSortAndVerify(cmp db.Compare, meta []*fileMetadata) error {
	if len(meta) <= 1 {
		return nil
	}

	sort.Slice(meta, func(i, j int) bool {
		return cmp(meta[i].smallest.UserKey, meta[j].smallest.UserKey) < 0
	})

	for i := 1; i < len(meta); i++ {
		if cmp(meta[i-1].largest.UserKey, meta[i].smallest.UserKey) >= 0 {
			return fmt.Errorf("files have overlapping ranges")
		}
	}
	return nil
}

func ingestCleanup(
	fs storage.Storage, dirname string, meta []*fileMetadata,
) error {
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

func ingestLink(
	opts *db.Options, dirname string, paths []string, meta []*fileMetadata,
) error {
	for i := range paths {
		target := dbFilename(dirname, fileTypeTable, meta[i].fileNum)
		err := opts.Storage.Link(paths[i], target)
		if err != nil {
			if err2 := ingestCleanup(opts.Storage, dirname, meta[:i]); err2 != nil {
				opts.Logger.Infof("ingest cleanup failed: %v", err2)
			}
			return err
		}
	}

	return nil
}

func ingestMemtableOverlaps(cmp db.Compare, mem flushable, meta []*fileMetadata) bool {
	{
		// Check overlap with point operations.
		iter := mem.newIter(nil)
		defer iter.Close()

		for _, m := range meta {
			if !iter.SeekGE(m.smallest.UserKey) {
				continue
			}
			if cmp(iter.Key().UserKey, m.largest.UserKey) <= 0 {
				return true
			}
		}
	}

	// Check overlap with range deletions.
	if iter := mem.newRangeDelIter(nil); iter != nil {
		defer iter.Close()
		for _, m := range meta {
			valid := iter.SeekLT(m.smallest.UserKey)
			if !valid {
				valid = iter.Next()
			}
			for ; valid; valid = iter.Next() {
				if cmp(iter.Key().UserKey, m.largest.UserKey) > 0 {
					// The start of the tombstone is after the largest key in the
					// ingested table.
					break
				}
				if cmp(iter.Value(), m.smallest.UserKey) > 0 {
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

func ingestUpdateSeqNum(
	opts *db.Options, dirname string, seqNum uint64, meta []*fileMetadata,
) error {
	for _, m := range meta {
		m.smallest = db.MakeInternalKey(m.smallest.UserKey, seqNum, m.smallest.Kind())
		m.largest = db.MakeInternalKey(m.largest.UserKey, seqNum, m.largest.Kind())
		// Setting smallestSeqNum == largestSeqNum triggers the setting of
		// Properties.GlobalSeqNum when an sstable is loaded.
		m.smallestSeqNum = seqNum
		m.largestSeqNum = seqNum

		// TODO(peter): Update the global sequence number property. This is only
		// necessary for compatibility with RocksDB.
	}
	return nil
}

func ingestTargetLevel(cmp db.Compare, v *version, meta *fileMetadata) int {
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
	meta, err := ingestLoad(d.opts, paths, pendingOutputs)
	if err != nil {
		return err
	}

	// Verify the sstables do not overlap.
	if err := ingestSortAndVerify(d.cmp.Compare, meta); err != nil {
		return err
	}

	// Hard link the sstables into the DB directory. Since the sstables aren't
	// referenced by a version, they won't be used. If the hard linking fails
	// (e.g. because the files reside on a different filesystem) we undo our work
	// and return an error.
	if err := ingestLink(d.opts, d.dirname, paths, meta); err != nil {
		return err
	}

	var mem flushable
	prepareLocked := func() {
		// NB: prepare is called with d.mu locked.

		// If the mutable memtable contains keys which overlap any of the sstables
		// then flush the memtable. Note that apply will wait for the flushing to
		// finish.
		if ingestMemtableOverlaps(d.cmp.Compare, d.mu.mem.mutable, meta) {
			mem = d.mu.mem.mutable
			err = d.makeRoomForWrite(nil)
			return
		}

		// Check to see if any files overlap with any of the immutable
		// memtables. The queue is ordered from oldest to newest. We want to wait
		// for the newest table that overlaps.
		for i := len(d.mu.mem.queue) - 1; i >= 0; i-- {
			m := d.mu.mem.queue[i]
			if ingestMemtableOverlaps(d.cmp.Compare, m, meta) {
				mem = m
				return
			}
		}
	}

	var ve *versionEdit
	apply := func(seqNum uint64) {
		if err != nil {
			// An error occurred during prepareLocked.
			return
		}

		// Update the sequence number for all of the sstables, both in the metadata
		// and the global sequence number property on disk.
		if err = ingestUpdateSeqNum(d.opts, d.dirname, seqNum, meta); err != nil {
			return
		}

		// If we flushed the mutable memtable in prepareLocked wait for the flush
		// to finish.
		if mem != nil {
			<-mem.flushed()
		}

		// Assign the sstables to the correct level in the LSM and apply the
		// version edit.
		ve, err = d.ingestApply(meta)
	}

	d.commit.AllocateSeqNum(prepareLocked, apply)

	if err != nil {
		if err2 := ingestCleanup(d.opts.Storage, d.dirname, meta); err2 != nil {
			d.opts.Logger.Infof("ingest cleanup failed: %v", err2)
		}
	}

	if d.opts.EventListener != nil && d.opts.EventListener.TableIngested != nil {
		info := db.TableIngestInfo{
			JobID:        jobID,
			GlobalSeqNum: meta[0].smallestSeqNum,
			Err:          err,
		}
		if ve != nil {
			info.Tables = make([]struct {
				db.TableInfo
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

func (d *DB) ingestApply(meta []*fileMetadata) (*versionEdit, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	ve := &versionEdit{
		newFiles: make([]newFileEntry, len(meta)),
	}
	current := d.mu.versions.currentVersion()
	for i := range meta {
		// Determine the lowest level in the LSM for which the sstable doesn't
		// overlap any existing files in the level.
		m := meta[i]
		ve.newFiles[i].level = ingestTargetLevel(d.cmp.Compare, current, m)
		ve.newFiles[i].meta = *m
	}
	if err := d.mu.versions.logAndApply(ve); err != nil {
		return nil, err
	}
	return ve, nil
}
