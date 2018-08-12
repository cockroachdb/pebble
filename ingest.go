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

type ingestMetadata struct {
	fileMetadata
	props *sstable.Properties
}

func ingestLoad1(opts *db.Options, path string, fileNum uint64) (*ingestMetadata, error) {
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

	meta := &ingestMetadata{
		props: &r.Properties,
	}
	meta.fileNum = fileNum
	meta.size = uint64(stat.Size())
	meta.smallest = db.InternalKey{}
	meta.largest = db.InternalKey{}

	iter := r.NewIter(nil)
	defer iter.Close()
	if iter.First(); iter.Valid() {
		meta.smallest = iter.Key()
	}
	if iter.Last(); iter.Valid() {
		meta.largest = iter.Key()
	}
	return meta, nil
}

func ingestLoad(opts *db.Options, paths []string, pending []uint64) ([]*ingestMetadata, error) {
	meta := make([]*ingestMetadata, len(paths))
	for i := range paths {
		var err error
		meta[i], err = ingestLoad1(opts, paths[i], pending[i])
		if err != nil {
			return nil, err
		}
	}
	return meta, nil
}

func ingestSortAndVerify(cmp db.Compare, meta []*ingestMetadata) error {
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
	fs storage.Storage, dirname string, meta []*ingestMetadata,
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
	fs storage.Storage, dirname string, paths []string, meta []*ingestMetadata,
) error {
	for i := range paths {
		target := dbFilename(dirname, fileTypeTable, meta[i].fileNum)
		err := fs.Link(paths[i], target)
		if err != nil {
			if err2 := ingestCleanup(fs, dirname, meta[:i]); err2 != nil {
				// TODO(peter): log a warning.
				panic(err2)
			}
			return err
		}
	}

	return nil
}

func ingestMemtableOverlaps(mem *memTable, meta []*ingestMetadata) bool {
	iter := mem.NewIter(nil)
	defer iter.Close()

	for _, m := range meta {
		iter.SeekGE(m.smallest.UserKey)
		if !iter.Valid() {
			continue
		}
		if mem.cmp(iter.Key().UserKey, m.largest.UserKey) <= 0 {
			return true
		}
	}
	return false
}

func ingestUpdateSeqNum(
	opts *db.Options, dirname string, seqNum uint64, meta []*ingestMetadata,
) error {
	for _, m := range meta {
		m.smallest = db.MakeInternalKey(m.smallest.UserKey, seqNum, m.smallest.Kind())
		m.largest = db.MakeInternalKey(m.largest.UserKey, seqNum, m.largest.Kind())
		m.smallestSeqNum = seqNum
		m.largestSeqNum = seqNum
		m.globalSeqNum = seqNum

		// TODO(peter): Update the global sequence number property. This is only
		// necessary for compatibility with RocksDB.
	}
	return nil
}

func ingestTargetLevel(v *version, meta *ingestMetadata) int {
	// TODO(peter): returning L0 is safe, but not optimal.
	return 0
}

// Ingest ingests a set of sstables into the DB. Ingestion of the files is
// atomic and semantically equivalent to creating a single batch containing all
// of the mutations in the sstables. Ingestion may require the memtable to be
// flushed. The ingested sstable files are moved into the DB and must reside on
// the same filesystem as the DB. Sstables can be created for ingestion using
// sstable.Writer.
func (d *DB) Ingest(paths []string) error {
	// Allocate file numbers for all of the files being ingested and mark them as
	// pending in order to prevent them from being deleted.
	d.mu.Lock()
	pendingOutputs := make([]uint64, len(paths))
	for i := range paths {
		pendingOutputs[i] = d.mu.versions.nextFileNum()
	}
	for _, fileNum := range pendingOutputs {
		d.mu.compact.pendingOutputs[fileNum] = struct{}{}
	}
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
	if err := ingestSortAndVerify(d.cmp, meta); err != nil {
		return err
	}

	// Hard link the sstables into the DB directory. Since the sstables aren't
	// referenced by a version, they won't be used. If the hard linking fails
	// (e.g. because the files reside on a different filesystem) we undo our work
	// and return an error.
	if err := ingestLink(d.opts.Storage, d.dirname, paths, meta); err != nil {
		return err
	}

	var mem *memTable
	prepareLocked := func() {
		// NB: prepare is called with d.mu locked.
		//
		// If the mutable memtable contains keys which overlap any of the sstables
		// then flush the memtable. Note that apply will wait for the flushing to
		// finish.
		if ingestMemtableOverlaps(d.mu.mem.mutable, meta) {
			mem = d.mu.mem.mutable
			err = d.makeRoomForWrite(nil)
			return
		}

		// TODO(peter): Check to see if any files overlap with any of the immutable
		// memtables. Record the last memtable for which that is true so that we
		// can wait for it to flush in apply.
	}

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
			<-mem.flushed
		}

		// Assign the sstables to the correct level in the LSM and apply the
		// version edit.
		err = d.ingestApply(meta)
	}

	d.commit.AllocateSeqNum(prepareLocked, apply)

	if err != nil {
		if err2 := ingestCleanup(d.opts.Storage, d.dirname, meta); err2 != nil {
			// TODO(peter): log a warning.
			panic(err2)
		}
	}
	return err
}

func (d *DB) ingestApply(meta []*ingestMetadata) error {
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
		ve.newFiles[i].level = ingestTargetLevel(current, m)
		ve.newFiles[i].meta = m.fileMetadata
	}
	return d.mu.versions.logAndApply(d.opts, d.dirname, ve)
}

// TODO(peter): Update sstable.Reader to use the global sequence number
// property.
