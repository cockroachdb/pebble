// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"sort"
	"sync/atomic"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/private"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
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

func ingestValidateKey(opts *Options, key *InternalKey) error {
	if key.Kind() == InternalKeyKindInvalid {
		return fmt.Errorf("pebble: external sstable has corrupted key: %s",
			key.Pretty(opts.Comparer.Format))
	}
	if key.SeqNum() != 0 {
		return fmt.Errorf("pebble: external sstable has non-zero seqnum: %s",
			key.Pretty(opts.Comparer.Format))
	}
	return nil
}

func ingestLoad1(opts *Options, path string, cacheID, fileNum uint64) (*fileMetadata, error) {
	stat, err := opts.FS.Stat(path)
	if err != nil {
		return nil, err
	}

	f, err := opts.FS.Open(path)
	if err != nil {
		return nil, err
	}

	cacheOpts := private.SSTableCacheOpts(cacheID, fileNum).(sstable.ReaderOption)
	r, err := sstable.NewReader(f, opts.MakeReaderOptions(), cacheOpts)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	meta := &fileMetadata{}
	meta.FileNum = fileNum
	meta.Size = uint64(stat.Size())
	meta.Smallest = InternalKey{}
	meta.Largest = InternalKey{}
	smallestSet, largestSet := false, false
	empty := true

	{
		iter := r.NewIter(nil /* lower */, nil /* upper */)
		defer iter.Close()
		if key, _ := iter.First(); key != nil {
			if err := ingestValidateKey(opts, key); err != nil {
				return nil, err
			}
			empty = false
			meta.Smallest = key.Clone()
			smallestSet = true
		}
		if key, _ := iter.Last(); key != nil {
			if err := ingestValidateKey(opts, key); err != nil {
				return nil, err
			}
			empty = false
			meta.Largest = key.Clone()
			largestSet = true
		}
		if err := iter.Error(); err != nil {
			return nil, err
		}
	}

	if iter := r.NewRangeDelIter(); iter != nil {
		defer iter.Close()
		if key, _ := iter.First(); key != nil {
			if err := ingestValidateKey(opts, key); err != nil {
				return nil, err
			}
			empty = false
			if !smallestSet ||
				base.InternalCompare(opts.Comparer.Compare, meta.Smallest, *key) > 0 {
				meta.Smallest = key.Clone()
			}
		}
		if key, val := iter.Last(); key != nil {
			if err := ingestValidateKey(opts, key); err != nil {
				return nil, err
			}
			empty = false
			end := base.MakeRangeDeleteSentinelKey(val)
			if !largestSet ||
				base.InternalCompare(opts.Comparer.Compare, meta.Largest, end) < 0 {
				meta.Largest = end.Clone()
			}
		}
	}

	if empty {
		return nil, nil
	}
	return meta, nil
}

func ingestLoad(
	opts *Options, paths []string, cacheID uint64, pending []uint64,
) ([]*fileMetadata, []string, error) {
	meta := make([]*fileMetadata, 0, len(paths))
	newPaths := make([]string, 0, len(paths))
	for i := range paths {
		m, err := ingestLoad1(opts, paths[i], cacheID, pending[i])
		if err != nil {
			return nil, nil, err
		}
		if m != nil {
			meta = append(meta, m)
			newPaths = append(newPaths, paths[i])
		}
	}
	return meta, newPaths, nil
}

// Struct for sorting metadatas by smallest user keys, while ensuring the
// matching path also gets swapped to the same index. For use in
// ingestSortAndVerify.
type metaAndPaths struct {
	meta  []*fileMetadata
	paths []string
	cmp   Compare
}

func (m metaAndPaths) Len() int {
	return len(m.meta)
}

func (m metaAndPaths) Less(i, j int) bool {
	return m.cmp(m.meta[i].Smallest.UserKey, m.meta[j].Smallest.UserKey) < 0
}

func (m metaAndPaths) Swap(i, j int) {
	m.meta[i], m.meta[j] = m.meta[j], m.meta[i]
	m.paths[i], m.paths[j] = m.paths[j], m.paths[i]
}

func ingestSortAndVerify(cmp Compare, meta []*fileMetadata, paths []string) error {
	if len(meta) <= 1 {
		return nil
	}

	sort.Sort(&metaAndPaths{
		meta:  meta,
		paths: paths,
		cmp:   cmp,
	})

	for i := 1; i < len(meta); i++ {
		if sstableKeyCompare(cmp, meta[i-1].Largest, meta[i].Smallest) >= 0 {
			return fmt.Errorf("pebble: external sstables have overlapping ranges")
		}
	}
	return nil
}

func ingestCleanup(fs vfs.FS, dirname string, meta []*fileMetadata) error {
	var firstErr error
	for i := range meta {
		target := base.MakeFilename(fs, dirname, fileTypeTable, meta[i].FileNum)
		if err := fs.Remove(target); err != nil {
			if firstErr != nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func ingestLink(
	jobID int, opts *Options, dirname string, paths []string, meta []*fileMetadata,
) error {
	// Wrap the normal filesystem with one which wraps newly created files with
	// vfs.NewSyncingFile.
	fs := syncingFS{
		FS: opts.FS,
		syncOpts: vfs.SyncingFileOptions{
			BytesPerSync: opts.BytesPerSync,
		},
	}

	for i := range paths {
		target := base.MakeFilename(fs, dirname, fileTypeTable, meta[i].FileNum)
		err := vfs.LinkOrCopy(fs, paths[i], target)
		if err != nil {
			if err2 := ingestCleanup(fs, dirname, meta[:i]); err2 != nil {
				opts.Logger.Infof("ingest cleanup failed: %v", err2)
			}
			return err
		}
		if opts.EventListener.TableCreated != nil {
			opts.EventListener.TableCreated(TableCreateInfo{
				JobID:   jobID,
				Reason:  "ingesting",
				Path:    target,
				FileNum: meta[i].FileNum,
			})
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
			key, _ := iter.SeekGE(m.Smallest.UserKey)
			if key == nil {
				continue
			}
			if cmp(key.UserKey, m.Largest.UserKey) <= 0 {
				return true
			}
		}
	}

	// Check overlap with range deletions.
	if iter := mem.newRangeDelIter(nil); iter != nil {
		defer iter.Close()
		for _, m := range meta {
			key, val := iter.SeekLT(m.Smallest.UserKey)
			if key == nil {
				key, val = iter.Next()
			}
			for ; key != nil; key, val = iter.Next() {
				if cmp(key.UserKey, m.Largest.UserKey) > 0 {
					// The start of the tombstone is after the largest key in the
					// ingested table.
					break
				}
				if cmp(val, m.Smallest.UserKey) > 0 {
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
		m.Smallest = base.MakeInternalKey(m.Smallest.UserKey, seqNum, m.Smallest.Kind())
		// Don't update the seqnum for the largest key if that key is a range
		// deletion sentinel key as doing so unintentionally extends the bounds of
		// the table.
		if m.Largest.Trailer != InternalKeyRangeDeleteSentinel {
			m.Largest = base.MakeInternalKey(m.Largest.UserKey, seqNum, m.Largest.Kind())
		}
		// Setting smallestSeqNum == largestSeqNum triggers the setting of
		// Properties.GlobalSeqNum when an sstable is loaded.
		m.SmallestSeqNum = seqNum
		m.LargestSeqNum = seqNum
		seqNum++

		// TODO(peter): Update the global sequence number property. This is only
		// necessary for compatibility with RocksDB.
	}
	return nil
}

func ingestTargetLevel(cmp Compare, v *version, meta *fileMetadata) int {
	// Find the lowest level which does not have any files which overlap meta. We
	// search from L0 to L6 looking for whether there are any files in the level
	// which overlap meta. We want the "lowest" level (where lower means
	// increasing level number) in order to reduce write amplification. We can't
	// place meta at or below a level in which it has overlap because doing so
	// could violate the invariant that for a given key the sequence numbers in
	// higher levels will be larger than those in lower levels.
	if len(v.Overlaps(0, cmp, meta.Smallest.UserKey, meta.Largest.UserKey)) != 0 {
		return 0
	}

	level := 1
	for ; level < numLevels; level++ {
		if len(v.Overlaps(level, cmp, meta.Smallest.UserKey, meta.Largest.UserKey)) != 0 {
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
// https://github.com/cockroachdb/pebble/issues/25 for an idea for how to fix
// this hiccup.
func (d *DB) Ingest(paths []string) error {
	if atomic.LoadInt32(&d.closed) != 0 {
		panic(ErrClosed)
	}
	if d.opts.ReadOnly {
		return ErrReadOnly
	}

	// Allocate file numbers for all of the files being ingested and mark them as
	// pending in order to prevent them from being deleted. Note that this causes
	// the file number ordering to be out of alignment with sequence number
	// ordering. The sorting of L0 tables by sequence number avoids relying on
	// that (busted) invariant.
	d.mu.Lock()
	pendingOutputs := make([]uint64, len(paths))
	for i := range paths {
		pendingOutputs[i] = d.mu.versions.getNextFileNum()
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

	// Load the metadata for all of the files being ingested. This step detects
	// and elides empty sstables.
	meta, paths, err := ingestLoad(d.opts, paths, d.cacheID, pendingOutputs)
	if err != nil {
		return err
	}
	if len(meta) == 0 {
		// All of the sstables to be ingested were empty. Nothing to do.
		return nil
	}

	// Verify the sstables do not overlap.
	if err := ingestSortAndVerify(d.cmp, meta, paths); err != nil {
		return err
	}

	// Hard link the sstables into the DB directory. Since the sstables aren't
	// referenced by a version, they won't be used. If the hard linking fails
	// (e.g. because the files reside on a different filesystem), ingestLink will
	// fall back to copying, and if that fails we undo our work and return an
	// error.
	if err := ingestLink(jobID, d.opts, d.dirname, paths, meta); err != nil {
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
		// Note that d.commit.mu is held by commitPipeline when calling prepare.

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

	d.commit.AllocateSeqNum(len(meta), prepare, apply)

	if err != nil {
		if err2 := ingestCleanup(d.opts.FS, d.dirname, meta); err2 != nil {
			d.opts.Logger.Infof("ingest cleanup failed: %v", err2)
		}
	}

	info := TableIngestInfo{
		JobID:        jobID,
		GlobalSeqNum: meta[0].SmallestSeqNum,
		Err:          err,
	}
	if ve != nil {
		info.Tables = make([]struct {
			TableInfo
			Level int
		}, len(ve.NewFiles))
		for i := range ve.NewFiles {
			e := &ve.NewFiles[i]
			info.Tables[i].Level = e.Level
			info.Tables[i].TableInfo = e.Meta.TableInfo()
		}
	}
	d.opts.EventListener.TableIngested(info)

	return err
}

func (d *DB) ingestApply(jobID int, meta []*fileMetadata) (*versionEdit, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	ve := &versionEdit{
		NewFiles: make([]newFileEntry, len(meta)),
	}
	metrics := make(map[int]*LevelMetrics)

	// Lock the manifest for writing before we use the current version to
	// determine the target level. This prevents two concurrent ingestion jobs
	// from using the same version to determine the target level, and also
	// provides serialization with concurrent compaction and flush jobs.
	d.mu.versions.logLock()
	current := d.mu.versions.currentVersion()
	for i := range meta {
		// Determine the lowest level in the LSM for which the sstable doesn't
		// overlap any existing files in the level.
		m := meta[i]
		f := &ve.NewFiles[i]
		f.Level = ingestTargetLevel(d.cmp, current, m)
		f.Meta = *m
		levelMetrics := metrics[f.Level]
		if levelMetrics == nil {
			levelMetrics = &LevelMetrics{}
			metrics[f.Level] = levelMetrics
		}
		levelMetrics.BytesIngested += m.Size
		levelMetrics.TablesIngested++
	}
	if err := d.mu.versions.logAndApply(jobID, ve, metrics, d.dataDir); err != nil {
		return nil, err
	}
	d.updateReadStateLocked()
	d.deleteObsoleteFiles(jobID)
	// The ingestion may have pushed a level over the threshold for compaction,
	// so check to see if one is necessary and schedule it.
	d.maybeScheduleCompaction()
	return ve, nil
}
