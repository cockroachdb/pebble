// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/record"
	"github.com/cockroachdb/pebble/vfs"
)

const numLevels = manifest.NumLevels

// Provide type aliases for the various manifest structs.
type bulkVersionEdit = manifest.BulkVersionEdit
type deletedFileEntry = manifest.DeletedFileEntry
type fileMetadata = manifest.FileMetadata
type newFileEntry = manifest.NewFileEntry
type version = manifest.Version
type versionEdit = manifest.VersionEdit
type versionList = manifest.VersionList

// versionSet manages a collection of immutable versions, and manages the
// creation of a new version from the most recent version. A new version is
// created from an existing version by applying a version edit which is just
// like it sounds: a delta from the previous version. Version edits are logged
// to the MANIFEST file, which is replayed at startup.
type versionSet struct {
	// Immutable fields.
	dirname string
	mu      *sync.Mutex
	opts    *Options
	fs      vfs.FS
	cmp     Compare
	cmpName string
	// Dynamic base level allows the dynamic base level computation to be
	// disabled. Used by tests which want to create specific LSM structures.
	dynamicBaseLevel bool

	// Mutable fields.
	versions versionList
	picker   compactionPicker

	metrics Metrics

	// A pointer to versionSet.addObsoleteLocked. Avoids allocating a new closure
	// on the creation of every version.
	obsoleteFn        func(obsolete []FileNum)
	obsoleteTables    []FileNum
	obsoleteManifests []FileNum
	obsoleteOptions   []FileNum

	// Zombie tables which have been removed from the current version but are
	// still referenced by an inuse iterator.
	zombieTables map[FileNum]uint64 // filenum -> size

	// minUnflushedLogNum is the smallest WAL log file number corresponding to
	// mutations that have not been flushed to an sstable.
	minUnflushedLogNum FileNum

	// The next file number. A single counter is used to assign file numbers
	// for the WAL, MANIFEST, sstable, and OPTIONS files.
	nextFileNum FileNum

	// The upper bound on sequence numbers that have been assigned so far.
	// A suffix of these sequence numbers may not have been written to a
	// WAL. Both logSeqNum and visibleSeqNum are atomically updated by the
	// commitPipeline.
	logSeqNum     uint64 // next seqNum to use for WAL writes
	visibleSeqNum uint64 // visible seqNum (<= logSeqNum)

	// The current manifest file number.
	manifestFileNum FileNum

	manifestFile vfs.File
	manifest     *record.Writer

	writing    bool
	writerCond sync.Cond
}

func (vs *versionSet) init(dirname string, opts *Options, mu *sync.Mutex) {
	vs.dirname = dirname
	vs.mu = mu
	vs.writerCond.L = mu
	vs.opts = opts
	vs.fs = opts.FS
	vs.cmp = opts.Comparer.Compare
	vs.cmpName = opts.Comparer.Name
	vs.dynamicBaseLevel = true
	vs.versions.Init(mu)
	vs.obsoleteFn = vs.addObsoleteLocked
	vs.zombieTables = make(map[FileNum]uint64)
	vs.nextFileNum = 1
}

// create creates a version set for a fresh DB.
func (vs *versionSet) create(
	jobID int, dirname string, dir vfs.File, opts *Options, mu *sync.Mutex,
) error {
	vs.init(dirname, opts, mu)
	newVersion := &version{}
	vs.append(newVersion)
	vs.picker = newCompactionPicker(newVersion, vs.opts, nil)

	// Note that a "snapshot" version edit is written to the manifest when it is
	// created.
	vs.manifestFileNum = vs.getNextFileNum()
	err := vs.createManifest(vs.dirname, vs.manifestFileNum, vs.minUnflushedLogNum, vs.nextFileNum)
	if err == nil {
		if err = vs.manifest.Flush(); err != nil {
			vs.opts.Logger.Fatalf("MANIFEST flush failed: %v", err)
		}
	}
	if err == nil {
		if err = vs.manifestFile.Sync(); err != nil {
			vs.opts.Logger.Fatalf("MANIFEST sync failed: %v", err)
		}
	}
	if err == nil {
		if err = setCurrentFile(vs.dirname, vs.fs, vs.manifestFileNum); err != nil {
			vs.opts.Logger.Fatalf("MANIFEST set current failed: %v", err)
		}
	}
	if err == nil {
		if err = dir.Sync(); err != nil {
			vs.opts.Logger.Fatalf("MANIFEST dirsync failed: %v", err)
		}
	}

	vs.opts.EventListener.ManifestCreated(ManifestCreateInfo{
		JobID:   jobID,
		Path:    base.MakeFilename(vs.fs, vs.dirname, fileTypeManifest, vs.manifestFileNum),
		FileNum: vs.manifestFileNum,
		Err:     err,
	})
	if err != nil {
		return err
	}
	return nil
}

// load loads the version set from the manifest file.
func (vs *versionSet) load(dirname string, opts *Options, mu *sync.Mutex) error {
	vs.init(dirname, opts, mu)

	// Read the CURRENT file to find the current manifest file.
	current, err := vs.fs.Open(base.MakeFilename(vs.fs, dirname, fileTypeCurrent, 0))
	if err != nil {
		return errors.Wrapf(err, "pebble: could not open CURRENT file for DB %q", dirname)
	}
	defer current.Close()
	stat, err := current.Stat()
	if err != nil {
		return err
	}
	n := stat.Size()
	if n == 0 {
		return errors.Errorf("pebble: CURRENT file for DB %q is empty", dirname)
	}
	if n > 4096 {
		return errors.Errorf("pebble: CURRENT file for DB %q is too large", dirname)
	}
	b := make([]byte, n)
	_, err = current.ReadAt(b, 0)
	if err != nil {
		return err
	}
	if b[n-1] != '\n' {
		return errors.Errorf("pebble: CURRENT file for DB %q is malformed", dirname)
	}
	b = bytes.TrimSpace(b)

	var ok bool
	if _, vs.manifestFileNum, ok = base.ParseFilename(vs.fs, string(b)); !ok {
		return errors.Errorf("pebble: MANIFEST name %q is malformed", errors.Safe(b))
	}

	// Read the versionEdits in the manifest file.
	var bve bulkVersionEdit
	manifest, err := vs.fs.Open(vs.fs.PathJoin(dirname, string(b)))
	if err != nil {
		return errors.Wrapf(err, "pebble: could not open manifest file %q for DB %q",
			errors.Safe(b), dirname)
	}
	defer manifest.Close()
	rr := record.NewReader(manifest, 0 /* logNum */)
	for {
		r, err := rr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		var ve versionEdit
		err = ve.Decode(r)
		if err != nil {
			return err
		}
		if ve.ComparerName != "" {
			if ve.ComparerName != vs.cmpName {
				return errors.Errorf("pebble: manifest file %q for DB %q: "+
					"comparer name from file %q != comparer name from Options %q",
					errors.Safe(b), dirname, errors.Safe(ve.ComparerName), errors.Safe(vs.cmpName))
			}
		}
		bve.Accumulate(&ve)
		if ve.MinUnflushedLogNum != 0 {
			vs.minUnflushedLogNum = ve.MinUnflushedLogNum
		}
		if ve.NextFileNum != 0 {
			vs.nextFileNum = ve.NextFileNum
		}
		if ve.LastSeqNum != 0 {
			// logSeqNum is the _next_ sequence number that will be assigned,
			// while LastSeqNum is the last assigned sequence number. Note that
			// this behaviour mimics that in RocksDB; the first sequence number
			// assigned is one greater than the one present in the manifest
			// (assuming no WALs contain higher sequence numbers than the
			// manifest's LastSeqNum). Increment LastSeqNum by 1 to get the
			// next sequence number that will be assigned.
			vs.logSeqNum = ve.LastSeqNum + 1
		}
	}
	// We have already set vs.nextFileNum = 2 at the beginning of the
	// function and could have only updated it to some other non-zero value,
	// so it cannot be 0 here.
	if vs.minUnflushedLogNum == 0 {
		if vs.nextFileNum >= 2 {
			// We either have a freshly created DB, or a DB created by RocksDB
			// that has not had a single flushed SSTable yet. This is because
			// RocksDB bumps up nextFileNum in this case without bumping up
			// minUnflushedLogNum, even if WALs with non-zero file numbers are
			// present in the directory.
		} else {
			return errors.Errorf("pebble: malformed manifest file %q for DB %q",
				errors.Safe(b), dirname)
		}
	}
	vs.markFileNumUsed(vs.minUnflushedLogNum)

	newVersion, _, err := bve.Apply(nil, vs.cmp, opts.Comparer.Format)
	if err != nil {
		return err
	}
	vs.append(newVersion)

	vs.picker = newCompactionPicker(newVersion, vs.opts, nil)

	for i := range vs.metrics.Levels {
		l := &vs.metrics.Levels[i]
		l.NumFiles = int64(len(newVersion.Files[i]))
		l.Size = uint64(totalSize(newVersion.Files[i]))
	}
	return nil
}

func (vs *versionSet) close() error {
	if vs.manifestFile != nil {
		if err := vs.manifestFile.Close(); err != nil {
			return err
		}
	}
	return nil
}

// logLock locks the manifest for writing. The lock must be released by either
// a call to logUnlock or logAndApply.
//
// DB.mu must be held when calling this method, but the mutex may be dropped and
// re-acquired during the course of this method.
func (vs *versionSet) logLock() {
	// Wait for any existing writing to the manifest to complete, then mark the
	// manifest as busy.
	for vs.writing {
		vs.writerCond.Wait()
	}
	vs.writing = true
}

// logUnlock releases the lock for manifest writing.
//
// DB.mu must be held when calling this method.
func (vs *versionSet) logUnlock() {
	if !vs.writing {
		vs.opts.Logger.Fatalf("MANIFEST not locked for writing")
	}
	vs.writing = false
	vs.writerCond.Signal()
}

// logAndApply logs the version edit to the manifest, applies the version edit
// to the current version, and installs the new version.
//
// DB.mu must be held when calling this method and will be released temporarily
// while performing file I/O. Requires that the manifest is locked for writing
// (see logLock). Will unconditionally release the manifest lock (via
// logUnlock) even if an error occurs.
//
// inProgressCompactions is called while DB.mu is held, to get the list of
// in-progress compactions.
func (vs *versionSet) logAndApply(
	jobID int,
	ve *versionEdit,
	metrics map[int]*LevelMetrics,
	dir vfs.File,
	inProgressCompactions func() []compactionInfo,
) error {
	if !vs.writing {
		vs.opts.Logger.Fatalf("MANIFEST not locked for writing")
	}
	defer vs.logUnlock()

	if ve.MinUnflushedLogNum != 0 {
		if ve.MinUnflushedLogNum < vs.minUnflushedLogNum ||
			vs.nextFileNum <= ve.MinUnflushedLogNum {
			panic(fmt.Sprintf("pebble: inconsistent versionEdit minUnflushedLogNum %d",
				ve.MinUnflushedLogNum))
		}
	}

	// This is the next manifest filenum, but if the current file is too big we
	// will write this ve to the next file which means what ve encodes is the
	// current filenum and not the next one.
	//
	// TODO(sbhola): figure out why this is correct and update comment.
	ve.NextFileNum = vs.nextFileNum

	// LastSeqNum is set to the current upper bound on the assigned sequence
	// numbers. Note that this is exactly the behavior of RocksDB. LastSeqNum is
	// used to initialize versionSet.logSeqNum and versionSet.visibleSeqNum on
	// replay. It must be higher than or equal to any than any sequence number
	// written to an sstable, including sequence numbers in ingested files.
	// Note that LastSeqNum is not (and cannot be) the minumum unflushed sequence
	// number. This is fallout from ingestion which allows a sequence number X to
	// be assigned to an ingested sstable even though sequence number X-1 resides
	// in an unflushed memtable. logSeqNum is the _next_ sequence number that
	// will be assigned, so subtract that by 1 to get the upper bound on the
	// last assigned sequence number.
	logSeqNum := atomic.LoadUint64(&vs.logSeqNum)
	ve.LastSeqNum = logSeqNum - 1
	if logSeqNum == 0 {
		// logSeqNum is initialized to 1 in Open() if there are no previous WAL
		// or manifest records, so this case should never happen.
		vs.opts.Logger.Fatalf("logSeqNum must be a positive integer: %d", logSeqNum)
	}

	currentVersion := vs.currentVersion()
	var newVersion *version

	// Generate a new manifest if we don't currently have one, or the current one
	// is too large.
	var newManifestFileNum FileNum
	if vs.manifest == nil || vs.manifest.Size() >= vs.opts.MaxManifestFileSize {
		newManifestFileNum = vs.getNextFileNum()
	}

	// Grab certain values before releasing vs.mu, in case createManifest() needs
	// to be called.
	minUnflushedLogNum := vs.minUnflushedLogNum
	nextFileNum := vs.nextFileNum

	var zombies map[FileNum]uint64
	if err := func() error {
		vs.mu.Unlock()
		defer vs.mu.Lock()

		var bve bulkVersionEdit
		bve.Accumulate(ve)

		var err error
		newVersion, zombies, err = bve.Apply(currentVersion, vs.cmp, vs.opts.Comparer.Format)
		if err != nil {
			return err
		}

		if newManifestFileNum != 0 {
			if err := vs.createManifest(vs.dirname, newManifestFileNum, minUnflushedLogNum, nextFileNum); err != nil {
				vs.opts.EventListener.ManifestCreated(ManifestCreateInfo{
					JobID:   jobID,
					Path:    base.MakeFilename(vs.fs, vs.dirname, fileTypeManifest, newManifestFileNum),
					FileNum: newManifestFileNum,
					Err:     err,
				})
				return err
			}
		}

		w, err := vs.manifest.Next()
		if err != nil {
			return err
		}
		// NB: Any error from this point on is considered fatal as we don't now if
		// the MANIFEST write occurred or not. Trying to determine that is
		// fraught. Instead we rely on the standard recovery mechanism run when a
		// database is open. In particular, that mechanism generates a new MANIFEST
		// and ensures it is synced.
		if err := ve.Encode(w); err != nil {
			vs.opts.Logger.Fatalf("MANIFEST write failed: %v", err)
			return err
		}
		if err := vs.manifest.Flush(); err != nil {
			vs.opts.Logger.Fatalf("MANIFEST flush failed: %v", err)
			return err
		}
		if err := vs.manifestFile.Sync(); err != nil {
			vs.opts.Logger.Fatalf("MANIFEST sync failed: %v", err)
			return err
		}
		if newManifestFileNum != 0 {
			if err := setCurrentFile(vs.dirname, vs.fs, newManifestFileNum); err != nil {
				vs.opts.Logger.Fatalf("MANIFEST set current failed: %v", err)
				return err
			}
			if err := dir.Sync(); err != nil {
				vs.opts.Logger.Fatalf("MANIFEST dirsync failed: %v", err)
				return err
			}
			vs.opts.EventListener.ManifestCreated(ManifestCreateInfo{
				JobID:   jobID,
				Path:    base.MakeFilename(vs.fs, vs.dirname, fileTypeManifest, newManifestFileNum),
				FileNum: newManifestFileNum,
			})
		}
		return nil
	}(); err != nil {
		return err
	}

	// Update the zombie tables set first, as installation of the new version
	// will unref the previous version which could result in addObsoleteLocked
	// being called.
	for fileNum, size := range zombies {
		vs.zombieTables[fileNum] = size
	}

	// Install the new version.
	vs.append(newVersion)
	if ve.MinUnflushedLogNum != 0 {
		vs.minUnflushedLogNum = ve.MinUnflushedLogNum
	}
	if newManifestFileNum != 0 {
		if vs.manifestFileNum != 0 {
			vs.obsoleteManifests = append(vs.obsoleteManifests, vs.manifestFileNum)
		}
		vs.manifestFileNum = newManifestFileNum
	}
	vs.picker = newCompactionPicker(newVersion, vs.opts, inProgressCompactions())
	if !vs.dynamicBaseLevel {
		vs.picker.forceBaseLevel1()
	}

	for level, update := range metrics {
		vs.metrics.Levels[level].Add(update)
	}
	for i := range vs.metrics.Levels {
		l := &vs.metrics.Levels[i]
		l.NumFiles = int64(len(newVersion.Files[i]))
		l.Size = uint64(totalSize(newVersion.Files[i]))
	}
	return nil
}

func (vs *versionSet) incrementCompactions() {
	vs.metrics.Compact.Count++
}

func (vs *versionSet) incrementFlushes() {
	vs.metrics.Flush.Count++
}

// createManifest creates a manifest file that contains a snapshot of vs.
func (vs *versionSet) createManifest(
	dirname string, fileNum, minUnflushedLogNum, nextFileNum FileNum,
) (err error) {
	var (
		filename     = base.MakeFilename(vs.fs, dirname, fileTypeManifest, fileNum)
		manifestFile vfs.File
		manifest     *record.Writer
	)
	defer func() {
		if manifest != nil {
			manifest.Close()
		}
		if manifestFile != nil {
			manifestFile.Close()
		}
		if err != nil {
			vs.fs.Remove(filename)
		}
	}()
	manifestFile, err = vs.fs.Create(filename)
	if err != nil {
		return err
	}
	manifest = record.NewWriter(manifestFile)

	snapshot := versionEdit{
		ComparerName: vs.cmpName,
	}
	for level, fileMetadata := range vs.currentVersion().Files {
		for _, meta := range fileMetadata {
			snapshot.NewFiles = append(snapshot.NewFiles, newFileEntry{
				Level: level,
				Meta:  meta,
			})
		}
	}

	// When creating a version snapshot for an existing DB, this snapshot VersionEdit will be
	// immediately followed by another VersionEdit (being written in logAndApply()). That
	// VersionEdit always contains a LastSeqNum, so we don't need to include that in the snapshot.
	// But it does not necessarily include MinUnflushedLogNum, NextFileNum, so we initialize those
	// using the corresponding fields in the versionSet (which came from the latest preceding
	// VersionEdit that had those fields).
	snapshot.MinUnflushedLogNum = minUnflushedLogNum
	snapshot.NextFileNum = nextFileNum

	w, err1 := manifest.Next()
	if err1 != nil {
		return err1
	}
	if err := snapshot.Encode(w); err != nil {
		return err
	}

	if vs.manifest != nil {
		vs.manifest.Close()
		vs.manifest = nil
	}
	if vs.manifestFile != nil {
		if err := vs.manifestFile.Close(); err != nil {
			return err
		}
		vs.manifestFile = nil
	}

	vs.manifest, manifest = manifest, nil
	vs.manifestFile, manifestFile = manifestFile, nil
	return nil
}

func (vs *versionSet) markFileNumUsed(fileNum FileNum) {
	if vs.nextFileNum <= fileNum {
		vs.nextFileNum = fileNum + 1
	}
}

func (vs *versionSet) getNextFileNum() FileNum {
	x := vs.nextFileNum
	vs.nextFileNum++
	return x
}

func (vs *versionSet) append(v *version) {
	if v.Refs() != 0 {
		panic("pebble: version should be unreferenced")
	}
	if !vs.versions.Empty() {
		vs.versions.Back().UnrefLocked()
	}
	v.Deleted = vs.obsoleteFn
	v.Ref()
	vs.versions.PushBack(v)
}

func (vs *versionSet) currentVersion() *version {
	return vs.versions.Back()
}

func (vs *versionSet) addLiveFileNums(m map[FileNum]struct{}) {
	current := vs.currentVersion()
	for v := vs.versions.Front(); true; v = v.Next() {
		for _, ff := range v.Files {
			for _, f := range ff {
				m[f.FileNum] = struct{}{}
			}
		}
		if v == current {
			break
		}
	}
}

func (vs *versionSet) addObsoleteLocked(obsolete []FileNum) {
	for _, fileNum := range obsolete {
		// Note that the obsolete tables are no longer zombie by the definition of
		// zombie, but we leave them in the zombie tables map until they are
		// deleted from disk.
		if _, ok := vs.zombieTables[fileNum]; !ok {
			vs.opts.Logger.Fatalf("MANIFEST obsolete table %s not marked as zombie", fileNum)
		}
	}
	vs.obsoleteTables = append(vs.obsoleteTables, obsolete...)
}
