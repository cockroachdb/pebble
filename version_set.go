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
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/record"
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
	// WARNING: The following struct `atomic` contains fields are accessed atomically.
	//
	// Go allocations are guaranteed to be 64-bit aligned which we take advantage
	// of by placing the 64-bit fields which we access atomically at the beginning
	// of the versionSet struct.
	// For more information, see https://golang.org/pkg/sync/atomic/#pkg-note-BUG.
	atomic struct {
		logSeqNum uint64 // next seqNum to use for WAL writes

		// The upper bound on sequence numbers that have been assigned so far.
		// A suffix of these sequence numbers may not have been written to a
		// WAL. Both logSeqNum and visibleSeqNum are atomically updated by the
		// commitPipeline.
		visibleSeqNum uint64 // visible seqNum (<= logSeqNum)

		// Number of bytes present in sstables being written by in-progress
		// compactions. This value will be zero if there are no in-progress
		// compactions. Updated and read atomically.
		atomicInProgressBytes int64
	}

	// Immutable fields.
	dirname string
	// Set to DB.mu.
	mu      *sync.Mutex
	opts    *Options
	fs      vfs.FS
	cmp     Compare
	cmpName string
	// Dynamic base level allows the dynamic base level computation to be
	// disabled. Used by tests which want to create specific LSM structures.
	dynamicBaseLevel bool

	// currentFS is a FS implementation that is used when interacting
	// with the CURRENT file.
	//
	// It must provide atomic Renames to ensure that manifest roations
	// are atomic. The vfs.FS interface does not guarantee atomicity of
	// Rename operations unless (vfs.FS).Attributes's RenameIsAtomic
	// field is true.  On initialization, we unwrap the opts.FS until we
	// find an FS implementation that supports atomic renames, which we
	// then use for the `CURRENT-00000x` file.
	currentFS vfs.FS
	// formatVers is the database's current format major version. It's
	// encoded within the numeric portion of the CURRENT-xxxxxx
	// filename.
	formatVers FormatMajorVersion

	// Mutable fields.
	versions versionList
	picker   compactionPicker

	metrics Metrics

	// A pointer to versionSet.addObsoleteLocked. Avoids allocating a new closure
	// on the creation of every version.
	obsoleteFn        func(obsolete []*manifest.FileMetadata)
	obsoleteTables    []*manifest.FileMetadata
	obsoleteManifests []fileInfo
	obsoleteOptions   []fileInfo

	// Zombie tables which have been removed from the current version but are
	// still referenced by an inuse iterator.
	zombieTables map[FileNum]uint64 // filenum -> size

	// minUnflushedLogNum is the smallest WAL log file number corresponding to
	// mutations that have not been flushed to an sstable.
	minUnflushedLogNum FileNum

	// The next file number. A single counter is used to assign file numbers
	// for the WAL, MANIFEST, sstable, and OPTIONS files.
	nextFileNum FileNum

	// The current manifest file number.
	manifestFileNum FileNum

	manifestFile vfs.File
	manifest     *record.Writer

	writing    bool
	writerCond sync.Cond
}

func getAtomicRenameFS(fs vfs.FS) (vfs.FS, error) {
	atomicRenameFS := fs
	for atomicRenameFS != nil && !atomicRenameFS.Attributes().RenameIsAtomic {
		atomicRenameFS = atomicRenameFS.Unwrap()
	}
	if atomicRenameFS == nil {
		return nil, errors.Newf("filesystem has no support for atomic renames: %s", fs.Attributes().Description)
	}
	return atomicRenameFS, nil
}

func (vs *versionSet) init(
	dirname string, opts *Options, formatVers FormatMajorVersion, mu *sync.Mutex,
) error {
	vs.dirname = dirname
	vs.mu = mu
	vs.writerCond.L = mu
	vs.opts = opts
	vs.fs = opts.FS
	vs.formatVers = formatVers
	vs.currentFS = opts.FS
	if formatVers >= FormatCurrentVersioned {
		var err error
		vs.currentFS, err = getAtomicRenameFS(opts.FS)
		if err != nil {
			return err
		}
	}
	vs.cmp = opts.Comparer.Compare
	vs.cmpName = opts.Comparer.Name
	vs.dynamicBaseLevel = true
	vs.versions.Init(mu)
	vs.obsoleteFn = vs.addObsoleteLocked
	vs.zombieTables = make(map[FileNum]uint64)
	vs.nextFileNum = 1
	return nil
}

// create creates a version set for a fresh DB.
func (vs *versionSet) create(
	jobID int, dirname string, dir vfs.File, opts *Options, mu *sync.Mutex,
) error {
	if err := vs.init(dirname, opts, opts.FormatMajorVersion, mu); err != nil {
		return err
	}
	newVersion := &version{}
	vs.append(newVersion)
	vs.picker = newCompactionPicker(newVersion, vs.opts, nil, vs.metrics.levelSizes())

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
		if err = setCurrentFile(vs.dirname, vs.currentFS, vs.formatVers, vs.manifestFileNum); err != nil {
			vs.opts.Logger.Fatalf("MANIFEST set current failed: %v", err)
		}
	}

	// Pebble versions at FormatMostCompatible used the `CURRENT` file,
	// instead of `CURRENT-xxxxxx` files. If the database is using a
	// later format version, write a `CURRENT` file that ensures
	// previous versions of Pebble will fail to open. The resulting
	// error message won't be the most indicative, but there's nothing
	// we can do to update old Pebble versions.
	if vs.formatVers > FormatMostCompatible {
		if err = setCurrentFile(vs.dirname, vs.opts.FS, FormatMostCompatible, 0 /* manifest file numbber */); err != nil {
			vs.opts.Logger.Fatalf("setting CURRENT tombstone: %v", err)
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

func readCurrentFile(fs vfs.FS, dirname string) (FormatMajorVersion, FileNum, error) {
	// First check for the existence of the new CURRENT-xxxxxx files,
	// using the atomic FS.
	atomicFS, err := getAtomicRenameFS(fs)
	if err != nil {
		return 0, 0, err
	}

	// The database's current format version is encoded in the
	// CURRENT-xxxxxx file. There may be multiple CURRENT-xxxxxx files
	// if the database crashed mid-upgrade. Use the highest (most
	// recent) value.
	ls, err := atomicFS.List(dirname)
	if err != nil {
		return 0, 0, err
	}
	formatVers := FormatMostCompatible
	for _, filename := range ls {
		typ, num, ok := base.ParseFilename(atomicFS, filename)
		if !ok || typ != fileTypeCurrent {
			continue
		}
		if formatVers < FormatMajorVersion(num) {
			formatVers = FormatMajorVersion(num)
		}
	}

	// All the CURRENT-xxxxxx files are accessed via the atomic rename
	// FS. If the database was created before the format major version
	// was introduced, formatVers will be `FormatMostCompatible` and
	// there will only be an unnumbered `CURRENT` file. This file exists
	// on the opts.FS, regardless of whether opts.FS provides atomic
	// renames.
	currentFS := atomicFS
	if formatVers == FormatMostCompatible {
		currentFS = fs

	}
	manifestFileNum, err := readCurrentFile1(dirname, currentFS, formatVers)
	if err != nil {
		return 0, 0, err
	}
	return formatVers, manifestFileNum, nil
}

func readCurrentFile1(
	dirname string, currentFS vfs.FS, formatVers FormatMajorVersion,
) (base.FileNum, error) {
	filename := base.MakeCurrentFilename(currentFS, dirname, formatVers)
	current, err := currentFS.Open(filename)
	if err != nil {
		return 0, errors.Wrapf(err, "pebble: could not open %q file for DB %q", filename, dirname)
	}
	defer current.Close()
	stat, err := current.Stat()
	if err != nil {
		return 0, err
	}
	n := stat.Size()
	if n == 0 {
		return 0, errors.Errorf("pebble: %q file for DB %q is empty", filename, dirname)
	}
	if n > 4096 {
		return 0, errors.Errorf("pebble: %q file for DB %q is too large", filename, dirname)
	}
	b := make([]byte, n)
	_, err = current.ReadAt(b, 0)
	if err != nil {
		return 0, err
	}
	if b[n-1] != '\n' {
		return 0, base.CorruptionErrorf("pebble: %q file for DB %q is malformed", filename, dirname)
	}
	b = bytes.TrimSpace(b)

	_, fn, ok := base.ParseFilename(currentFS, string(b))
	if !ok {
		return 0, base.CorruptionErrorf("pebble: MANIFEST name %q is malformed", errors.Safe(b))
	}
	return fn, nil
}

// load loads the version set from the manifest file.
func (vs *versionSet) load(
	dirname string,
	opts *Options,
	formatVers FormatMajorVersion,
	manifestFileNum base.FileNum,
	mu *sync.Mutex,
) error {
	if err := vs.init(dirname, opts, formatVers, mu); err != nil {
		return err
	}
	vs.formatVers = formatVers
	vs.manifestFileNum = manifestFileNum
	manifestFilename := base.MakeFilename(vs.fs, dirname, fileTypeManifest, vs.manifestFileNum)

	// Read the versionEdits in the manifest file.
	var bve bulkVersionEdit
	bve.AddedByFileNum = make(map[base.FileNum]*fileMetadata)
	manifest, err := vs.fs.Open(manifestFilename)
	if err != nil {
		return errors.Wrapf(err, "pebble: could not open manifest file %q for DB %q",
			errors.Safe(vs.fs.PathBase(manifestFilename)), dirname)
	}
	defer manifest.Close()
	rr := record.NewReader(manifest, 0 /* logNum */)
	for {
		r, err := rr.Next()
		if err == io.EOF || record.IsInvalidRecord(err) {
			break
		}
		if err != nil {
			return errors.Wrapf(err, "pebble: error when loading manifest file %q",
				errors.Safe(vs.fs.PathBase(manifestFilename)))
		}
		var ve versionEdit
		err = ve.Decode(r)
		if err != nil {
			// Break instead of returning an error if the record is corrupted
			// or invalid.
			if err == io.EOF || record.IsInvalidRecord(err) {
				break
			}
			return err
		}
		if ve.ComparerName != "" {
			if ve.ComparerName != vs.cmpName {
				return errors.Errorf("pebble: manifest file %q for DB %q: "+
					"comparer name from file %q != comparer name from Options %q",
					errors.Safe(vs.fs.PathBase(manifestFilename)), dirname, errors.Safe(ve.ComparerName), errors.Safe(vs.cmpName))
			}
		}
		if err := bve.Accumulate(&ve); err != nil {
			return err
		}
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
			vs.atomic.logSeqNum = ve.LastSeqNum + 1
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
			return base.CorruptionErrorf("pebble: malformed manifest file %q for DB %q",
				errors.Safe(vs.fs.PathBase(manifestFilename)), dirname)
		}
	}
	vs.markFileNumUsed(vs.minUnflushedLogNum)

	newVersion, _, err := bve.Apply(nil, vs.cmp, opts.Comparer.FormatKey, opts.FlushSplitBytes, opts.Experimental.ReadCompactionRate)
	if err != nil {
		return err
	}
	newVersion.L0Sublevels.InitCompactingFileInfo(nil /* in-progress compactions */)
	vs.append(newVersion)

	for i := range vs.metrics.Levels {
		l := &vs.metrics.Levels[i]
		l.NumFiles = int64(newVersion.Levels[i].Len())
		files := newVersion.Levels[i].Slice()
		l.Size = int64(files.SizeSum())
	}
	vs.picker = newCompactionPicker(newVersion, vs.opts, nil, vs.metrics.levelSizes())
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
	logSeqNum := atomic.LoadUint64(&vs.atomic.logSeqNum)
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
	var prevManifestFileSize uint64
	if vs.manifest == nil || vs.manifest.Size() >= vs.opts.MaxManifestFileSize {
		newManifestFileNum = vs.getNextFileNum()
		prevManifestFileSize = uint64(vs.manifest.Size())
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
		if err := bve.Accumulate(ve); err != nil {
			return err
		}

		var err error
		newVersion, zombies, err = bve.Apply(currentVersion, vs.cmp, vs.opts.Comparer.FormatKey, vs.opts.FlushSplitBytes, vs.opts.Experimental.ReadCompactionRate)
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
			if err := setCurrentFile(vs.dirname, vs.currentFS, vs.formatVers, newManifestFileNum); err != nil {
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

	// Now that DB.mu is held again, initialize compacting file info in
	// L0Sublevels.
	inProgress := inProgressCompactions()

	newVersion.L0Sublevels.InitCompactingFileInfo(inProgressL0Compactions(inProgress))

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
			vs.obsoleteManifests = append(vs.obsoleteManifests, fileInfo{
				fileNum:  vs.manifestFileNum,
				fileSize: prevManifestFileSize,
			})
		}
		vs.manifestFileNum = newManifestFileNum
	}

	for level, update := range metrics {
		vs.metrics.Levels[level].Add(update)
	}
	for i := range vs.metrics.Levels {
		l := &vs.metrics.Levels[i]
		l.Sublevels = 0
		if l.NumFiles > 0 {
			l.Sublevels = 1
		}
		if invariants.Enabled {
			if count := int64(newVersion.Levels[i].Len()); l.NumFiles != count {
				vs.opts.Logger.Fatalf("versionSet metrics L%d NumFiles = %d, actual count = %d", i, l.NumFiles, count)
			}
			levelFiles := newVersion.Levels[i].Slice()
			if size := int64(levelFiles.SizeSum()); l.Size != size {
				vs.opts.Logger.Fatalf("versionSet metrics L%d Size = %d, actual size = %d", i, l.Size, size)
			}
		}
	}
	vs.metrics.Levels[0].Sublevels = int32(len(newVersion.L0SublevelFiles))

	vs.picker = newCompactionPicker(newVersion, vs.opts, inProgress, vs.metrics.levelSizes())
	if !vs.dynamicBaseLevel {
		vs.picker.forceBaseLevel1()
	}

	return nil
}

func (vs *versionSet) incrementCompactions(kind compactionKind) {
	switch kind {
	case compactionKindDefault:
		vs.metrics.Compact.Count++
		vs.metrics.Compact.DefaultCount++

	case compactionKindFlush:
		vs.metrics.Flush.Count++

	case compactionKindMove:
		vs.metrics.Compact.Count++
		vs.metrics.Compact.MoveCount++

	case compactionKindDeleteOnly:
		vs.metrics.Compact.Count++
		vs.metrics.Compact.DeleteOnlyCount++

	case compactionKindElisionOnly:
		vs.metrics.Compact.Count++
		vs.metrics.Compact.ElisionOnlyCount++

	case compactionKindRead:
		vs.metrics.Compact.Count++
		vs.metrics.Compact.ReadCount++
	}
}

func (vs *versionSet) incrementFlushes() {
	vs.metrics.Flush.Count++
}

func (vs *versionSet) incrementCompactionBytes(numBytes int64) {
	atomic.AddInt64(&vs.atomic.atomicInProgressBytes, numBytes)
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
	for level, levelMetadata := range vs.currentVersion().Levels {
		iter := levelMetadata.Iter()
		for meta := iter.First(); meta != nil; meta = iter.Next() {
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
		for _, lm := range v.Levels {
			iter := lm.Iter()
			for f := iter.First(); f != nil; f = iter.Next() {
				m[f.FileNum] = struct{}{}
			}
		}
		if v == current {
			break
		}
	}
}

func (vs *versionSet) addObsoleteLocked(obsolete []*manifest.FileMetadata) {
	for _, fileMeta := range obsolete {
		// Note that the obsolete tables are no longer zombie by the definition of
		// zombie, but we leave them in the zombie tables map until they are
		// deleted from disk.
		if _, ok := vs.zombieTables[fileMeta.FileNum]; !ok {
			vs.opts.Logger.Fatalf("MANIFEST obsolete table %s not marked as zombie", fileMeta.FileNum)
		}
	}
	vs.obsoleteTables = append(vs.obsoleteTables, obsolete...)
	vs.incrementObsoleteTablesLocked(obsolete)
}

func (vs *versionSet) incrementObsoleteTablesLocked(obsolete []*manifest.FileMetadata) {
	for _, fileMeta := range obsolete {
		vs.metrics.Table.ObsoleteCount++
		vs.metrics.Table.ObsoleteSize += fileMeta.Size
	}
}

func newFileMetrics(newFiles []manifest.NewFileEntry) map[int]*LevelMetrics {
	m := map[int]*LevelMetrics{}
	for _, nf := range newFiles {
		lm := m[nf.Level]
		if lm == nil {
			lm = &LevelMetrics{}
			m[nf.Level] = lm
		}
		lm.NumFiles++
		lm.Size += int64(nf.Meta.Size)
	}
	return m
}
