// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/atomicfs"
)

const numLevels = manifest.NumLevels

const manifestMarkerName = `manifest`

// Provide type aliases for the various manifest structs.
type version = manifest.Version
type versionEdit = manifest.VersionEdit
type versionList = manifest.VersionList

// versionSet manages a collection of immutable versions, and manages the
// creation of a new version from the most recent version. A new version is
// created from an existing version by applying a version edit which is just
// like it sounds: a delta from the previous version. Version edits are logged
// to the MANIFEST file, which is replayed at startup.
type versionSet struct {
	// Next seqNum to use for WAL writes.
	logSeqNum base.AtomicSeqNum

	// The upper bound on sequence numbers that have been assigned so far. A
	// suffix of these sequence numbers may not have been written to a WAL. Both
	// logSeqNum and visibleSeqNum are atomically updated by the commitPipeline.
	// visibleSeqNum is <= logSeqNum.
	visibleSeqNum base.AtomicSeqNum

	// Number of bytes present in sstables being written by in-progress
	// compactions. This value will be zero if there are no in-progress
	// compactions. Updated and read atomically.
	atomicInProgressBytes atomic.Int64

	// Immutable fields.
	dirname  string
	provider objstorage.Provider
	// Set to DB.mu.
	mu   *sync.Mutex
	opts *Options
	fs   vfs.FS
	cmp  *base.Comparer
	// Dynamic base level allows the dynamic base level computation to be
	// disabled. Used by tests which want to create specific LSM structures.
	dynamicBaseLevel bool

	// Mutable fields.
	versions    versionList
	l0Organizer *manifest.L0Organizer
	// blobFiles is the set of blob files referenced by the current version.
	// blobFiles is protected by the manifest logLock (not vs.mu).
	blobFiles manifest.CurrentBlobFileSet
	picker    compactionPicker
	// curCompactionConcurrency is updated whenever picker is updated.
	// INVARIANT: >= 1.
	curCompactionConcurrency atomic.Int32

	// Not all metrics are kept here. See DB.Metrics().
	metrics Metrics

	// A pointer to versionSet.addObsoleteLocked. Avoids allocating a new closure
	// on the creation of every version.
	obsoleteFn func(manifest.ObsoleteFiles)
	// obsolete{Tables,Blobs,Manifests,Options} are sorted by file number ascending.
	obsoleteTables    []objectInfo
	obsoleteBlobs     []objectInfo
	obsoleteManifests []obsoleteFile
	obsoleteOptions   []obsoleteFile

	// Zombie tables which have been removed from the current version but are
	// still referenced by an inuse iterator.
	zombieTables zombieObjects
	// Zombie blobs which have been removed from the current version but are
	// still referenced by an inuse iterator.
	zombieBlobs zombieObjects
	// virtualBackings contains information about the FileBackings which support
	// virtual sstables in the latest version. It is mainly used to determine when
	// a backing is no longer in use by the tables in the latest version; this is
	// not a trivial problem because compactions and other operations can happen
	// in parallel (and they can finish in unpredictable order).
	//
	// This mechanism is complementary to the backing Ref/Unref mechanism, which
	// determines when a backing is no longer used by *any* live version and can
	// be removed.
	//
	// In principle this should have been a copy-on-write structure, with each
	// Version having its own record of its virtual backings (similar to the
	// B-tree that holds the tables). However, in practice we only need it for the
	// latest version, so we use a simpler structure and store it in the
	// versionSet instead.
	//
	// virtualBackings is modified under DB.mu and the log lock. If it is accessed
	// under DB.mu and a version update is in progress, it reflects the state of
	// the next version.
	virtualBackings manifest.VirtualBackings

	// minUnflushedLogNum is the smallest WAL log file number corresponding to
	// mutations that have not been flushed to an sstable.
	minUnflushedLogNum base.DiskFileNum

	// The next file number. A single counter is used to assign file
	// numbers for the WAL, MANIFEST, sstable, and OPTIONS files.
	nextFileNum atomic.Uint64

	// The current manifest file number.
	manifestFileNum base.DiskFileNum
	manifestMarker  *atomicfs.Marker

	manifestFile          vfs.File
	manifest              *record.Writer
	getFormatMajorVersion func() FormatMajorVersion

	writing    bool
	writerCond sync.Cond
	// State for deciding when to write a snapshot. Protected by mu.
	rotationHelper record.RotationHelper

	pickedCompactionCache pickedCompactionCache
}

func (vs *versionSet) init(
	dirname string,
	provider objstorage.Provider,
	opts *Options,
	marker *atomicfs.Marker,
	getFMV func() FormatMajorVersion,
	mu *sync.Mutex,
) {
	vs.dirname = dirname
	vs.provider = provider
	vs.mu = mu
	vs.writerCond.L = mu
	vs.opts = opts
	vs.fs = opts.FS
	vs.cmp = opts.Comparer
	vs.dynamicBaseLevel = true
	vs.versions.Init(mu)
	vs.l0Organizer = manifest.NewL0Organizer(opts.Comparer, opts.FlushSplitBytes)
	vs.obsoleteFn = vs.addObsoleteLocked
	vs.zombieTables = makeZombieObjects()
	vs.zombieBlobs = makeZombieObjects()
	vs.virtualBackings = manifest.MakeVirtualBackings()
	vs.nextFileNum.Store(1)
	vs.manifestMarker = marker
	vs.getFormatMajorVersion = getFMV
}

// create creates a version set for a fresh DB.
func (vs *versionSet) create(
	jobID JobID,
	dirname string,
	provider objstorage.Provider,
	opts *Options,
	marker *atomicfs.Marker,
	getFormatMajorVersion func() FormatMajorVersion,
	mu *sync.Mutex,
) error {
	vs.init(dirname, provider, opts, marker, getFormatMajorVersion, mu)
	emptyVersion := manifest.NewInitialVersion(opts.Comparer)
	vs.append(emptyVersion)
	vs.blobFiles.Init(nil)

	vs.setCompactionPicker(
		newCompactionPickerByScore(emptyVersion, vs.l0Organizer, &vs.virtualBackings, vs.opts, nil))
	// Note that a "snapshot" version edit is written to the manifest when it is
	// created.
	vs.manifestFileNum = vs.getNextDiskFileNum()
	err := vs.createManifest(vs.dirname, vs.manifestFileNum, vs.minUnflushedLogNum, vs.nextFileNum.Load(), nil /* virtualBackings */)
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
		// NB: Move() is responsible for syncing the data directory.
		if err = vs.manifestMarker.Move(base.MakeFilename(base.FileTypeManifest, vs.manifestFileNum)); err != nil {
			vs.opts.Logger.Fatalf("MANIFEST set current failed: %v", err)
		}
	}

	vs.opts.EventListener.ManifestCreated(ManifestCreateInfo{
		JobID:   int(jobID),
		Path:    base.MakeFilepath(vs.fs, vs.dirname, base.FileTypeManifest, vs.manifestFileNum),
		FileNum: vs.manifestFileNum,
		Err:     err,
	})
	if err != nil {
		return err
	}
	return nil
}

// load loads the version set from the manifest file.
func (vs *versionSet) load(
	dirname string,
	provider objstorage.Provider,
	opts *Options,
	manifestFileNum base.DiskFileNum,
	marker *atomicfs.Marker,
	getFormatMajorVersion func() FormatMajorVersion,
	mu *sync.Mutex,
) error {
	vs.init(dirname, provider, opts, marker, getFormatMajorVersion, mu)

	vs.manifestFileNum = manifestFileNum
	manifestPath := base.MakeFilepath(opts.FS, dirname, base.FileTypeManifest, vs.manifestFileNum)
	manifestFilename := opts.FS.PathBase(manifestPath)

	// Read the versionEdits in the manifest file.
	var bve manifest.BulkVersionEdit
	bve.AllAddedTables = make(map[base.TableNum]*manifest.TableMetadata)
	manifestFile, err := vs.fs.Open(manifestPath)
	if err != nil {
		return errors.Wrapf(err, "pebble: could not open manifest file %q for DB %q",
			errors.Safe(manifestFilename), dirname)
	}
	defer manifestFile.Close()
	rr := record.NewReader(manifestFile, 0 /* logNum */)
	for {
		r, err := rr.Next()
		if err == io.EOF || record.IsInvalidRecord(err) {
			break
		}
		if err != nil {
			return errors.Wrapf(err, "pebble: error when loading manifest file %q",
				errors.Safe(manifestFilename))
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
			if ve.ComparerName != vs.cmp.Name {
				return errors.Errorf("pebble: manifest file %q for DB %q: "+
					"comparer name from file %q != comparer name from Options %q",
					errors.Safe(manifestFilename), dirname, errors.Safe(ve.ComparerName), errors.Safe(vs.cmp.Name))
			}
		}
		if err := bve.Accumulate(&ve); err != nil {
			return err
		}
		if ve.MinUnflushedLogNum != 0 {
			vs.minUnflushedLogNum = ve.MinUnflushedLogNum
		}
		if ve.NextFileNum != 0 {
			vs.nextFileNum.Store(ve.NextFileNum)
		}
		if ve.LastSeqNum != 0 {
			// logSeqNum is the _next_ sequence number that will be assigned,
			// while LastSeqNum is the last assigned sequence number. Note that
			// this behaviour mimics that in RocksDB; the first sequence number
			// assigned is one greater than the one present in the manifest
			// (assuming no WALs contain higher sequence numbers than the
			// manifest's LastSeqNum). Increment LastSeqNum by 1 to get the
			// next sequence number that will be assigned.
			//
			// If LastSeqNum is less than SeqNumStart, increase it to at least
			// SeqNumStart to leave ample room for reserved sequence numbers.
			if ve.LastSeqNum+1 < base.SeqNumStart {
				vs.logSeqNum.Store(base.SeqNumStart)
			} else {
				vs.logSeqNum.Store(ve.LastSeqNum + 1)
			}
		}
	}
	// We have already set vs.nextFileNum = 2 at the beginning of the
	// function and could have only updated it to some other non-zero value,
	// so it cannot be 0 here.
	if vs.minUnflushedLogNum == 0 {
		if vs.nextFileNum.Load() >= 2 {
			// We either have a freshly created DB, or a DB created by RocksDB
			// that has not had a single flushed SSTable yet. This is because
			// RocksDB bumps up nextFileNum in this case without bumping up
			// minUnflushedLogNum, even if WALs with non-zero file numbers are
			// present in the directory.
		} else {
			return base.CorruptionErrorf("pebble: malformed manifest file %q for DB %q",
				errors.Safe(manifestFilename), dirname)
		}
	}
	vs.markFileNumUsed(vs.minUnflushedLogNum)

	// Populate the virtual backings for virtual sstables since we have finished
	// version edit accumulation.
	for _, b := range bve.AddedFileBacking {
		vs.virtualBackings.AddAndRef(b)
	}

	for _, addedLevel := range bve.AddedTables {
		for _, m := range addedLevel {
			if m.Virtual {
				vs.virtualBackings.AddTable(m)
			}
		}
	}

	if invariants.Enabled {
		// There should be no deleted tables or backings, since we're starting from
		// an empty state.
		for _, deletedLevel := range bve.DeletedTables {
			if len(deletedLevel) != 0 {
				panic("deleted files after manifest replay")
			}
		}
		if len(bve.RemovedFileBacking) > 0 {
			panic("deleted backings after manifest replay")
		}
	}

	emptyVersion := manifest.NewInitialVersion(opts.Comparer)
	newVersion, err := bve.Apply(emptyVersion, opts.Experimental.ReadCompactionRate)
	if err != nil {
		return err
	}
	vs.l0Organizer.PerformUpdate(vs.l0Organizer.PrepareUpdate(&bve, newVersion), newVersion)
	vs.l0Organizer.InitCompactingFileInfo(nil /* in-progress compactions */)
	vs.blobFiles.Init(&bve)
	vs.append(newVersion)

	for i := range vs.metrics.Levels {
		l := &vs.metrics.Levels[i]
		l.TablesCount = int64(newVersion.Levels[i].Len())
		files := newVersion.Levels[i].Slice()
		l.TablesSize = int64(files.TableSizeSum())
	}
	for _, l := range newVersion.Levels {
		for f := range l.All() {
			if !f.Virtual {
				isLocal, localSize := sizeIfLocal(f.TableBacking, vs.provider)
				vs.metrics.Table.Local.LiveSize = uint64(int64(vs.metrics.Table.Local.LiveSize) + localSize)
				if isLocal {
					vs.metrics.Table.Local.LiveCount++
				}
			}
		}
	}
	vs.virtualBackings.ForEach(func(backing *manifest.TableBacking) {
		isLocal, localSize := sizeIfLocal(backing, vs.provider)
		vs.metrics.Table.Local.LiveSize = uint64(int64(vs.metrics.Table.Local.LiveSize) + localSize)
		if isLocal {
			vs.metrics.Table.Local.LiveCount++
		}
	})

	vs.setCompactionPicker(
		newCompactionPickerByScore(newVersion, vs.l0Organizer, &vs.virtualBackings, vs.opts, nil))
	return nil
}

func (vs *versionSet) close() error {
	if vs.manifestFile != nil {
		if err := vs.manifestFile.Close(); err != nil {
			return err
		}
	}
	if vs.manifestMarker != nil {
		if err := vs.manifestMarker.Close(); err != nil {
			return err
		}
	}
	return nil
}

// logLock locks the manifest for writing. The lock must be released by
// a call to logUnlock.
//
// DB.mu must be held when calling this method, but the mutex may be dropped and
// re-acquired during the course of this method.
func (vs *versionSet) logLock() {
	// Wait for any existing writing to the manifest to complete, then mark the
	// manifest as busy.
	for vs.writing {
		// Note: writerCond.L is DB.mu, so we unlock it while we wait.
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

func (vs *versionSet) logUnlockAndInvalidatePickedCompactionCache() {
	vs.pickedCompactionCache.invalidate()
	vs.logUnlock()
}

// versionUpdate is returned by the function passed to UpdateVersionLocked.
//
// If VE is nil, there is no update to apply (but it is not an error).
type versionUpdate struct {
	VE      *manifest.VersionEdit
	JobID   JobID
	Metrics levelMetricsDelta
	// InProgressCompactionFn is called while DB.mu is held after the I/O part of
	// the update was performed. It should return any compactions that are
	// in-progress (excluding than the one that is being applied).
	InProgressCompactionsFn func() []compactionInfo
	ForceManifestRotation   bool
}

// UpdateVersionLocked is used to update the current version.
//
// DB.mu must be held. UpdateVersionLocked first waits for any other version
// update to complete, releasing and reacquiring DB.mu.
//
// UpdateVersionLocked then calls updateFn which builds a versionUpdate, while
// holding DB.mu. The updateFn can release and reacquire DB.mu (it should
// attempt to do as much work as possible outside of the lock).
//
// UpdateVersionLocked fills in the following fields of the VersionEdit:
// NextFileNum, LastSeqNum, RemovedBackingTables. The removed backing tables are
// those backings that are no longer used (in the new version) after applying
// the edit (as per vs.virtualBackings). Other than these fields, the
// VersionEdit must be complete.
//
// New table backing references (TableBacking.Ref) are taken as part of applying
// the version edit. The state of the virtual backings (vs.virtualBackings) is
// updated before logging to the manifest and installing the latest version;
// this is ok because any failure in those steps is fatal.
//
// If updateFn returns an error, no update is applied and that same error is returned.
// If versionUpdate.VE is nil, the no update is applied (and no error is returned).
func (vs *versionSet) UpdateVersionLocked(updateFn func() (versionUpdate, error)) error {
	vs.logLock()
	defer vs.logUnlockAndInvalidatePickedCompactionCache()

	vu, err := updateFn()
	if err != nil || vu.VE == nil {
		return err
	}

	if !vs.writing {
		vs.opts.Logger.Fatalf("MANIFEST not locked for writing")
	}

	ve := vu.VE
	if ve.MinUnflushedLogNum != 0 {
		if ve.MinUnflushedLogNum < vs.minUnflushedLogNum ||
			vs.nextFileNum.Load() <= uint64(ve.MinUnflushedLogNum) {
			panic(fmt.Sprintf("pebble: inconsistent versionEdit minUnflushedLogNum %d",
				ve.MinUnflushedLogNum))
		}
	}

	// This is the next manifest filenum, but if the current file is too big we
	// will write this ve to the next file which means what ve encodes is the
	// current filenum and not the next one.
	//
	// TODO(sbhola): figure out why this is correct and update comment.
	ve.NextFileNum = vs.nextFileNum.Load()

	// LastSeqNum is set to the current upper bound on the assigned sequence
	// numbers. Note that this is exactly the behavior of RocksDB. LastSeqNum is
	// used to initialize versionSet.logSeqNum and versionSet.visibleSeqNum on
	// replay. It must be higher than or equal to any than any sequence number
	// written to an sstable, including sequence numbers in ingested files.
	// Note that LastSeqNum is not (and cannot be) the minimum unflushed sequence
	// number. This is fallout from ingestion which allows a sequence number X to
	// be assigned to an ingested sstable even though sequence number X-1 resides
	// in an unflushed memtable. logSeqNum is the _next_ sequence number that
	// will be assigned, so subtract that by 1 to get the upper bound on the
	// last assigned sequence number.
	logSeqNum := vs.logSeqNum.Load()
	ve.LastSeqNum = logSeqNum - 1
	if logSeqNum == 0 {
		// logSeqNum is initialized to 1 in Open() if there are no previous WAL
		// or manifest records, so this case should never happen.
		vs.opts.Logger.Fatalf("logSeqNum must be a positive integer: %d", logSeqNum)
	}

	currentVersion := vs.currentVersion()
	var newVersion *version

	// Generate a new manifest if we don't currently have one, or forceRotation
	// is true, or the current one is too large.
	//
	// For largeness, we do not exclusively use MaxManifestFileSize size
	// threshold since we have had incidents where due to either large keys or
	// large numbers of files, each edit results in a snapshot + write of the
	// edit. This slows the system down since each flush or compaction is
	// writing a new manifest snapshot. The primary goal of the size-based
	// rollover logic is to ensure that when reopening a DB, the number of edits
	// that need to be replayed on top of the snapshot is "sane". Rolling over
	// to a new manifest after each edit is not relevant to that goal.
	//
	// Consider the following cases:
	// - The number of live files F in the DB is roughly stable: after writing
	//   the snapshot (with F files), say we require that there be enough edits
	//   such that the cumulative number of files in those edits, E, be greater
	//   than F. This will ensure that the total amount of time in
	//   UpdateVersionLocked that is spent in snapshot writing is ~50%.
	//
	// - The number of live files F in the DB is shrinking drastically, say from
	//   F to F/10: This can happen for various reasons, like wide range
	//   tombstones, or large numbers of smaller than usual files that are being
	//   merged together into larger files. And say the new files generated
	//   during this shrinkage is insignificant compared to F/10, and so for
	//   this example we will assume it is effectively 0. After this shrinking,
	//   E = 0.9F, and so if we used the previous snapshot file count, F, as the
	//   threshold that needs to be exceeded, we will further delay the snapshot
	//   writing. Which means on DB reopen we will need to replay 0.9F edits to
	//   get to a version with 0.1F files. It would be better to create a new
	//   snapshot when E exceeds the number of files in the current version.
	//
	// - The number of live files F in the DB is growing via perfect ingests
	//   into L6: Say we wrote the snapshot when there were F files and now we
	//   have 10F files, so E = 9F. We will further delay writing a new
	//   snapshot. This case can be critiqued as contrived, but we consider it
	//   nonetheless.
	//
	// The logic below uses the min of the last snapshot file count and the file
	// count in the current version.
	vs.rotationHelper.AddRecord(int64(len(ve.DeletedTables) + len(ve.NewTables)))
	sizeExceeded := vs.manifest.Size() >= vs.opts.MaxManifestFileSize
	requireRotation := vu.ForceManifestRotation || vs.manifest == nil

	var nextSnapshotFilecount int64
	for i := range vs.metrics.Levels {
		nextSnapshotFilecount += vs.metrics.Levels[i].TablesCount
	}
	if sizeExceeded && !requireRotation {
		requireRotation = vs.rotationHelper.ShouldRotate(nextSnapshotFilecount)
	}
	var newManifestFileNum base.DiskFileNum
	var prevManifestFileSize uint64
	var newManifestVirtualBackings []*manifest.TableBacking
	if requireRotation {
		newManifestFileNum = vs.getNextDiskFileNum()
		prevManifestFileSize = uint64(vs.manifest.Size())

		// We want the virtual backings *before* applying the version edit, because
		// the new manifest will contain the pre-apply version plus the last version
		// edit.
		newManifestVirtualBackings = vs.virtualBackings.Backings()
	}

	// Grab certain values before releasing vs.mu, in case createManifest() needs
	// to be called.
	minUnflushedLogNum := vs.minUnflushedLogNum
	nextFileNum := vs.nextFileNum.Load()

	// Note: this call populates ve.RemovedBackingTables.
	zombieBackings, removedVirtualBackings, localTablesLiveDelta :=
		getZombieTablesAndUpdateVirtualBackings(ve, &vs.virtualBackings, vs.provider)

	var l0Update manifest.L0PreparedUpdate
	if err := func() error {
		vs.mu.Unlock()
		defer vs.mu.Lock()

		if vs.getFormatMajorVersion() < FormatVirtualSSTables && len(ve.CreatedBackingTables) > 0 {
			return base.AssertionFailedf("MANIFEST cannot contain virtual sstable records due to format major version")
		}

		// Rotate the manifest if necessary. Rotating the manifest involves
		// creating a new file and writing an initial version edit containing a
		// snapshot of the current version. This initial version edit will
		// reflect the Version prior to the pending version edit (`ve`). Once
		// we've created the new manifest with the previous version state, we'll
		// append the version edit `ve` to the tail of the new manifest.
		if newManifestFileNum != 0 {
			if err := vs.createManifest(vs.dirname, newManifestFileNum, minUnflushedLogNum, nextFileNum, newManifestVirtualBackings); err != nil {
				vs.opts.EventListener.ManifestCreated(ManifestCreateInfo{
					JobID:   int(vu.JobID),
					Path:    base.MakeFilepath(vs.fs, vs.dirname, base.FileTypeManifest, newManifestFileNum),
					FileNum: newManifestFileNum,
					Err:     err,
				})
				return errors.Wrap(err, "MANIFEST create failed")
			}
		}

		// Call ApplyAndUpdateVersionEdit before accumulating the version edit.
		// If any blob files are no longer referenced, the version edit will be
		// updated to explicitly record the deletion of the blob files. This can
		// happen here because vs.blobFiles is protected by the manifest logLock
		// (NOT vs.mu). We only read or write vs.blobFiles while holding the
		// manifest lock.
		if err := vs.blobFiles.ApplyAndUpdateVersionEdit(ve); err != nil {
			return errors.Wrap(err, "MANIFEST blob files apply and update failed")
		}

		var bulkEdit manifest.BulkVersionEdit
		err := bulkEdit.Accumulate(ve)
		if err != nil {
			return errors.Wrap(err, "MANIFEST accumulate failed")
		}
		newVersion, err = bulkEdit.Apply(currentVersion, vs.opts.Experimental.ReadCompactionRate)
		if err != nil {
			return errors.Wrap(err, "MANIFEST apply failed")
		}
		l0Update = vs.l0Organizer.PrepareUpdate(&bulkEdit, newVersion)

		w, err := vs.manifest.Next()
		if err != nil {
			return errors.Wrap(err, "MANIFEST next record write failed")
		}

		// NB: Any error from this point on is considered fatal as we don't know if
		// the MANIFEST write occurred or not. Trying to determine that is
		// fraught. Instead we rely on the standard recovery mechanism run when a
		// database is open. In particular, that mechanism generates a new MANIFEST
		// and ensures it is synced.
		if err := ve.Encode(w); err != nil {
			return errors.Wrap(err, "MANIFEST write failed")
		}
		if err := vs.manifest.Flush(); err != nil {
			return errors.Wrap(err, "MANIFEST flush failed")
		}
		if err := vs.manifestFile.Sync(); err != nil {
			return errors.Wrap(err, "MANIFEST sync failed")
		}
		if newManifestFileNum != 0 {
			// NB: Move() is responsible for syncing the data directory.
			if err := vs.manifestMarker.Move(base.MakeFilename(base.FileTypeManifest, newManifestFileNum)); err != nil {
				return errors.Wrap(err, "MANIFEST set current failed")
			}
			vs.opts.EventListener.ManifestCreated(ManifestCreateInfo{
				JobID:   int(vu.JobID),
				Path:    base.MakeFilepath(vs.fs, vs.dirname, base.FileTypeManifest, newManifestFileNum),
				FileNum: newManifestFileNum,
			})
		}
		return nil
	}(); err != nil {
		// Any error encountered during any of the operations in the previous
		// closure are considered fatal. Treating such errors as fatal is preferred
		// to attempting to unwind various file and b-tree reference counts, and
		// re-generating L0 sublevel metadata. This may change in the future, if
		// certain manifest / WAL operations become retryable. For more context, see
		// #1159 and #1792.
		vs.opts.Logger.Fatalf("%s", err)
		return err
	}

	if requireRotation {
		// Successfully rotated.
		vs.rotationHelper.Rotate(nextSnapshotFilecount)
	}
	// Now that DB.mu is held again, initialize compacting file info in
	// L0Sublevels.
	inProgress := vu.InProgressCompactionsFn()

	zombieBlobs, localBlobLiveDelta := getZombieBlobFilesAndComputeLocalMetrics(ve, vs.provider)
	vs.l0Organizer.PerformUpdate(l0Update, newVersion)
	vs.l0Organizer.InitCompactingFileInfo(inProgressL0Compactions(inProgress))

	// Update the zombie objects sets first, as installation of the new version
	// will unref the previous version which could result in addObsoleteLocked
	// being called.
	for _, b := range zombieBackings {
		vs.zombieTables.Add(objectInfo{
			fileInfo: fileInfo{
				FileNum:  b.backing.DiskFileNum,
				FileSize: b.backing.Size,
			},
			isLocal: b.isLocal,
		})
	}
	for _, zb := range zombieBlobs {
		vs.zombieBlobs.Add(zb)
	}
	// Unref the removed backings and report those that already became obsolete.
	// Note that the only case where we report obsolete tables here is when
	// VirtualBackings.Protect/Unprotect was used to keep a backing alive without
	// it being used in the current version.
	var obsoleteVirtualBackings manifest.ObsoleteFiles
	for _, b := range removedVirtualBackings {
		if b.backing.Unref() == 0 {
			obsoleteVirtualBackings.TableBackings = append(obsoleteVirtualBackings.TableBackings, b.backing)
		}
	}
	vs.addObsoleteLocked(obsoleteVirtualBackings)

	// Install the new version.
	vs.append(newVersion)

	if ve.MinUnflushedLogNum != 0 {
		vs.minUnflushedLogNum = ve.MinUnflushedLogNum
	}
	if newManifestFileNum != 0 {
		if vs.manifestFileNum != 0 {
			vs.obsoleteManifests = append(vs.obsoleteManifests, obsoleteFile{
				fileType: base.FileTypeManifest,
				fs:       vs.fs,
				path:     base.MakeFilepath(vs.fs, vs.dirname, base.FileTypeManifest, vs.manifestFileNum),
				fileNum:  vs.manifestFileNum,
				fileSize: prevManifestFileSize,
				isLocal:  true,
			})
		}
		vs.manifestFileNum = newManifestFileNum
	}

	vs.metrics.updateLevelMetrics(vu.Metrics)
	for i := range vs.metrics.Levels {
		l := &vs.metrics.Levels[i]
		l.TablesCount = int64(newVersion.Levels[i].Len())
		l.VirtualTablesCount = newVersion.Levels[i].NumVirtual
		l.VirtualTablesSize = newVersion.Levels[i].VirtualTableSize
		l.TablesSize = int64(newVersion.Levels[i].TableSize())
		l.EstimatedReferencesSize = newVersion.Levels[i].EstimatedReferenceSize()
		l.Sublevels = 0
		if l.TablesCount > 0 {
			l.Sublevels = 1
		}
		if invariants.Enabled {
			levelFiles := newVersion.Levels[i].Slice()
			if size := int64(levelFiles.TableSizeSum()); l.TablesSize != size {
				vs.opts.Logger.Fatalf("versionSet metrics L%d Size = %d, actual size = %d", i, l.TablesSize, size)
			}
			refSize := uint64(0)
			for f := range levelFiles.All() {
				refSize += f.EstimatedReferenceSize()
			}
			if refSize != l.EstimatedReferencesSize {
				vs.opts.Logger.Fatalf("versionSet metrics L%d EstimatedReferencesSize = %d, recomputed size = %d", i, l.EstimatedReferencesSize, refSize)
			}

			if nVirtual := levelFiles.NumVirtual(); nVirtual != l.VirtualTablesCount {
				vs.opts.Logger.Fatalf(
					"versionSet metrics L%d NumVirtual = %d, actual NumVirtual = %d",
					i, l.VirtualTablesCount, nVirtual,
				)
			}
			if vSize := levelFiles.VirtualTableSizeSum(); vSize != l.VirtualTablesSize {
				vs.opts.Logger.Fatalf(
					"versionSet metrics L%d Virtual size = %d, actual size = %d",
					i, l.VirtualTablesSize, vSize,
				)
			}
		}
	}
	vs.metrics.Levels[0].Sublevels = int32(len(newVersion.L0SublevelFiles))
	vs.metrics.Table.Local.LiveSize = uint64(int64(vs.metrics.Table.Local.LiveSize) + localTablesLiveDelta.size)
	vs.metrics.Table.Local.LiveCount = uint64(int64(vs.metrics.Table.Local.LiveCount) + localTablesLiveDelta.count)
	vs.metrics.BlobFiles.Local.LiveSize = uint64(int64(vs.metrics.BlobFiles.Local.LiveSize) + localBlobLiveDelta.size)
	vs.metrics.BlobFiles.Local.LiveCount = uint64(int64(vs.metrics.BlobFiles.Local.LiveCount) + localBlobLiveDelta.count)

	vs.setCompactionPicker(
		newCompactionPickerByScore(newVersion, vs.l0Organizer, &vs.virtualBackings, vs.opts, inProgress))
	if !vs.dynamicBaseLevel {
		vs.picker.forceBaseLevel1()
	}
	return nil
}

func (vs *versionSet) setCompactionPicker(picker *compactionPickerByScore) {
	vs.picker = picker
	vs.curCompactionConcurrency.Store(int32(picker.getCompactionConcurrency()))
}

type tableBackingInfo struct {
	backing *manifest.TableBacking
	isLocal bool
}

type fileMetricDelta struct {
	count int64
	size  int64
}

// getZombieTablesAndUpdateVirtualBackings updates the virtual backings with the
// changes in the versionEdit and populates ve.RemovedBackingTables.
// Returns:
//   - zombieBackings: all backings (physical and virtual) that will no longer be
//     needed when we apply ve.
//   - removedVirtualBackings: the virtual backings that will be removed by the
//     VersionEdit and which must be Unref()ed by the caller. These backings
//     match ve.RemovedBackingTables.
//   - localLiveSizeDelta: the delta in local live bytes.
func getZombieTablesAndUpdateVirtualBackings(
	ve *versionEdit, virtualBackings *manifest.VirtualBackings, provider objstorage.Provider,
) (zombieBackings, removedVirtualBackings []tableBackingInfo, localLiveDelta fileMetricDelta) {
	// First, deal with the physical tables.
	//
	// A physical backing has become unused if it is in DeletedFiles but not in
	// NewFiles or CreatedBackingTables.
	//
	// Note that for the common case where there are very few elements, the map
	// will stay on the stack.
	stillUsed := make(map[base.DiskFileNum]struct{})
	for _, nf := range ve.NewTables {
		if !nf.Meta.Virtual {
			stillUsed[nf.Meta.TableBacking.DiskFileNum] = struct{}{}
			isLocal, localFileDelta := sizeIfLocal(nf.Meta.TableBacking, provider)
			localLiveDelta.size += localFileDelta
			if isLocal {
				localLiveDelta.count++
			}
		}
	}
	for _, b := range ve.CreatedBackingTables {
		stillUsed[b.DiskFileNum] = struct{}{}
	}
	for _, m := range ve.DeletedTables {
		if !m.Virtual {
			// NB: this deleted file may also be in NewFiles or
			// CreatedBackingTables, due to a file moving between levels, or
			// becoming virtualized. In which case there is no change due to this
			// file in the localLiveSizeDelta -- the subtraction below compensates
			// for the addition.
			isLocal, localFileDelta := sizeIfLocal(m.TableBacking, provider)
			localLiveDelta.size -= localFileDelta
			if isLocal {
				localLiveDelta.count--
			}
			if _, ok := stillUsed[m.TableBacking.DiskFileNum]; !ok {
				zombieBackings = append(zombieBackings, tableBackingInfo{
					backing: m.TableBacking,
					isLocal: isLocal,
				})
			}
		}
	}

	// Now deal with virtual tables.
	//
	// When a virtual table moves between levels we AddTable() then RemoveTable(),
	// which works out.
	for _, b := range ve.CreatedBackingTables {
		virtualBackings.AddAndRef(b)
		isLocal, localFileDelta := sizeIfLocal(b, provider)
		localLiveDelta.size += localFileDelta
		if isLocal {
			localLiveDelta.count++
		}
	}
	for _, nf := range ve.NewTables {
		if nf.Meta.Virtual {
			virtualBackings.AddTable(nf.Meta)
		}
	}
	for _, m := range ve.DeletedTables {
		if m.Virtual {
			virtualBackings.RemoveTable(m)
		}
	}

	if unused := virtualBackings.Unused(); len(unused) > 0 {
		// Virtual backings that are no longer used are zombies and are also added
		// to RemovedBackingTables (before the version edit is written to disk).
		ve.RemovedBackingTables = make([]base.DiskFileNum, len(unused))
		for i, b := range unused {
			isLocal, localFileDelta := sizeIfLocal(b, provider)
			localLiveDelta.size -= localFileDelta
			if isLocal {
				localLiveDelta.count--
			}
			ve.RemovedBackingTables[i] = b.DiskFileNum
			zombieBackings = append(zombieBackings, tableBackingInfo{
				backing: b,
				isLocal: isLocal,
			})
			virtualBackings.Remove(b.DiskFileNum)
		}
		removedVirtualBackings = zombieBackings[len(zombieBackings)-len(unused):]
	}
	return zombieBackings, removedVirtualBackings, localLiveDelta
}

// getZombieBlobFilesAndComputeLocalMetrics constructs objectInfos for all
// zombie blob files, and computes the metric deltas for live files overall and
// locally.
func getZombieBlobFilesAndComputeLocalMetrics(
	ve *versionEdit, provider objstorage.Provider,
) (zombieBlobFiles []objectInfo, localLiveDelta fileMetricDelta) {
	for _, b := range ve.NewBlobFiles {
		if objstorage.IsLocalBlobFile(provider, b.FileNum) {
			localLiveDelta.count++
			localLiveDelta.size += int64(b.Size)
		}
	}
	zombieBlobFiles = make([]objectInfo, 0, len(ve.DeletedBlobFiles))
	for _, b := range ve.DeletedBlobFiles {
		isLocal := objstorage.IsLocalBlobFile(provider, b.FileNum)
		if isLocal {
			localLiveDelta.count--
			localLiveDelta.size -= int64(b.Size)
		}
		zombieBlobFiles = append(zombieBlobFiles, objectInfo{
			fileInfo: fileInfo{
				FileNum:  b.FileNum,
				FileSize: b.Size,
			},
			isLocal: isLocal,
		})
	}
	return zombieBlobFiles, localLiveDelta
}

// sizeIfLocal returns backing.Size if the backing is a local file, else 0.
func sizeIfLocal(
	backing *manifest.TableBacking, provider objstorage.Provider,
) (isLocal bool, localSize int64) {
	isLocal = objstorage.IsLocalTable(provider, backing.DiskFileNum)
	if isLocal {
		return true, int64(backing.Size)
	}
	return false, 0
}

func (vs *versionSet) incrementCompactions(
	kind compactionKind,
	extraLevels []*compactionLevel,
	pickerMetrics pickedCompactionMetrics,
	bytesWritten int64,
	compactionErr error,
) {
	if kind == compactionKindFlush || kind == compactionKindIngestedFlushable {
		vs.metrics.Flush.Count++
	} else {
		vs.metrics.Compact.Count++
		if compactionErr != nil {
			if errors.Is(compactionErr, ErrCancelledCompaction) {
				vs.metrics.Compact.CancelledCount++
				vs.metrics.Compact.CancelledBytes += bytesWritten
			} else {
				vs.metrics.Compact.FailedCount++
			}
		}
	}

	switch kind {
	case compactionKindDefault:
		vs.metrics.Compact.DefaultCount++

	case compactionKindFlush, compactionKindIngestedFlushable:

	case compactionKindMove:
		vs.metrics.Compact.MoveCount++

	case compactionKindDeleteOnly:
		vs.metrics.Compact.DeleteOnlyCount++

	case compactionKindElisionOnly:
		vs.metrics.Compact.ElisionOnlyCount++

	case compactionKindRead:
		vs.metrics.Compact.ReadCount++

	case compactionKindTombstoneDensity:
		vs.metrics.Compact.TombstoneDensityCount++

	case compactionKindRewrite:
		vs.metrics.Compact.RewriteCount++

	case compactionKindCopy:
		vs.metrics.Compact.CopyCount++

	default:
		if invariants.Enabled {
			panic("unhandled compaction kind")
		}
	}
	if len(extraLevels) > 0 {
		vs.metrics.Compact.MultiLevelCount++
	}
}

func (vs *versionSet) incrementCompactionBytes(numBytes int64) {
	vs.atomicInProgressBytes.Add(numBytes)
}

// createManifest creates a manifest file that contains a snapshot of vs.
func (vs *versionSet) createManifest(
	dirname string,
	fileNum, minUnflushedLogNum base.DiskFileNum,
	nextFileNum uint64,
	virtualBackings []*manifest.TableBacking,
) (err error) {
	var (
		filename       = base.MakeFilepath(vs.fs, dirname, base.FileTypeManifest, fileNum)
		manifestFile   vfs.File
		manifestWriter *record.Writer
	)
	defer func() {
		if manifestWriter != nil {
			_ = manifestWriter.Close()
		}
		if manifestFile != nil {
			_ = manifestFile.Close()
		}
		if err != nil {
			_ = vs.fs.Remove(filename)
		}
	}()
	manifestFile, err = vs.fs.Create(filename, "pebble-manifest")
	if err != nil {
		return err
	}
	manifestWriter = record.NewWriter(manifestFile)

	snapshot := manifest.VersionEdit{
		ComparerName: vs.cmp.Name,
		// When creating a version snapshot for an existing DB, this snapshot
		// VersionEdit will be immediately followed by another VersionEdit
		// (being written in UpdateVersionLocked()). That VersionEdit always
		// contains a LastSeqNum, so we don't need to include that in the
		// snapshot.  But it does not necessarily include MinUnflushedLogNum,
		// NextFileNum, so we initialize those using the corresponding fields in
		// the versionSet (which came from the latest preceding VersionEdit that
		// had those fields).
		MinUnflushedLogNum:   minUnflushedLogNum,
		NextFileNum:          nextFileNum,
		CreatedBackingTables: virtualBackings,
		NewBlobFiles:         vs.blobFiles.Metadatas(),
	}
	// Add all extant sstables in the current version.
	for level, levelMetadata := range vs.currentVersion().Levels {
		for meta := range levelMetadata.All() {
			snapshot.NewTables = append(snapshot.NewTables, manifest.NewTableEntry{
				Level: level,
				Meta:  meta,
			})
		}
	}

	w, err1 := manifestWriter.Next()
	if err1 != nil {
		return err1
	}
	if err := snapshot.Encode(w); err != nil {
		return err
	}

	if vs.manifest != nil {
		if err := vs.manifest.Close(); err != nil {
			return err
		}
		vs.manifest = nil
	}
	if vs.manifestFile != nil {
		if err := vs.manifestFile.Close(); err != nil {
			return err
		}
		vs.manifestFile = nil
	}

	vs.manifest, manifestWriter = manifestWriter, nil
	vs.manifestFile, manifestFile = manifestFile, nil
	return nil
}

// NB: This method is not safe for concurrent use. It is only safe
// to be called when concurrent changes to nextFileNum are not expected.
func (vs *versionSet) markFileNumUsed(fileNum base.DiskFileNum) {
	if vs.nextFileNum.Load() <= uint64(fileNum) {
		vs.nextFileNum.Store(uint64(fileNum + 1))
	}
}

// getNextTableNum returns a new table number.
//
// Can be called without the versionSet's mutex being held.
func (vs *versionSet) getNextTableNum() base.TableNum {
	x := vs.nextFileNum.Add(1) - 1
	return base.TableNum(x)
}

// Can be called without the versionSet's mutex being held.
func (vs *versionSet) getNextDiskFileNum() base.DiskFileNum {
	x := vs.nextFileNum.Add(1) - 1
	return base.DiskFileNum(x)
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
	if invariants.Enabled {
		// Verify that the virtualBackings contains all the backings referenced by
		// the version.
		for _, l := range v.Levels {
			for f := range l.All() {
				if f.Virtual {
					if _, ok := vs.virtualBackings.Get(f.TableBacking.DiskFileNum); !ok {
						panic(fmt.Sprintf("%s is not in virtualBackings", f.TableBacking.DiskFileNum))
					}
				}
			}
		}
	}
}

func (vs *versionSet) currentVersion() *version {
	return vs.versions.Back()
}

func (vs *versionSet) addLiveFileNums(m map[base.DiskFileNum]struct{}) {
	current := vs.currentVersion()
	for v := vs.versions.Front(); true; v = v.Next() {
		for _, lm := range v.Levels {
			for f := range lm.All() {
				m[f.TableBacking.DiskFileNum] = struct{}{}
				for _, ref := range f.BlobReferences {
					m[ref.FileNum] = struct{}{}
				}
			}
		}
		if v == current {
			break
		}
	}
	// virtualBackings contains backings that are referenced by some virtual
	// tables in the latest version (which are handled above), and backings that
	// are not but are still alive because of the protection mechanism (see
	// manifset.VirtualBackings). This loop ensures the latter get added to the
	// map.
	vs.virtualBackings.ForEach(func(b *manifest.TableBacking) {
		m[b.DiskFileNum] = struct{}{}
	})
}

// addObsoleteLocked will add the fileInfo associated with obsolete backing
// sstables to the obsolete tables list.
//
// The file backings in the obsolete list must not appear more than once.
//
// DB.mu must be held when addObsoleteLocked is called.
func (vs *versionSet) addObsoleteLocked(obsolete manifest.ObsoleteFiles) {
	if obsolete.Count() == 0 {
		return
	}

	// Note that the zombie objects transition from zombie *to* obsolete, and
	// will no longer be considered zombie.

	newlyObsoleteTables := make([]objectInfo, len(obsolete.TableBackings))
	for i, bs := range obsolete.TableBackings {
		newlyObsoleteTables[i] = vs.zombieTables.Extract(bs.DiskFileNum)
	}
	vs.obsoleteTables = mergeObjectInfos(vs.obsoleteTables, newlyObsoleteTables)

	newlyObsoleteBlobFiles := make([]objectInfo, len(obsolete.BlobFiles))
	for i, bf := range obsolete.BlobFiles {
		newlyObsoleteBlobFiles[i] = vs.zombieBlobs.Extract(bf.FileNum)
	}
	vs.obsoleteBlobs = mergeObjectInfos(vs.obsoleteBlobs, newlyObsoleteBlobFiles)
	vs.updateObsoleteObjectMetricsLocked()
}

// addObsolete will acquire DB.mu, so DB.mu must not be held when this is
// called.
func (vs *versionSet) addObsolete(obsolete manifest.ObsoleteFiles) {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	vs.addObsoleteLocked(obsolete)
}

func (vs *versionSet) updateObsoleteObjectMetricsLocked() {
	vs.metrics.Table.ObsoleteCount = int64(len(vs.obsoleteTables))
	vs.metrics.Table.ObsoleteSize = 0
	vs.metrics.Table.Local.ObsoleteSize = 0
	vs.metrics.Table.Local.ObsoleteCount = 0
	for _, fi := range vs.obsoleteTables {
		vs.metrics.Table.ObsoleteSize += fi.FileSize
		if fi.isLocal {
			vs.metrics.Table.Local.ObsoleteSize += fi.FileSize
			vs.metrics.Table.Local.ObsoleteCount++
		}
	}
	vs.metrics.BlobFiles.ObsoleteCount = uint64(len(vs.obsoleteBlobs))
	vs.metrics.BlobFiles.ObsoleteSize = 0
	vs.metrics.BlobFiles.Local.ObsoleteSize = 0
	vs.metrics.BlobFiles.Local.ObsoleteCount = 0
	for _, fi := range vs.obsoleteBlobs {
		vs.metrics.BlobFiles.ObsoleteSize += fi.FileSize
		if fi.isLocal {
			vs.metrics.BlobFiles.Local.ObsoleteSize += fi.FileSize
			vs.metrics.BlobFiles.Local.ObsoleteCount++
		}
	}
}

func findCurrentManifest(
	fs vfs.FS, dirname string, ls []string,
) (marker *atomicfs.Marker, manifestNum base.DiskFileNum, exists bool, err error) {
	// Locating a marker should succeed even if the marker has never been placed.
	var filename string
	marker, filename, err = atomicfs.LocateMarkerInListing(fs, dirname, manifestMarkerName, ls)
	if err != nil {
		return nil, 0, false, err
	}

	if filename == "" {
		// The marker hasn't been set yet. This database doesn't exist.
		return marker, 0, false, nil
	}

	var ok bool
	_, manifestNum, ok = base.ParseFilename(fs, filename)
	if !ok {
		return marker, 0, false, base.CorruptionErrorf("pebble: MANIFEST name %q is malformed", errors.Safe(filename))
	}
	return marker, manifestNum, true, nil
}

func newFileMetrics(newFiles []manifest.NewTableEntry) levelMetricsDelta {
	var m levelMetricsDelta
	for _, nf := range newFiles {
		lm := m[nf.Level]
		if lm == nil {
			lm = &LevelMetrics{}
			m[nf.Level] = lm
		}
		lm.TablesCount++
		lm.TablesSize += int64(nf.Meta.Size)
		lm.EstimatedReferencesSize += nf.Meta.EstimatedReferenceSize()
	}
	return m
}
