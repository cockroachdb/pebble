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
type bulkVersionEdit = manifest.BulkVersionEdit
type deletedFileEntry = manifest.DeletedTableEntry
type tableMetadata = manifest.TableMetadata
type physicalMeta = manifest.PhysicalTableMeta
type virtualMeta = manifest.VirtualTableMeta
type fileBacking = manifest.FileBacking
type newTableEntry = manifest.NewTableEntry
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
	versions versionList
	picker   compactionPicker
	// curCompactionConcurrency is updated whenever picker is updated.
	// INVARIANT: >= 1.
	curCompactionConcurrency atomic.Int32

	// Not all metrics are kept here. See DB.Metrics().
	metrics Metrics

	// A pointer to versionSet.addObsoleteLocked. Avoids allocating a new closure
	// on the creation of every version.
	obsoleteFn        func(manifest.ObsoleteFiles)
	obsoleteTables    []objectInfo
	obsoleteBlobs     []objectInfo
	obsoleteManifests []fileInfo
	obsoleteOptions   []fileInfo

	// Zombie tables which have been removed from the current version but are
	// still referenced by an inuse iterator.
	zombieTables map[base.DiskFileNum]objectInfo

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

// objectInfo describes an object in object storage (either a sstable or a blob
// file).
type objectInfo struct {
	fileInfo
	isLocal bool
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
	vs.obsoleteFn = vs.addObsoleteLocked
	vs.zombieTables = make(map[base.DiskFileNum]objectInfo)
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
	var bve bulkVersionEdit
	emptyVersion := manifest.NewVersion(opts.Comparer)
	newVersion, err := bve.Apply(emptyVersion, opts.FlushSplitBytes, opts.Experimental.ReadCompactionRate)
	if err != nil {
		return err
	}
	vs.append(newVersion)

	vs.setCompactionPicker(
		newCompactionPickerByScore(newVersion, &vs.virtualBackings, vs.opts, nil))
	// Note that a "snapshot" version edit is written to the manifest when it is
	// created.
	vs.manifestFileNum = vs.getNextDiskFileNum()
	err = vs.createManifest(vs.dirname, vs.manifestFileNum, vs.minUnflushedLogNum, vs.nextFileNum.Load(), nil /* virtualBackings */)
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
	var bve bulkVersionEdit
	bve.AddedTablesByFileNum = make(map[base.FileNum]*tableMetadata)
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

	// Populate the fileBackingMap and the FileBacking for virtual sstables since
	// we have finished version edit accumulation.
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

	emptyVersion := manifest.NewVersion(opts.Comparer)
	newVersion, err := bve.Apply(emptyVersion, opts.FlushSplitBytes, opts.Experimental.ReadCompactionRate)
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
	for _, l := range newVersion.Levels {
		iter := l.Iter()
		for f := iter.First(); f != nil; f = iter.Next() {
			if !f.Virtual {
				_, localSize := sizeIfLocal(f.FileBacking, vs.provider)
				vs.metrics.Table.Local.LiveSize = uint64(int64(vs.metrics.Table.Local.LiveSize) + localSize)
			}
		}
	}
	vs.virtualBackings.ForEach(func(backing *fileBacking) {
		_, localSize := sizeIfLocal(backing, vs.provider)
		vs.metrics.Table.Local.LiveSize = uint64(int64(vs.metrics.Table.Local.LiveSize) + localSize)
	})

	vs.setCompactionPicker(
		newCompactionPickerByScore(newVersion, &vs.virtualBackings, vs.opts, nil))
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

// logLock locks the manifest for writing. The lock must be released by either
// a call to logUnlock or logAndApply.
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

// logAndApply logs the version edit to the manifest, applies the version edit
// to the current version, and installs the new version.
//
// logAndApply fills in the following fields of the VersionEdit: NextFileNum,
// LastSeqNum, RemovedBackingTables. The removed backing tables are those
// backings that are no longer used (in the new version) after applying the edit
// (as per vs.virtualBackings). Other than these fields, the VersionEdit must be
// complete.
//
// New table backing references (FileBacking.Ref) are taken as part of applying
// the version edit. The state of the virtual backings (vs.virtualBackings) is
// updated before logging to the manifest and installing the latest version;
// this is ok because any failure in those steps is fatal.
// TODO(radu): remove the error return.
//
// DB.mu must be held when calling this method and will be released temporarily
// while performing file I/O. Requires that the manifest is locked for writing
// (see logLock). Will unconditionally release the manifest lock (via
// logUnlock) even if an error occurs.
//
// inProgressCompactions is called while DB.mu is held, to get the list of
// in-progress compactions.
func (vs *versionSet) logAndApply(
	jobID JobID,
	ve *versionEdit,
	metrics map[int]*LevelMetrics,
	forceRotation bool,
	inProgressCompactions func() []compactionInfo,
) error {
	if !vs.writing {
		vs.opts.Logger.Fatalf("MANIFEST not locked for writing")
	}
	defer vs.logUnlockAndInvalidatePickedCompactionCache()

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
	//   than F. This will ensure that the total amount of time in logAndApply
	//   that is spent in snapshot writing is ~50%.
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
	requireRotation := forceRotation || vs.manifest == nil

	var nextSnapshotFilecount int64
	for i := range vs.metrics.Levels {
		nextSnapshotFilecount += vs.metrics.Levels[i].NumFiles
	}
	if sizeExceeded && !requireRotation {
		requireRotation = vs.rotationHelper.ShouldRotate(nextSnapshotFilecount)
	}
	var newManifestFileNum base.DiskFileNum
	var prevManifestFileSize uint64
	var newManifestVirtualBackings []*fileBacking
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
	zombieBackings, removedVirtualBackings, localLiveSizeDelta :=
		getZombiesAndUpdateVirtualBackings(ve, &vs.virtualBackings, vs.provider)

	if err := func() error {
		vs.mu.Unlock()
		defer vs.mu.Lock()

		if vs.getFormatMajorVersion() < FormatVirtualSSTables && len(ve.CreatedBackingTables) > 0 {
			return base.AssertionFailedf("MANIFEST cannot contain virtual sstable records due to format major version")
		}
		var b bulkVersionEdit
		err := b.Accumulate(ve)
		if err != nil {
			return errors.Wrap(err, "MANIFEST accumulate failed")
		}
		newVersion, err = b.Apply(
			currentVersion, vs.opts.FlushSplitBytes, vs.opts.Experimental.ReadCompactionRate,
		)
		if err != nil {
			return errors.Wrap(err, "MANIFEST apply failed")
		}

		if newManifestFileNum != 0 {
			if err := vs.createManifest(vs.dirname, newManifestFileNum, minUnflushedLogNum, nextFileNum, newManifestVirtualBackings); err != nil {
				vs.opts.EventListener.ManifestCreated(ManifestCreateInfo{
					JobID:   int(jobID),
					Path:    base.MakeFilepath(vs.fs, vs.dirname, base.FileTypeManifest, newManifestFileNum),
					FileNum: newManifestFileNum,
					Err:     err,
				})
				return errors.Wrap(err, "MANIFEST create failed")
			}
		}

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
				JobID:   int(jobID),
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
	inProgress := inProgressCompactions()

	newVersion.L0Sublevels.InitCompactingFileInfo(inProgressL0Compactions(inProgress))

	// Update the zombie tables set first, as installation of the new version
	// will unref the previous version which could result in addObsoleteLocked
	// being called.
	for _, b := range zombieBackings {
		vs.zombieTables[b.backing.DiskFileNum] = objectInfo{
			fileInfo: fileInfo{
				FileNum:  b.backing.DiskFileNum,
				FileSize: b.backing.Size,
			},
			isLocal: b.isLocal,
		}
	}

	// Unref the removed backings and report those that already became obsolete.
	// Note that the only case where we report obsolete tables here is when
	// VirtualBackings.Protect/Unprotect was used to keep a backing alive without
	// it being used in the current version.
	var obsoleteVirtualBackings manifest.ObsoleteFiles
	for _, b := range removedVirtualBackings {
		if b.backing.Unref() == 0 {
			obsoleteVirtualBackings.FileBackings = append(obsoleteVirtualBackings.FileBackings, b.backing)
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
			vs.obsoleteManifests = append(vs.obsoleteManifests, fileInfo{
				FileNum:  vs.manifestFileNum,
				FileSize: prevManifestFileSize,
			})
		}
		vs.manifestFileNum = newManifestFileNum
	}

	for level, update := range metrics {
		vs.metrics.Levels[level].Add(update)
	}
	for i := range vs.metrics.Levels {
		l := &vs.metrics.Levels[i]
		l.NumFiles = int64(newVersion.Levels[i].Len())
		l.NumVirtualFiles = newVersion.Levels[i].NumVirtual
		l.VirtualSize = newVersion.Levels[i].VirtualSize
		l.Size = int64(newVersion.Levels[i].Size())

		l.Sublevels = 0
		if l.NumFiles > 0 {
			l.Sublevels = 1
		}
		if invariants.Enabled {
			levelFiles := newVersion.Levels[i].Slice()
			if size := int64(levelFiles.SizeSum()); l.Size != size {
				vs.opts.Logger.Fatalf("versionSet metrics L%d Size = %d, actual size = %d", i, l.Size, size)
			}
			if nVirtual := levelFiles.NumVirtual(); nVirtual != l.NumVirtualFiles {
				vs.opts.Logger.Fatalf(
					"versionSet metrics L%d NumVirtual = %d, actual NumVirtual = %d",
					i, l.NumVirtualFiles, nVirtual,
				)
			}
			if vSize := levelFiles.VirtualSizeSum(); vSize != l.VirtualSize {
				vs.opts.Logger.Fatalf(
					"versionSet metrics L%d Virtual size = %d, actual size = %d",
					i, l.VirtualSize, vSize,
				)
			}
		}
	}
	vs.metrics.Levels[0].Sublevels = int32(len(newVersion.L0SublevelFiles))
	vs.metrics.Table.Local.LiveSize = uint64(int64(vs.metrics.Table.Local.LiveSize) + localLiveSizeDelta)

	vs.setCompactionPicker(
		newCompactionPickerByScore(newVersion, &vs.virtualBackings, vs.opts, inProgress))
	if !vs.dynamicBaseLevel {
		vs.picker.forceBaseLevel1()
	}
	return nil
}

func (vs *versionSet) setCompactionPicker(picker *compactionPickerByScore) {
	vs.picker = picker
	vs.curCompactionConcurrency.Store(int32(picker.getCompactionConcurrency()))
}

type fileBackingInfo struct {
	backing *fileBacking
	isLocal bool
}

// getZombiesAndUpdateVirtualBackings updates the virtual backings with the
// changes in the versionEdit and populates ve.RemovedBackingTables.
// Returns:
//   - zombieBackings: all backings (physical and virtual) that will no longer be
//     needed when we apply ve.
//   - removedVirtualBackings: the virtual backings that will be removed by the
//     VersionEdit and which must be Unref()ed by the caller. These backings
//     match ve.RemovedBackingTables.
//   - localLiveSizeDelta: the delta in local live bytes.
func getZombiesAndUpdateVirtualBackings(
	ve *versionEdit, virtualBackings *manifest.VirtualBackings, provider objstorage.Provider,
) (zombieBackings, removedVirtualBackings []fileBackingInfo, localLiveSizeDelta int64) {
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
			stillUsed[nf.Meta.FileBacking.DiskFileNum] = struct{}{}
			_, localFileDelta := sizeIfLocal(nf.Meta.FileBacking, provider)
			localLiveSizeDelta += localFileDelta
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
			isLocal, localFileDelta := sizeIfLocal(m.FileBacking, provider)
			localLiveSizeDelta -= localFileDelta
			if _, ok := stillUsed[m.FileBacking.DiskFileNum]; !ok {
				zombieBackings = append(zombieBackings, fileBackingInfo{
					backing: m.FileBacking,
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
		_, localFileDelta := sizeIfLocal(b, provider)
		localLiveSizeDelta += localFileDelta
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
			localLiveSizeDelta -= localFileDelta
			ve.RemovedBackingTables[i] = b.DiskFileNum
			zombieBackings = append(zombieBackings, fileBackingInfo{
				backing: b,
				isLocal: isLocal,
			})
			virtualBackings.Remove(b.DiskFileNum)
		}
		removedVirtualBackings = zombieBackings[len(zombieBackings)-len(unused):]
	}
	return zombieBackings, removedVirtualBackings, localLiveSizeDelta
}

// sizeIfLocal returns backing.Size if the backing is a local file, else 0.
func sizeIfLocal(
	backing *fileBacking, provider objstorage.Provider,
) (isLocal bool, localSize int64) {
	isLocal = objstorage.IsLocalTable(provider, backing.DiskFileNum)
	if isLocal {
		return true, int64(backing.Size)
	}
	return false, 0
}

func (vs *versionSet) incrementCompactions(
	kind compactionKind, extraLevels []*compactionLevel, pickerMetrics compactionPickerMetrics,
) {
	switch kind {
	case compactionKindDefault:
		vs.metrics.Compact.Count++
		vs.metrics.Compact.DefaultCount++

	case compactionKindFlush, compactionKindIngestedFlushable:
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

	case compactionKindTombstoneDensity:
		vs.metrics.Compact.Count++
		vs.metrics.Compact.TombstoneDensityCount++

	case compactionKindRewrite:
		vs.metrics.Compact.Count++
		vs.metrics.Compact.RewriteCount++

	case compactionKindCopy:
		vs.metrics.Compact.Count++
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
	virtualBackings []*fileBacking,
) (err error) {
	var (
		filename     = base.MakeFilepath(vs.fs, dirname, base.FileTypeManifest, fileNum)
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
	manifestFile, err = vs.fs.Create(filename, "pebble-manifest")
	if err != nil {
		return err
	}
	manifest = record.NewWriter(manifestFile)

	snapshot := versionEdit{
		ComparerName: vs.cmp.Name,
	}

	for level, levelMetadata := range vs.currentVersion().Levels {
		iter := levelMetadata.Iter()
		for meta := iter.First(); meta != nil; meta = iter.Next() {
			snapshot.NewTables = append(snapshot.NewTables, newTableEntry{
				Level: level,
				Meta:  meta,
			})
		}
	}

	snapshot.CreatedBackingTables = virtualBackings

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

// NB: This method is not safe for concurrent use. It is only safe
// to be called when concurrent changes to nextFileNum are not expected.
func (vs *versionSet) markFileNumUsed(fileNum base.DiskFileNum) {
	if vs.nextFileNum.Load() <= uint64(fileNum) {
		vs.nextFileNum.Store(uint64(fileNum + 1))
	}
}

// getNextFileNum returns the next file number to be used.
//
// Can be called without the versionSet's mutex being held.
func (vs *versionSet) getNextFileNum() base.FileNum {
	x := vs.nextFileNum.Add(1) - 1
	return base.FileNum(x)
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
			iter := l.Iter()
			for f := iter.First(); f != nil; f = iter.Next() {
				if f.Virtual {
					if _, ok := vs.virtualBackings.Get(f.FileBacking.DiskFileNum); !ok {
						panic(fmt.Sprintf("%s is not in virtualBackings", f.FileBacking.DiskFileNum))
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
			iter := lm.Iter()
			for f := iter.First(); f != nil; f = iter.Next() {
				m[f.FileBacking.DiskFileNum] = struct{}{}
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
	vs.virtualBackings.ForEach(func(b *fileBacking) {
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

	obsoleteFileInfo := make([]objectInfo, len(obsolete.FileBackings))
	for i, bs := range obsolete.FileBackings {
		obsoleteFileInfo[i].FileNum = bs.DiskFileNum
		obsoleteFileInfo[i].FileSize = bs.Size
	}

	if invariants.Enabled {
		dedup := make(map[base.DiskFileNum]struct{})
		for _, fi := range obsoleteFileInfo {
			dedup[fi.FileNum] = struct{}{}
		}
		if len(dedup) != len(obsoleteFileInfo) {
			panic("pebble: duplicate FileBacking present in obsolete list")
		}
	}

	for i, fi := range obsoleteFileInfo {
		// Note that the obsolete tables are no longer zombie by the definition of
		// zombie, but we leave them in the zombie tables map until they are
		// deleted from disk.
		//
		// TODO(sumeer): this means that the zombie metrics, like ZombieSize,
		// computed in DB.Metrics are also being counted in the obsolete metrics.
		// Was this intentional?
		info, ok := vs.zombieTables[fi.FileNum]
		if !ok {
			vs.opts.Logger.Fatalf("MANIFEST obsolete table %s not marked as zombie", fi.FileNum)
		}
		obsoleteFileInfo[i].isLocal = info.isLocal
	}

	vs.obsoleteTables = append(vs.obsoleteTables, obsoleteFileInfo...)
	vs.updateObsoleteTableMetricsLocked()
}

// addObsolete will acquire DB.mu, so DB.mu must not be held when this is
// called.
func (vs *versionSet) addObsolete(obsolete manifest.ObsoleteFiles) {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	vs.addObsoleteLocked(obsolete)
}

func (vs *versionSet) updateObsoleteTableMetricsLocked() {
	vs.metrics.Table.ObsoleteCount = int64(len(vs.obsoleteTables))
	vs.metrics.Table.ObsoleteSize = 0
	vs.metrics.Table.Local.ObsoleteSize = 0
	for _, fi := range vs.obsoleteTables {
		vs.metrics.Table.ObsoleteSize += fi.FileSize
		if fi.isLocal {
			vs.metrics.Table.Local.ObsoleteSize += fi.FileSize
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

func newFileMetrics(newFiles []manifest.NewTableEntry) map[int]*LevelMetrics {
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
