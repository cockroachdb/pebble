// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"io"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/metrics"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/atomicfs"
)

// recoverState reads the named database directory and recovers the set of files
// encoding the database state at the moment the previous process exited.
// recoverState is read only and does not mutate the on-disk state.
func recoverState(opts *Options, dirname string) (s *recoveredState, err error) {
	rs := &recoveredState{
		dirname: dirname,
		fs:      opts.FS,
	}
	if err := rs.init(opts, dirname); err != nil {
		return nil, errors.CombineErrors(err, rs.Close())
	}
	return rs, nil
}

func (rs *recoveredState) init(opts *Options, dirname string) error {
	// List the directory contents. This also happens to include WAL log files,
	// if they are in the same dir.
	var err error
	if rs.ls, err = opts.FS.List(dirname); err != nil {
		return errors.Wrapf(err, "pebble: database %q", dirname)
	}
	// Find the currently format major version and active manifest.
	rs.fmv, rs.fmvMarker, err = lookupFormatMajorVersion(opts.FS, dirname, rs.ls)
	if err != nil {
		return errors.Wrapf(err, "pebble: database %q", dirname)
	}

	// Open the object storage provider.
	providerSettings := opts.MakeObjStorageProviderSettings(dirname)
	providerSettings.FSDirInitialListing = rs.ls
	rs.objProvider, err = objstorageprovider.Open(providerSettings)
	if err != nil {
		return errors.Wrapf(err, "pebble: database %q", dirname)
	}

	// Determine which manifest is current, and if one exists, replay it to
	// recover the current Version of the LSM.
	var manifestExists bool
	rs.manifestMarker, rs.manifestFileNum, manifestExists, err = findCurrentManifest(opts.FS, dirname, rs.ls)
	if err != nil {
		return errors.Wrapf(err, "pebble: database %q", dirname)
	}
	if manifestExists {
		recoveredVersion, err := recoverVersion(opts, dirname, rs.objProvider, rs.manifestFileNum)
		if err != nil {
			return err
		}
		if !opts.DisableConsistencyCheck {
			if err := checkConsistency(recoveredVersion.version, rs.objProvider); err != nil {
				return err
			}
		}
		rs.recoveredVersion = recoveredVersion
	}

	// Identify the maximal file number in the directory. We do not want to
	// reuse any existing file numbers even if they are obsolete file numbers to
	// avoid modifying an ingested sstable's original external file.
	//
	// We also identify the most recent OPTIONS file, so we can validate our
	// configured Options against the previous options, and we collect any
	// orphaned temporary files that should be removed.
	var previousOptionsFileNum base.DiskFileNum
	for _, filename := range rs.ls {
		ft, fn, ok := base.ParseFilename(opts.FS, filename)
		if !ok {
			continue
		}
		rs.maxFilenumUsed = max(rs.maxFilenumUsed, fn)
		switch ft {
		case base.FileTypeLog:
			// Ignore.
		case base.FileTypeOptions:
			if previousOptionsFileNum < fn {
				previousOptionsFileNum = fn
				rs.previousOptionsFilename = filename
			}
		case base.FileTypeTemp, base.FileTypeOldTemp:
			rs.obsoleteTempFilenames = append(rs.obsoleteTempFilenames, filename)
		}
	}
	return nil
}

// recoveredState encapsulates state recovered from reading the database
// directory.
type recoveredState struct {
	dirname                 string
	fmv                     FormatMajorVersion
	fmvMarker               *atomicfs.Marker
	fs                      vfs.FS
	ls                      []string
	manifestMarker          *atomicfs.Marker
	manifestFileNum         base.DiskFileNum
	maxFilenumUsed          base.DiskFileNum
	obsoleteTempFilenames   []string
	objProvider             objstorage.Provider
	previousOptionsFilename string
	recoveredVersion        *recoveredVersion
}

// RemoveObsolete removes obsolete files uncovered during recovery.
func (rs *recoveredState) RemoveObsolete() error {
	var err error
	// Atomic markers may leave behind obsolete files if there's a crash
	// mid-update.
	if rs.fmvMarker != nil {
		err = errors.CombineErrors(err, rs.fmvMarker.RemoveObsolete())
	}
	if rs.manifestMarker != nil {
		err = errors.CombineErrors(err, rs.manifestMarker.RemoveObsolete())
	}
	// Some codepaths write to a temporary file and then rename it to its final
	// location when complete.  A temp file is leftover if a process exits
	// before the rename. Remove any that were found.
	for _, filename := range rs.obsoleteTempFilenames {
		err = errors.CombineErrors(err, rs.fs.Remove(rs.fs.PathJoin(rs.dirname, filename)))
	}
	return err
}

// Close closes resources held by the RecoveredState, including open file
// descriptors.
func (rs *recoveredState) Close() error {
	var err error
	if rs.fmvMarker != nil {
		err = errors.CombineErrors(err, rs.fmvMarker.Close())
	}
	if rs.manifestMarker != nil {
		err = errors.CombineErrors(err, rs.manifestMarker.Close())
	}
	if rs.objProvider != nil {
		err = errors.CombineErrors(err, rs.objProvider.Close())
	}
	return err
}

// recoveredVersion describes the latest Version of the LSM recovered by
// replaying a manifest file.
type recoveredVersion struct {
	manifestFileNum    base.DiskFileNum
	minUnflushedLogNum base.DiskFileNum
	nextFileNum        base.DiskFileNum
	logSeqNum          base.SeqNum
	latest             *latestVersionState
	metrics            Metrics
	version            *manifest.Version
}

// recoverVersion replays the named manifest file to recover the latest version
// of the LSM from persisted state.
func recoverVersion(
	opts *Options, dirname string, provider objstorage.Provider, manifestFileNum base.DiskFileNum,
) (*recoveredVersion, error) {
	vs := &recoveredVersion{
		manifestFileNum: manifestFileNum,
		nextFileNum:     1,
		logSeqNum:       base.SeqNumStart,
		latest: &latestVersionState{
			l0Organizer:     manifest.NewL0Organizer(opts.Comparer, opts.FlushSplitBytes),
			virtualBackings: manifest.MakeVirtualBackings(),
		},
	}
	manifestPath := base.MakeFilepath(opts.FS, dirname, base.FileTypeManifest, vs.manifestFileNum)
	manifestFilename := opts.FS.PathBase(manifestPath)

	// Read the versionEdits in the manifest file.
	var bve manifest.BulkVersionEdit
	bve.AllAddedTables = make(map[base.TableNum]*manifest.TableMetadata)
	manifestFile, err := opts.FS.Open(manifestPath)
	if err != nil {
		return nil, errors.Wrapf(err, "pebble: could not open manifest file %q for DB %q",
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
			return nil, errors.Wrapf(err, "pebble: error when loading manifest file %q",
				errors.Safe(manifestFilename))
		}
		var ve manifest.VersionEdit
		err = ve.Decode(r)
		if err != nil {
			// Break instead of returning an error if the record is corrupted
			// or invalid.
			if err == io.EOF || record.IsInvalidRecord(err) {
				break
			}
			return nil, err
		}
		if ve.ComparerName != "" {
			if ve.ComparerName != opts.Comparer.Name {
				return nil, errors.Errorf("pebble: manifest file %q for DB %q: "+
					"comparer name from file %q != comparer name from Options %q",
					errors.Safe(manifestFilename), dirname, errors.Safe(ve.ComparerName), errors.Safe(opts.Comparer.Name))
			}
		}
		if err := bve.Accumulate(&ve); err != nil {
			return nil, err
		}
		if ve.MinUnflushedLogNum != 0 {
			vs.minUnflushedLogNum = ve.MinUnflushedLogNum
		}
		if ve.NextFileNum != 0 {
			vs.nextFileNum = base.DiskFileNum(ve.NextFileNum)
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
			vs.logSeqNum = max(ve.LastSeqNum+1, base.SeqNumStart)
		}
	}

	// We have already set vs.nextFileNum=1 at the beginning of the function and
	// could have only updated it to some other non-zero value, so it cannot be
	// 0 here.
	if vs.minUnflushedLogNum == 0 {
		if vs.nextFileNum >= 2 {
			// We either have a freshly created DB, or a DB created by RocksDB
			// that has not had a single flushed SSTable yet. This is because
			// RocksDB bumps up nextFileNum in this case without bumping up
			// minUnflushedLogNum, even if WALs with non-zero file numbers are
			// present in the directory.
		} else {
			return nil, base.CorruptionErrorf("pebble: malformed manifest file %q for DB %q",
				errors.Safe(manifestFilename), dirname)
		}
	}
	vs.nextFileNum = max(vs.nextFileNum, vs.minUnflushedLogNum+1)

	// Populate the virtual backings for virtual sstables since we have finished
	// version edit accumulation.
	for _, b := range bve.AddedFileBacking {
		isLocal := objstorage.IsLocalTable(provider, b.DiskFileNum)
		vs.latest.virtualBackings.AddAndRef(b, isLocal)
	}
	for l, addedLevel := range bve.AddedTables {
		for _, m := range addedLevel {
			if m.Virtual {
				vs.latest.virtualBackings.AddTable(m, l)
			}
		}
	}

	if invariants.Enabled {
		// There should be no deleted tables or backings, since we're starting
		// from an empty state.
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
		return nil, err
	}
	vs.latest.l0Organizer.PerformUpdate(vs.latest.l0Organizer.PrepareUpdate(&bve, newVersion), newVersion)
	vs.latest.l0Organizer.InitCompactingFileInfo(nil /* in-progress compactions */)
	vs.latest.blobFiles.Init(&bve, manifest.BlobRewriteHeuristic{
		CurrentTime: opts.private.timeNow,
		MinimumAge:  opts.Experimental.ValueSeparationPolicy().RewriteMinimumAge,
	})
	vs.version = newVersion
	setBasicLevelMetrics(&vs.metrics, newVersion)
	vs.metrics.Table.Live.All = metrics.CountAndSize{}
	for i := range vs.metrics.Levels {
		vs.metrics.Table.Live.All.Accumulate(vs.metrics.Levels[i].Tables)
	}
	for _, l := range newVersion.Levels {
		for f := range l.All() {
			if !f.Virtual {
				if isLocal, localSize := sizeIfLocal(f.TableBacking, provider); isLocal {
					vs.metrics.Table.Live.Local.Inc(localSize)
				}
			}
		}
	}
	for backing := range vs.latest.virtualBackings.All() {
		if isLocal, localSize := sizeIfLocal(backing, provider); isLocal {
			vs.metrics.Table.Live.Local.Inc(localSize)
		}
	}
	return vs, nil
}
