// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"io"
	"os"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/v2/internal/base"
	"github.com/cockroachdb/pebble/v2/internal/manifest"
	"github.com/cockroachdb/pebble/v2/record"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/cockroachdb/pebble/v2/vfs/atomicfs"
)

// checkpointOptions hold the optional parameters to construct checkpoint
// snapshots.
type checkpointOptions struct {
	// flushWAL set to true will force a flush and sync of the WAL prior to
	// checkpointing.
	flushWAL bool

	// If set, any SSTs that don't overlap with these spans are excluded from a checkpoint.
	restrictToSpans []CheckpointSpan
}

// CheckpointOption set optional parameters used by `DB.Checkpoint`.
type CheckpointOption func(*checkpointOptions)

// WithFlushedWAL enables flushing and syncing the WAL prior to constructing a
// checkpoint. This guarantees that any writes committed before calling
// DB.Checkpoint will be part of that checkpoint.
//
// Note that this setting can only be useful in cases when some writes are
// performed with Sync = false. Otherwise, the guarantee will already be met.
//
// Passing this option is functionally equivalent to calling
// DB.LogData(nil, Sync) right before DB.Checkpoint.
func WithFlushedWAL() CheckpointOption {
	return func(opt *checkpointOptions) {
		opt.flushWAL = true
	}
}

// WithRestrictToSpans specifies spans of interest for the checkpoint. Any SSTs
// that don't overlap with any of these spans are excluded from the checkpoint.
//
// Note that the checkpoint can still surface keys outside of these spans (from
// the WAL and from SSTs that partially overlap with these spans). Moreover,
// these surface keys aren't necessarily "valid" in that they could have been
// modified but the SST containing the modification is excluded.
func WithRestrictToSpans(spans []CheckpointSpan) CheckpointOption {
	return func(opt *checkpointOptions) {
		opt.restrictToSpans = spans
	}
}

// CheckpointSpan is a key range [Start, End) (inclusive on Start, exclusive on
// End) of interest for a checkpoint.
type CheckpointSpan struct {
	Start []byte
	End   []byte
}

// excludeFromCheckpoint returns true if an SST file should be excluded from the
// checkpoint because it does not overlap with the spans of interest
// (opt.restrictToSpans).
func excludeFromCheckpoint(f *manifest.TableMetadata, opt *checkpointOptions, cmp Compare) bool {
	if len(opt.restrictToSpans) == 0 {
		// Option not set; don't exclude anything.
		return false
	}
	for _, s := range opt.restrictToSpans {
		spanBounds := base.UserKeyBoundsEndExclusive(s.Start, s.End)
		if f.Overlaps(cmp, &spanBounds) {
			return false
		}
	}
	// None of the restrictToSpans overlapped; we can exclude this file.
	return true
}

// mkdirAllAndSyncParents creates destDir and any of its missing parents.
// Those missing parents, as well as the closest existing ancestor, are synced.
// Returns a handle to the directory created at destDir.
func mkdirAllAndSyncParents(fs vfs.FS, destDir string) (vfs.File, error) {
	// Collect paths for all directories between destDir (excluded) and its
	// closest existing ancestor (included).
	var parentPaths []string
	for parentPath := fs.PathDir(destDir); ; parentPath = fs.PathDir(parentPath) {
		parentPaths = append(parentPaths, parentPath)
		if fs.PathDir(parentPath) == parentPath {
			break
		}
		_, err := fs.Stat(parentPath)
		if err == nil {
			// Exit loop at the closest existing ancestor.
			break
		}
		if !oserror.IsNotExist(err) {
			return nil, err
		}
	}
	// Create destDir and any of its missing parents.
	if err := fs.MkdirAll(destDir, 0755); err != nil {
		return nil, err
	}
	// Sync all the parent directories up to the closest existing ancestor,
	// included.
	for _, parentPath := range parentPaths {
		parentDir, err := fs.OpenDir(parentPath)
		if err != nil {
			return nil, err
		}
		err = parentDir.Sync()
		if err != nil {
			_ = parentDir.Close()
			return nil, err
		}
		err = parentDir.Close()
		if err != nil {
			return nil, err
		}
	}
	return fs.OpenDir(destDir)
}

// Checkpoint constructs a snapshot of the DB instance in the specified
// directory. The WAL, MANIFEST, OPTIONS, and sstables will be copied into the
// snapshot. Hard links will be used when possible. Beware of the significant
// space overhead for a checkpoint if hard links are disabled. Also beware that
// even if hard links are used, the space overhead for the checkpoint will
// increase over time as the DB performs compactions.
//
// Note that shared files in a checkpoint could get deleted if the DB is
// restarted after a checkpoint operation, as the reference for the checkpoint
// is only maintained in memory. This is okay as long as users of Checkpoint
// crash shortly afterwards with a "poison file" preventing further restarts.
func (d *DB) Checkpoint(
	destDir string, opts ...CheckpointOption,
) (
	ckErr error, /* used in deferred cleanup */
) {
	opt := &checkpointOptions{}
	for _, fn := range opts {
		fn(opt)
	}

	if _, err := d.opts.FS.Stat(destDir); !oserror.IsNotExist(err) {
		if err == nil {
			return &os.PathError{
				Op:   "checkpoint",
				Path: destDir,
				Err:  oserror.ErrExist,
			}
		}
		return err
	}

	if opt.flushWAL && !d.opts.DisableWAL {
		// Write an empty log-data record to flush and sync the WAL.
		if err := d.LogData(nil /* data */, Sync); err != nil {
			return err
		}
	}

	// Disable file deletions.
	// We acquire a reference on the version down below that will prevent any
	// sstables or blob files from becoming "obsolete" and potentially deleted,
	// but this doesn't protect the current WALs or manifests.
	d.mu.Lock()
	d.disableFileDeletions()
	defer func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		d.enableFileDeletions()
	}()

	// TODO(peter): RocksDB provides the option to roll the manifest if the
	// MANIFEST size is too large. Should we do this too?

	// Lock the manifest before getting the current version. We need the
	// length of the manifest that we read to match the current version that
	// we read, otherwise we might copy a versionEdit not reflected in the
	// sstables we copy/link.
	d.mu.versions.logLock()
	// Get the the current version and the current manifest file number.
	current := d.mu.versions.currentVersion()
	formatVers := d.FormatMajorVersion()
	manifestFileNum := d.mu.versions.manifestFileNum
	manifestSize := d.mu.versions.manifest.Size()
	optionsFileNum := d.optionsFileNum

	virtualBackingFiles := make(map[base.DiskFileNum]struct{})
	d.mu.versions.latest.virtualBackings.ForEach(func(backing *manifest.TableBacking) {
		virtualBackingFiles[backing.DiskFileNum] = struct{}{}
	})
	versionBlobFiles := d.mu.versions.latest.blobFiles.Metadatas()

	// Acquire the logs while holding mutexes to ensure we don't race with a
	// flush that might mark a log that's relevant to `current` as obsolete
	// before our call to List.
	allLogicalLogs := d.mu.log.manager.List()

	// Release the manifest and DB.mu so we don't block other operations on the
	// database.
	//
	// But first reference the version to ensure that the version's in-memory
	// state and its physical files remain available for the checkpoint. In
	// particular, the Version.BlobFileSet is only valid while a version is
	// referenced.
	current.Ref()
	d.mu.versions.logUnlock()
	d.mu.Unlock()
	defer current.Unref()

	// Wrap the normal filesystem with one which wraps newly created files with
	// vfs.NewSyncingFile.
	fs := vfs.NewSyncingFS(d.opts.FS, vfs.SyncingFileOptions{
		NoSyncOnClose: d.opts.NoSyncOnClose,
		BytesPerSync:  d.opts.BytesPerSync,
	})

	// Create the dir and its parents (if necessary), and sync them.
	var dir vfs.File
	defer func() {
		if dir != nil {
			_ = dir.Close()
		}
		if ckErr != nil {
			// Attempt to cleanup on error.
			_ = fs.RemoveAll(destDir)
		}
	}()
	dir, ckErr = mkdirAllAndSyncParents(fs, destDir)
	if ckErr != nil {
		return ckErr
	}

	{
		// Copy the OPTIONS.
		srcPath := base.MakeFilepath(fs, d.dirname, base.FileTypeOptions, optionsFileNum)
		destPath := fs.PathJoin(destDir, fs.PathBase(srcPath))
		ckErr = copyCheckpointOptions(fs, srcPath, destPath)
		if ckErr != nil {
			return ckErr
		}
	}

	{
		// Set the format major version in the destination directory.
		var versionMarker *atomicfs.Marker
		versionMarker, _, ckErr = atomicfs.LocateMarker(fs, destDir, formatVersionMarkerName)
		if ckErr != nil {
			return ckErr
		}

		// We use the marker to encode the active format version in the
		// marker filename. Unlike other uses of the atomic marker,
		// there is no file with the filename `formatVers.String()` on
		// the filesystem.
		ckErr = versionMarker.Move(formatVers.String())
		if ckErr != nil {
			return ckErr
		}
		ckErr = versionMarker.Close()
		if ckErr != nil {
			return ckErr
		}
	}

	var excludedTables map[manifest.DeletedTableEntry]*manifest.TableMetadata
	var includedBlobFiles map[base.BlobFileID]struct{}
	var remoteFiles []base.DiskFileNum
	// Set of TableBacking.DiskFileNum which will be required by virtual sstables
	// in the checkpoint.
	requiredVirtualBackingFiles := make(map[base.DiskFileNum]struct{})

	copyFile := func(typ base.FileType, fileNum base.DiskFileNum) error {
		meta, err := d.objProvider.Lookup(typ, fileNum)
		if err != nil {
			return err
		}
		if meta.IsRemote() {
			// We don't copy remote files. This is desirable as checkpointing is
			// supposed to be a fast operation, and references to remote files can
			// always be resolved by any checkpoint readers by reading the object
			// catalog. We don't add this file to excludedFiles either, as that'd
			// cause it to be deleted in the second manifest entry which is also
			// inaccurate.
			remoteFiles = append(remoteFiles, meta.DiskFileNum)
			return nil
		}
		srcPath := base.MakeFilepath(fs, d.dirname, typ, fileNum)
		destPath := fs.PathJoin(destDir, fs.PathBase(srcPath))
		return vfs.LinkOrCopy(fs, srcPath, destPath)
	}

	// Link or copy the sstables.
	for l := range current.Levels {
		iter := current.Levels[l].Iter()
		for f := iter.First(); f != nil; f = iter.Next() {
			if excludeFromCheckpoint(f, opt, d.cmp) {
				if excludedTables == nil {
					excludedTables = make(map[manifest.DeletedTableEntry]*manifest.TableMetadata)
				}
				excludedTables[manifest.DeletedTableEntry{
					Level:   l,
					FileNum: f.TableNum,
				}] = f
				continue
			}

			// Copy any referenced blob files that have not already been copied.
			if len(f.BlobReferences) > 0 {
				if includedBlobFiles == nil {
					includedBlobFiles = make(map[base.BlobFileID]struct{})
				}
				for _, ref := range f.BlobReferences {
					if _, ok := includedBlobFiles[ref.FileID]; !ok {
						includedBlobFiles[ref.FileID] = struct{}{}

						// Map the BlobFileID to a DiskFileNum in the current version.
						diskFileNum, ok := current.BlobFiles.Lookup(ref.FileID)
						if !ok {
							return errors.Errorf("blob file %s not found", ref.FileID)
						}
						ckErr = copyFile(base.FileTypeBlob, diskFileNum)
						if ckErr != nil {
							return ckErr
						}
					}
				}
			}

			tableBacking := f.TableBacking
			if f.Virtual {
				if _, ok := requiredVirtualBackingFiles[tableBacking.DiskFileNum]; ok {
					continue
				}
				requiredVirtualBackingFiles[tableBacking.DiskFileNum] = struct{}{}
			}
			ckErr = copyFile(base.FileTypeTable, tableBacking.DiskFileNum)
			if ckErr != nil {
				return ckErr
			}
		}
	}

	var removeBackingTables []base.DiskFileNum
	for diskFileNum := range virtualBackingFiles {
		if _, ok := requiredVirtualBackingFiles[diskFileNum]; !ok {
			// The backing sstable associated with fileNum is no longer
			// required.
			removeBackingTables = append(removeBackingTables, diskFileNum)
		}
	}
	// Record the blob files that are not referenced by any included sstables.
	// When we write the MANIFEST of the checkpoint, we'll include a final
	// VersionEdit that removes these blob files so that the checkpointed
	// manifest is consistent.
	var excludedBlobFiles map[manifest.DeletedBlobFileEntry]*manifest.PhysicalBlobFile
	if len(includedBlobFiles) < len(versionBlobFiles) {
		excludedBlobFiles = make(map[manifest.DeletedBlobFileEntry]*manifest.PhysicalBlobFile, len(versionBlobFiles)-len(includedBlobFiles))
		for _, meta := range versionBlobFiles {
			if _, ok := includedBlobFiles[meta.FileID]; !ok {
				excludedBlobFiles[manifest.DeletedBlobFileEntry{
					FileID:  meta.FileID,
					FileNum: meta.Physical.FileNum,
				}] = meta.Physical
			}
		}
	}

	ckErr = d.writeCheckpointManifest(
		fs, formatVers, destDir, dir, manifestFileNum, manifestSize,
		excludedTables, removeBackingTables, excludedBlobFiles,
	)
	if ckErr != nil {
		return ckErr
	}
	if len(remoteFiles) > 0 {
		ckErr = d.objProvider.CheckpointState(fs, destDir, remoteFiles)
		if ckErr != nil {
			return ckErr
		}
	}

	// Copy the WAL files. We copy rather than link because WAL file recycling
	// will cause the WAL files to be reused which would invalidate the
	// checkpoint. It's possible allLogicalLogs includes logs that are not
	// relevant (beneath the version's MinUnflushedLogNum). These extra files
	// are harmless. The earlier (wal.Manager).List call will not include
	// obsolete logs that are sitting in the recycler or have already been
	// passed off to the cleanup manager for deletion.
	//
	// TODO(jackson): It would be desirable to copy all recycling and obsolete
	// WALs to aid corruption postmortem debugging should we need them.
	for _, log := range allLogicalLogs {
		for i := 0; i < log.NumSegments(); i++ {
			srcFS, srcPath := log.SegmentLocation(i)
			destPath := fs.PathJoin(destDir, srcFS.PathBase(srcPath))
			ckErr = vfs.CopyAcrossFS(srcFS, srcPath, fs, destPath)
			if ckErr != nil {
				return ckErr
			}
		}
	}

	// Sync and close the checkpoint directory.
	ckErr = dir.Sync()
	if ckErr != nil {
		return ckErr
	}
	ckErr = dir.Close()
	dir = nil
	return ckErr
}

// copyCheckpointOptions copies an OPTIONS file, commenting out some options
// that existed on the original database but no longer apply to the checkpointed
// database. For example, the entire [WAL Failover] stanza is commented out
// because Checkpoint will copy all WAL segment files from both the primary and
// secondary WAL directories into the checkpoint.
func copyCheckpointOptions(fs vfs.FS, srcPath, dstPath string) error {
	var buf bytes.Buffer
	f, err := fs.Open(srcPath)
	if err != nil {
		return err
	}
	defer f.Close()
	b, err := io.ReadAll(f)
	if err != nil {
		return err
	}
	// Copy the OPTIONS file verbatim, but commenting out the [WAL Failover]
	// section.
	err = parseOptions(string(b), parseOptionsFuncs{
		visitNewSection: func(startOff, endOff int, section string) error {
			if section == "WAL Failover" {
				buf.WriteString("# ")
			}
			buf.Write(b[startOff:endOff])
			return nil
		},
		visitKeyValue: func(startOff, endOff int, section, key, value string) error {
			if section == "WAL Failover" {
				buf.WriteString("# ")
			}
			buf.Write(b[startOff:endOff])
			return nil
		},
		visitCommentOrWhitespace: func(startOff, endOff int, line string) error {
			buf.Write(b[startOff:endOff])
			return nil
		},
	})
	if err != nil {
		return err
	}
	nf, err := fs.Create(dstPath, vfs.WriteCategoryUnspecified)
	if err != nil {
		return err
	}
	_, err = io.Copy(nf, &buf)
	if err != nil {
		return err
	}
	return errors.CombineErrors(nf.Sync(), nf.Close())
}

func (d *DB) writeCheckpointManifest(
	fs vfs.FS,
	formatVers FormatMajorVersion,
	destDirPath string,
	destDir vfs.File,
	manifestFileNum base.DiskFileNum,
	manifestSize int64,
	excludedTables map[manifest.DeletedTableEntry]*manifest.TableMetadata,
	removeBackingTables []base.DiskFileNum,
	excludedBlobFiles map[manifest.DeletedBlobFileEntry]*manifest.PhysicalBlobFile,
) error {
	// Copy the MANIFEST, and create a pointer to it. We copy rather
	// than link because additional version edits added to the
	// MANIFEST after we took our snapshot of the sstables will
	// reference sstables that aren't in our checkpoint. For a
	// similar reason, we need to limit how much of the MANIFEST we
	// copy.
	// If some files are excluded from the checkpoint, also append a block that
	// records those files as deleted.
	if err := func() error {
		srcPath := base.MakeFilepath(fs, d.dirname, base.FileTypeManifest, manifestFileNum)
		destPath := fs.PathJoin(destDirPath, fs.PathBase(srcPath))
		src, err := fs.Open(srcPath, vfs.SequentialReadsOption)
		if err != nil {
			return err
		}
		defer src.Close()

		dst, err := fs.Create(destPath, vfs.WriteCategoryUnspecified)
		if err != nil {
			return err
		}
		defer dst.Close()

		// Copy all existing records. We need to copy at the record level in case we
		// need to append another record with the excluded files (we cannot simply
		// append a record after a raw data copy; see
		// https://github.com/cockroachdb/cockroach/issues/100935).
		r := record.NewReader(&io.LimitedReader{R: src, N: manifestSize}, manifestFileNum)
		w := record.NewWriter(dst)
		for {
			rr, err := r.Next()
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			rw, err := w.Next()
			if err != nil {
				return err
			}
			if _, err := io.Copy(rw, rr); err != nil {
				return err
			}
		}

		if len(excludedTables) > 0 || len(excludedBlobFiles) > 0 {
			// Write out an additional VersionEdit that deletes the excluded SST files.
			ve := manifest.VersionEdit{
				DeletedTables:        excludedTables,
				RemovedBackingTables: removeBackingTables,
				DeletedBlobFiles:     excludedBlobFiles,
			}

			rw, err := w.Next()
			if err != nil {
				return err
			}
			if err := ve.Encode(rw); err != nil {
				return err
			}
		}
		if err := w.Close(); err != nil {
			return err
		}
		return dst.Sync()
	}(); err != nil {
		return err
	}

	var manifestMarker *atomicfs.Marker
	manifestMarker, _, err := atomicfs.LocateMarker(fs, destDirPath, manifestMarkerName)
	if err != nil {
		return err
	}
	if err := manifestMarker.Move(base.MakeFilename(base.FileTypeManifest, manifestFileNum)); err != nil {
		return err
	}
	return manifestMarker.Close()
}
