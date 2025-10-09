// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
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
	rs.manifestMarker, rs.manifestFileNum, rs.manifestExists, err = findCurrentManifest(opts.FS, dirname, rs.ls)
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
	manifestExists          bool
	maxFilenumUsed          base.DiskFileNum
	obsoleteTempFilenames   []string
	objProvider             objstorage.Provider
	previousOptionsFilename string
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
