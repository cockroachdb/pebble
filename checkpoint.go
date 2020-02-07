// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"os"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/vfs"
)

// Checkpoint constructs a snapshot of the DB instance in the specified
// directory. The WAL, MANIFEST, OPTIONS, and sstables will be copied into the
// snapshot. Hard links will be used when possible. Beware of the significant
// space overhead for a checkpoint if hard links are disabled. Also beware that
// even if hard links are used, the space overhead for the checkpoint will
// increase over time as the DB performs compactions.
func (d *DB) Checkpoint(destDir string) (err error) {
	if _, err := d.opts.FS.Stat(destDir); !os.IsNotExist(err) {
		if err == nil {
			return &os.PathError{
				Op:   "checkpoint",
				Path: destDir,
				Err:  os.ErrExist,
			}
		}
		return err
	}

	// Disable file deletions.
	d.mu.Lock()
	d.disableFileDeletions()
	defer func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		d.enableFileDeletions()
	}()

	// TODO(peter): RocksDB provides the option to flush if the WAL size is too
	// large, or roll the manifest if the MANIFEST size is too large. Should we
	// do this too?

	// Get the unflushed log files, the current version, and the current manifest
	// file number.
	memQueue := d.mu.mem.queue
	current := d.mu.versions.currentVersion()
	manifestFileNum := d.mu.versions.manifestFileNum
	manifestSize := d.mu.versions.manifest.Size()
	optionsFileNum := d.optionsFileNum

	// Release DB.mu so we don't block other operations on the database.
	d.mu.Unlock()

	// Wrap the normal filesystem with one which wraps newly created files with
	// vfs.NewSyncingFile.
	fs := syncingFS{
		FS: d.opts.FS,
		syncOpts: vfs.SyncingFileOptions{
			BytesPerSync: d.opts.BytesPerSync,
		},
	}
	// TODO(peter): We don't call sync on the parent directory of destDir. In
	// fact, if multiple directories are created, we don't call sync on any of
	// the parent directories.
	if err := fs.MkdirAll(destDir, 0755); err != nil {
		return err
	}
	dir, err := fs.OpenDir(destDir)
	if err != nil {
		return err
	}

	defer func() {
		dir.Close()

		if err != nil {
			// Attempt to cleanup on error.
			paths, _ := fs.List(destDir)
			for _, path := range paths {
				_ = fs.Remove(path)
			}
			_ = fs.Remove(destDir)
		}
	}()

	{
		// Link or copy the OPTIONS.
		srcPath := base.MakeFilename(fs, d.dirname, fileTypeOptions, optionsFileNum)
		destPath := fs.PathJoin(destDir, fs.PathBase(srcPath))
		if err := vfs.LinkOrCopy(fs, srcPath, destPath); err != nil {
			return err
		}
	}

	{
		// Copy the MANIFEST, and create CURRENT. We copy rather than link because
		// additional version edits added to the MANIFEST after we took our
		// snapshot of the sstables will reference sstables that aren't in our
		// checkpoint. For a similar reason, we need to limit how much of the
		// MANIFEST we copy.
		srcPath := base.MakeFilename(fs, d.dirname, fileTypeManifest, manifestFileNum)
		destPath := fs.PathJoin(destDir, fs.PathBase(srcPath))
		if err := vfs.LimitedCopy(fs, srcPath, destPath, manifestSize); err != nil {
			return err
		}
		if err := setCurrentFile(destDir, fs, manifestFileNum); err != nil {
			return err
		}
	}

	// Link or copy the sstables.
	for l := range current.Files {
		level := current.Files[l]
		for i := range level {
			srcPath := base.MakeFilename(fs, d.dirname, fileTypeTable, level[i].FileNum)
			destPath := fs.PathJoin(destDir, fs.PathBase(srcPath))
			if err := vfs.LinkOrCopy(fs, srcPath, destPath); err != nil {
				return err
			}
		}
	}

	// Copy the WAL files. We copy rather than link because WAL file recycling
	// will cause the WAL files to be reused which would invalidate the
	// checkpoint.
	for i := range memQueue {
		logNum := memQueue[i].logNum
		if logNum == 0 {
			continue
		}
		srcPath := base.MakeFilename(fs, d.walDirname, fileTypeLog, logNum)
		destPath := fs.PathJoin(destDir, fs.PathBase(srcPath))
		if err := vfs.Copy(fs, srcPath, destPath); err != nil {
			return err
		}
	}

	// Sync the destination directory.
	return dir.Sync()
}
