// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import (
	"path/filepath"

	"github.com/cockroachdb/pebble/vfs"
)

// Cleaner cleans obsolete files.
type Cleaner interface {
	Clean(fs vfs.FS, fileType FileType, bytesPerSync int, path string) error
}

// DeleteCleaner deletes file.
type DeleteCleaner struct{}

// Clean removes file.
func (DeleteCleaner) Clean(fs vfs.FS, fileType FileType, bytesPerSync int, path string) error {
	return fs.Remove(path)
}

func (DeleteCleaner) String() string {
	return "Delete"
}

// ArchiveCleaner archives file instead delete.
type ArchiveCleaner struct{}

// Clean archives file.
func (ArchiveCleaner) Clean(fs vfs.FS, fileType FileType, bytesPerSync int, path string) error {
	switch fileType {
	case FileTypeLog, FileTypeManifest, FileTypeTable:
		destDir := filepath.Dir(path) + "/archive"

		fs := SyncingFS{
			FS: fs,
			SyncOpts: vfs.SyncingFileOptions{
				BytesPerSync: bytesPerSync,
			},
		}

		if err := fs.MkdirAll(destDir, 0755); err != nil {
			return err
		}

		destPath := fs.PathJoin(destDir, fs.PathBase(path))
		return fs.Rename(path, destPath)

	default:
		return fs.Remove(path)
	}
}

func (ArchiveCleaner) String() string {
	return "Archive"
}
