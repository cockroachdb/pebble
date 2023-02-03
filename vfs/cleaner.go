// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs

import "github.com/cockroachdb/pebble/internal/base"

// Cleaner cleans obsolete files.
type Cleaner interface {
	Clean(fs FS, fileType base.FileType, path string) error
}

// NeedsFileContents is implemented by a cleaner that needs the contents of the
// files that it is being asked to clean.
type NeedsFileContents interface {
	needsFileContents()
}

// DeleteCleaner deletes file.
type DeleteCleaner struct{}

// Clean removes file.
func (DeleteCleaner) Clean(fs FS, fileType base.FileType, path string) error {
	return fs.Remove(path)
}

func (DeleteCleaner) String() string {
	return "delete"
}

// ArchiveCleaner archives file instead delete.
type ArchiveCleaner struct{}

var _ NeedsFileContents = ArchiveCleaner{}

// Clean archives file.
func (ArchiveCleaner) Clean(fs FS, fileType base.FileType, path string) error {
	switch fileType {
	case base.FileTypeLog, base.FileTypeManifest, base.FileTypeTable:
		destDir := fs.PathJoin(fs.PathDir(path), "archive")

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
	return "archive"
}

func (ArchiveCleaner) needsFileContents() {
}
