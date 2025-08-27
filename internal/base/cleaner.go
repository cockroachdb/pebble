// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import "github.com/cockroachdb/pebble/v2/vfs"

// Cleaner cleans obsolete files.
type Cleaner interface {
	Clean(fs vfs.FS, fileType FileType, path string) error
}

// NeedsFileContents is implemented by a cleaner that needs the contents of the
// files that it is being asked to clean.
type NeedsFileContents interface {
	needsFileContents()
}

// DeleteCleaner deletes file.
type DeleteCleaner struct{}

// Clean removes file.
func (DeleteCleaner) Clean(fs vfs.FS, fileType FileType, path string) error {
	return fs.Remove(path)
}

func (DeleteCleaner) String() string {
	return "delete"
}

// ArchiveCleaner archives file instead delete.
type ArchiveCleaner struct{}

var _ NeedsFileContents = ArchiveCleaner{}

// Clean archives file.
//
// TODO(sumeer): for log files written to the secondary FS, the archiving will
// also write to the secondary. We should consider archiving to the primary.
func (ArchiveCleaner) Clean(fs vfs.FS, fileType FileType, path string) error {
	switch fileType {
	case FileTypeLog, FileTypeManifest, FileTypeTable, FileTypeBlob:
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
