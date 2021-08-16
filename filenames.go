// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/vfs"
)

type fileType = base.FileType

// FileNum is an identifier for a file within a database.
type FileNum = base.FileNum

const (
	fileTypeLog       = base.FileTypeLog
	fileTypeLock      = base.FileTypeLock
	fileTypeTable     = base.FileTypeTable
	fileTypeManifest  = base.FileTypeManifest
	fileTypeCurrent   = base.FileTypeCurrent
	fileTypeCurrentV2 = base.FileTypeCurrentV2
	fileTypeOptions   = base.FileTypeOptions
	fileTypeTemp      = base.FileTypeTemp
)

func setCurrentFile(dirname string, fs, atomicRenameFS vfs.FS, fileNum FileNum) error {
	// RocksDB and older versions of Pebble use a file named `CURRENT`
	// that contains the filename of the currently active manifest.
	// Manifest rotations depend on atomically renaming a file to this
	// path.
	//
	// Many vfs.FS implementations wrap other FS implementations,
	// sometimes losing the operating system's atomicity guarantees. An
	// ill-timed crash while using one of these FSes could leave a store
	// in an unrecoverable state without a `CURRENT` file.
	//
	// To correct this, we require that a vfs.FS without atomic renames
	// allows Pebble to access the underlying vfs.FS that does provide
	// atomic renames for the purpose of manifest rotations. This is a
	// backwards incompatible change, so we introduce a new `CURRENT-v2`
	// file.
	//
	// To maintain backwards compatibility, we continue to update the
	// old `CURRENT` file. If the database is opened by a previous
	// Pebble version that does not know about `CURRENT-v2`, the
	// `CURRENT` file may be updated without making a corresponding
	// update to the `CURRENT-v2` file. To account for this, newer
	// versions of Pebble read both the `CURRENT` and `CURRENT-v2` files
	// on startup, continuing using whichever's manifest has a higher
	// file number.

	type currentFile struct {
		fs          vfs.FS
		newFilename string
	}
	currentFiles := []currentFile{
		// NB: The fileTypeCurrentV2 file must be updated first.
		{
			fs:          atomicRenameFS,
			newFilename: base.MakeFilename(fs, dirname, fileTypeCurrentV2, fileNum),
		},
		{
			fs:          fs,
			newFilename: base.MakeFilename(fs, dirname, fileTypeCurrent, fileNum),
		},
	}

	for _, v := range currentFiles {
		oldFilename := base.MakeFilename(fs, dirname, fileTypeTemp, fileNum)
		v.fs.Remove(oldFilename)
		f, err := v.fs.Create(oldFilename)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(f, "MANIFEST-%s\n", fileNum); err != nil {
			return err
		}
		if err := f.Sync(); err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		if err := v.fs.Rename(oldFilename, v.newFilename); err != nil {
			return err
		}
	}
	return nil
}
