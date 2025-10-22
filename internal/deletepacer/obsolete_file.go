// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package deletepacer

import (
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/vfs"
)

// ObsoleteFile describes a file that is to be deleted.
type ObsoleteFile struct {
	FileType base.FileType
	FS       vfs.FS
	Path     string
	FileNum  base.DiskFileNum
	FileSize uint64 // approx for log files
	IsLocal  bool
}

// pacingBytes returns the size of the file, or 0 if deleting the file does not
// require pacing.
func (of ObsoleteFile) pacingBytes() uint64 {
	// We only need to pace local objects--sstables and blob files.
	if of.IsLocal && (of.FileType == base.FileTypeTable || of.FileType == base.FileTypeBlob) {
		return of.FileSize
	}
	return 0
}
