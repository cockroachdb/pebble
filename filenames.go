// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import "github.com/cockroachdb/pebble/internal/base"

type fileType = base.FileType

// FileNum is an identifier for a file within a database.
type FileNum = base.FileNum

const (
	fileTypeLog      = base.FileTypeLog
	fileTypeLock     = base.FileTypeLock
	fileTypeTable    = base.FileTypeTable
	fileTypeManifest = base.FileTypeManifest
	fileTypeOptions  = base.FileTypeOptions
	fileTypeTemp     = base.FileTypeTemp
	fileTypeOldTemp  = base.FileTypeOldTemp
)
