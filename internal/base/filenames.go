// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/pebble/vfs"
)

// FileType enumerates the types of files found in a DB.
type FileType int

// The FileType enumeration.
const (
	FileTypeLog FileType = iota
	FileTypeLock
	FileTypeTable
	FileTypeManifest
	FileTypeCurrent
	FileTypeOptions
)

// MakeFilename builds a filename from components.
func MakeFilename(fs vfs.FS, dirname string, fileType FileType, fileNum uint64) string {
	switch fileType {
	case FileTypeLog:
		return fs.PathJoin(dirname, fmt.Sprintf("%06d.log", fileNum))
	case FileTypeLock:
		return fs.PathJoin(dirname, "LOCK")
	case FileTypeTable:
		return fs.PathJoin(dirname, fmt.Sprintf("%06d.sst", fileNum))
	case FileTypeManifest:
		return fs.PathJoin(dirname, fmt.Sprintf("MANIFEST-%06d", fileNum))
	case FileTypeCurrent:
		return fs.PathJoin(dirname, "CURRENT")
	case FileTypeOptions:
		return fs.PathJoin(dirname, fmt.Sprintf("OPTIONS-%06d", fileNum))
	}
	panic("unreachable")
}

// ParseFilename parses the components from a filename.
func ParseFilename(fs vfs.FS, filename string) (fileType FileType, fileNum uint64, ok bool) {
	filename = fs.PathBase(filename)
	switch {
	case filename == "CURRENT":
		return FileTypeCurrent, 0, true
	case filename == "LOCK":
		return FileTypeLock, 0, true
	case strings.HasPrefix(filename, "MANIFEST-"):
		u, err := strconv.ParseUint(filename[len("MANIFEST-"):], 10, 64)
		if err != nil {
			break
		}
		return FileTypeManifest, u, true
	case strings.HasPrefix(filename, "OPTIONS-"):
		u, err := strconv.ParseUint(filename[len("OPTIONS-"):], 10, 64)
		if err != nil {
			break
		}
		return FileTypeOptions, u, true
	default:
		i := strings.IndexByte(filename, '.')
		if i < 0 {
			break
		}
		u, err := strconv.ParseUint(filename[:i], 10, 64)
		if err != nil {
			break
		}
		switch filename[i+1:] {
		case "sst":
			return FileTypeTable, u, true
		case "log":
			return FileTypeLog, u, true
		}
	}
	return 0, 0, false
}
