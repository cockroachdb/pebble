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

// FileNum is an internal DB indentifier for a file.
type FileNum uint64

// String returns a string representation of the file number.
func (fn FileNum) String() string { return fmt.Sprintf("%06d", fn) }

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
	FileTypeTemp
)

// MakeFilename builds a filename from components.
func MakeFilename(fs vfs.FS, dirname string, fileType FileType, fileNum FileNum) string {
	switch fileType {
	case FileTypeLog:
		return fs.PathJoin(dirname, fmt.Sprintf("%s.log", fileNum))
	case FileTypeLock:
		return fs.PathJoin(dirname, "LOCK")
	case FileTypeTable:
		return fs.PathJoin(dirname, fmt.Sprintf("%s.sst", fileNum))
	case FileTypeManifest:
		return fs.PathJoin(dirname, fmt.Sprintf("MANIFEST-%s", fileNum))
	case FileTypeCurrent:
		return fs.PathJoin(dirname, "CURRENT")
	case FileTypeOptions:
		return fs.PathJoin(dirname, fmt.Sprintf("OPTIONS-%s", fileNum))
	case FileTypeTemp:
		return fs.PathJoin(dirname, fmt.Sprintf("CURRENT.%s.dbtmp", fileNum))
	}
	panic("unreachable")
}

// ParseFilename parses the components from a filename.
func ParseFilename(fs vfs.FS, filename string) (fileType FileType, fileNum FileNum, ok bool) {
	filename = fs.PathBase(filename)
	switch {
	case filename == "CURRENT":
		return FileTypeCurrent, 0, true
	case filename == "LOCK":
		return FileTypeLock, 0, true
	case strings.HasPrefix(filename, "MANIFEST-"):
		fileNum, ok = parseFileNum(filename[len("MANIFEST-"):])
		if !ok {
			break
		}
		return FileTypeManifest, fileNum, true
	case strings.HasPrefix(filename, "OPTIONS-"):
		fileNum, ok = parseFileNum(filename[len("OPTIONS-"):])
		if !ok {
			break
		}
		return FileTypeOptions, fileNum, ok
	case strings.HasPrefix(filename, "CURRENT.") && strings.HasSuffix(filename, ".dbtmp"):
		s := strings.TrimSuffix(filename[len("CURRENT."):], ".dbtmp")
		fileNum, ok = parseFileNum(s)
		if !ok {
			break
		}
		return FileTypeTemp, fileNum, ok
	default:
		i := strings.IndexByte(filename, '.')
		if i < 0 {
			break
		}
		fileNum, ok = parseFileNum(filename[:i])
		if !ok {
			break
		}
		switch filename[i+1:] {
		case "sst":
			return FileTypeTable, fileNum, true
		case "log":
			return FileTypeLog, fileNum, true
		}
	}
	return 0, fileNum, false
}

func parseFileNum(s string) (fileNum FileNum, ok bool) {
	u, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return fileNum, false
	}
	return FileNum(u), true
}
