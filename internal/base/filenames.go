// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
func MakeFilename(dirname string, fileType FileType, fileNum uint64) string {
	for len(dirname) > 0 && dirname[len(dirname)-1] == os.PathSeparator {
		dirname = dirname[:len(dirname)-1]
	}
	switch fileType {
	case FileTypeLog:
		return fmt.Sprintf("%s%c%06d.log", dirname, os.PathSeparator, fileNum)
	case FileTypeLock:
		return fmt.Sprintf("%s%cLOCK", dirname, os.PathSeparator)
	case FileTypeTable:
		return fmt.Sprintf("%s%c%06d.sst", dirname, os.PathSeparator, fileNum)
	case FileTypeManifest:
		return fmt.Sprintf("%s%cMANIFEST-%06d", dirname, os.PathSeparator, fileNum)
	case FileTypeCurrent:
		return fmt.Sprintf("%s%cCURRENT", dirname, os.PathSeparator)
	case FileTypeOptions:
		return fmt.Sprintf("%s%cOPTIONS-%06d", dirname, os.PathSeparator, fileNum)
	}
	panic("unreachable")
}

// ParseFilename parses the components from a filename.
func ParseFilename(filename string) (fileType FileType, fileNum uint64, ok bool) {
	filename = filepath.Base(filename)
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
