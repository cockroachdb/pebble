// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/petermattis/pebble/vfs"
)

type fileType int

const (
	fileTypeLog fileType = iota
	fileTypeLock
	fileTypeTable
	fileTypeManifest
	fileTypeCurrent
	fileTypeOptions
)

func dbFilename(dirname string, fileType fileType, fileNum uint64) string {
	for len(dirname) > 0 && dirname[len(dirname)-1] == os.PathSeparator {
		dirname = dirname[:len(dirname)-1]
	}
	switch fileType {
	case fileTypeLog:
		return fmt.Sprintf("%s%c%06d.log", dirname, os.PathSeparator, fileNum)
	case fileTypeLock:
		return fmt.Sprintf("%s%cLOCK", dirname, os.PathSeparator)
	case fileTypeTable:
		return fmt.Sprintf("%s%c%06d.sst", dirname, os.PathSeparator, fileNum)
	case fileTypeManifest:
		return fmt.Sprintf("%s%cMANIFEST-%06d", dirname, os.PathSeparator, fileNum)
	case fileTypeCurrent:
		return fmt.Sprintf("%s%cCURRENT", dirname, os.PathSeparator)
	case fileTypeOptions:
		return fmt.Sprintf("%s%cOPTIONS-%06d", dirname, os.PathSeparator, fileNum)
	}
	panic("unreachable")
}

func parseDBFilename(filename string) (fileType fileType, fileNum uint64, ok bool) {
	filename = filepath.Base(filename)
	switch {
	case filename == "CURRENT":
		return fileTypeCurrent, 0, true
	case filename == "LOCK":
		return fileTypeLock, 0, true
	case strings.HasPrefix(filename, "MANIFEST-"):
		u, err := strconv.ParseUint(filename[len("MANIFEST-"):], 10, 64)
		if err != nil {
			break
		}
		return fileTypeManifest, u, true
	case strings.HasPrefix(filename, "OPTIONS-"):
		u, err := strconv.ParseUint(filename[len("OPTIONS-"):], 10, 64)
		if err != nil {
			break
		}
		return fileTypeOptions, u, true
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
			return fileTypeTable, u, true
		case "log":
			return fileTypeLog, u, true
		}
	}
	return 0, 0, false
}

func setCurrentFile(dirname string, fs vfs.FS, fileNum uint64) error {
	newFilename := dbFilename(dirname, fileTypeCurrent, fileNum)
	oldFilename := fmt.Sprintf("%s.%06d.dbtmp", newFilename, fileNum)
	fs.Remove(oldFilename)
	f, err := fs.Create(oldFilename)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(f, "MANIFEST-%06d\n", fileNum); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return fs.Rename(oldFilename, newFilename)
}
