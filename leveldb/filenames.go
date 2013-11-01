// Copyright 2012 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package leveldb

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"code.google.com/p/leveldb-go/leveldb/db"
)

type fileType int

const (
	fileTypeLog fileType = iota
	fileTypeLock
	fileTypeTable
	fileTypeOldFashionedTable
	fileTypeManifest
	fileTypeCurrent
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
		return fmt.Sprintf("%s%c%06d.ldb", dirname, os.PathSeparator, fileNum)
	case fileTypeOldFashionedTable:
		return fmt.Sprintf("%s%c%06d.sst", dirname, os.PathSeparator, fileNum)
	case fileTypeManifest:
		return fmt.Sprintf("%s%cMANIFEST-%06d", dirname, os.PathSeparator, fileNum)
	case fileTypeCurrent:
		return fmt.Sprintf("%s%cCURRENT", dirname, os.PathSeparator)
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
		case "ldb":
			return fileTypeTable, u, true
		case "log":
			return fileTypeLog, u, true
		case "sst":
			return fileTypeOldFashionedTable, u, true
		}
	}
	return 0, 0, false
}

func setCurrentFile(dirname string, fs db.FileSystem, fileNum uint64) error {
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
	if err := f.Close(); err != nil {
		return err
	}
	return fs.Rename(oldFilename, newFilename)
}
