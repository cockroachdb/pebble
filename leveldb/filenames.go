// Copyright 2012 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package leveldb

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"code.google.com/p/leveldb-go/leveldb/db"
)

const (
	fileTypeLog = iota
	fileTypeLock
	fileTypeTable
	fileTypeManifest
	fileTypeCurrent
)

func dbFilename(dirname string, fileType int, fileNum uint64) string {
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
	}
	panic("unreachable")
}

// logFileNum returns the fileNum of the given log file, or 0 if that file is
// not a log file.
func logFileNum(filename string) uint64 {
	if !strings.HasSuffix(filename, ".log") {
		return 0
	}
	filename = filename[:len(filename)-4]
	u, err := strconv.ParseUint(filename, 10, 64)
	if err != nil {
		return 0
	}
	return u
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
