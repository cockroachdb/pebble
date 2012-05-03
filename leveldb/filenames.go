// Copyright 2012 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package leveldb

import (
	"fmt"
	"os"
)

const (
	fileTypeLog = iota
	fileTypeLock
	fileTypeTable
	fileTypeManifest
	fileTypeCurrent
)

func filename(dirname string, fileType int, fileNum uint64) string {
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
