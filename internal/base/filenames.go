// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/redact"
)

// FileNum is an internal DB identifier for a file.
type FileNum uint64

// String returns a string representation of the file number.
func (fn FileNum) String() string { return fmt.Sprintf("%06d", fn) }

// DiskFileNum converts a FileNum to a DiskFileNum. DiskFileNum should only be
// called if the caller can ensure that the FileNum belongs to a physical file
// on disk. These could be manifests, log files, physical sstables on disk, the
// options file, but not virtual sstables.
func (fn FileNum) DiskFileNum() DiskFileNum {
	return DiskFileNum{uint64(fn)}
}

// A DiskFileNum is just a FileNum belonging to a file which exists on disk.
// Note that a FileNum is an internal DB identifier and it could belong to files
// which don't exist on disk. An example would be virtual sstable FileNums.
// Converting a DiskFileNum to a FileNum is always valid, whereas converting a
// FileNum to DiskFileNum may not be valid an care should be taken to prove that
// the FileNum actually exists on disk.
type DiskFileNum struct {
	Val uint64
}

func (rh DiskFileNum) String() string { return fmt.Sprintf("%06d", rh.Val) }

// FileNum converts a DiskFileNum to a FileNum. This conversion is always valid.
func (rh DiskFileNum) FileNum() FileNum {
	return FileNum(rh.Val)
}

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
	FileTypeOldTemp
	FileTypeTemp
)

// MakeFilename builds a filename from components.
func MakeFilename(fileType FileType, rh DiskFileNum) string {
	switch fileType {
	case FileTypeLog:
		return fmt.Sprintf("%s.log", rh)
	case FileTypeLock:
		return "LOCK"
	case FileTypeTable:
		return fmt.Sprintf("%s.sst", rh)
	case FileTypeManifest:
		return fmt.Sprintf("MANIFEST-%s", rh)
	case FileTypeCurrent:
		return "CURRENT"
	case FileTypeOptions:
		return fmt.Sprintf("OPTIONS-%s", rh)
	case FileTypeOldTemp:
		return fmt.Sprintf("CURRENT.%s.dbtmp", rh)
	case FileTypeTemp:
		return fmt.Sprintf("temporary.%s.dbtmp", rh)
	}
	panic("unreachable")
}

// MakeFilepath builds a filepath from components.
func MakeFilepath(fs vfs.FS, dirname string, fileType FileType, rh DiskFileNum) string {
	return fs.PathJoin(dirname, MakeFilename(fileType, rh))
}

// ParseFilename parses the components from a filename.
func ParseFilename(fs vfs.FS, filename string) (fileType FileType, rh DiskFileNum, ok bool) {
	filename = fs.PathBase(filename)
	switch {
	case filename == "CURRENT":
		return FileTypeCurrent, DiskFileNum{0}, true
	case filename == "LOCK":
		return FileTypeLock, DiskFileNum{0}, true
	case strings.HasPrefix(filename, "MANIFEST-"):
		rh, ok = parseReadHandle(filename[len("MANIFEST-"):])
		if !ok {
			break
		}
		return FileTypeManifest, rh, true
	case strings.HasPrefix(filename, "OPTIONS-"):
		rh, ok = parseReadHandle(filename[len("OPTIONS-"):])
		if !ok {
			break
		}
		return FileTypeOptions, rh, ok
	case strings.HasPrefix(filename, "CURRENT.") && strings.HasSuffix(filename, ".dbtmp"):
		s := strings.TrimSuffix(filename[len("CURRENT."):], ".dbtmp")
		rh, ok = parseReadHandle(s)
		if !ok {
			break
		}
		return FileTypeOldTemp, rh, ok
	case strings.HasPrefix(filename, "temporary.") && strings.HasSuffix(filename, ".dbtmp"):
		s := strings.TrimSuffix(filename[len("temporary."):], ".dbtmp")
		rh, ok = parseReadHandle(s)
		if !ok {
			break
		}
		return FileTypeTemp, rh, ok
	default:
		i := strings.IndexByte(filename, '.')
		if i < 0 {
			break
		}
		rh, ok = parseReadHandle(filename[:i])
		if !ok {
			break
		}
		switch filename[i+1:] {
		case "sst":
			return FileTypeTable, rh, true
		case "log":
			return FileTypeLog, rh, true
		}
	}
	return 0, rh, false
}

func parseReadHandle(s string) (rh DiskFileNum, ok bool) {
	u, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return rh, false
	}
	return DiskFileNum{u}, true
}

// A Fataler fatals a process with a message when called.
type Fataler interface {
	Fatalf(format string, args ...interface{})
}

// MustExist checks if err is an error indicating a file does not exist.
// If it is, it lists the containing directory's files to annotate the error
// with counts of the various types of files and invokes the provided fataler.
// See cockroachdb/cockroach#56490.
func MustExist(fs vfs.FS, filename string, fataler Fataler, err error) {
	if err == nil || !oserror.IsNotExist(err) {
		return
	}

	ls, lsErr := fs.List(fs.PathDir(filename))
	if lsErr != nil {
		// TODO(jackson): if oserror.IsNotExist(lsErr), the the data directory
		// doesn't exist anymore. Another process likely deleted it before
		// killing the process. We want to fatal the process, but without
		// triggering error reporting like Sentry.
		fataler.Fatalf("%s:\norig err: %s\nlist err: %s", redact.Safe(fs.PathBase(filename)), err, lsErr)
	}
	var total, unknown, tables, logs, manifests int
	total = len(ls)
	for _, f := range ls {
		typ, _, ok := ParseFilename(fs, f)
		if !ok {
			unknown++
			continue
		}
		switch typ {
		case FileTypeTable:
			tables++
		case FileTypeLog:
			logs++
		case FileTypeManifest:
			manifests++
		}
	}

	fataler.Fatalf("%s:\n%s\ndirectory contains %d files, %d unknown, %d tables, %d logs, %d manifests",
		fs.PathBase(filename), err, total, unknown, tables, logs, manifests)
}
