// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/redact"
)

// FileNum is an internal DB identifier for a table. Tables can be physical (in
// which case the FileNum also identifies the backing object) or virtual.
type FileNum uint64

// String returns a string representation of the file number.
func (fn FileNum) String() string { return fmt.Sprintf("%06d", fn) }

// SafeFormat implements redact.SafeFormatter.
func (fn FileNum) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("%06d", redact.SafeUint(fn))
}

// PhysicalTableDiskFileNum converts the FileNum of a physical table to the
// backing DiskFileNum. The underlying numbers always match for physical tables.
func PhysicalTableDiskFileNum(n FileNum) DiskFileNum {
	return DiskFileNum(n)
}

// PhysicalTableFileNum converts the DiskFileNum backing a physical table into
// the table's FileNum. The underlying numbers always match for physical tables.
func PhysicalTableFileNum(f DiskFileNum) FileNum {
	return FileNum(f)
}

// A DiskFileNum identifies a file or object with exists on disk.
type DiskFileNum uint64

func (dfn DiskFileNum) String() string { return fmt.Sprintf("%06d", dfn) }

// SafeFormat implements redact.SafeFormatter.
func (dfn DiskFileNum) SafeFormat(w redact.SafePrinter, verb rune) {
	w.Printf("%06d", redact.SafeUint(dfn))
}

// FileType enumerates the types of files found in a DB.
type FileType int

// The FileType enumeration.
const (
	FileTypeLog FileType = iota
	FileTypeLock
	FileTypeTable
	FileTypeManifest
	FileTypeOptions
	FileTypeOldTemp
	FileTypeTemp
	FileTypeBlob
)

var fileTypeStrings = [...]string{
	FileTypeLog:      "log",
	FileTypeLock:     "lock",
	FileTypeTable:    "sstable",
	FileTypeManifest: "manifest",
	FileTypeOptions:  "options",
	FileTypeOldTemp:  "old-temp",
	FileTypeTemp:     "temp",
	FileTypeBlob:     "blob",
}

// FileTypeFromName parses a FileType from its string representation.
func FileTypeFromName(name string) FileType {
	for i, s := range fileTypeStrings {
		if s == name {
			return FileType(i)
		}
	}
	panic(fmt.Sprintf("unknown file type: %q", name))
}

// SafeFormat implements redact.SafeFormatter.
func (ft FileType) SafeFormat(w redact.SafePrinter, _ rune) {
	if ft < 0 || int(ft) >= len(fileTypeStrings) {
		w.Print(redact.SafeString("unknown"))
		return
	}
	w.Print(redact.SafeString(fileTypeStrings[ft]))
}

// String implements fmt.Stringer.
func (ft FileType) String() string {
	return redact.StringWithoutMarkers(ft)
}

// MakeFilename builds a filename from components.
func MakeFilename(fileType FileType, dfn DiskFileNum) string {
	switch fileType {
	case FileTypeLog:
		panic("the pebble/wal pkg is responsible for constructing WAL filenames")
	case FileTypeLock:
		return "LOCK"
	case FileTypeTable:
		return fmt.Sprintf("%s.sst", dfn)
	case FileTypeManifest:
		return fmt.Sprintf("MANIFEST-%s", dfn)
	case FileTypeOptions:
		return fmt.Sprintf("OPTIONS-%s", dfn)
	case FileTypeOldTemp:
		return fmt.Sprintf("CURRENT.%s.dbtmp", dfn)
	case FileTypeTemp:
		return fmt.Sprintf("temporary.%s.dbtmp", dfn)
	case FileTypeBlob:
		return fmt.Sprintf("%s.blob", dfn)
	}
	panic("unreachable")
}

// MakeFilepath builds a filepath from components.
func MakeFilepath(fs vfs.FS, dirname string, fileType FileType, dfn DiskFileNum) string {
	return fs.PathJoin(dirname, MakeFilename(fileType, dfn))
}

// ParseFilename parses the components from a filename.
func ParseFilename(fs vfs.FS, filename string) (fileType FileType, dfn DiskFileNum, ok bool) {
	filename = fs.PathBase(filename)
	switch {
	case filename == "LOCK":
		return FileTypeLock, 0, true
	case strings.HasPrefix(filename, "MANIFEST-"):
		dfn, ok = ParseDiskFileNum(filename[len("MANIFEST-"):])
		if !ok {
			break
		}
		return FileTypeManifest, dfn, true
	case strings.HasPrefix(filename, "OPTIONS-"):
		dfn, ok = ParseDiskFileNum(filename[len("OPTIONS-"):])
		if !ok {
			break
		}
		return FileTypeOptions, dfn, ok
	case strings.HasPrefix(filename, "CURRENT.") && strings.HasSuffix(filename, ".dbtmp"):
		s := strings.TrimSuffix(filename[len("CURRENT."):], ".dbtmp")
		dfn, ok = ParseDiskFileNum(s)
		if !ok {
			break
		}
		return FileTypeOldTemp, dfn, ok
	case strings.HasPrefix(filename, "temporary.") && strings.HasSuffix(filename, ".dbtmp"):
		s := strings.TrimSuffix(filename[len("temporary."):], ".dbtmp")
		dfn, ok = ParseDiskFileNum(s)
		if !ok {
			break
		}
		return FileTypeTemp, dfn, ok
	default:
		i := strings.IndexByte(filename, '.')
		if i < 0 {
			break
		}
		dfn, ok = ParseDiskFileNum(filename[:i])
		if !ok {
			break
		}
		switch filename[i+1:] {
		case "sst":
			return FileTypeTable, dfn, true
		case "blob":
			return FileTypeBlob, dfn, true
		}
	}
	return 0, dfn, false
}

// ParseDiskFileNum parses the provided string as a disk file number.
func ParseDiskFileNum(s string) (dfn DiskFileNum, ok bool) {
	u, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return dfn, false
	}
	return DiskFileNum(u), true
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
	err = AddDetailsToNotExistError(fs, filename, err)
	fataler.Fatalf("%+v", err)
}

// AddDetailsToNotExistError annotates an unexpected not-exist error with
// information about the directory contents.
func AddDetailsToNotExistError(fs vfs.FS, filename string, err error) error {
	ls, lsErr := fs.List(fs.PathDir(filename))
	if lsErr != nil {
		// TODO(jackson): if oserror.IsNotExist(lsErr), the data directory
		// doesn't exist anymore. Another process likely deleted it before
		// killing the process. We want to fatal the process, but without
		// triggering error reporting like Sentry.
		return errors.WithDetailf(err, "list err: %+v", lsErr)
	}
	var total, unknown, tables, logs, manifests int
	total = len(ls)
	for _, f := range ls {
		// The file format of log files is an implementation detail of the wal/
		// package that the internal/base package is not privy to. We can't call
		// into the wal package because that would introduce a cyclical
		// dependency. For our purposes, an exact count isn't important and we
		// just count files with .log extensions.
		if filepath.Ext(f) == ".log" {
			logs++
			continue
		}
		typ, _, ok := ParseFilename(fs, f)
		if !ok {
			unknown++
			continue
		}
		switch typ {
		case FileTypeTable:
			tables++
		case FileTypeManifest:
			manifests++
		}
	}

	return errors.WithDetailf(err, "filename: %s; directory contains %d files, %d unknown, %d tables, %d logs, %d manifests",
		filename, total, unknown, tables, logs, manifests)
}

// FileInfo provides some rudimentary information about a file.
type FileInfo struct {
	FileNum  DiskFileNum
	FileSize uint64
}
