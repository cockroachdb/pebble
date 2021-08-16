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

// FormatMajorVersion is a constant controlling the format of persisted
// data. Backwards incompatible changes to durable formats are gated
// behind a new format major version.
//
// At any point, a database's format major version may be bumped.
// However, once a database's format major version is increased,
// previous versions of Pebble will refuse to open the database.
//
// The zero value format is the earliest format with maximum
// backwards compatibility.
type FormatMajorVersion uint64

const (
	// FormatDefault leaves the format version unspecified. The
	// FormatDefault constant may be ratcheted upwards over time.
	FormatDefault FormatMajorVersion = iota
	// FormatMostCompatible maintains the most backwards compatibility,
	// maintaining bi-directional compatibility with RocksDB 6.2.1 in
	// the particular configuration described in the Pebble README.
	FormatMostCompatible
	// FormatCurrentVersioned is the first backwards-incompatible change
	// made to Pebble, replacing the CURRENT file with a versioned
	// equivalent (CURRENT-XXXXXX) where the numeric portion encodes the
	// format major version.
	FormatCurrentVersioned
	// FormatNewest always contains the most recent format major version.
	FormatNewest FormatMajorVersion = FormatCurrentVersioned
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
	case FileTypeOptions:
		return fs.PathJoin(dirname, fmt.Sprintf("OPTIONS-%s", fileNum))
	case FileTypeTemp:
		return fs.PathJoin(dirname, fmt.Sprintf("CURRENT.%s.dbtmp", fileNum))
	}
	panic("unreachable")
}

// MakeCurrentFilename builds a filename for the current file.
func MakeCurrentFilename(fs vfs.FS, dirname string, formatVers FormatMajorVersion) string {
	if formatVers == FormatDefault {
		panic("cannot create current filename with default constant")
	}
	// The FormatMostCompatible format predates versioned CURRENT-XXXXXX
	// files and uses the `CURRENT` filename shared by RocksDB, LevelDB,
	// etc.
	if formatVers == FormatMostCompatible {
		return fs.PathJoin(dirname, "CURRENT")
	}
	return fs.PathJoin(dirname, fmt.Sprintf("CURRENT-%s", FileNum(formatVers)))
}

// ParseFilename parses the components from a filename.
func ParseFilename(fs vfs.FS, filename string) (fileType FileType, fileNum FileNum, ok bool) {
	filename = fs.PathBase(filename)
	switch {
	case filename == "CURRENT":
		return FileTypeCurrent, 0, true
	case strings.HasPrefix(filename, "CURRENT-"):
		fileNum, ok = parseFileNum(filename[len("CURRENT-"):])
		if !ok {
			break
		}
		return FileTypeCurrent, fileNum, true
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
