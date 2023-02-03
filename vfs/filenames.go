// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs

import (
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/redact"
)

// MakeFilepath builds a filepath from components.
func MakeFilepath(fs FS, dirname string, fileType base.FileType, fileNum base.FileNum) string {
	return fs.PathJoin(dirname, base.MakeFilename(fileType, fileNum))
}

// ParseFilepath parses the components from a filepath.
func ParseFilepath(fs FS, filename string) (fileType base.FileType, fileNum base.FileNum, ok bool) {
	return base.ParseFilename(fs.PathBase(filename))
}

// A Fataler fatals a process with a message when called.
type Fataler interface {
	Fatalf(format string, args ...interface{})
}

// MustExist checks if err is an error indicating a file does not exist.
// If it is, it lists the containing directory's files to annotate the error
// with counts of the various types of files and invokes the provided fataler.
// See cockroachdb/cockroach#56490.
func MustExist(fs FS, filename string, fataler Fataler, err error) {
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
		typ, _, ok := ParseFilepath(fs, f)
		if !ok {
			unknown++
			continue
		}
		switch typ {
		case base.FileTypeTable:
			tables++
		case base.FileTypeLog:
			logs++
		case base.FileTypeManifest:
			manifests++
		}
	}

	fataler.Fatalf("%s:\n%s\ndirectory contains %d files, %d unknown, %d tables, %d logs, %d manifests",
		fs.PathBase(filename), err, total, unknown, tables, logs, manifests)
}
