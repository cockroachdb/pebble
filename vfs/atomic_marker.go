// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors/oserror"
)

// LocateMarker loads the current state of a marker.
func LocateMarker(fs FS, dir, markerName string) (*AtomicMarker, error) {
	ls, err := fs.List(dir)
	if err != nil {
		return nil, err
	}

	var markerFilename string
	var markerIteration uint64
	var obsolete []string
	for _, filename := range ls {
		name, iter, _, ok := parseMarkerFilename(filename)
		if !ok || name != markerName {
			continue
		}

		if markerIteration < iter && markerFilename != "" {
			obsolete = append(obsolete, markerFilename)
		}
		if markerFilename == "" || markerIteration < iter {
			markerFilename = filename
			markerIteration = iter
		}
	}

	return &AtomicMarker{
		fs:            fs,
		dir:           dir,
		name:          markerName,
		filename:      markerFilename,
		iter:          markerIteration,
		obsoleteFiles: obsolete,
	}, nil
}

// AtomicMarker provides an interface for marking a single file on the
// filesystem. The marker may be atomically moved from name to name.
// AtomicMarker is not safe for concurrent use. Multiple processes may
// not read or move the same marker simulatenously.
type AtomicMarker struct {
	fs  FS
	dir string
	// name identifies the marker.
	name string
	// filename contains the entire filename of the current marker. It
	// has a format of `marker.<name>.<iter>.<value>`.
	filename string
	// iter holds the current iteration value. It matches the iteration
	// value encoded within filename, if filename is non-empty. Iter is
	// monotonically increasing over the lifetime of a marker.
	iter uint64
	// obsoleteFiles holds a list of files discovered by LocateMarker
	// that are old values for this marker. These files may be leftover
	// if the filesystem doesn't guarantee atomic renames (eg, if it's
	// implemented as a link(newpath), remove(oldpath), and a crash in
	// between may leave an entry at the old path)
	obsoleteFiles []string
}

func markerFilename(name string, iter uint64, value string) string {
	return fmt.Sprintf("marker.%s.%06d.%s", name, iter, value)
}

func parseMarkerFilename(s string) (name string, iter uint64, value string, ok bool) {
	// Check for and remove the `marker.` prefix.
	if !strings.HasPrefix(s, `marker.`) {
		return "", 0, "", false
	}
	s = s[len(`marker.`):]

	// Extract the marker's name.
	i := strings.IndexByte(s, '.')
	if i == -1 {
		return "", 0, "", false
	}
	name = s[:i]
	s = s[i+1:]

	// Extract the marker's iteration number.
	i = strings.IndexByte(s, '.')
	if i == -1 {
		return "", 0, "", false
	}
	var err error
	iter, err = strconv.ParseUint(s[:i], 10, 64)
	if err != nil {
		return "", 0, "", false
	}

	// Everything after the iteration's `.` delimiter is the value.
	s = s[i+1:]

	return name, iter, s, true
}

// Current returns the currently marked filename, or the empty string if
// no file is marked.
func (a *AtomicMarker) Current() string {
	_, _, v, ok := parseMarkerFilename(a.filename)
	if !ok {
		return ""
	}
	return v
}

// Move atomically moves the marker to mark the provided filename.
//
// The provided filename does not need to exist on the filesystem.
func (a *AtomicMarker) Move(filename string) error {
	dstIter := a.iter + 1
	dstFilename := markerFilename(a.name, dstIter, filename)
	dstPath := a.fs.PathJoin(a.dir, dstFilename)
	switch a.filename {
	case "":
		// The marker has never been placed. Create a new file.
		f, err := a.fs.Create(dstPath)
		if err != nil {
			return err
		}
		a.filename = dstFilename
		a.iter = dstIter
		if err := f.Close(); err != nil {
			return err
		}
	case dstFilename:
		// Nothing to do; the marker is already placed on this file.
		// We still fallthrough to syncing the directory, because the
		// marker may have been successfully moved in the OS buffer
		// cache but may not have been synced to persistent storage.
	default:
		// Move the marker from its old filename to this one.
		if err := a.fs.Rename(a.fs.PathJoin(a.dir, a.filename), dstPath); err != nil {
			return err
		}
		a.filename = dstFilename
		a.iter = dstIter
	}
	// Sync the directory to ensure marker movement is synced.
	dir, err := a.fs.OpenDir(a.dir)
	if err != nil {
		return err
	}
	if err := dir.Sync(); err != nil {
		return err
	}
	return dir.Close()
}

// RemoveObsolete removes any obsolete files discovered during
// LocateMarker.
func (a *AtomicMarker) RemoveObsolete() error {
	obsolete := a.obsoleteFiles
	for _, filename := range obsolete {
		if err := a.fs.Remove(a.fs.PathJoin(a.dir, filename)); err != nil && !oserror.IsNotExist(err) {
			return err
		}
		a.obsoleteFiles = obsolete[1:]
	}
	a.obsoleteFiles = nil
	return nil
}
