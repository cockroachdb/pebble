// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
)

// LocateMarker loads the current state of a marker.
func LocateMarker(fs FS, dir, markerName string) (*AtomicMarker, error) {
	ls, err := fs.List(dir)
	if err != nil {
		return nil, err
	}

	markerSuffix := fmt.Sprintf(".marker.%s", markerName)
	var markerFilename string
	for _, filename := range ls {
		if strings.HasSuffix(filename, markerSuffix) {
			if markerFilename != "" {
				return nil, errors.Newf("multiple atomic markers in for %q", markerName)
			}
			markerFilename = filename
		}
	}

	return &AtomicMarker{
		fs:       fs,
		name:     markerName,
		suffix:   markerSuffix,
		dir:      dir,
		filename: markerFilename,
	}, nil
}

// AtomicMarker provides an interface for marking a single file on
// the filesystem. The marker may be atomically moved from file to file.
// AtomicMarker is not safe for concurrent use. Multiple processes may
// not read or move the same marker simulatenously.
type AtomicMarker struct {
	fs       FS
	name     string
	suffix   string
	dir      string
	filename string
}

// Current returns the currently marked filename, or the empty string if
// no file is marked.
func (a *AtomicMarker) Current() string {
	return strings.TrimSuffix(a.filename, a.suffix)
}

// Move atomically moves the marker to mark the provided filename.
func (a *AtomicMarker) Move(filename string) error {
	dstFilename := filename + a.suffix
	dstPath := a.fs.PathJoin(a.dir, dstFilename)
	switch a.filename {
	case "":
		// The marker has never been placed. Create a new file.
		f, err := a.fs.Create(dstPath)
		if err != nil {
			return err
		}
		a.filename = dstFilename
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
