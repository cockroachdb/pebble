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

	markerPrefix := fmt.Sprintf("marker.%s.", markerName)
	var markerFilename string
	for _, filename := range ls {
		if strings.HasPrefix(filename, markerPrefix) {
			if markerFilename != "" {
				return nil, errors.Newf("multiple atomic markers for %q", markerName)
			}
			markerFilename = filename
		}
	}

	return &AtomicMarker{
		fs:       fs,
		name:     markerName,
		prefix:   markerPrefix,
		dir:      dir,
		filename: markerFilename,
	}, nil
}

// AtomicMarker provides an interface for marking a single file on the
// filesystem. The marker may be atomically moved from name to name.
// AtomicMarker is not safe for concurrent use. Multiple processes may
// not read or move the same marker simulatenously.
type AtomicMarker struct {
	fs       FS
	name     string
	prefix   string
	dir      string
	filename string
}

// Current returns the currently marked filename, or the empty string if
// no file is marked.
func (a *AtomicMarker) Current() string {
	return strings.TrimPrefix(a.filename, a.prefix)
}

// Move atomically moves the marker to mark the provided filename.
//
// The provided filename does not need to exist on the filesystem.
func (a *AtomicMarker) Move(filename string) error {
	dstFilename := a.prefix + filename
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
		//
		// NB: We're relying on Rename being atomic in the case that the
		// destination path does *not* already exist. We require that
		// any crash will always leave exactly one path pointing to the
		// file.
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
