// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import "github.com/cockroachdb/pebble/vfs"

// SyncingFS wraps a vfs.FS with one that wraps newly created files with
// vfs.NewSyncingFile.
type SyncingFS struct {
	vfs.FS
	SyncOpts vfs.SyncingFileOptions
}

func (fs SyncingFS) Create(name string) (vfs.File, error) {
	f, err := fs.FS.Create(name)
	if err != nil {
		return nil, err
	}
	return vfs.NewSyncingFile(f, fs.SyncOpts), nil
}
