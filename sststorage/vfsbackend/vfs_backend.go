// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfsbackend

import (
	"os"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/sststorage"
	"github.com/cockroachdb/pebble/vfs"
)

// Settings for the vfs.FS-based sstable storage backend.
type Settings struct {
	// NoSyncOnClose decides whether the implementation will enforce a
	// close-time synchronization (e.g., fdatasync() or sync_file_range())
	// on files it writes to. Setting this to true removes the guarantee for a
	// sync on close. Some implementations can still issue a non-blocking sync.
	NoSyncOnClose bool

	// BytesPerSync enables periodic syncing of files in order to smooth out
	// writes to disk. This option does not provide any persistence guarantee, but
	// is used to avoid latency spikes if the OS automatically decides to write
	// out a large chunk of dirty filesystem buffers.
	BytesPerSync int
}

// DefaultSettings are default settings, suitable for tests.
var DefaultSettings = Settings{
	NoSyncOnClose: false,
	BytesPerSync:  512 * 1024, // 512 KB
}

// New creates a vfs.FS-based sstable storage backend.
func New(fs vfs.FS, dirName string, st Settings) sststorage.Backend {
	return &vfsBackend{
		fs:       fs,
		dirName:  dirName,
		settings: st,
	}
}

type vfsBackend struct {
	fs      vfs.FS
	dirName string

	settings Settings
}

var _ sststorage.Backend = (*vfsBackend)(nil)

// Path is part of the sststorage.Backend interface.
func (b *vfsBackend) Path(fileNum base.FileNum) string {
	return base.MakeFilepath(b.fs, b.dirName, base.FileTypeTable, fileNum)
}

// OpenForReading is part of the sststorage.Backend interface.
func (b *vfsBackend) OpenForReading(fileNum base.FileNum) (sststorage.Readable, error) {
	filename := b.Path(fileNum)
	file, err := b.fs.Open(filename, vfs.RandomReadsOption)
	if err != nil {
		return nil, err
	}
	if osFile, ok := file.(*os.File); ok {
		return newReadable(osFile, b.fs, filename)
	}
	return newGenericReadable(file)
}

// Create is part of the sststorage.Backend interface.
func (b *vfsBackend) Create(fileNum base.FileNum) (sststorage.Writable, error) {
	file, err := b.fs.Create(b.Path(fileNum))
	if err != nil {
		return nil, err
	}
	file = vfs.NewSyncingFile(file, vfs.SyncingFileOptions{
		NoSyncOnClose: b.settings.NoSyncOnClose,
		BytesPerSync:  b.settings.BytesPerSync,
	})
	return newBufferedWritable(file), nil
}

// Remove is part of the sststorage.Backend interface.
func (b *vfsBackend) Remove(fileNum base.FileNum) error {
	return b.fs.Remove(b.Path(fileNum))
}
