// Copyright 2014 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build darwin || dragonfly || freebsd || (linux && arm) || netbsd || openbsd || solaris
// +build darwin dragonfly freebsd linux,arm netbsd openbsd solaris

package vfs

import (
	"os"
	"syscall"

	"github.com/cockroachdb/errors"
)

func wrapOSFile(osFile *os.File) File {
	return unixFile{osFile}
}

func (defaultFS) OpenDir(name string) (File, error) {
	f, err := os.OpenFile(name, syscall.O_CLOEXEC, 0)
	return unixFile{f}, errors.WithStack(err)
}

// Assert that unixFile implements vfs.File.
var _ File = unixFile{}

type unixFile struct {
	*os.File
}

func (f unixFile) Attributes() FileAttributes {
	// No support for preallocation, fsyncdata or sync_file_range.
	return FileAttributes{}
}

func (f unixFile) Preallocate(offset, length int64) error {
	// It is ok for correctness to no-op file preallocation. WAL recycling is the
	// more important mechanism for WAL sync performance and it doesn't rely on
	// fallocate or posix_fallocate in order to be effective.
	return nil
}

func (f unixFile) SyncData() error {
	return f.Sync()
}

func (f unixFile) SyncTo(int64) error {
	return f.Sync()
}
