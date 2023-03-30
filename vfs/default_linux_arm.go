// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build linux && arm
// +build linux,arm

package vfs

import (
	"os"
	"syscall"

	"github.com/cockroachdb/errors"
	"golang.org/x/sys/unix"
)

func wrapOSFileImpl(f *os.File) File {
	lf := &linuxOnArmFile{File: f, fd: f.Fd()}
	if lf.fd != InvalidFd {
		lf.useSyncRange = isSyncRangeSupported(lf.fd)
	}
	return lf
}

func (defaultFS) OpenDir(name string) (File, error) {
	f, err := os.OpenFile(name, syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &linuxOnArmDir{f}, nil
}

// Assert that linuxOnArmFile and linuxOnArmDir implement vfs.File.
var (
	_ File = (*linuxOnArmDir)(nil)
	_ File = (*linuxOnArmFile)(nil)
)

type linuxOnArmDir struct {
	*os.File
}

func (d *linuxOnArmDir) Prefetch(offset int64, length int64) error      { return nil }
func (d *linuxOnArmDir) Preallocate(offset, length int64) error         { return nil }
func (d *linuxOnArmDir) SyncData() error                                { return d.Sync() }
func (d *linuxOnArmDir) SyncTo(offset int64) (fullSync bool, err error) { return false, nil }

type linuxOnArmFile struct {
	*os.File
	fd           uintptr
	useSyncRange bool
}

func (f *linuxOnArmFile) Prefetch(offset int64, length int64) error {
	_, _, err := unix.Syscall(unix.SYS_READAHEAD, uintptr(f.fd), uintptr(offset), uintptr(length))
	return err
}

func (f *linuxOnArmFile) Preallocate(offset, length int64) error { return nil }

func (f *linuxOnArmFile) SyncData() error {
	// TODO(radu): does arm support unix.Fdatasync?
	return f.Sync()
}

func (f *linuxOnArmFile) SyncTo(int64) (fullSync bool, err error) {
	// syscall.SyncFileRange is not defined form arm (see https://github.com/cockroachdb/pebble/issues/171).
	if err = f.Sync(); err != nil {
		return false, err
	}
	return true, nil
}
