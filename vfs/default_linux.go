// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build linux && !arm
// +build linux,!arm

package vfs

import (
	"os"
	"syscall"

	"github.com/cockroachdb/errors"
	"golang.org/x/sys/unix"
)

func wrapOSFile(f *os.File) File {
	lf := &linuxFile{File: f, fd: f.Fd()}
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
	return &linuxDir{f}, nil
}

// Assert that linuxFile and linuxDir implement vfs.File.
var (
	_ File = (*linuxDir)(nil)
	_ File = (*linuxFile)(nil)
)

type linuxDir struct {
	*os.File
}

func (d *linuxDir) Attributes() FileAttributes             { return FileAttributes{CanSyncTo: false} }
func (d *linuxDir) Preallocate(offset, length int64) error { return nil }
func (d *linuxDir) SyncData() error                        { return d.Sync() }
func (d *linuxDir) SyncTo(offset int64) error              { return nil }

type linuxFile struct {
	*os.File
	fd           uintptr
	useSyncRange bool
}

func (f *linuxFile) Attributes() FileAttributes {
	return FileAttributes{CanSyncTo: f.useSyncRange}
}

func (f *linuxFile) Preallocate(offset, length int64) error {
	return preallocExtend(f.fd, offset, length)
}

func (f *linuxFile) SyncData() error {
	return unix.Fdatasync(int(f.fd))
}

func (f *linuxFile) SyncTo(offset int64) error {
	if !f.useSyncRange {
		return unix.Fdatasync(int(f.fd))
	}

	const (
		waitBefore = 0x1
		write      = 0x2
		// waitAfter = 0x4
	)

	// By specifying write|waitBefore for the flags, we're instructing
	// SyncFileRange to a) wait for any outstanding data being written to finish,
	// and b) to queue any other dirty data blocks in the range [0,offset] for
	// writing. The actual writing of this data will occur asynchronously. The
	// use of `waitBefore` is to limit how much dirty data is allowed to
	// accumulate. Linux sometimes behaves poorly when a large amount of dirty
	// data accumulates, impacting other I/O operations.
	return unix.SyncFileRange(int(f.fd), 0, offset, write|waitBefore)
}

type syncFileRange func(fd int, off int64, n int64, flags int) (err error)

// sync_file_range depends on both the filesystem, and the broader kernel
// support. In particular, Windows Subsystem for Linux does not support
// sync_file_range, even when used with ext{2,3,4}. syncRangeSmokeTest performs
// a test of of sync_file_range, returning false on ENOSYS, and true otherwise.
func syncRangeSmokeTest(fd uintptr, syncFn syncFileRange) bool {
	err := syncFn(int(fd), 0 /* offset */, 0 /* nbytes */, 0 /* flags */)
	return err != unix.ENOSYS
}

func isSyncRangeSupported(fd uintptr) bool {
	var stat unix.Statfs_t
	if err := unix.Fstatfs(int(fd), &stat); err != nil {
		return false
	}

	// Allowlist which filesystems we allow using sync_file_range with as some
	// filesystems treat that syscall as a noop (notably ZFS). A allowlist is
	// used instead of a denylist in order to have a more graceful failure mode
	// in case a filesystem we haven't tested is encountered. Currently only
	// ext2/3/4 are known to work properly.
	const extMagic = 0xef53
	switch stat.Type {
	case extMagic:
		return syncRangeSmokeTest(fd, unix.SyncFileRange)
	}
	return false
}
