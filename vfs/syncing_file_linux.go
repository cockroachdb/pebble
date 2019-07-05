// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// +build linux,!arm

package vfs

import (
	"sync/atomic"
	"syscall"
)

type syncFileRange func(fd int, off int64, n int64, flags int) (err error)

// sync_file_range depends on both the filesystem, and the broader kernel
// support. In particular, Windows Subsystem for Linux does not support
// sync_file_range, even when used with ext{2,3,4}. syncRangeSmokeTest performs
// a test of of sync_file_range, returning false on ENOSYS, and true otherwise.
func syncRangeSmokeTest(fd uintptr, fn syncFileRange) bool {
	err := fn(int(fd), 0 /* offset */, 0 /* nbytes */, 0 /* flags */)
	return err != syscall.ENOSYS
}

func isSyncRangeSupported(fd uintptr) bool {
	var stat syscall.Statfs_t
	if err := syscall.Fstatfs(int(fd), &stat); err != nil {
		return false
	}

	// Whitelist which filesystems we allow using sync_file_range with as some
	// filesystems treat that syscall as a noop (notably ZFS). A whitelist is
	// used instead of a blacklist in order to have a more graceful failure mode
	// in case a filesystem we haven't tested is encountered. Currently only
	// ext2/3/4 are known to work properly.
	const extMagic = 0xef53
	switch stat.Type {
	case extMagic:
		return syncRangeSmokeTest(fd, syscall.SyncFileRange)
	}
	return false
}

func (f *syncingFile) init() {
	if f.fd == 0 {
		return
	}
	f.useSyncRange = isSyncRangeSupported(f.fd)
	if f.useSyncRange {
		f.syncTo = f.syncToRange
	} else {
		f.syncTo = f.syncToFdatasync
	}
}

func (f *syncingFile) syncData() error {
	if f.fd == 0 {
		return f.File.Sync()
	}
	return syscall.Fdatasync(int(f.fd))
}

func (f *syncingFile) syncToFdatasync(_ int64) error {
	f.ratchetSyncOffset(atomic.LoadInt64(&f.atomic.offset))
	return f.syncData()
}

func (f *syncingFile) syncToRange(offset int64) error {
	const (
		waitBefore = 0x1
		write      = 0x2
		// waitAfter = 0x4
	)

	f.ratchetSyncOffset(offset)
	return syscall.SyncFileRange(int(f.fd), 0, offset, write|waitBefore)
}
