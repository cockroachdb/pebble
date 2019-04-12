// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// +build linux

package storage

import (
	"os"
	"syscall"
)

func isSyncRangeSupported(fd int) bool {
	var stat syscall.Statfs_t
	if err := syscall.Fstatfs(fd, &stat); err != nil {
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
		return true
	}
	return false
}

func (f *syncingFile) init() {
	t, ok := f.File.(*os.File)
	if !ok {
		return
	}
	f.fd = int(t.Fd())
	f.useSyncRange = isSyncRangeSupported(f.fd)
	if f.useSyncRange {
		f.syncTo = f.syncToRange
	} else {
		f.syncTo = f.syncToFdatasync
	}
}

func (f *syncingFile) syncToFdatasync(_ int64) error {
	f.syncOffset = f.offset
	return syscall.Fdatasync(f.fd)
}

func (f *syncingFile) syncToRange(offset int64) error {
	const (
		// waitAfter = 0x1
		write      = 0x2
		waitBefore = 0x4
	)

	f.syncOffset = offset
	return syscall.SyncFileRange(f.fd, 0, offset, write|waitBefore)
}
