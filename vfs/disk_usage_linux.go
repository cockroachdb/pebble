// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// +build linux

package vfs

import "golang.org/x/sys/unix"

func (defaultFS) GetFreeSpace(path string) (uint64, error) {
	stat := unix.Statfs_t{}
	if err := unix.Statfs(path, &stat); err != nil {
		return 0, err
	}

	// We use stat.Frsize here rather than stat.Bsize because on
	// Linux Bfree is in Frsize units.
	//
	// On most filesystems Frsize and Bsize will be set to the
	// same value, but on some filesystems bsize returns the
	// "optimal transfer block size"[1] which may be different
	// (typically larger) than the actual block size.
	//
	// This confusion is cleared up in the statvfs[2] libc function,
	// but the statfs system call used above varies across
	// platforms.
	//
	// Frsize is used by GNU coreutils and other libraries, so
	// this also helps ensure that we get the same results as one
	// would get if they ran `df` on the given path.
	//
	// [1] https://man7.org/linux/man-pages/man2/statfs.2.html
	// [2] https://man7.org/linux/man-pages/man3/statvfs.3.html
	return uint64(stat.Frsize) * stat.Bfree, nil
}
