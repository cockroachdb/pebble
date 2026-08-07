// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build solaris

package vfs

import "golang.org/x/sys/unix"

func (defaultFS) GetDiskUsage(path string) (DiskUsage, error) {
	stat := unix.Statvfs_t{}
	if err := unix.Statvfs(path, &stat); err != nil {
		return DiskUsage{}, err
	}

	freeBytes := stat.Frsize * stat.Bfree
	availBytes := stat.Frsize * stat.Bavail
	totalBytes := stat.Frsize * stat.Blocks
	return DiskUsage{
		AvailBytes: availBytes,
		TotalBytes: totalBytes,
		UsedBytes:  totalBytes - freeBytes,
	}, nil
}
