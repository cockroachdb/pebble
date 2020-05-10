// Copyright 2014 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// +build darwin linux openbsd dragonfly

package vfs

import (
	"golang.org/x/sys/unix"
)

// TODO: Does path have to be a file? Or could it be a directory?
// Currently we pass it a directory.
func (defaultFS) GetFreeSpace(path string) (uint64, error) {
	stat := unix.Statfs_t{}
	if err := unix.Statfs(path, &stat); err != nil {
		return 0, err
	}
	return uint64(stat.Bsize) * stat.Bfree, nil
}
