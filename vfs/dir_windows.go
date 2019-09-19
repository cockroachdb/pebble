// Copyright 2014 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// +build windows

package vfs

import (
	"os"
	"syscall"
)

type windowsDir struct {
	File
}

func (windowsDir) Sync() error {
	// Silently ignore Sync() on Windows. This is the same behavior as
	// RocksDB. See port/win/io_win.cc:WinDirectory::Fsync().
	return nil
}

func (defaultFS) OpenDir(name string) (File, error) {
	f, err := os.OpenFile(name, syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return windowsDir{f}, nil
}
