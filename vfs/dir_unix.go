// Copyright 2014 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// +build darwin dragonfly freebsd linux netbsd openbsd solaris

package vfs

import (
	"os"
	"syscall"
)

func (defaultFS) OpenDir(name string) (File, error) {
	return os.OpenFile(name, syscall.O_CLOEXEC, 0)
}
