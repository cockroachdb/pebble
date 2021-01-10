// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// +build dragonfly freebsd linux netbsd openbsd

package vfs

import (
	"syscall"
	"unsafe"
)

// Copied from src/internal/poll/fd_writev_unix.go.
func writev(fd int, iovecs []syscall.Iovec) (uintptr, error) {
	var (
		r uintptr
		e syscall.Errno
	)
	for {
		r, _, e = syscall.Syscall(syscall.SYS_WRITEV, uintptr(fd), uintptr(unsafe.Pointer(&iovecs[0])), uintptr(len(iovecs)))
		if e != syscall.EINTR {
			break
		}
		// Repeat syscall on EINTR. If any bytes had already been written, a
		// partial success would have been returned instead of the error.
	}
	if e != 0 {
		return r, e
	}
	return r, nil
}
