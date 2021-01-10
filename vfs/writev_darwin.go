// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// +build darwin

package vfs

import (
	"syscall"
	_ "unsafe" // required by go:linkname
)

// Copied from src/internal/poll/fd_writev_darwin.go.
//go:linkname writev syscall.writev
//go:noescape
func writev(fd int, iovecs []syscall.Iovec) (n uintptr, err error)
