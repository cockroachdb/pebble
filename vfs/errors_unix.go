// Copyright 2014 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// +build darwin dragonfly freebsd linux

package vfs

import (
	"errors"

	"golang.org/x/sys/unix"
)

func IsNoSpaceError(err error) bool {
	return errors.Is(err, unix.ENOSPC)
}
