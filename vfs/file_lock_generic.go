// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// +build !darwin,!dragonfly,!freebsd,!linux,!netbsd,!openbsd,!solaris,!windows

package vfs

import (
	"fmt"
	"io"
	"runtime"
)

func (defFS) Lock(name string) (io.Closer, error) {
	return nil, fmt.Errorf("pebble: file locking is not implemented on %s/%s",
		runtime.GOOS, runtime.GOARCH)
}
