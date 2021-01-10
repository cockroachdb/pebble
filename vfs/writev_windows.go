// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// +build windows

package vfs

import "os"

// Will prevent calls to writevFile from writev.go.
const writevSupported = false

// Never called.
func writevFile(f *os.File, bufs [][]byte) (n int, err error) {
	panic("unimplemented")
}
