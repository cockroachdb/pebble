// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build msan

package compression

import (
	"runtime"
	"unsafe"
)

// msanWrite is used to inform MemorySanitizer that the memory in p was written
// to; this is necessary for some compression algorithms which use assembly code.
func msanWrite(p []byte) {
	if len(p) > 0 {
		runtime.MSanWrite(unsafe.Pointer(&p[0]), len(p))
	}
}
