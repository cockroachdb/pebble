// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package randvar

import (
	_ "unsafe" // required by go:linkname
)

// Uint32 returns a lock free uint32 value.
//go:linkname Uint32 runtime.fastrand
func Uint32() uint32

// Uint32N returns a lock free uint32 value in in [0,n).
//go:linkname Uint32N runtime.fastrandn
func Uint32N() uint32
