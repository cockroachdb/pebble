// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build 386 || amd64p32 || arm || armbe || mips || mipsle || mips64p32 || mips64p32le || ppc || sparc
// +build 386 amd64p32 arm armbe mips mipsle mips64p32 mips64p32le ppc sparc

package manual

import "math"

const (
	// MaxArrayLen is a safe maximum length for slices on this architecture.
	// https://groups.google.com/g/golang-nuts/c/y5OpHR0VEdY/m/Mnq6biJbCwAJ
	MaxArrayLen = math.MaxUint32 / 4
)
