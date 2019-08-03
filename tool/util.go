// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tool

import (
	"io"
	"os"
)

var stdout = io.Writer(os.Stdout)
var stderr = io.Writer(os.Stderr)
var osExit = os.Exit
var dbNum uint64

func nextDBNum() uint64 {
	dbNum++
	return dbNum
}
