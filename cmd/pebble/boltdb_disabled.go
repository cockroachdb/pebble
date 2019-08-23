// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// +build !boltdb

package main

import "log"

func newBoltDB(dir string) DB {
	log.Fatalf("pebble not compiled with BoltDB support: recompile with \"-tags boltdb\"\n")
	return nil
}
