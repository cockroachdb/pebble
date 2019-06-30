// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// +build !rocksdb

package main

import "log"

func newRocksDB(dir string) DB {
	log.Fatalf("pebble not compiled with RocksDB support: recompile with \"-tags rocksdb\"\n")
	return nil
}
