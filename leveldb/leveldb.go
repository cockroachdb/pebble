// Copyright 2012 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package leveldb provides an ordered key/value store.
//
// BUG: This package is incomplete.
package leveldb

// This file is a placeholder for listing import dependencies.

import (
	_ "code.google.com/p/leveldb-go/leveldb/crc"
	_ "code.google.com/p/leveldb-go/leveldb/db"
	_ "code.google.com/p/leveldb-go/leveldb/memdb"
	_ "code.google.com/p/leveldb-go/leveldb/record"
	_ "code.google.com/p/leveldb-go/leveldb/table"
)
