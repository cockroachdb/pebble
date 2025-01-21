// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/vfs"
)

func main() {
	// The test fixture code opens "testdata/h.txt" so we have to move up.
	dir, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	if filepath.Base(dir) != "testdata" {
		panic("This program must be run from sstable/testdata")
	}
	if err := os.Chdir(filepath.Dir(dir)); err != nil {
		panic(err)
	}
	for _, fixture := range sstable.TestFixtures {
		fmt.Printf("Generating %s\n", fixture.Filename)
		if err := fixture.Build(vfs.Default, filepath.Join("testdata", fixture.Filename)); err != nil {
			panic(err)
		}
	}
}
