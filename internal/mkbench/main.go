// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import "flag"

const (
	defaultDir        = "data"
	defaultCookedFile = "data.js"
)

func main() {
	var dir, in, out string
	flag.StringVar(&dir, "dir", defaultDir, "path to data directory")
	flag.StringVar(&in, "in", defaultCookedFile, "path to (possibly non-empty) input cooked data file")
	flag.StringVar(&out, "out", defaultCookedFile, "path to output data file")
	flag.Parse()

	// Parse the YCSB benchmark data.
	ParseYCSB(dir, in, out)
}
