// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

// LocalSSTables represents a set of sstables on local disk to be ingested into
// the DB, along with their associated blob files.
type LocalSSTables []LocalSST

// TotalFiles returns the total number of files (sstables + blob files)
// represented by this LocalSSTables.
func (l LocalSSTables) TotalFiles() int {
	total := len(l)
	for _, sst := range l {
		total += len(sst.BlobPaths)
	}
	return total
}

// LocalSST represents a single sstable on local disk to be ingested into
// the DB, along with its associated blob files.
type LocalSST struct {
	Path      string
	BlobPaths []string
}
