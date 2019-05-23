// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

// Merge merges oldValue and newValue, and returns the merged value. The buf
// parameter can be used to store the newly merged value in order to avoid
// memory allocations. The merge operation must be associative. That is, for
// the values A, B, C:
//
//   Merge(A, Merge(B, C)) == Merge(Merge(A, B), C)
//
// Examples of merge operators are integer addition, list append, and string
// concatenation.
//
// Note that during forward scans merges are processed from newest to oldest
// value, and in reverse scans merges are processed from oldest to newest
// value. DO NOT rely on this ordering: compactions will create partial merge
// results that may not show up in simple tests.
type Merge func(key, oldValue, newValue, buf []byte) []byte

// Merger defines an associative merge operation. The merge operation merges
// two or more values for a single key. A merge operation is requested by
// writing a value using {Batch,DB}.Merge(). The value at that key is merged
// with any existing value. It is valid to Set a value at a key and then Merge
// a new value. Similar to non-merged values, a merged value can be deleted by
// either Delete or DeleteRange.
//
// The merge operation is invoked when a merge value is encountered during a
// read, either during a compaction or during iteration.
type Merger struct {
	Merge Merge

	// Name is the name of the merger.
	//
	// Pebble stores the merger name on disk, and opening a database with a
	// different merger from the one it was created with will result in an error.
	Name string
}

// DefaultMerger is the default implementation of the Merger interface. It
// concatenates the two values to merge.
var DefaultMerger = &Merger{
	Merge: func(key, oldValue, newValue, buf []byte) []byte {
		return append(append(buf, oldValue...), newValue...)
	},

	Name: "pebble.concatenate",
}
