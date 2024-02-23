// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package lsmview

// Data encodes the data necessary to generate an LSM diagram.
type Data struct {
	// LSM levels in newest-to-oldest order.
	Levels []Level `json:"levels"`

	// Keys contains all table boundary keys, in sorted key order.
	Keys []string `json:"keys"`
}

// Level contains the data for a level of the LSM.
type Level struct {
	Name   string  `json:"level_name"`
	Tables []Table `json:"tables"`
}

// Table contains the data for a table.
type Table struct {
	Label string `json:"label"`
	Size  uint64 `json:"size"`
	// SmallestKey, LargestKey are indexes into the Data.Keys list.
	SmallestKey int      `json:"smallest_key"`
	LargestKey  int      `json:"largest_key"`
	Details     []string `json:"details"`
}
