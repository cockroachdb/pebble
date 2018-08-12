// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble // import "github.com/petermattis/pebble"

import "testing"

func TestIngestLoad(t *testing.T) {
	// TODO(peter): Test loading of metadata.
}

func TestIngestVerify(t *testing.T) {
	// TODO(peter): Test that ingestVerify detects overlapping sstables.
}

func TestIngestLink(t *testing.T) {
	// TODO(peter): Test linking of tables into the DB directory. Test cleanup
	// when one of the tables cannot be linked.
}

func TestIngestMemtableOverlaps(t *testing.T) {
	// TODO(peter): Test detection of memtable overlaps.
}

func TestIngestApply(t *testing.T) {
	// TODO(peter): Test various cases for ingesting sstables into the correct
	// level of the LSM.
}

func TestIngestGlobalSeqNum(t *testing.T) {
	// TODO(peter): Test that the sequence number for entries added via ingestion
	// is correct.
}
