// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble // import "github.com/petermattis/pebble"

// Ingest ingests a set of sstables into the DB. Ingestion of the files is
// atomic and semantically equivalent to creating a single batch containing all
// of the mutations in the sstables. Ingestion may require the memtable to be
// flushed. The ingested sstable files are moved into the DB and must reside on
// the same filesystem as the DB. Sstables can be created for ingestion using
// sstable.Writer.
func (d *DB) Ingest(paths []string) error {
	// TODO(peter): The specified paths are ingested atomically into the DB at
	// the highest level in the LSM for which they don't overlap any existing
	// files in the level. If the sstable overlaps existing keys in the DB, it is
	// assigned the current seqnum. Otherwise, it is given the seqnum 0. The
	// global seqnum in the ingested file is overwritten. (It would be better to
	// store the global seqnum externally from the file so that the file doesn't
	// have to be modified). If the ingested file overlaps with keys in the
	// memtable, the memtable is flushed.
	//
	// 1. Load the metadata for the sstables. Determine the largest and smallest
	//    key and seqNum.
	//
	// 2. Add the sstables to DB.mu.compact.pendingOutputs to prevent them from
	//    being garbage collected.
	//
	// 3. Hard link the sstables into the DB directory. Since the sstables aren't
	//    referenced by a version, they won't be used. If the hard linking fails
	//    because the files reside on a different filesystem, we can undo our
	//    work and return an error.

	prepare := func() {
		// TODO(peter):
		//
		// 1. If the sstables overlap keys in the memtable, flush the memtable.
	}

	apply := func(seqNum uint64) {
		// TODO(peter):
		//
		// 1. Assign level and seqNum for the ingested files.
		//
		// 2. Log and apply the version edit.
	}

	d.commit.AllocateSeqNum(prepare, apply)

	panic("pebble.DB: Ingest unimplemented")
}
