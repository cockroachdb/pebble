// Copyright 2012 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pebble // import "github.com/petermattis/pebble"

// Ingest ingests a set of sstables into the DB. Ingestion of the files is
// atomic and semantically equivalent to creating a single batch containing all
// of the mutations in the sstables. Ingestion may require the memtable to be
// flushed. The ingested sstable files are moved into the DB and must reside on
// the same filesystem as the DB. Sstables can be created for ingestion using
// sstable.Writer.
func (d *DB) Ingest(paths []string) error {
	panic("pebble.DB: Ingest unimplemented")

	// TODO(peter): The specified paths are ingested atomically into the DB at
	// the highest level in the LSM for which they don't overlap any existing
	// files in the level. If the sstable overlaps existing keys in the DB, it is
	// assigned the current seqnum. Otherwise, it is given the seqnum 0. The
	// global seqnum in the ingested file is overwritten. (It would be better to
	// store the global seqnum externally from the file so that the file doesn't
	// have to be modified). If the ingested file overlaps with keys in the
	// memtable, the memtable is flushed.
	//
	// 1. Preventing ingest files from being garbage collected.
	//
	// 2. Block user writes.
	//
	// 3. Flush the memtable (if necessary).
	//
	// 4. Assign level and global seqnum for ingested files.
	//
	// 5. Copy/move the ingested files into the DB.
	//
	// 6. Log and apply the version edit.
}
