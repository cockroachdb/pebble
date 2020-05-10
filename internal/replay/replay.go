// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package replay implements facilities for replaying writes to a database.
package replay

import (
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/private"
)

// Open opens a database for replaying flushed and ingested tables. It's
// intended for use by the `pebble bench compact` command.
func Open(dirname string, opts *pebble.Options) (*DB, error) {
	d, err := pebble.Open(dirname, opts)
	if err != nil {
		return nil, err
	}
	return &DB{d: d}, nil
}

// A DB is a wrapper around a Pebble database for replaying table-level
// writes to a database. It is not safe for concurrent access.
type DB struct {
	done bool
	d    *pebble.DB
}

// Ingest ingests a set of sstables into the DB.
func (d *DB) Ingest(paths []string) error {
	if d.done {
		panic("replay already finished")
	}
	return d.d.Ingest(paths)
}

// FlushExternal simulates a flush of the sstable at path, linking it directly
// into level zero.
func (d *DB) FlushExternal(path string, meta *manifest.FileMetadata) error {
	if d.done {
		panic("replay already finished")
	}
	return private.FlushExternalTable(d.d, path, meta)
}

// Metrics returns the underlying DB's Metrics.
func (d *DB) Metrics() *pebble.Metrics {
	if d.done {
		panic("replay already finished")
	}
	return d.d.Metrics()
}

// Done finishes a replay, returning the underlying database.  All of the
// *replay.DB's methods except Close will error if called after Done.
func (d *DB) Done() *pebble.DB {
	if d.done {
		panic("replay already finished")
	}
	d.done = true
	return d.d
}

// Close closes a replay and the underlying database.
// If Close is called after Done, Close does nothing.
func (d *DB) Close() error {
	if d.done {
		// Allow clients to defer Close()
		return nil
	}
	d.done = true
	return d.d.Close()
}
