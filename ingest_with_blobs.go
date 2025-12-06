// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"

	"github.com/cockroachdb/errors"
)

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
// Any blob files provided must have been written at the time of writing
// the sst, meaning they contain values solely for the sstable at Path.
type LocalSST struct {
	Path      string
	BlobPaths []string
}

// IngestLocal ingests the provided local sstables into the DB.
// If a valid exciseSpan is provided, the ingested sstables will have any keys
// in that span excised (removed) during ingestion.
func (d *DB) IngestLocal(
	ctx context.Context, localSSTs LocalSSTables, exciseSpan KeyRange,
) (IngestOperationStats, error) {
	if err := d.closed.Load(); err != nil {
		panic(err)
	}
	if d.opts.ReadOnly {
		return IngestOperationStats{}, ErrReadOnly
	}

	if exciseSpan.Valid() {
		// Excise is only supported on prefix keys.
		if d.opts.Comparer.Split(exciseSpan.Start) != len(exciseSpan.Start) {
			return IngestOperationStats{}, errors.New("IngestLocal called with suffixed start key")
		}
		if d.opts.Comparer.Split(exciseSpan.End) != len(exciseSpan.End) {
			return IngestOperationStats{}, errors.New("IngestLocal called with suffixed end key")
		}
	}

	args := ingestArgs{
		Local:              localSSTs,
		ExciseSpan:         exciseSpan,
		ExciseBoundsPolicy: tightExciseBounds,
	}

	return d.ingest(ctx, args)
}
