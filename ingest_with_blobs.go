// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/objstorage"
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

// closeReadables closes all readables in the slice, combining any errors.
func closeReadables(readables []objstorage.Readable) error {
	var errs error
	for _, r := range readables {
		errs = errors.CombineErrors(errs, r.Close())
	}
	return errs
}

func createBlobReadables(
	ctx context.Context, opts *Options, blobPaths []string,
) ([]objstorage.Readable, error) {
	readables := make([]objstorage.Readable, 0, len(blobPaths))
	for _, path := range blobPaths {
		f, err := opts.FS.Open(path)
		if err != nil {
			// Close any readables we've already created before returning the error.
			return nil, errors.CombineErrors(err, closeReadables(readables))
		}

		readable, err := objstorage.NewSimpleReadable(f)
		if err != nil {
			// Close the file and any readables we've already created.
			closeErr := errors.CombineErrors(f.Close(), closeReadables(readables))
			return nil, errors.CombineErrors(err, closeErr)
		}
		readables = append(readables, readable)
	}
	return readables, nil
}
