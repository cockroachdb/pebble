// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sststorage

import (
	"io"

	"github.com/cockroachdb/pebble/internal/base"
)

// Backend is the interface used by sstable code to read and write sstables.
type Backend interface {
	// OpenForReading opens an existing sstable.
	OpenForReading(fileNum base.FileNum) (Readable, error)

	// Create creates a new sstable and opens it for writing.
	Create(fileNum base.FileNum) (Writable, error)

	// Remove removes an sstable.
	Remove(fileNum base.FileNum) error

	// Path returns an internal, implementation-specific path for an sstable. It
	// is used for informative purposes (e.g. logging).
	Path(fileNum base.FileNum) string
}

// Readable is the handle for an sstable that is open for reading.
type Readable interface {
	io.ReaderAt
	io.Closer

	// Size returns the size of the sstable.
	Size() int64

	// NewReadaheadHandle creates a read-ahead handle which encapsulates
	// read-ahead state. To benefit from read-ahead, ReadaheadHandle.ReadAt must
	// be used (as opposed to Readable.ReadAt).
	//
	// The ReadaheadHandle must be closed before the Readable is closed.
	//
	// Multiple separate ReadaheadHandles can be used.
	NewReadaheadHandle() ReadaheadHandle
}

// ReadaheadHandle is used to perform reads that might benefit from read-ahead.
type ReadaheadHandle interface {
	io.ReaderAt
	io.Closer

	// MaxReadahead configures the implementation to expect large sequential
	// reads. Used to skip any initial read-ahead ramp-up.
	MaxReadahead()

	// RecordCacheHit informs the implementation that we were able to retrieve a
	// block from cache.
	RecordCacheHit(offset, size int64)
}

// Writable is the handle for an sstable that is open for writing.
type Writable interface {
	// Unlike the specification for io.Writer.Write(), the Writable.Write()
	// method *is* allowed to modify the slice passed in, whether temporarily
	// or permanently. Callers of Write() need to take this into account.
	io.Writer
	io.Closer

	Sync() error
}
