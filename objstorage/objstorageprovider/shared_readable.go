// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorageprovider

import (
	"context"
	"io"

	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/shared"
)

// sharedReadable is a very simple implementation of Readable on top of the
// ReadCloser returned by shared.Storage.CreateObject.
type sharedReadable struct {
	storage shared.Storage
	objName string
	size    int64
}

var _ objstorage.Readable = (*sharedReadable)(nil)

func newSharedReadable(storage shared.Storage, objName string, size int64) *sharedReadable {
	return &sharedReadable{
		storage: storage,
		objName: objName,
		size:    size,
	}
}

func (r *sharedReadable) ReadAt(ctx context.Context, p []byte, offset int64) error {
	rh := sharedReadHandle{
		readable: r,
	}
	defer func() { _ = rh.Close() }()
	return rh.ReadAt(ctx, p, offset)
}

func (r *sharedReadable) Close() error {
	r.storage = nil
	return nil
}

func (r *sharedReadable) Size() int64 {
	return r.size
}

func (r *sharedReadable) NewReadHandle(_ context.Context) objstorage.ReadHandle {
	// TODO(radu): use a pool.
	return &sharedReadHandle{readable: r}
}

type sharedReadHandle struct {
	readable   *sharedReadable
	lastReader io.ReadCloser
	lastOffset int64
}

var _ objstorage.ReadHandle = (*sharedReadHandle)(nil)

func (r *sharedReadHandle) ReadAt(_ context.Context, p []byte, offset int64) error {
	// See if this continues the previous read so that we can reuse the last reader.
	if r.lastReader == nil || r.lastOffset != offset {
		// We need to create a new reader.
		r.closeLastReader()
		reader, _, err := r.readable.storage.ReadObjectAt(r.readable.objName, offset)
		if err != nil {
			return err
		}
		r.lastReader = reader
		r.lastOffset = offset
	}
	for n := 0; n < len(p); {
		nn, err := r.lastReader.Read(p[n:])
		n += nn
		if err != nil {
			// Don't rely on the reader again after hitting an error; some
			// implementations don't correctly keep track of the current position in
			// error cases.
			r.closeLastReader()
			return err
		}
	}
	r.lastOffset += int64(len(p))
	return nil
}

func (r *sharedReadHandle) closeLastReader() {
	if r.lastReader != nil {
		_ = r.lastReader.Close()
		r.lastReader = nil
	}
}

func (r *sharedReadHandle) Close() error {
	r.closeLastReader()
	r.readable = nil
	return nil
}

func (r *sharedReadHandle) MaxReadahead() {}

func (r *sharedReadHandle) RecordCacheHit(_ context.Context, offset, size int64) {}
