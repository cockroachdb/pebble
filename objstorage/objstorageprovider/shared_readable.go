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

	// rh is used for direct ReadAt calls without a read handle.
	rh sharedReadHandle
}

var _ objstorage.Readable = (*sharedReadable)(nil)

func newSharedReadable(storage shared.Storage, objName string, size int64) *sharedReadable {
	r := &sharedReadable{
		storage: storage,
		objName: objName,
		size:    size,
	}
	r.rh.readable = r
	return r
}

func (r *sharedReadable) ReadAt(ctx context.Context, p []byte, offset int64) (n int, err error) {
	return r.rh.ReadAt(ctx, p, offset)
}

func (r *sharedReadable) Close() error {
	err := r.rh.Close()
	r.storage = nil
	return err
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

func (r *sharedReadHandle) ReadAt(_ context.Context, p []byte, offset int64) (n int, err error) {
	// See if this continues the previous read so that we can reuse the last reader.
	if r.lastReader == nil || r.lastOffset != offset {
		// We need to create a new reader.
		if r.lastReader != nil {
			if err := r.lastReader.Close(); err != nil {
				return 0, err
			}
			r.lastReader = nil
		}
		reader, _, err := r.readable.storage.ReadObjectAt(r.readable.objName, offset)
		if err != nil {
			return 0, err
		}
		r.lastReader = reader
		r.lastOffset = offset
	}
	n, err = io.ReadFull(r.lastReader, p)
	r.lastOffset += int64(n)
	return n, err
}

func (r *sharedReadHandle) Close() error {
	var err error
	if r.lastReader != nil {
		err = r.lastReader.Close()
		r.lastReader = nil
	}
	r.readable = nil
	return err
}

func (r *sharedReadHandle) MaxReadahead() {}

func (r *sharedReadHandle) RecordCacheHit(_ context.Context, offset, size int64) {}
