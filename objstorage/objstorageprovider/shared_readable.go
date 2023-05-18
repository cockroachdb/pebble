// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorageprovider

import (
	"context"

	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/shared"
)

// sharedReadable is a very simple implementation of Readable on top of the
// ReadCloser returned by shared.Storage.CreateObject.
type sharedReadable struct {
	objReader shared.ObjectReader
	size      int64
}

var _ objstorage.Readable = (*sharedReadable)(nil)

func newSharedReadable(objReader shared.ObjectReader, size int64) *sharedReadable {
	return &sharedReadable{
		objReader: objReader,
		size:      size,
	}
}

func (r *sharedReadable) ReadAt(ctx context.Context, p []byte, offset int64) error {
	return r.objReader.ReadAt(ctx, p, offset)
}

func (r *sharedReadable) Close() error {
	defer func() { r.objReader = nil }()
	return r.objReader.Close()
}

func (r *sharedReadable) Size() int64 {
	return r.size
}

func (r *sharedReadable) NewReadHandle(_ context.Context) objstorage.ReadHandle {
	// TODO(radu): use a pool.
	return &sharedReadHandle{readable: r}
}

type sharedReadHandle struct {
	readable *sharedReadable
}

var _ objstorage.ReadHandle = (*sharedReadHandle)(nil)

func (r *sharedReadHandle) ReadAt(ctx context.Context, p []byte, offset int64) error {
	return r.readable.ReadAt(ctx, p, offset)
}

func (r *sharedReadHandle) Close() error {
	r.readable = nil
	return nil
}

func (r *sharedReadHandle) SetupForCompaction() {}

func (r *sharedReadHandle) RecordCacheHit(_ context.Context, offset, size int64) {}
