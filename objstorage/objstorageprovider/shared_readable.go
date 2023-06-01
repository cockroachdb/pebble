// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorageprovider

import (
	"context"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider/sharedcache"
	"github.com/cockroachdb/pebble/objstorage/shared"
)

// sharedReadable is a very simple implementation of Readable on top of the
// ReadCloser returned by shared.Storage.CreateObject.
type sharedReadable struct {
	objReader shared.ObjectReader
	size      int64
	fileNum   base.DiskFileNum
	provider  *provider
}

var _ objstorage.Readable = (*sharedReadable)(nil)

func (p *provider) newSharedReadable(
	objReader shared.ObjectReader, size int64, fileNum base.DiskFileNum,
) *sharedReadable {
	return &sharedReadable{
		objReader: objReader,
		size:      size,
		fileNum:   fileNum,
		provider:  p,
	}
}

// ReadAt is part of the objstorage.Readable interface.
func (r *sharedReadable) ReadAt(ctx context.Context, p []byte, offset int64) error {
	return r.readInternal(ctx, p, offset, false /* forCompaction */)
}

// readInternal performs a read for the object, using the cache when
// appropriate.
func (r *sharedReadable) readInternal(
	ctx context.Context, p []byte, offset int64, forCompaction bool,
) error {
	if cache := r.provider.shared.cache; cache != nil {
		flags := sharedcache.ReadFlags{
			// Don't add data to the cache if this read is for a compaction.
			ReadOnly: forCompaction,
		}
		return r.provider.shared.cache.ReadAt(ctx, r.fileNum, p, offset, r.objReader, flags)
	}
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
	rh := &sharedReadHandle{readable: r}
	rh.readahead.state = makeReadaheadState()
	return rh
}

type sharedReadHandle struct {
	readable  *sharedReadable
	readahead struct {
		state  readaheadState
		data   []byte
		offset int64
	}
	forCompaction bool
}

var _ objstorage.ReadHandle = (*sharedReadHandle)(nil)

// ReadAt is part of the objstorage.ReadHandle interface.
func (r *sharedReadHandle) ReadAt(ctx context.Context, p []byte, offset int64) error {
	readaheadSize := r.maybeReadahead(offset, len(p))

	// Check if we already have the data from a previous read-ahead.
	if rhSize := int64(len(r.readahead.data)); rhSize > 0 {
		if r.readahead.offset <= offset && r.readahead.offset+rhSize > offset {
			n := copy(p, r.readahead.data[offset-r.readahead.offset:])
			if n == len(p) {
				// All data was available.
				return nil
			}
			// Use the data that we had and do a shorter read.
			offset += int64(n)
			p = p[n:]
			readaheadSize -= n
		}
	}

	if readaheadSize > len(p) {
		r.readahead.offset = offset
		// TODO(radu): we need to somehow account for this memory.
		if cap(r.readahead.data) >= readaheadSize {
			r.readahead.data = r.readahead.data[:readaheadSize]
		} else {
			r.readahead.data = make([]byte, readaheadSize)
		}

		if err := r.readable.readInternal(ctx, r.readahead.data, offset, r.forCompaction); err != nil {
			// Make sure we don't treat the data as valid next time.
			r.readahead.data = r.readahead.data[:0]
			return err
		}
		copy(p, r.readahead.data)
		return nil
	}

	return r.readable.readInternal(ctx, p, offset, r.forCompaction)
}

func (r *sharedReadHandle) maybeReadahead(offset int64, len int) int {
	// TODO(radu): maxReadaheadSize is only 256KB, we probably want 1MB+.
	if r.forCompaction {
		return maxReadaheadSize
	}
	return int(r.readahead.state.maybeReadahead(offset, int64(len)))
}

// Close is part of the objstorage.ReadHandle interface.
func (r *sharedReadHandle) Close() error {
	r.readable = nil
	r.readahead.data = nil
	return nil
}

// SetupForCompaction is part of the objstorage.ReadHandle interface.
func (r *sharedReadHandle) SetupForCompaction() {
	r.forCompaction = true
}

// RecordCacheHit is part of the objstorage.ReadHandle interface.
func (r *sharedReadHandle) RecordCacheHit(_ context.Context, offset, size int64) {
	if !r.forCompaction {
		r.readahead.state.recordCacheHit(offset, size)
	}
}
