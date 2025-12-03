// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorageprovider

import (
	"context"
	"sync"

	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/vfs"
)

// newColdReadableWithHotMeta returns an objstorage.Readable that reads the main
// data from the wrapped "cold storage" readable, and the metadata from a
// separate file in a local filesystem. The separate file contains a suffix of
// the full file, starting at metaStartOffset.
func newColdReadableWithHotMeta(
	cold objstorage.Readable, metaFS vfs.FS, metaFilepath string, metaStartOffset int64,
) *coldReadableWithHotMeta {
	r := &coldReadableWithHotMeta{
		cold: cold,
	}
	r.meta.fs = metaFS
	r.meta.filepath = metaFilepath
	r.meta.startOffset = metaStartOffset
	return r
}

type coldReadableWithHotMeta struct {
	cold objstorage.Readable

	meta struct {
		fs          vfs.FS
		filepath    string
		startOffset int64
		once        struct {
			sync.Once
			file vfs.File
			err  error
		}
	}
}

var _ objstorage.Readable = (*coldReadableWithHotMeta)(nil)

// readMetaAt reads from the metadata file at the given offset.
func (r *coldReadableWithHotMeta) readMetaAt(p []byte, off int64) error {
	r.meta.once.Do(func() {
		r.meta.once.file, r.meta.once.err = r.meta.fs.Open(r.meta.filepath, vfs.RandomReadsOption)
	})
	if r.meta.once.err != nil {
		return r.meta.once.err
	}
	_, err := r.meta.once.file.ReadAt(p, off)
	return err
}

// ReadAt is part of the objstorage.Readable interface.
func (r *coldReadableWithHotMeta) ReadAt(ctx context.Context, p []byte, off int64) error {
	// We don't expect reads that span both regions, but in that case it is
	// correct to read it all from the cold file (which contains all the data).
	if off < r.meta.startOffset {
		return r.cold.ReadAt(ctx, p, off)
	}
	return r.readMetaAt(p, off-r.meta.startOffset)
}

// Close is part of the objstorage.Readable interface.
func (r *coldReadableWithHotMeta) Close() error {
	err := r.cold.Close()
	if r.meta.once.file != nil {
		err = firstError(err, r.meta.once.file.Close())
		r.meta.once.file = nil
	}
	return err
}

// Size is part of the objstorage.Readable interface.
func (r *coldReadableWithHotMeta) Size() int64 {
	return r.cold.Size()
}

// NewReadHandle is part of the objstorage.Readable interface.
func (r *coldReadableWithHotMeta) NewReadHandle(
	readBeforeSize objstorage.ReadBeforeSize,
) objstorage.ReadHandle {
	// The readBeforeSize is used to optimize reading the metadata suffix of a
	// file, for cases where small reads are expensive. In our case, the metadata
	// is specifically in a hot tier file, for which small reads are cheap (so we
	// ignore readBeforeSize, like fileReadable).
	//
	// Since we are using the local file for metadata, we pass NoReadBefore for
	// the cold file.
	return &coldReadHandle{
		r:    r,
		cold: r.cold.NewReadHandle(objstorage.NoReadBefore),
	}
}

type coldReadHandle struct {
	r    *coldReadableWithHotMeta
	cold objstorage.ReadHandle
}

var _ objstorage.ReadHandle = (*coldReadHandle)(nil)

// ReadAt is part of the objstorage.ReadHandle interface.
func (rh *coldReadHandle) ReadAt(ctx context.Context, p []byte, off int64) error {
	if off < rh.r.meta.startOffset {
		// Read from cold storage only.
		return rh.cold.ReadAt(ctx, p, off)
	}
	// Read from metadata only.
	return rh.r.readMetaAt(p, off-rh.r.meta.startOffset)
}

// Close is part of the objstorage.ReadHandle interface.
func (rh *coldReadHandle) Close() error {
	return rh.cold.Close()
}

// SetupForCompaction is part of the objstorage.ReadHandle interface.
func (rh *coldReadHandle) SetupForCompaction() {
	rh.cold.SetupForCompaction()
}

// RecordCacheHit is part of the objstorage.ReadHandle interface.
func (rh *coldReadHandle) RecordCacheHit(ctx context.Context, offset, size int64) {
	// We don't use prefetching for the metadata portion, so we only need to
	// report cache hits to the cold readable.
	if offset < rh.r.meta.startOffset {
		rh.cold.RecordCacheHit(ctx, offset, min(size, rh.r.meta.startOffset-offset))
	}
}
