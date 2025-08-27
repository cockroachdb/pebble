// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorageprovider

import (
	"context"
	"io"
	"sync"

	"github.com/cockroachdb/pebble/v2/internal/base"
	"github.com/cockroachdb/pebble/v2/objstorage"
	"github.com/cockroachdb/pebble/v2/objstorage/objstorageprovider/sharedcache"
	"github.com/cockroachdb/pebble/v2/objstorage/remote"
)

// NewRemoteReadable creates an objstorage.Readable out of a remote.ObjectReader.
func NewRemoteReadable(objReader remote.ObjectReader, size int64) objstorage.Readable {
	return &remoteReadable{
		objReader: objReader,
		size:      size,
	}
}

const remoteMaxReadaheadSize = 1024 * 1024 /* 1MB */

// Number of concurrent compactions is bounded and significantly lower than
// the number of concurrent queries, and compactions consume reads from a few
// levels, so there is no risk of high memory usage due to a higher readahead
// size. So set this higher than remoteMaxReadaheadSize
const remoteReadaheadSizeForCompaction = 8 * 1024 * 1024 /* 8MB */

// remoteReadable is a very simple implementation of Readable on top of the
// remote.ObjectReader returned by remote.Storage.ReadObject. It is stateless
// and can be called concurrently.
type remoteReadable struct {
	objReader     remote.ObjectReader
	size          int64
	fileNum       base.DiskFileNum
	cache         *sharedcache.Cache
	errIsNotExist func(error) bool
}

var _ objstorage.Readable = (*remoteReadable)(nil)

func (p *provider) newRemoteReadable(
	objReader remote.ObjectReader,
	size int64,
	fileNum base.DiskFileNum,
	errIsNotExist func(error) bool,
) *remoteReadable {
	return &remoteReadable{
		objReader:     objReader,
		size:          size,
		fileNum:       fileNum,
		cache:         p.remote.cache,
		errIsNotExist: errIsNotExist,
	}
}

// ReadAt is part of the objstorage.Readable interface.
func (r *remoteReadable) ReadAt(ctx context.Context, p []byte, offset int64) error {
	return r.readInternal(ctx, p, offset, false /* forCompaction */)
}

// readInternal performs a read for the object, using the cache when
// appropriate.
func (r *remoteReadable) readInternal(
	ctx context.Context, p []byte, offset int64, forCompaction bool,
) error {
	var err error
	if r.cache != nil {
		flags := sharedcache.ReadFlags{
			// Don't add data to the cache if this read is for a compaction.
			ReadOnly: forCompaction,
		}
		err = r.cache.ReadAt(ctx, r.fileNum, p, offset, r.objReader, r.size, flags)
	} else {
		err = r.objReader.ReadAt(ctx, p, offset)
	}
	if err != nil && r.errIsNotExist(err) {
		// If a file goes missing, we consider this a corruption error.
		err = base.MarkCorruptionError(err)
	}
	return err
}

func (r *remoteReadable) Close() error {
	defer func() { r.objReader = nil }()
	return r.objReader.Close()
}

func (r *remoteReadable) Size() int64 {
	return r.size
}

// TODO(sumeer): both readBeforeSize and ReadHandle.SetupForCompaction are
// initial configuration of a ReadHandle. So they should both be passed as
// Options to NewReadHandle. But currently the latter is a separate method.
// This is because of how the sstable.Reader calls setupForCompaction on the
// iterators after they are constructed. Consider fixing this oddity.

func (r *remoteReadable) NewReadHandle(
	readBeforeSize objstorage.ReadBeforeSize,
) objstorage.ReadHandle {
	rh := remoteReadHandlePool.Get().(*remoteReadHandle)
	*rh = remoteReadHandle{readable: r, readBeforeSize: readBeforeSize, buffered: rh.buffered}
	rh.readAheadState = makeReadaheadState(remoteMaxReadaheadSize)
	return rh
}

// TODO(sumeer): add test for remoteReadHandle.

// remoteReadHandle supports doing larger reads, and buffering the additional
// data, to serve future reads. It is not thread-safe. There are two kinds of
// larger reads (a) read-ahead (for sequential data reads), (b) read-before,
// for non-data reads.
//
// For both (a) and (b), the goal is to reduce the number of reads since
// remote read latency and cost can be high. We have to balance this with
// buffers consuming too much memory, since there can be a large number of
// iterators holding remoteReadHandles open for every level.
//
// For (b) we have to two use-cases:
//
//   - When a sstable.Reader is opened, it needs to read the footer, metaindex
//     block and meta properties block. It starts by reading the footer which is
//     at the end of the table and then proceeds to read the other two. Instead
//     of doing 3 tiny reads, we would like to do one read.
//
//   - When a single-level or two-level iterator is opened, it reads the
//     (top-level) index block first. When the iterator is used, it will
//     typically follow this by reading the filter block (since SeeKPrefixGE is
//     common in CockroachDB). For a two-level iterator it will also read the
//     lower-level index blocks which are after the filter block and before the
//     top-level index block. It would be ideal if reading the top-level index
//     block read enough to include the filter block. And for two-level
//     iterators this would also include the lower-level index blocks.
//
// In both use-cases we want the first read from the remoteReadable to do a
// larger read, and read bytes earlier than the requested read, hence
// "read-before". Subsequent reads from the remoteReadable can use the usual
// readahead logic (for the second use-case above, this can help with
// sequential reads of the lower-level index blocks when the read-before was
// insufficient to satisfy such reads). In the first use-case, the block cache
// is not used. In the second use-case, the block cache is used, and if the
// first read, which reads the top-level index, has a cache hit, we do not do
// any read-before, since we assume that with some locality in the workload
// the other reads will also have a cache hit (it is also messier code to try
// to preserve some read-before).
//
// Note that both use-cases can often occur near each other if there is enough
// locality of access, in which case file cache and block cache misses are
// mainly happening for new sstables created by compactions -- in this case a
// user-facing read will cause a file cache miss and a new sstable.Reader to
// be created, followed by an iterator creation. We don't currently combine
// the reads across the Reader and the iterator creation, since the code
// structure is not simple enough, but we could consider that in the future.
type remoteReadHandle struct {
	readable       *remoteReadable
	readBeforeSize objstorage.ReadBeforeSize
	readAheadState readaheadState
	buffered       struct {
		data   []byte
		offset int64
	}
	forCompaction bool
}

var _ objstorage.ReadHandle = (*remoteReadHandle)(nil)

var remoteReadHandlePool = sync.Pool{
	New: func() interface{} {
		return &remoteReadHandle{}
	},
}

// ReadAt is part of the objstorage.ReadHandle interface.
func (r *remoteReadHandle) ReadAt(ctx context.Context, p []byte, offset int64) error {
	var extraBytesBefore int64
	if r.readBeforeSize > 0 {
		if int(r.readBeforeSize) > len(p) {
			extraBytesBefore = min(int64(int(r.readBeforeSize)-len(p)), offset)
		}
		// Only first read uses read-before.
		r.readBeforeSize = 0
	}
	readaheadSize := r.maybeReadahead(offset, len(p))

	// Prefer read-before to read-ahead since only first call does read-before.
	// Also, since this is the first call, the buffer must be empty.
	if extraBytesBefore > 0 {
		r.buffered.offset = offset - extraBytesBefore
		err := r.readToBuffer(ctx, offset-extraBytesBefore, len(p)+int(extraBytesBefore))
		if err != nil {
			return err
		}
		copy(p, r.buffered.data[int(extraBytesBefore):])
		return nil
	}
	// Check if we already have the data from a previous read-ahead/read-before.
	if rhSize := int64(len(r.buffered.data)); rhSize > 0 {
		// We only consider the case where we have a prefix of the needed data. We
		// could enhance this to utilize a suffix of the needed data.
		if r.buffered.offset <= offset && r.buffered.offset+rhSize > offset {
			n := copy(p, r.buffered.data[offset-r.buffered.offset:])
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
		// Don't try to read past EOF.
		if offset+int64(readaheadSize) > r.readable.size {
			readaheadSize = int(r.readable.size - offset)
			if readaheadSize <= 0 {
				// This shouldn't happen in practice (Pebble should never try to read
				// past EOF).
				return io.EOF
			}
		}
		if err := r.readToBuffer(ctx, offset, readaheadSize); err != nil {
			return err
		}
		copy(p, r.buffered.data)
		return nil
	}

	return r.readable.readInternal(ctx, p, offset, r.forCompaction)
}

func (r *remoteReadHandle) maybeReadahead(offset int64, len int) int {
	if r.forCompaction {
		return remoteReadaheadSizeForCompaction
	}
	return int(r.readAheadState.maybeReadahead(offset, int64(len)))
}

func (r *remoteReadHandle) readToBuffer(ctx context.Context, offset int64, length int) error {
	r.buffered.offset = offset
	// TODO(radu): we need to somehow account for this memory.
	if cap(r.buffered.data) >= length {
		r.buffered.data = r.buffered.data[:length]
	} else {
		r.buffered.data = make([]byte, length)
	}
	if err := r.readable.readInternal(
		ctx, r.buffered.data, r.buffered.offset, r.forCompaction); err != nil {
		// Make sure we don't treat the data as valid next time.
		r.buffered.data = r.buffered.data[:0]
		return err
	}
	return nil
}

// Close is part of the objstorage.ReadHandle interface.
func (r *remoteReadHandle) Close() error {
	buf := r.buffered.data[:0]
	*r = remoteReadHandle{}
	r.buffered.data = buf
	remoteReadHandlePool.Put(r)
	return nil
}

// SetupForCompaction is part of the objstorage.ReadHandle interface.
func (r *remoteReadHandle) SetupForCompaction() {
	r.forCompaction = true
}

// RecordCacheHit is part of the objstorage.ReadHandle interface.
func (r *remoteReadHandle) RecordCacheHit(_ context.Context, offset, size int64) {
	if !r.forCompaction {
		r.readAheadState.recordCacheHit(offset, size)
	}
	if r.readBeforeSize > 0 {
		r.readBeforeSize = 0
	}
}
