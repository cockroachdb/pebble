// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfsbackend

import (
	"fmt"
	"os"
	"sync"

	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/sststorage"
	"github.com/cockroachdb/pebble/vfs"
)

// readable implements sststorage.Readable on top of a vfs.File that is backed
// by an OS descriptor.
type readable struct {
	file vfs.File
	fd   uintptr
	size int64

	// The following fields are used to possibly open the file again using the
	// sequential reads option (see readaheadHandle).
	filename string
	fs       vfs.FS
}

var _ sststorage.Readable = (*readable)(nil)

func newReadable(file vfs.File, fd uintptr, fs vfs.FS, filename string) (*readable, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	r := &readable{
		file:     file,
		fd:       fd,
		size:     info.Size(),
		filename: filename,
		fs:       fs,
	}
	invariants.SetFinalizer(r, func(obj interface{}) {
		if obj.(*readable).file != nil {
			fmt.Fprintf(os.Stderr, "Readable was not closed")
			os.Exit(1)
		}
	})
	return r, nil
}

// ReadAt is part of the sststorage.Readable interface.
func (r *readable) ReadAt(p []byte, off int64) (n int, err error) {
	return r.file.ReadAt(p, off)
}

// Close is part of the sststorage.Readable interface.
func (r *readable) Close() error {
	defer func() { r.file = nil }()
	return r.file.Close()
}

// Size is part of the sststorage.Readable interface.
func (r *readable) Size() int64 {
	return r.size
}

// NewReadaheadHandle is part of the sststorage.Readable interface.
func (r *readable) NewReadaheadHandle() sststorage.ReadaheadHandle {
	rh := readaheadHandlePool.Get().(*readaheadHandle)
	rh.r = r
	return rh
}

type readaheadHandle struct {
	r  *readable
	rs readaheadState

	// sequentialFile holds a file descriptor to the same underlying File,
	// except with fadvise(FADV_SEQUENTIAL) called on it to take advantage of
	// OS-level readahead. Once this is non-nil, the other variables in
	// readaheadState don't matter much as we defer to OS-level readahead.
	sequentialFile vfs.File
}

var _ sststorage.ReadaheadHandle = (*readaheadHandle)(nil)

var readaheadHandlePool = sync.Pool{
	New: func() interface{} {
		i := &readaheadHandle{}
		// Note: this is a no-op if invariants are disabled or race is enabled.
		invariants.SetFinalizer(i, func(obj interface{}) {
			if obj.(*readaheadHandle).r != nil {
				fmt.Fprintf(os.Stderr, "ReadaheadHandle was not closed")
				os.Exit(1)
			}
		})
		return i
	},
}

// Close is part of the sststorage.ReadaheadHandle interface.
func (rh *readaheadHandle) Close() error {
	var err error
	if rh.sequentialFile != nil {
		err = rh.sequentialFile.Close()
	}
	*rh = readaheadHandle{}
	readaheadHandlePool.Put(rh)
	return err
}

// ReadAt is part of the sststorage.ReadaheadHandle interface.
func (rh *readaheadHandle) ReadAt(p []byte, offset int64) (n int, err error) {
	if rh.sequentialFile != nil {
		// Use OS-level read-ahead.
		return rh.sequentialFile.ReadAt(p, offset)
	}
	if readaheadSize := rh.rs.maybeReadahead(offset, int64(len(p))); readaheadSize > 0 {
		if readaheadSize >= maxReadaheadSize {
			// We've reached the maximum readahead size. Beyond this point, rely on
			// OS-level readahead.
			rh.MaxReadahead()
		} else {
			_ = vfs.Prefetch(rh.r.fd, uint64(offset), uint64(readaheadSize))
		}
	}
	return rh.r.file.ReadAt(p, offset)
}

// MaxReadahead is part of the sststorage.ReadaheadHandle interface.
func (rh *readaheadHandle) MaxReadahead() {
	if rh.sequentialFile != nil {
		return
	}

	// TODO(radu): we could share the reopened file descriptor across multiple
	// handles.
	f, err := rh.r.fs.Open(rh.r.filename, vfs.SequentialReadsOption)
	if err == nil {
		rh.sequentialFile = f
	}
}

// RecordCacheHit is part of the sststorage.ReadaheadHandle interface.
func (rh *readaheadHandle) RecordCacheHit(offset, size int64) {
	if rh.sequentialFile != nil {
		// Using OS-level readahead, so do nothing.
		return
	}
	rh.rs.recordCacheHit(offset, size)
}

const (
	// Constants for dynamic readahead of data blocks. Note that the size values
	// make sense as some multiple of the default block size; and they should
	// both be larger than the default block size.
	minFileReadsForReadahead = 2
	// TODO(bilal): Have the initial size value be a factor of the block size,
	// as opposed to a hardcoded value.
	initialReadaheadSize = 64 << 10  /* 64KB */
	maxReadaheadSize     = 256 << 10 /* 256KB */
)

// readaheadState contains state variables related to readahead. Updated on
// file reads.
type readaheadState struct {
	// Number of sequential reads.
	numReads int64
	// Size issued to the next call to Prefetch. Starts at or above
	// initialReadaheadSize and grows exponentially until maxReadaheadSize.
	size int64
	// prevSize is the size used in the last Prefetch call.
	prevSize int64
	// The byte offset up to which the OS has been asked to read ahead / cached.
	// When reading ahead, reads up to this limit should not incur an IO
	// operation. Reads after this limit can benefit from a new call to
	// Prefetch.
	limit int64
}

func (rs *readaheadState) recordCacheHit(offset, blockLength int64) {
	currentReadEnd := offset + blockLength
	if rs.numReads >= minFileReadsForReadahead {
		if currentReadEnd >= rs.limit && offset <= rs.limit+maxReadaheadSize {
			// This is a read that would have resulted in a readahead, had it
			// not been a cache hit.
			rs.limit = currentReadEnd
			return
		}
		if currentReadEnd < rs.limit-rs.prevSize || offset > rs.limit+maxReadaheadSize {
			// We read too far away from rs.limit to benefit from readahead in
			// any scenario. Reset all variables.
			rs.numReads = 1
			rs.limit = currentReadEnd
			rs.size = initialReadaheadSize
			rs.prevSize = 0
			return
		}
		// Reads in the range [rs.limit - rs.prevSize, rs.limit] end up
		// here. This is a read that is potentially benefitting from a past
		// readahead.
		return
	}
	if currentReadEnd >= rs.limit && offset <= rs.limit+maxReadaheadSize {
		// Blocks are being read sequentially and would benefit from readahead
		// down the line.
		rs.numReads++
		return
	}
	// We read too far ahead of the last read, or before it. This indicates
	// a random read, where readahead is not desirable. Reset all variables.
	rs.numReads = 1
	rs.limit = currentReadEnd
	rs.size = initialReadaheadSize
	rs.prevSize = 0
}

// maybeReadahead updates state and determines whether to issue a readahead /
// prefetch call for a block read at offset for blockLength bytes.
// Returns a size value (greater than 0) that should be prefetched if readahead
// would be beneficial.
func (rs *readaheadState) maybeReadahead(offset, blockLength int64) int64 {
	currentReadEnd := offset + blockLength
	if rs.numReads >= minFileReadsForReadahead {
		// The minimum threshold of sequential reads to justify reading ahead
		// has been reached.
		// There are two intervals: the interval being read:
		// [offset, currentReadEnd]
		// as well as the interval where a read would benefit from read ahead:
		// [rs.limit, rs.limit + rs.size]
		// We increase the latter interval to
		// [rs.limit, rs.limit + maxReadaheadSize] to account for cases where
		// readahead may not be beneficial with a small readahead size, but over
		// time the readahead size would increase exponentially to make it
		// beneficial.
		if currentReadEnd >= rs.limit && offset <= rs.limit+maxReadaheadSize {
			// We are doing a read in the interval ahead of
			// the last readahead range. In the diagrams below, ++++ is the last
			// readahead range, ==== is the range represented by
			// [rs.limit, rs.limit + maxReadaheadSize], and ---- is the range
			// being read.
			//
			//               rs.limit           rs.limit + maxReadaheadSize
			//         ++++++++++|===========================|
			//
			//              |-------------|
			//            offset       currentReadEnd
			//
			// This case is also possible, as are all cases with an overlap
			// between [rs.limit, rs.limit + maxReadaheadSize] and [offset,
			// currentReadEnd]:
			//
			//               rs.limit           rs.limit + maxReadaheadSize
			//         ++++++++++|===========================|
			//
			//                                            |-------------|
			//                                         offset       currentReadEnd
			//
			//
			rs.numReads++
			rs.limit = offset + rs.size
			rs.prevSize = rs.size
			// Increase rs.size for the next read.
			rs.size *= 2
			if rs.size > maxReadaheadSize {
				rs.size = maxReadaheadSize
			}
			return rs.prevSize
		}
		if currentReadEnd < rs.limit-rs.prevSize || offset > rs.limit+maxReadaheadSize {
			// The above conditional has rs.limit > rs.prevSize to confirm that
			// rs.limit - rs.prevSize would not underflow.
			// We read too far away from rs.limit to benefit from readahead in
			// any scenario. Reset all variables.
			// The case where we read too far ahead:
			//
			// (rs.limit - rs.prevSize)    (rs.limit)   (rs.limit + maxReadaheadSize)
			//                    |+++++++++++++|=============|
			//
			//                                                  |-------------|
			//                                             offset       currentReadEnd
			//
			// Or too far behind:
			//
			// (rs.limit - rs.prevSize)    (rs.limit)   (rs.limit + maxReadaheadSize)
			//                    |+++++++++++++|=============|
			//
			//    |-------------|
			// offset       currentReadEnd
			//
			rs.numReads = 1
			rs.limit = currentReadEnd
			rs.size = initialReadaheadSize
			rs.prevSize = 0

			return 0
		}
		// Reads in the range [rs.limit - rs.prevSize, rs.limit] end up
		// here. This is a read that is potentially benefitting from a past
		// readahead, but there's no reason to issue a readahead call at the
		// moment.
		//
		// (rs.limit - rs.prevSize)            (rs.limit + maxReadaheadSize)
		//                    |+++++++++++++|===============|
		//                             (rs.limit)
		//
		//                        |-------|
		//                     offset    currentReadEnd
		//
		rs.numReads++
		return 0
	}
	if currentReadEnd >= rs.limit && offset <= rs.limit+maxReadaheadSize {
		// Blocks are being read sequentially and would benefit from readahead
		// down the line.
		//
		//                       (rs.limit)   (rs.limit + maxReadaheadSize)
		//                         |=============|
		//
		//                    |-------|
		//                offset    currentReadEnd
		//
		rs.numReads++
		return 0
	}
	// We read too far ahead of the last read, or before it. This indicates
	// a random read, where readahead is not desirable. Reset all variables.
	//
	// (rs.limit - maxReadaheadSize)  (rs.limit)   (rs.limit + maxReadaheadSize)
	//                     |+++++++++++++|=============|
	//
	//                                                    |-------|
	//                                                offset    currentReadEnd
	//
	rs.numReads = 1
	rs.limit = currentReadEnd
	rs.size = initialReadaheadSize
	rs.prevSize = 0
	return 0
}

// genericReadable implements sststorage.Readable on top of any vfs.File.
// This implementation does not support read-ahead.
type genericReadable struct {
	file vfs.File
	size int64

	rh sststorage.NoopReadaheadHandle
}

var _ sststorage.Readable = (*genericReadable)(nil)

func newGenericReadable(file vfs.File) (*genericReadable, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	r := &genericReadable{
		file: file,
		size: info.Size(),
		rh:   sststorage.MakeNoopReadaheadHandle(file),
	}
	invariants.SetFinalizer(r, func(obj interface{}) {
		if obj.(*genericReadable).file != nil {
			fmt.Fprintf(os.Stderr, "Readable was not closed")
			os.Exit(1)
		}
	})
	return r, nil
}

// ReadAt is part of the sststorage.Readable interface.
func (r *genericReadable) ReadAt(p []byte, off int64) (n int, err error) {
	return r.file.ReadAt(p, off)
}

// Close is part of the sststorage.Readable interface.
func (r *genericReadable) Close() error {
	defer func() { r.file = nil }()
	return r.file.Close()
}

// Size is part of the sststorage.Readable interface.
func (r *genericReadable) Size() int64 {
	return r.size
}

// NewReadaheadHandle is part of the sststorage.Readable interface.
func (r *genericReadable) NewReadaheadHandle() sststorage.ReadaheadHandle {
	return &r.rh
}

// TestingCheckMaxReadahead returns true if the ReadaheadHandle has switched to
// OS-level read-ahead.
func TestingCheckMaxReadahead(rh sststorage.ReadaheadHandle) bool {
	return rh.(*readaheadHandle).sequentialFile != nil
}
