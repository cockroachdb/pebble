// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorage

import (
	"fmt"
	"os"
	"sync"

	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/vfs"
)

// fileReadable implements objstorage.Readable on top of vfs.File.
type fileReadable struct {
	file vfs.File
	size int64

	// The following fields are used to possibly open the file again using the
	// sequential reads option (see readaheadHandle).
	filename string
	fs       vfs.FS
}

var _ Readable = (*fileReadable)(nil)

func newFileReadable(file vfs.File, fs vfs.FS, filename string) (*fileReadable, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	r := &fileReadable{
		file:     file,
		size:     info.Size(),
		filename: filename,
		fs:       fs,
	}
	invariants.SetFinalizer(r, func(obj interface{}) {
		if obj.(*fileReadable).file != nil {
			fmt.Fprintf(os.Stderr, "Readable was not closed")
			os.Exit(1)
		}
	})
	return r, nil
}

// ReadAt is part of the objstorage.Readable interface.
func (r *fileReadable) ReadAt(p []byte, off int64) (n int, err error) {
	return r.file.ReadAt(p, off)
}

// Close is part of the objstorage.Readable interface.
func (r *fileReadable) Close() error {
	defer func() { r.file = nil }()
	return r.file.Close()
}

// Size is part of the objstorage.Readable interface.
func (r *fileReadable) Size() int64 {
	return r.size
}

// NewReadaheadHandle is part of the objstorage.Readable interface.
func (r *fileReadable) NewReadaheadHandle() ReadaheadHandle {
	rh := readaheadHandlePool.Get().(*readaheadHandle)
	rh.r = r
	return rh
}

type readaheadHandle struct {
	r  *fileReadable
	rs readaheadState

	// sequentialFile holds a file descriptor to the same underlying File,
	// except with fadvise(FADV_SEQUENTIAL) called on it to take advantage of
	// OS-level readahead. Once this is non-nil, the other variables in
	// readaheadState don't matter much as we defer to OS-level readahead.
	sequentialFile vfs.File
}

var _ ReadaheadHandle = (*readaheadHandle)(nil)

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

// Close is part of the objstorage.ReadaheadHandle interface.
func (rh *readaheadHandle) Close() error {
	var err error
	if rh.sequentialFile != nil {
		err = rh.sequentialFile.Close()
	}
	*rh = readaheadHandle{}
	readaheadHandlePool.Put(rh)
	return err
}

// ReadAt is part of the objstorage.ReadaheadHandle interface.
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
			_ = rh.r.file.Prefetch(offset, readaheadSize)
		}
	}
	return rh.r.file.ReadAt(p, offset)
}

// MaxReadahead is part of the objstorage.ReadaheadHandle interface.
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

// RecordCacheHit is part of the objstorage.ReadaheadHandle interface.
func (rh *readaheadHandle) RecordCacheHit(offset, size int64) {
	if rh.sequentialFile != nil {
		// Using OS-level readahead, so do nothing.
		return
	}
	rh.rs.recordCacheHit(offset, size)
}

// genericFileReadable implements objstorage.Readable on top of any vfs.File.
// This implementation does not support read-ahead.
type genericFileReadable struct {
	file vfs.File
	size int64

	rh NoopReadaheadHandle
}

var _ Readable = (*genericFileReadable)(nil)

func newGenericFileReadable(file vfs.File) (*genericFileReadable, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	r := &genericFileReadable{
		file: file,
		size: info.Size(),
		rh:   MakeNoopReadaheadHandle(file),
	}
	invariants.SetFinalizer(r, func(obj interface{}) {
		if obj.(*genericFileReadable).file != nil {
			fmt.Fprintf(os.Stderr, "Readable was not closed")
			os.Exit(1)
		}
	})
	return r, nil
}

// ReadAt is part of the objstorage.Readable interface.
func (r *genericFileReadable) ReadAt(p []byte, off int64) (n int, err error) {
	return r.file.ReadAt(p, off)
}

// Close is part of the objstorage.Readable interface.
func (r *genericFileReadable) Close() error {
	defer func() { r.file = nil }()
	return r.file.Close()
}

// Size is part of the objstorage.Readable interface.
func (r *genericFileReadable) Size() int64 {
	return r.size
}

// NewReadaheadHandle is part of the objstorage.Readable interface.
func (r *genericFileReadable) NewReadaheadHandle() ReadaheadHandle {
	return &r.rh
}

// TestingCheckMaxReadahead returns true if the ReadaheadHandle has switched to
// OS-level read-ahead.
func TestingCheckMaxReadahead(rh ReadaheadHandle) bool {
	return rh.(*readaheadHandle).sequentialFile != nil
}
