// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs

import (
	"sync/atomic"
)

// SyncingFileOptions holds the options for a syncingFile.
type SyncingFileOptions struct {
	BytesPerSync    int
	PreallocateSize int
}

type syncingFile struct {
	File
	fd              uintptr
	useSyncRange    bool
	bytesPerSync    int64
	preallocateSize int64
	atomic          struct {
		offset     int64
		syncOffset int64
	}
	preallocatedBlocks int64
	syncTo             func(offset int64) error
}

// NewSyncingFile wraps a writable file and ensures that data is synced
// periodically as it is written. The syncing does not provide persistency
// guarantees, but is used to avoid latency spikes if the OS automatically
// decides to write out a large chunk of dirty filesystem buffers. If
// bytesPerSync is zero, the original file is returned as no syncing is
// requested.
func NewSyncingFile(f File, opts SyncingFileOptions) File {
	s := &syncingFile{
		File:            f,
		bytesPerSync:    int64(opts.BytesPerSync),
		preallocateSize: int64(opts.PreallocateSize),
	}

	type fd interface {
		Fd() uintptr
	}
	if d, ok := f.(fd); ok {
		s.fd = d.Fd()
	}

	s.init()
	return s
}

// NB: syncingFile.Write is unsafe for concurrent use!
func (f *syncingFile) Write(p []byte) (n int, err error) {
	_ = f.preallocate(atomic.LoadInt64(&f.atomic.offset) + int64(n))

	n, err = f.File.Write(p)
	if err != nil {
		return n, err
	}
	// The offset is updated atomically so that it can be accessed safely from
	// Sync.
	atomic.AddInt64(&f.atomic.offset, int64(n))
	if err := f.maybeSync(); err != nil {
		return 0, err
	}
	return n, nil
}

func (f *syncingFile) preallocate(offset int64) error {
	if f.fd == 0 || f.preallocateSize == 0 {
		return nil
	}

	newPreallocatedBlocks := (offset + f.preallocateSize - 1) / f.preallocateSize
	if newPreallocatedBlocks <= f.preallocatedBlocks {
		return nil
	}

	length := f.preallocateSize * (newPreallocatedBlocks - f.preallocatedBlocks)
	offset = f.preallocateSize * f.preallocatedBlocks
	f.preallocatedBlocks = newPreallocatedBlocks
	return preallocExtend(f.fd, offset, length)
}

func (f *syncingFile) ratchetSyncOffset(offset int64) {
	for {
		syncOffset := atomic.LoadInt64(&f.atomic.syncOffset)
		if syncOffset >= offset {
			return
		}
		if atomic.CompareAndSwapInt64(&f.atomic.syncOffset, syncOffset, offset) {
			return
		}
	}
}

func (f *syncingFile) Sync() error {
	// We update syncOffset (atomically) in order to avoid spurious syncs in
	// maybeSync. Note that even if syncOffset is larger than the current file
	// offset, we still need to call the underlying file's sync for persistence
	// guarantees (which are not provided by sync_file_range).
	f.ratchetSyncOffset(atomic.LoadInt64(&f.atomic.offset))
	return f.syncData()
}

func (f *syncingFile) maybeSync() error {
	if f.bytesPerSync <= 0 {
		return nil
	}

	// From the RocksDB source:
	//
	//   We try to avoid sync to the last 1MB of data. For two reasons:
	//   (1) avoid rewrite the same page that is modified later.
	//   (2) for older version of OS, write can block while writing out
	//       the page.
	//   Xfs does neighbor page flushing outside of the specified ranges. We
	//   need to make sure sync range is far from the write offset.
	const syncRangeBuffer = 1 << 20 // 1 MB
	offset := atomic.LoadInt64(&f.atomic.offset)
	if offset <= syncRangeBuffer {
		return nil
	}

	const syncRangeAlignment = 4 << 10 // 4 KB
	syncToOffset := offset - syncRangeBuffer
	syncToOffset -= syncToOffset % syncRangeAlignment
	syncOffset := atomic.LoadInt64(&f.atomic.syncOffset)
	if syncToOffset < 0 || (syncToOffset-syncOffset) < f.bytesPerSync {
		return nil
	}

	if f.fd == 0 {
		return f.Sync()
	}
	return f.syncTo(syncToOffset)
}
