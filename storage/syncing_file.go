// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package storage

type syncingFile struct {
	File
	fd           int
	useSyncRange bool
	bytesPerSync int64
	offset       int64
	syncOffset   int64
	syncTo       func(offset int64) error
}

// NewSyncingFile wraps a writable file and ensures that data is synced
// periodically as it is written. The syncing does not provide persistency
// guarantees, but is used to avoid latency spikes if the OS automatically
// decides to write out a large chunk of dirty filesystem buffers. If
// bytesPerSync is zero, the original file is returned as no syncing is
// requested.
func NewSyncingFile(f File, bytesPerSync int) File {
	if bytesPerSync <= 0 {
		return f
	}
	s := &syncingFile{
		File:         f,
		fd:           -1,
		bytesPerSync: int64(bytesPerSync),
	}
	s.init()
	return s
}

// NB: syncingFile.Write is unsafe for concurrent use!
func (f *syncingFile) Write(p []byte) (n int, err error) {
	n, err = f.File.Write(p)
	if err != nil {
		return n, err
	}
	f.offset += int64(n)
	if err := f.maybeSync(); err != nil {
		return 0, err
	}
	return n, nil
}

func (f *syncingFile) Sync() error {
	// Do not update syncingFile.syncOffset. This method can be called
	// concurrently with Write, so any update to syncOffset and access to offset
	// would need to be done atomically. But even with atomic operations, we'd
	// still need to call to the underlying file's Sync because sync_file_range
	// does not provide any persistence guarantees. We could possibly avoid a few
	// calls to sync_file_range if there was a recent explicit Sync call, but
	// those calls should be super-fast if there is no dirty data in the file.
	return f.File.Sync()
}

func (f *syncingFile) maybeSync() error {
	// From the RocksDB source:
	//
	//   We try to avoid sync to the last 1MB of data. For two reasons:
	//   (1) avoid rewrite the same page that is modified later.
	//   (2) for older version of OS, write can block while writing out
	//       the page.
	//   Xfs does neighbor page flushing outside of the specified ranges. We
	//   need to make sure sync range is far from the write offset.
	const syncRangeBuffer = 1 << 20 // 1 MB
	if f.offset <= syncRangeBuffer {
		return nil
	}

	const syncRangeAlignment = 4 << 10 // 4 KB
	syncToOffset := f.offset - syncRangeBuffer
	syncToOffset -= syncToOffset % syncRangeAlignment
	if syncToOffset < 0 || (syncToOffset-f.syncOffset) < f.bytesPerSync {
		return nil
	}

	if f.fd < 0 {
		return f.Sync()
	}
	return f.syncTo(syncToOffset)
}
