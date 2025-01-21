// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package wal

import (
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2/internal/base"
)

// TODO(sumeer): hide LogRecycler once rest of Pebble is using wal.Manager.

// LogRecycler recycles WAL log files. It holds a set of log file numbers that
// are available for reuse. Writing to a recycled log file is faster than to a
// new log file on some common filesystems (xfs, and ext3/4) due to avoiding
// metadata updates.
type LogRecycler struct {
	// The maximum number of log files to maintain for recycling.
	limit int

	// The minimum log number that is allowed to be recycled. Log numbers smaller
	// than this will be subject to immediate deletion. This is used to prevent
	// recycling a log written by a previous instance of the DB which may not
	// have had log recycling enabled. If that previous instance of the DB was
	// RocksDB, the old non-recyclable log record headers will be present.
	minRecycleLogNum base.DiskFileNum

	mu struct {
		sync.Mutex
		logs      []base.FileInfo
		maxLogNum base.DiskFileNum
	}
}

// Init initialized the LogRecycler.
func (r *LogRecycler) Init(maxNumLogFiles int) {
	r.limit = maxNumLogFiles
}

// MinRecycleLogNum returns the current minimum log number that is allowed to
// be recycled.
func (r *LogRecycler) MinRecycleLogNum() NumWAL {
	return NumWAL(r.minRecycleLogNum)
}

// SetMinRecycleLogNum sets the minimum log number that is allowed to be
// recycled.
func (r *LogRecycler) SetMinRecycleLogNum(n NumWAL) {
	r.minRecycleLogNum = base.DiskFileNum(n)
}

// Add attempts to recycle the log file specified by logInfo. Returns true if
// the log file should not be deleted (i.e. the log is being recycled), and
// false otherwise.
func (r *LogRecycler) Add(logInfo base.FileInfo) bool {
	if logInfo.FileNum < r.minRecycleLogNum {
		return false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if logInfo.FileNum <= r.mu.maxLogNum {
		// The log file number was already considered for recycling. Don't consider
		// it again. This avoids a race between adding the same log file for
		// recycling multiple times, and removing the log file for actual
		// reuse. Note that we return true because the log was already considered
		// for recycling and either it was deleted on the previous attempt (which
		// means we shouldn't get here) or it was recycled and thus the file
		// shouldn't be deleted.
		return true
	}
	r.mu.maxLogNum = logInfo.FileNum
	if len(r.mu.logs) >= r.limit {
		return false
	}
	r.mu.logs = append(r.mu.logs, logInfo)
	return true
}

// Peek returns the log at the head of the recycling queue, or the zero value
// fileInfo and false if the queue is empty.
func (r *LogRecycler) Peek() (base.FileInfo, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.mu.logs) == 0 {
		return base.FileInfo{}, false
	}
	return r.mu.logs[0], true
}

// Stats return current stats.
func (r *LogRecycler) Stats() (count int, size uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	count = len(r.mu.logs)
	for i := 0; i < count; i++ {
		size += r.mu.logs[i].FileSize
	}
	return count, size
}

// Pop removes the log number at the head of the recycling queue, enforcing
// that it matches the specified logNum. An error is returned of the recycling
// queue is empty or the head log number does not match the specified one.
func (r *LogRecycler) Pop(logNum base.DiskFileNum) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.mu.logs) == 0 {
		return errors.New("pebble: log recycler empty")
	}
	if r.mu.logs[0].FileNum != logNum {
		return errors.Errorf("pebble: log recycler invalid %d vs %v", logNum, errors.Safe(fileInfoNums(r.mu.logs)))
	}
	r.mu.logs = r.mu.logs[1:]
	return nil
}

// LogNumsForTesting returns the current set of recyclable logs.
func (r *LogRecycler) LogNumsForTesting() []base.DiskFileNum {
	r.mu.Lock()
	defer r.mu.Unlock()
	return fileInfoNums(r.mu.logs)
}

func (r *LogRecycler) maxLogNumForTesting() base.DiskFileNum {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.maxLogNum
}

func fileInfoNums(finfos []base.FileInfo) []base.DiskFileNum {
	if len(finfos) == 0 {
		return nil
	}
	nums := make([]base.DiskFileNum, len(finfos))
	for i := range finfos {
		nums[i] = finfos[i].FileNum
	}
	return nums
}
