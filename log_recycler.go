// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"sync"
)

type logRecycler struct {
	limit int
	mu    struct {
		sync.Mutex
		logNums   []uint64
		maxLogNum uint64
	}
}

// add attempts to recycle the log file specified by logNum. Returns true if
// the log file should not be deleted (i.e. the log is being recycled), and
// false otherwise.
func (r *logRecycler) add(logNum uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if logNum <= r.mu.maxLogNum {
		// The log file number was already considered for recycling. Don't consider
		// it again. This avoids a race between adding the same log file for
		// recycling multiple times, and removing the log file for actual
		// reuse. Note that we return true because the log was already considered
		// for recycling and either it was deleted on the previous attempt (which
		// means we shouldn't get here) or it was recycled and thus the file
		// shouldn't be deleted.
		return true
	}
	r.mu.maxLogNum = logNum
	if len(r.mu.logNums) >= r.limit {
		return false
	}
	r.mu.logNums = append(r.mu.logNums, logNum)
	return true
}

// peek returns the log number at the head of the recycling queue, or zero if
// the queue is empty.
func (r *logRecycler) peek() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.mu.logNums) == 0 {
		return 0
	}
	return r.mu.logNums[0]
}

func (r *logRecycler) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.mu.logNums)
}

// pop removes the log number at the head of the recycling queue, enforcing
// that it matches the specifed logNum. An error is returned of the recycling
// queue is empty or the head log number does not match the specified one.
func (r *logRecycler) pop(logNum uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.mu.logNums) == 0 {
		return fmt.Errorf("pebble: log recycler empty")
	}
	if r.mu.logNums[0] != logNum {
		return fmt.Errorf("pebble: log recycler invalid %d vs %d", logNum, r.mu.logNums)
	}
	r.mu.logNums = r.mu.logNums[1:]
	return nil
}
