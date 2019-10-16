// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"sync/atomic"
)

// readState encapsulates the state needed for reading (the current version and
// list of memtables). Loading the readState is done without grabbing
// DB.mu. Instead, a separate DB.readState.RWMutex is used for
// synchronization. This mutex solely covers the current readState object which
// means it is rarely or ever contended.
//
// Note that various fancy lock-free mechanisms can be imagined for loading the
// readState, but benchmarking showed the ones considered to purely be
// pessimizations. The RWMutex version is a single atomic increment for the
// RLock and an atomic decrement for the RUnlock. It is difficult to do better
// than that without something like thread-local storage which isn't available
// in Go.
type readState struct {
	refcnt    int32
	current   *version
	memtables []flushable
}

// ref adds a reference to the readState.
func (s *readState) ref() {
	atomic.AddInt32(&s.refcnt, 1)
}

// unref removes a reference to the readState. If this was the last reference,
// the reference the readState holds on the version is released. Requires DB.mu
// is NOT held as version.unref() will acquire it. See unrefLocked() if DB.mu
// is held by the caller.
func (s *readState) unref() {
	if atomic.AddInt32(&s.refcnt, -1) == 0 {
		s.current.Unref()
		for _, mem := range s.memtables {
			mem.readerUnref()
		}
	}
}

// unrefLocked removes a reference to the readState. If this was the last
// reference, the reference the readState holds on the version is
// released. Requires DB.mu is held as version.unrefLocked() requires it. See
// unref() if DB.mu is NOT held by the caller.
func (s *readState) unrefLocked() {
	if atomic.AddInt32(&s.refcnt, -1) == 0 {
		s.current.UnrefLocked()
		for _, mem := range s.memtables {
			mem.readerUnref()
		}
	}
}

// loadReadState returns the current readState. The returned readState must be
// unreferenced when the caller is finished with it.
func (d *DB) loadReadState() *readState {
	d.readState.RLock()
	state := d.readState.val
	state.ref()
	d.readState.RUnlock()
	return state
}

// updateReadStateLocked creates a new readState from the current version and
// list of memtables. Requires DB.mu is held.
func (d *DB) updateReadStateLocked() {
	s := &readState{
		refcnt:    1,
		current:   d.mu.versions.currentVersion(),
		memtables: d.mu.mem.queue,
	}
	s.current.Ref()
	for _, mem := range s.memtables {
		mem.readerRef()
	}

	d.readState.Lock()
	old := d.readState.val
	d.readState.val = s
	d.readState.Unlock()

	if old != nil {
		old.unrefLocked()
	}
}
