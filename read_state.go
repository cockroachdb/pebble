// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
)

// readState encapsulates the state needed for reading (the current version and
// list of memtables). Loading the readState is done without grabbing
// DB.mu. Instead, a separate DB.readState.RWMutex is used for
// synchronization. This mutex covers the current readState object, which
// means it is rarely or ever contended. Readers without a snapshot also load
// visibleSeqNum under it, so that no readState update falls between the two
// loads (see loadReadStateAndSeqNum).
//
// Note that various fancy lock-free mechanisms can be imagined for loading the
// readState, but benchmarking showed the ones considered to purely be
// pessimizations. The RWMutex version is a single atomic increment for the
// RLock and an atomic decrement for the RUnlock. It is difficult to do better
// than that without something like thread-local storage which isn't available
// in Go.
type readState struct {
	db        *DB
	refcnt    atomic.Int32
	current   *manifest.Version
	memtables flushableList
}

// ref adds a reference to the readState.
func (s *readState) ref() {
	s.refcnt.Add(1)
}

// unref removes a reference to the readState. If this was the last reference,
// the reference the readState holds on the version is released. Requires DB.mu
// is NOT held as version.unref() will acquire it. See unrefLocked() if DB.mu
// is held by the caller.
func (s *readState) unref() {
	if s.refcnt.Add(-1) != 0 {
		return
	}
	s.current.Unref()
	for _, mem := range s.memtables {
		mem.readerUnref(true)
	}

	// The last reference to the readState was released. Check to see if there
	// are new obsolete objects to delete.
	s.db.maybeScheduleObsoleteObjectDeletion()
}

// unrefLocked removes a reference to the readState. If this was the last
// reference, the reference the readState holds on the version is
// released.
//
// DB.mu must be held. See unref() if DB.mu is NOT held by the caller.
func (s *readState) unrefLocked() {
	if s.refcnt.Add(-1) != 0 {
		return
	}
	s.current.UnrefLocked()
	for _, mem := range s.memtables {
		mem.readerUnrefLocked(true)
	}

	// In this code path, the caller is responsible for scheduling obsolete
	// object deletions as necessary.
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

// loadReadStateAndSeqNum returns the current readState along with the
// visibleSeqNum to read it at, for a read without a snapshot. Reading the
// readState at the seqnum is an exact view of the LSM at that seqnum: the
// readState holds everything visible at the seqnum, and nothing else in it is
// visible at the seqnum. The returned readState must be unreferenced when the
// caller is finished with it.
func (d *DB) loadReadStateAndSeqNum() (*readState, base.SeqNum) {
	// Both loads happen under one RLock, so no readState update falls between
	// them. Together with the following, that makes the pair exact:
	//
	//  - Every change is in the readState by the time its seqnum is published.
	//    A batch reserves space in the mutable memtable while holding
	//    commitPipeline.mu, and is applied and published after that. A large
	//    batch is instead queued as a flushable. Memtable rotations, which queue
	//    such batches, also require commitPipeline.mu, and install the new
	//    mutable memtable and queue in the readState before releasing it. An
	//    ingest's tables are either queued as a flushable the same way, or added
	//    to the version by a readState update that also publishes the ingest's
	//    seqnums (see ingestApply).
	//
	//  - Keys are hidden by their seqnums. An excise is not, so it enters the
	//    version only once its seqnum is published: ingestApply publishes it in
	//    the readState update that applies the excise, and a queued flushable
	//    ingest, which represents its excise as range deletion keys at its own
	//    seqnums, is flushed only after its seqnums are published (see
	//    flushable.seqNumUpperBound).
	//
	//  - The flushes and compactions the readState reflects dropped only keys
	//    shadowed at the seqnum. A flush starts only once every seqnum it
	//    flushes is published (see flushable.seqNumUpperBound), and compactions,
	//    including delete-only ones, read only published sstables, so they drop
	//    a key only in favor of a newer key or range deletion that was below
	//    visibleSeqNum when they started. visibleSeqNum only grows, so the seqnum
	//    sees whatever replaced each dropped key.
	//
	// Loading the seqnum before the readState would break the last point: a
	// flush or compaction finishing in between could drop keys visible at the
	// seqnum in favor of newer ones that are not. Loading it after the
	// readState, outside the RLock, would let a readState update fall between
	// the two loads: an ingest could add its tables to the version and publish
	// its seqnums in between, and the read would miss the ingested keys while
	// seeing later writes to a memtable in the readState.
	d.readState.RLock()
	state := d.readState.val
	state.ref()
	seqNum := d.mu.versions.visibleSeqNum.Load()
	d.readState.RUnlock()
	if fn := d.opts.private.testingAfterLoadReadStateAndSeqNumFunc; fn != nil {
		fn()
	}
	return state, seqNum
}

// updateReadStateLocked creates a new readState from the current version and
// list of memtables. Requires DB.mu is held. If checker is not nil, it is
// called after installing the new readState.
func (d *DB) updateReadStateLocked(checker func(*DB) error) {
	d.updateReadStateAndPublishLocked(checker, 0 /* publishSeqNum */)
}

// updateReadStateAndPublishLocked is like updateReadStateLocked, but if
// publishSeqNum is non-zero it also advances visibleSeqNum to publishSeqNum
// (if it is not there already) while installing the new readState, so that
// loadReadStateAndSeqNum observes both changes or neither. publishSeqNum must
// not exceed logSeqNum. Requires DB.mu is held.
func (d *DB) updateReadStateAndPublishLocked(checker func(*DB) error, publishSeqNum base.SeqNum) {
	if publishSeqNum != 0 {
		if logSeqNum := d.mu.versions.logSeqNum.Load(); publishSeqNum > logSeqNum {
			panic(errors.AssertionFailedf("pebble: publish to %d is ahead of logSeqNum %d",
				errors.Safe(publishSeqNum), errors.Safe(logSeqNum)))
		}
	}
	s := &readState{
		db:        d,
		current:   d.mu.versions.currentVersion(),
		memtables: d.mu.mem.queue,
	}
	s.refcnt.Store(1)
	s.current.Ref()
	for _, mem := range s.memtables {
		mem.readerRef()
	}

	d.readState.Lock()
	old := d.readState.val
	d.readState.val = s
	for publishSeqNum != 0 {
		// The CAS loop keeps visibleSeqNum monotonic even if another publish
		// races with this one, as in commitPipeline.publish.
		cur := d.mu.versions.visibleSeqNum.Load()
		if publishSeqNum <= cur || d.mu.versions.visibleSeqNum.CompareAndSwap(cur, publishSeqNum) {
			break
		}
	}
	d.readState.Unlock()
	if checker != nil {
		if err := checker(d); err != nil {
			d.opts.Logger.Fatalf("checker failed with error: %s", err)
		}
	}
	if old != nil {
		old.unrefLocked()
	}
}
