// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"io"
	"math"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/rangekey"
)

// ErrSnapshotExcised is returned by EventuallyFileOnlySnapshot if one of its
// key ranges got excised before it could be converted into a FileOnlySnapshot.
var ErrSnapshotExcised = errors.New("pebble: snapshot excised before conversion to file-only")

// Snapshot provides a read-only point-in-time view of the DB state.
type Snapshot struct {
	// The db the snapshot was created from.
	db     *DB
	seqNum uint64

	// Set if part of an EventuallyFileOnlySnapshot.
	efos *EventuallyFileOnlySnapshot

	// The list the snapshot is linked into.
	list *snapshotList

	// The next/prev link for the snapshotList doubly-linked list of snapshots.
	prev, next *Snapshot
}

var _ Reader = (*Snapshot)(nil)

// Get gets the value for the given key. It returns ErrNotFound if the Snapshot
// does not contain the key.
//
// The caller should not modify the contents of the returned slice, but it is
// safe to modify the contents of the argument after Get returns. The returned
// slice will remain valid until the returned Closer is closed. On success, the
// caller MUST call closer.Close() or a memory leak will occur.
func (s *Snapshot) Get(key []byte) ([]byte, io.Closer, error) {
	if s.db == nil {
		panic(ErrClosed)
	}
	return s.db.getInternal(key, nil /* batch */, s)
}

// NewIter returns an iterator that is unpositioned (Iterator.Valid() will
// return false). The iterator can be positioned via a call to SeekGE,
// SeekLT, First or Last.
func (s *Snapshot) NewIter(o *IterOptions) *Iterator {
	return s.NewIterWithContext(context.Background(), o)
}

// NewIterWithContext is like NewIter, and additionally accepts a context for
// tracing.
func (s *Snapshot) NewIterWithContext(ctx context.Context, o *IterOptions) *Iterator {
	if s.db == nil {
		panic(ErrClosed)
	}
	return s.db.newIter(ctx, nil /* batch */, snapshotIterOpts{snapshot: s}, o)
}

// ScanInternal scans all internal keys within the specified bounds, truncating
// any rangedels and rangekeys to those bounds. For use when an external user
// needs to be aware of all internal keys that make up a key range.
//
// See comment on db.ScanInternal for the behaviour that can be expected of
// point keys deleted by range dels and keys masked by range keys.
func (s *Snapshot) ScanInternal(
	ctx context.Context,
	lower, upper []byte,
	visitPointKey func(key *InternalKey, value LazyValue) error,
	visitRangeDel func(start, end []byte, seqNum uint64) error,
	visitRangeKey func(start, end []byte, keys []rangekey.Key) error,
	visitSharedFile func(sst *SharedSSTMeta) error,
) error {
	if s.db == nil {
		panic(ErrClosed)
	}
	iter := s.db.newInternalIter(s, &scanInternalOptions{
		IterOptions: IterOptions{
			KeyTypes:   IterKeyTypePointsAndRanges,
			LowerBound: lower,
			UpperBound: upper,
		},
		skipSharedLevels: visitSharedFile != nil,
	})
	defer iter.close()

	return scanInternalImpl(ctx, lower, upper, iter, visitPointKey, visitRangeDel, visitRangeKey, visitSharedFile)
}

// Close closes the snapshot, releasing its resources. Close must be called.
// Failure to do so will result in a tiny memory leak and a large leak of
// resources on disk due to the entries the snapshot is preventing from being
// deleted.
func (s *Snapshot) Close() error {
	if s.db == nil {
		panic(ErrClosed)
	}
	s.db.mu.Lock()
	s.db.mu.snapshots.remove(s)

	// If s was the previous earliest snapshot, we might be able to reclaim
	// disk space by dropping obsolete records that were pinned by s.
	if e := s.db.mu.snapshots.earliest(); e > s.seqNum {
		s.db.maybeScheduleCompactionPicker(pickElisionOnly)
	}
	s.db.mu.Unlock()
	s.db = nil
	return nil
}

type snapshotList struct {
	root Snapshot
}

func (l *snapshotList) init() {
	l.root.next = &l.root
	l.root.prev = &l.root
}

func (l *snapshotList) empty() bool {
	return l.root.next == &l.root
}

func (l *snapshotList) count() int {
	if l.empty() {
		return 0
	}
	var count int
	for i := l.root.next; i != &l.root; i = i.next {
		count++
	}
	return count
}

func (l *snapshotList) earliest() uint64 {
	v := uint64(math.MaxUint64)
	if !l.empty() {
		v = l.root.next.seqNum
	}
	return v
}

func (l *snapshotList) toSlice() []uint64 {
	if l.empty() {
		return nil
	}
	var results []uint64
	for i := l.root.next; i != &l.root; i = i.next {
		results = append(results, i.seqNum)
	}
	return results
}

func (l *snapshotList) pushBack(s *Snapshot) {
	if s.list != nil || s.prev != nil || s.next != nil {
		panic("pebble: snapshot list is inconsistent")
	}
	s.prev = l.root.prev
	s.prev.next = s
	s.next = &l.root
	s.next.prev = s
	s.list = l
}

func (l *snapshotList) remove(s *Snapshot) {
	if s == &l.root {
		panic("pebble: cannot remove snapshot list root node")
	}
	if s.list != l {
		panic("pebble: snapshot list is inconsistent")
	}
	s.prev.next = s.next
	s.next.prev = s.prev
	s.next = nil // avoid memory leaks
	s.prev = nil // avoid memory leaks
	s.list = nil // avoid memory leaks
}

// EventuallyFileOnlySnapshot (aka EFOS) provides a read-only point-in-time view
// of the DB state, similar to Snapshot. It removes itself from the Snapshot list
// once all memtables with seqnums < seqNum have been flushed, and grabs a ref
// of the current Version once that has happened. If an Excise happens before
// this point, an error is returned and the EFOS is closed. This setup allows the
// EFOS to be an Excise-aware version of Snapshots that also doesn't induce
// greater write-amp in the future, as it holds onto files like an iterator (but
// not memtables) instead of forcing compactions to hold onto obsolete keys.
//
// An EventuallyFileOnlySnapshot also takes a list of KeyRanges at
// instantiation time. These are saved inside the underlying Snapshot, and
// any excises that happen during it
type EventuallyFileOnlySnapshot struct {
	mu struct {
		sync.RWMutex

		// Either the {snap,readState} fields are set below, or the version is set at
		// any given point of time. If a snapshot is referenced, this is not a
		// file-only snapshot yet, and if a version is set (and ref'd) this is a
		// file-only snapshot.

		// The wrapped regular snapshot, if not a file-only snapshot yet.
		snap      *Snapshot
		readState *readState
		// The wrapped version reference, if a file-only snapshot.
		vers *version
	}

	// Key ranges to watch for an excise on.
	protectedRanges []KeyRange
	// excised, if true, signals that the above ranges were excised during the
	// lifetime of this snapshot.
	excised atomic.Bool

	// The db the snapshot was created from.
	db     *DB
	seqNum uint64

	closed chan struct{}
	wg     sync.WaitGroup
}

// Calls maybeTransitionToFileOnlySnapshot in a separate goroutine.
func (es *EventuallyFileOnlySnapshot) maybeTransitionToFileOnlySnapshotAsync(
	earliestUnflushedSeqNum uint64, vers *version,
) {
	if es.excised.Load() {
		// The caller has already reffed the version, so we should do an unref.
		vers.Unref()
		return
	}

	es.wg.Add(1)
	go func() {
		defer es.wg.Done()
		select {
		case <-es.closed:
			vers.Unref()
			return
		default:
		}
		_ = es.maybeTransitionToFileOnlySnapshot(earliestUnflushedSeqNum, vers)
	}()
}

// Transitions this EventuallyFileOnlySnapshot to a file-only snapshot. Requires
// no mutexes to be held, however it requires earliestUnflushedSeqNum and
// vers to correspond to the same Version from a past acquisition of db.mu.
// vers must have been Ref()'d before that mutex was released.
func (es *EventuallyFileOnlySnapshot) maybeTransitionToFileOnlySnapshot(
	earliestUnflushedSeqNum uint64, vers *version,
) error {
	if base.Visible(earliestUnflushedSeqNum, es.seqNum, InternalKeySeqNumMax) {
		vers.Unref()
		// Cannot transition.
		return nil
	}

	es.mu.Lock()
	select {
	case <-es.closed:
		vers.Unref()
		es.mu.Unlock()
		return nil
	default:
	}
	var oldSnap *Snapshot
	var oldReadState *readState
	if es.mu.snap != nil {
		// The caller has already called Ref() on vers.
		es.mu.vers = vers
		// NB: The callers should have already done a check of es.excised.
		oldSnap = es.mu.snap
		oldReadState = es.mu.readState
		es.mu.snap = nil
		es.mu.readState = nil
	} else {
		vers.Unref()
	}
	es.mu.Unlock()
	// It's okay to close a snapshot even if iterators are already open on it.
	if oldSnap != nil {
		oldReadState.unref()
		return oldSnap.Close()
	}
	return nil
}

// WaitForFileOnlySnapshot blocks the calling goroutine until this snapshot
// has been converted into a file-only snapshot (i.e. all memtables containing
// keys < seqNum are flushed). An error is returned if an excise operation
// happens that conflicts with this snapshot, or if the db is closed during this
// wait. Idempotent; can be called multiple times with no side effects.
func (es *EventuallyFileOnlySnapshot) WaitForFileOnlySnapshot() error {
	// Fast path.
	es.mu.RLock()
	if es.mu.snap == nil {
		es.mu.RUnlock()
		return nil
	}
	es.mu.RUnlock()

	es.db.mu.Lock()
	earliestUnflushedSeqNum := es.db.getEarliestUnflushedSeqNumLocked()
	vers := es.db.mu.versions.currentVersion()
	for earliestUnflushedSeqNum < es.seqNum {
		select {
		case <-es.closed:
			es.db.mu.Unlock()
			return nil
		default:
		}
		// Check if the current mutable memtable contains keys less than seqNum.
		// If so, rotate it.
		if es.db.mu.mem.mutable.logSeqNum < es.seqNum {
			es.db.commit.mu.Lock()
			if err := es.db.makeRoomForWrite(nil /* batch */); err != nil {
				es.db.commit.mu.Unlock()
				es.db.mu.Unlock()
				return err
			}
			es.db.commit.mu.Unlock()
		}
		es.db.maybeScheduleFlush()
		es.db.mu.compact.cond.Wait()

		earliestUnflushedSeqNum = es.db.getEarliestUnflushedSeqNumLocked()
		vers = es.db.mu.versions.currentVersion()
	}
	if es.excised.Load() {
		es.db.mu.Unlock()
		return ErrSnapshotExcised
	}
	vers.Ref()
	es.db.mu.Unlock()

	// earliestUnflushedSeqNum >= es.seqNum, so this will always transition
	// (unless a concurrent call to it makes the transition).
	return es.maybeTransitionToFileOnlySnapshot(earliestUnflushedSeqNum, vers)
}

// Close closes the file-only snapshot and releases all referenced resources.
// Not idempotent.
func (es *EventuallyFileOnlySnapshot) Close() error {
	close(es.closed)
	es.mu.RLock()
	defer es.mu.RUnlock()

	if es.mu.snap != nil {
		if err := es.mu.snap.Close(); err != nil {
			return err
		}
		es.mu.readState.unref()
	}
	if es.mu.vers != nil {
		es.mu.vers.Unref()
	}
	es.wg.Wait()
	return nil
}

// Get implements the Reader interface.
func (es *EventuallyFileOnlySnapshot) Get(key []byte) (value []byte, closer io.Closer, err error) {
	panic("unimplemented")
}

// NewIter returns an iterator that is unpositioned (Iterator.Valid() will
// return false). The iterator can be positioned via a call to SeekGE,
// SeekLT, First or Last.
func (es *EventuallyFileOnlySnapshot) NewIter(o *IterOptions) *Iterator {
	return es.NewIterWithContext(context.Background(), o)
}

// NewIterWithContext is like NewIter, and additionally accepts a context for
// tracing.
func (es *EventuallyFileOnlySnapshot) NewIterWithContext(
	ctx context.Context, o *IterOptions,
) *Iterator {
	select {
	case <-es.closed:
		panic(ErrClosed)
	default:
	}
	es.mu.RLock()
	defer es.mu.RUnlock()
	if es.mu.snap != nil {
		sOpts := snapshotIterOpts{snapshot: es.mu.snap, readState: es.mu.readState}
		return es.db.newIter(ctx, nil /* batch */, sOpts, o)
	}

	sOpts := snapshotIterOpts{seqNum: es.seqNum, vers: es.mu.vers}
	return es.db.newIter(ctx, nil /* batch */, sOpts, o)
}
