// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"io"
	"math"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2/internal/base"
	"github.com/cockroachdb/pebble/v2/internal/invariants"
	"github.com/cockroachdb/pebble/v2/internal/manifest"
	"github.com/cockroachdb/pebble/v2/rangekey"
	"github.com/cockroachdb/pebble/v2/sstable/block"
)

// Snapshot provides a read-only point-in-time view of the DB state.
type Snapshot struct {
	// The db the snapshot was created from.
	db     *DB
	seqNum base.SeqNum

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
func (s *Snapshot) NewIter(o *IterOptions) (*Iterator, error) {
	return s.NewIterWithContext(context.Background(), o)
}

// NewIterWithContext is like NewIter, and additionally accepts a context for
// tracing.
func (s *Snapshot) NewIterWithContext(ctx context.Context, o *IterOptions) (*Iterator, error) {
	if s.db == nil {
		panic(ErrClosed)
	}
	return s.db.newIter(ctx, nil /* batch */, newIterOpts{
		snapshot: snapshotIterOpts{seqNum: s.seqNum},
	}, o), nil
}

// ScanInternal scans all internal keys within the specified bounds, truncating
// any rangedels and rangekeys to those bounds. For use when an external user
// needs to be aware of all internal keys that make up a key range.
//
// See comment on db.ScanInternal for the behaviour that can be expected of
// point keys deleted by range dels and keys masked by range keys.
func (s *Snapshot) ScanInternal(
	ctx context.Context,
	category block.Category,
	lower, upper []byte,
	visitPointKey func(key *InternalKey, value LazyValue, iterInfo IteratorLevel) error,
	visitRangeDel func(start, end []byte, seqNum base.SeqNum) error,
	visitRangeKey func(start, end []byte, keys []rangekey.Key) error,
	visitSharedFile func(sst *SharedSSTMeta) error,
	visitExternalFile func(sst *ExternalFile) error,
) error {
	if s.db == nil {
		panic(ErrClosed)
	}
	scanInternalOpts := &scanInternalOptions{
		category:          category,
		visitPointKey:     visitPointKey,
		visitRangeDel:     visitRangeDel,
		visitRangeKey:     visitRangeKey,
		visitSharedFile:   visitSharedFile,
		visitExternalFile: visitExternalFile,
		IterOptions: IterOptions{
			KeyTypes:   IterKeyTypePointsAndRanges,
			LowerBound: lower,
			UpperBound: upper,
		},
	}

	iter, err := s.db.newInternalIter(ctx, snapshotIterOpts{seqNum: s.seqNum}, scanInternalOpts)
	if err != nil {
		return err
	}
	defer iter.close()

	return scanInternalImpl(ctx, lower, upper, iter, scanInternalOpts)
}

// closeLocked is similar to Close(), except it requires that db.mu be held
// by the caller.
func (s *Snapshot) closeLocked() error {
	s.db.mu.snapshots.remove(s)

	// If s was the previous earliest snapshot, we might be able to reclaim
	// disk space by dropping obsolete records that were pinned by s.
	if e := s.db.mu.snapshots.earliest(); e > s.seqNum {
		// NB: maybeScheduleCompaction also picks elision-only compactions.
		s.db.maybeScheduleCompaction()
	}
	s.db = nil
	return nil
}

// Close closes the snapshot, releasing its resources. Close must be called.
// Failure to do so will result in a tiny memory leak and a large leak of
// resources on disk due to the entries the snapshot is preventing from being
// deleted.
//
// d.mu must NOT be held by the caller.
func (s *Snapshot) Close() error {
	db := s.db
	if db == nil {
		panic(ErrClosed)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return s.closeLocked()
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

func (l *snapshotList) earliest() base.SeqNum {
	v := base.SeqNum(math.MaxUint64)
	if !l.empty() {
		v = l.root.next.seqNum
	}
	return v
}

func (l *snapshotList) toSlice() []base.SeqNum {
	if l.empty() {
		return nil
	}
	var results []base.SeqNum
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
// of the database state, similar to Snapshot. An EventuallyFileOnlySnapshot
// induces less write amplification than Snapshot, at the cost of increased space
// amplification. While a Snapshot may increase write amplification across all
// flushes and compactions for the duration of its lifetime, an
// EventuallyFileOnlySnapshot only incurs that cost for flushes/compactions if
// memtables at the time of EFOS instantiation contained keys that the EFOS is
// interested in (i.e. its protectedRanges). In that case, the EFOS prevents
// elision of keys visible to it, similar to a Snapshot, until those memtables
// are flushed, and once that happens, the "EventuallyFileOnlySnapshot"
// transitions to a file-only snapshot state in which it pins zombies sstables
// like an open Iterator would, without pinning any memtables. Callers that can
// tolerate the increased space amplification of pinning zombie sstables until
// the snapshot is closed may prefer EventuallyFileOnlySnapshots for their
// reduced write amplification. Callers that desire the benefits of the file-only
// state that requires no pinning of memtables should call
// `WaitForFileOnlySnapshot()` before relying on the EFOS to keep producing iterators
// with zero write-amp and zero pinning of memtables in memory.
//
// EventuallyFileOnlySnapshots interact with the IngestAndExcise operation in
// subtle ways. The IngestAndExcise can force the transition of an EFOS to a
// file-only snapshot if an excise overlaps with the EFOS bounds.
type EventuallyFileOnlySnapshot struct {
	mu struct {
		// NB: If both this mutex and db.mu are being grabbed, db.mu should be
		// grabbed _before_ grabbing this one.
		sync.Mutex

		// Either the snap field is set below, or the version is set at any given
		// point of time. If a snapshot is referenced, this is not a file-only
		// snapshot yet, and if a version is set (and ref'd) this is a file-only
		// snapshot.

		// The wrapped regular snapshot, if not a file-only snapshot yet.
		snap *Snapshot
		// The wrapped version reference, if a file-only snapshot.
		vers *manifest.Version
	}

	// Key ranges to watch for an excise on.
	protectedRanges []KeyRange

	// The db the snapshot was created from.
	db     *DB
	seqNum base.SeqNum
	closed chan struct{}
}

func (d *DB) makeEventuallyFileOnlySnapshot(keyRanges []KeyRange) *EventuallyFileOnlySnapshot {
	isFileOnly := true

	d.mu.Lock()
	defer d.mu.Unlock()
	seqNum := d.mu.versions.visibleSeqNum.Load()
	// Check if any of the keyRanges overlap with a memtable.
	for i := range d.mu.mem.queue {
		d.mu.mem.queue[i].computePossibleOverlaps(func(bounded) shouldContinue {
			isFileOnly = false
			return stopIteration
		}, sliceAsBounded(keyRanges)...)
	}
	es := &EventuallyFileOnlySnapshot{
		db:              d,
		seqNum:          seqNum,
		protectedRanges: keyRanges,
		closed:          make(chan struct{}),
	}
	if isFileOnly {
		es.mu.vers = d.mu.versions.currentVersion()
		es.mu.vers.Ref()
	} else {
		s := &Snapshot{
			db:     d,
			seqNum: seqNum,
		}
		s.efos = es
		es.mu.snap = s
		d.mu.snapshots.pushBack(s)
	}
	return es
}

// Transitions this EventuallyFileOnlySnapshot to a file-only snapshot. Requires
// earliestUnflushedSeqNum and vers to correspond to the same Version from the
// current or a past acquisition of db.mu. vers must have been Ref()'d before
// that mutex was released, if it was released.
//
// NB: The caller is expected to check for es.excised before making this
// call.
//
// d.mu must be held when calling this method.
func (es *EventuallyFileOnlySnapshot) transitionToFileOnlySnapshot(vers *manifest.Version) error {
	es.mu.Lock()
	select {
	case <-es.closed:
		vers.UnrefLocked()
		es.mu.Unlock()
		return ErrClosed
	default:
	}
	if es.mu.snap == nil {
		es.mu.Unlock()
		panic("pebble: tried to transition an eventually-file-only-snapshot twice")
	}
	// The caller has already called Ref() on vers.
	es.mu.vers = vers
	// NB: The callers should have already done a check of es.excised.
	oldSnap := es.mu.snap
	es.mu.snap = nil
	es.mu.Unlock()
	return oldSnap.closeLocked()
}

// hasTransitioned returns true if this EFOS has transitioned to a file-only
// snapshot.
func (es *EventuallyFileOnlySnapshot) hasTransitioned() bool {
	es.mu.Lock()
	defer es.mu.Unlock()
	return es.mu.vers != nil
}

// waitForFlush waits for a flush on any memtables that need to be flushed
// before this EFOS can transition to a file-only snapshot. If this EFOS is
// waiting on a flush of the mutable memtable, it forces a rotation within
// `dur` duration. For immutable memtables, it schedules a flush and waits for
// it to finish.
func (es *EventuallyFileOnlySnapshot) waitForFlush(ctx context.Context, dur time.Duration) error {
	es.db.mu.Lock()
	defer es.db.mu.Unlock()

	earliestUnflushedSeqNum := es.db.getEarliestUnflushedSeqNumLocked()
	for earliestUnflushedSeqNum < es.seqNum {
		select {
		case <-es.closed:
			return ErrClosed
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		// Check if the current mutable memtable contains keys less than seqNum.
		// If so, rotate it.
		if es.db.mu.mem.mutable.logSeqNum < es.seqNum && dur.Nanoseconds() > 0 {
			es.db.maybeScheduleDelayedFlush(es.db.mu.mem.mutable, dur)
		} else {
			// Find the last memtable that contains seqNums less than es.seqNum,
			// and force a flush on it.
			var mem *flushableEntry
			for i := range es.db.mu.mem.queue {
				if es.db.mu.mem.queue[i].logSeqNum < es.seqNum {
					mem = es.db.mu.mem.queue[i]
				}
			}
			mem.flushForced = true
			es.db.maybeScheduleFlush()
		}
		es.db.mu.compact.cond.Wait()

		earliestUnflushedSeqNum = es.db.getEarliestUnflushedSeqNumLocked()
	}
	return nil
}

// WaitForFileOnlySnapshot blocks the calling goroutine until this snapshot
// has been converted into a file-only snapshot (i.e. all memtables containing
// keys < seqNum are flushed). A duration can be passed in, and if nonzero,
// a delayed flush will be scheduled at that duration if necessary.
//
// Idempotent; can be called multiple times with no side effects.
func (es *EventuallyFileOnlySnapshot) WaitForFileOnlySnapshot(
	ctx context.Context, dur time.Duration,
) error {
	if es.hasTransitioned() {
		return nil
	}

	if err := es.waitForFlush(ctx, dur); err != nil {
		return err
	}

	if invariants.Enabled {
		// Since we aren't returning an error, we _must_ have transitioned to a
		// file-only snapshot by now.
		if !es.hasTransitioned() {
			panic("expected EFOS to have transitioned to file-only snapshot after flush")
		}
	}
	return nil
}

// Close closes the file-only snapshot and releases all referenced resources.
// Not idempotent.
func (es *EventuallyFileOnlySnapshot) Close() error {
	close(es.closed)
	es.db.mu.Lock()
	defer es.db.mu.Unlock()
	es.mu.Lock()
	defer es.mu.Unlock()

	if es.mu.snap != nil {
		if err := es.mu.snap.closeLocked(); err != nil {
			return err
		}
	}
	if es.mu.vers != nil {
		es.mu.vers.UnrefLocked()
	}
	return nil
}

// Get implements the Reader interface.
func (es *EventuallyFileOnlySnapshot) Get(key []byte) (value []byte, closer io.Closer, err error) {
	// TODO(jackson): Use getInternal.
	iter, err := es.NewIter(nil)
	if err != nil {
		return nil, nil, err
	}
	if !iter.SeekPrefixGE(key) {
		if err = firstError(iter.Error(), iter.Close()); err != nil {
			return nil, nil, err
		}
		return nil, nil, ErrNotFound
	}
	if !es.db.equal(iter.Key(), key) {
		return nil, nil, firstError(iter.Close(), ErrNotFound)
	}
	return iter.Value(), iter, nil
}

// NewIter returns an iterator that is unpositioned (Iterator.Valid() will
// return false). The iterator can be positioned via a call to SeekGE,
// SeekLT, First or Last.
func (es *EventuallyFileOnlySnapshot) NewIter(o *IterOptions) (*Iterator, error) {
	return es.NewIterWithContext(context.Background(), o)
}

// NewIterWithContext is like NewIter, and additionally accepts a context for
// tracing.
func (es *EventuallyFileOnlySnapshot) NewIterWithContext(
	ctx context.Context, o *IterOptions,
) (*Iterator, error) {
	select {
	case <-es.closed:
		panic(ErrClosed)
	default:
	}

	es.mu.Lock()
	defer es.mu.Unlock()
	if es.mu.vers != nil {
		sOpts := snapshotIterOpts{seqNum: es.seqNum, vers: es.mu.vers}
		return es.db.newIter(ctx, nil /* batch */, newIterOpts{snapshot: sOpts}, o), nil
	}

	sOpts := snapshotIterOpts{seqNum: es.seqNum}
	iter := es.db.newIter(ctx, nil /* batch */, newIterOpts{snapshot: sOpts}, o)
	return iter, nil
}

// ScanInternal scans all internal keys within the specified bounds, truncating
// any rangedels and rangekeys to those bounds. For use when an external user
// needs to be aware of all internal keys that make up a key range.
//
// See comment on db.ScanInternal for the behaviour that can be expected of
// point keys deleted by range dels and keys masked by range keys.
func (es *EventuallyFileOnlySnapshot) ScanInternal(
	ctx context.Context,
	category block.Category,
	lower, upper []byte,
	visitPointKey func(key *InternalKey, value LazyValue, iterInfo IteratorLevel) error,
	visitRangeDel func(start, end []byte, seqNum base.SeqNum) error,
	visitRangeKey func(start, end []byte, keys []rangekey.Key) error,
	visitSharedFile func(sst *SharedSSTMeta) error,
	visitExternalFile func(sst *ExternalFile) error,
) error {
	if es.db == nil {
		panic(ErrClosed)
	}
	var sOpts snapshotIterOpts
	opts := &scanInternalOptions{
		category: category,
		IterOptions: IterOptions{
			KeyTypes:   IterKeyTypePointsAndRanges,
			LowerBound: lower,
			UpperBound: upper,
		},
		visitPointKey:     visitPointKey,
		visitRangeDel:     visitRangeDel,
		visitRangeKey:     visitRangeKey,
		visitSharedFile:   visitSharedFile,
		visitExternalFile: visitExternalFile,
	}
	es.mu.Lock()
	if es.mu.vers != nil {
		sOpts = snapshotIterOpts{
			seqNum: es.seqNum,
			vers:   es.mu.vers,
		}
	} else {
		sOpts = snapshotIterOpts{
			seqNum: es.seqNum,
		}
	}
	es.mu.Unlock()
	iter, err := es.db.newInternalIter(ctx, sOpts, opts)
	if err != nil {
		return err
	}
	defer iter.close()

	return scanInternalImpl(ctx, lower, upper, iter, opts)
}
