// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package cache

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/swiss"
)

// readShard coordinates the read of a block that will be put in the cache. It
// ensures only one goroutine is reading a block, and other callers block
// until that goroutine is done (with success or failure). In the case of
// success, the other goroutines will use the value that was read, even if it
// is too large to be placed in the cache, or got evicted from the cache
// before they got scheduled. In the case of a failure (read error or context
// cancellation), one of the waiters will be given a turn to do the read.
//
// This turn-taking ensures that a large number of concurrent attempts to read
// the same block that is not in the cache does not result in the same number
// of reads from the filesystem (or remote storage). We have seen large spikes
// in memory usage (and CPU usage for memory allocation/deallocation) without
// this turn-taking.
//
// It introduces a small risk related to context cancellation -- if many
// readers assigned a turn exceed their deadline while doing the read and
// report an error, a reader with a longer deadline can unnecessarily wait. We
// accept this risk for now since the primary production use in CockroachDB is
// filesystem reads, where context cancellation is not respected. We do
// introduce an error duration metric emitted in traces that can be used to
// quantify such wasteful waiting. Note that this same risk extends to waiting
// on the Options.LoadBlockSema, so the error duration metric includes the
// case of an error when waiting on the semaphore (as a side effect of that
// waiting happening in the caller, sstable.Reader).
//
// Design choices and motivation:
//
//   - readShard is tightly integrated with a cache shard: At its core,
//     readShard is a map with synchronization. For the same reason the cache is
//     sharded (for higher concurrency by sharding the mutex), it is beneficial
//     to shard synchronization on readShard. By making readShard a member of
//     shard, this sharding is trivially accomplished. Additionally, the code
//     feels cleaner when there isn't a race between a cache miss, followed by
//     creating a readEntry that is no longer needed because someone else has
//     done the read since the miss and inserted into the cache. By making the
//     readShard use shard.mu, such a race is avoided. A side benefit is that
//     the cache interaction can be hidden behind readEntry.SetReadValue. One
//     disadvantage of this tightly integrated design is that it does not
//     encompass readers that will put the read value into a block.BufferPool --
//     we don't worry about those since block.BufferPool is only used for
//     compactions and there is at most one compaction reader of a block. There
//     is the possibility that the compaction reader and a user-facing iterator
//     reader will do duplicate reads, but we accept that deficiency.
//
//   - readMap is separate from shard.blocks map: One could have a design which
//     extends the cache entry and unifies the two maps. However, we never want
//     to evict a readEntry while there are readers waiting for the block read
//     (including the case where the corresponding file is being removed from
//     shard.files). Also, the number of stable cache entries is huge and
//     therefore is manually allocated, while the number of readEntries is small
//     (so manual allocation isn't necessary). For these reasons we maintain a
//     separate map. This separation also results in more modular code, instead
//     of piling more stuff into shard.
type readShard struct {
	// shard is only used for locking, and calling shard.Set.
	shard *shard
	// Protected by shard.mu.
	//
	// shard.mu is never held when acquiring readEntry.mu. shard.mu is a shared
	// resource and must be released quickly.
	shardMu struct {
		readMap swiss.Map[key, *readEntry]
	}
}

func (rs *readShard) Init(shard *shard) *readShard {
	*rs = readShard{
		shard: shard,
	}
	// Choice of 16 is arbitrary.
	rs.shardMu.readMap.Init(16)
	return rs
}

// getReadEntryLocked gets a *readEntry for (id, fileNum, offset). shard.mu is
// already write locked.
func (rs *readShard) getReadEntryLocked(id ID, fileNum base.DiskFileNum, offset uint64) *readEntry {
	k := key{fileKey{id, fileNum}, offset}
	e, ok := rs.shardMu.readMap.Get(k)
	if !ok {
		e = newReadEntry(rs, id, fileNum, offset)
		rs.shardMu.readMap.Put(k, e)
	} else {
		e.refCount.acquireAllowZero()
	}
	return e
}

func (rs *readShard) lenForTesting() int {
	rs.shard.mu.Lock()
	defer rs.shard.mu.Unlock()
	return rs.shardMu.readMap.Len()
}

// readEntry is used to coordinate between concurrent attempted readers of the
// same block.
type readEntry struct {
	readShard *readShard
	id        ID
	fileNum   base.DiskFileNum
	offset    uint64
	mu        struct {
		sync.RWMutex
		// v, when non-nil, has a ref from readEntry, which is unreffed when
		// readEntry is deleted from the readMap.
		v *Value
		// isReading and ch together capture the state of whether someone has been
		// granted a turn to read, and of readers waiting for that read to finish.
		// ch is lazily allocated since most readEntries will not see concurrent
		// readers. This lazy allocation results in one transition of ch from nil
		// to non-nil, so waiters can read this non-nil ch and block on reading
		// from it without holding mu.
		//
		// ch is written to, to signal one waiter to start doing the read. ch is
		// closed when the value is successfully read and has been stored in v, so
		// that all waiters wake up and read v. ch is a buffered channel with a
		// capacity of 1.
		//
		// State transitions when trying to wait for turn:
		// Case !isReading:
		//   set isReading=true; Drain the ch if non-nil and non-empty; proceed
		//   with turn to do the read.
		// Case isReading:
		//   allocate ch if nil; wait on ch
		// Finished reading successfully:
		//   set isReading=false; if ch is non-nil, close ch.
		// Finished reading with failure:
		//   set isReading=false; if ch is non-nil, write to ch.
		//
		// INVARIANT:
		// isReading => ch is nil or ch is empty.
		isReading bool
		ch        chan struct{}
		// Total duration of reads and semaphore waiting that resulted in error.
		errorDuration time.Duration
		readStart     time.Time
	}
	// Count of ReadHandles that refer to this readEntry. Increments always hold
	// shard.mu. So if this is found to be 0 while holding shard.mu, it is safe
	// to delete readEntry from readShard.shardMu.readMap.
	refCount refcnt
}

var readEntryPool = sync.Pool{
	New: func() interface{} {
		return &readEntry{}
	},
}

func newReadEntry(rs *readShard, id ID, fileNum base.DiskFileNum, offset uint64) *readEntry {
	e := readEntryPool.Get().(*readEntry)
	*e = readEntry{
		readShard: rs,
		id:        id,
		fileNum:   fileNum,
		offset:    offset,
	}
	e.refCount.init(1)
	return e
}

// waitForReadPermissionOrHandle returns either an already read value (in
// Handle), an error (if the context was cancelled), or neither, which is a
// directive to the caller to do the read. In this last case the caller must
// call either setReadValue or setReadError.
//
// In all cases, errorDuration is populated with the total duration that
// readers that observed an error (setReadError) spent in doing the read. This
// duration can be greater than the time spend in waitForReadPermissionHandle,
// since some of these errors could have occurred prior to this call. But it
// serves as a rough indicator of whether turn taking could have caused higher
// latency due to context cancellation.
func (e *readEntry) waitForReadPermissionOrHandle(
	ctx context.Context,
) (h Handle, errorDuration time.Duration, err error) {
	constructHandleLocked := func() Handle {
		if e.mu.v == nil {
			panic("value is nil")
		}
		e.mu.v.acquire()
		return Handle{value: e.mu.v}
	}
	becomeReaderLocked := func() {
		if e.mu.v != nil {
			panic("value is non-nil")
		}
		if e.mu.isReading {
			panic("isReading is already true")
		}
		e.mu.isReading = true
		if e.mu.ch != nil {
			// Drain the channel, so that no one else mistakenly believes they
			// should read.
			select {
			case <-e.mu.ch:
			default:
			}
		}
		e.mu.readStart = time.Now()
	}
	unlockAndUnrefAndTryRemoveFromMap := func(readLock bool) (errorDuration time.Duration) {
		removeState := e.makeTryRemoveStateLocked()
		errorDuration = e.mu.errorDuration
		if readLock {
			e.mu.RUnlock()
		} else {
			e.mu.Unlock()
		}
		unrefAndTryRemoveFromMap(removeState)
		return errorDuration
	}

	for {
		e.mu.Lock()
		if e.mu.v != nil {
			// Value has already been read.
			h := constructHandleLocked()
			errorDuration = unlockAndUnrefAndTryRemoveFromMap(false)
			return h, errorDuration, nil
		}
		// Not already read. Wait for turn to do the read or for someone else to do
		// the read.
		if !e.mu.isReading {
			// Have permission to do the read.
			becomeReaderLocked()
			errorDuration = e.mu.errorDuration
			e.mu.Unlock()
			return Handle{}, errorDuration, nil
		}
		if e.mu.ch == nil {
			// Rare case when multiple readers are concurrently trying to read. If
			// this turns out to be common enough we could use a sync.Pool.
			e.mu.ch = make(chan struct{}, 1)
		}
		ch := e.mu.ch
		e.mu.Unlock()
		select {
		case <-ctx.Done():
			e.mu.RLock()
			errorDuration = unlockAndUnrefAndTryRemoveFromMap(true)
			return Handle{}, errorDuration, ctx.Err()
		case _, ok := <-ch:
			if !ok {
				// Channel closed, so value was read.
				e.mu.RLock()
				if e.mu.v == nil {
					panic("value is nil")
				}
				h := constructHandleLocked()
				errorDuration = unlockAndUnrefAndTryRemoveFromMap(true)
				return h, errorDuration, nil
			}
			// Else, probably granted permission to do the read. NB: since isReading
			// is false, someone else can slip through before this thread acquires
			// e.mu, and take the turn. So try to actually get the turn by trying
			// again in the loop.
		}
	}
}

// tryRemoveState captures the state needed by tryRemoveFromMap. The caller
// constructs it before calling unrefAndTryRemoveFromMap since it typically
// held readEntry.mu, which avoids acquiring it again in
// unrefAndTryRemoveFromMap.
type tryRemoveState struct {
	rs *readShard
	k  key
	e  *readEntry
}

// makeTryRemoveStateLocked initializes tryRemoveState.
func (e *readEntry) makeTryRemoveStateLocked() tryRemoveState {
	return tryRemoveState{
		rs: e.readShard,
		k:  key{fileKey{e.id, e.fileNum}, e.offset},
		e:  e,
	}
}

// unrefAndTryRemoveFromMap tries to remove s.k => s.e from the map in s.rs.
// It is possible that after unreffing that s.e has already been removed, and
// is now back in the sync.Pool, or being reused (for the same or different
// key). This is because after unreffing, which caused the s.e.refCount to
// become zero, but before acquiring shard.mu, it could have been incremented
// and decremented concurrently, and some other goroutine could have observed
// a different decrement to 0, and raced ahead and deleted s.e from the
// readMap.
func unrefAndTryRemoveFromMap(s tryRemoveState) {
	if !s.e.refCount.release() {
		return
	}
	s.rs.shard.mu.Lock()
	e2, ok := s.rs.shardMu.readMap.Get(s.k)
	if !ok || e2 != s.e {
		// Already removed.
		s.rs.shard.mu.Unlock()
		return
	}
	if s.e.refCount.value() != 0 {
		s.rs.shard.mu.Unlock()
		return
	}
	// k => e and e.refCount == 0. And it cannot be incremented since
	// shard.mu.Lock() is held. So remove from map.
	s.rs.shardMu.readMap.Delete(s.k)
	s.rs.shard.mu.Unlock()

	// Free s.e.
	s.e.mu.Lock()
	if s.e.mu.v != nil {
		s.e.mu.v.release()
		s.e.mu.v = nil
	}
	s.e.mu.Unlock()
	*s.e = readEntry{}
	readEntryPool.Put(s.e)
}

func (e *readEntry) setReadValue(v *Value) Handle {
	// Add to the cache before taking another ref for readEntry, since the cache
	// expects ref=1 when it is called.
	//
	// TODO(sumeer): if e.refCount > 1, we should consider overriding to ensure
	// that it is added as etHot. The common case will be e.refCount = 1, and we
	// don't want to acquire e.mu twice, so one way to do this would be relax
	// the invariant in shard.Set that requires Value.refs() == 1. Then we can
	// do the work under e.mu before calling shard.Set.
	h := e.readShard.shard.Set(e.id, e.fileNum, e.offset, v)
	e.mu.Lock()
	// Acquire a ref for readEntry, since we are going to remember it in e.mu.v.
	v.acquire()
	if e.mu.v != nil {
		panic("value already set")
	}
	e.mu.v = v
	if !e.mu.isReading {
		panic("isReading is false")
	}
	e.mu.isReading = false
	if e.mu.ch != nil {
		// Inform all waiters so they can use e.mu.v. Not all readers have called
		// readEntry.waitForReadPermissionOrHandle, and those will also use
		// e.mu.v.
		close(e.mu.ch)
	}
	removeState := e.makeTryRemoveStateLocked()
	e.mu.Unlock()
	unrefAndTryRemoveFromMap(removeState)
	return h
}

func (e *readEntry) setReadError(err error) {
	e.mu.Lock()
	if !e.mu.isReading {
		panic("isReading is false")
	}
	e.mu.isReading = false
	if e.mu.ch != nil {
		select {
		case e.mu.ch <- struct{}{}:
		default:
			panic("channel is not empty")
		}
	}
	e.mu.errorDuration += time.Since(e.mu.readStart)
	removeState := e.makeTryRemoveStateLocked()
	e.mu.Unlock()
	unrefAndTryRemoveFromMap(removeState)
}

// ReadHandle represents a contract with a caller that had a miss when doing a
// cache lookup, and wants to do a read and insert the read block into the
// cache. The contract applies when ReadHandle.Valid returns true, in which
// case the caller has been assigned the turn to do the read (and others are
// potentially waiting for it).
//
// Contract:
//
// The caller must immediately start doing a read, or can first wait on a
// shared resource that would also block a different reader if it was assigned
// the turn instead (specifically, this refers to Options.LoadBlockSema).
// After the read, it must either call SetReadValue or SetReadError depending
// on whether the read succeeded or failed.
type ReadHandle struct {
	entry *readEntry
}

// Valid returns true for a valid ReadHandle.
func (rh ReadHandle) Valid() bool {
	return rh.entry != nil
}

// SetReadValue provides the Value that the caller has read. The caller is
// responsible for releasing the returned Handle when it is no longer needed.
func (rh ReadHandle) SetReadValue(v *Value) Handle {
	return rh.entry.setReadValue(v)
}

// SetReadError specifies that the caller has encountered a read error.
func (rh ReadHandle) SetReadError(err error) {
	rh.entry.setReadError(err)
}
