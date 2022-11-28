// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
)

// flushable defines the interface for immutable memtables.
type flushable interface {
	newIter(o *IterOptions) internalIterator
	newFlushIter(o *IterOptions, bytesFlushed *uint64) internalIterator
	newRangeDelIter(o *IterOptions) keyspan.FragmentIterator
	newRangeKeyIter(o *IterOptions) keyspan.FragmentIterator
	containsRangeKeys() bool
	// inuseBytes returns the number of inuse bytes by the flushable.
	inuseBytes() uint64
	// totalBytes returns the total number of bytes allocated by the flushable.
	totalBytes() uint64
	// readyForFlush returns true when the flushable is ready for flushing. See
	// memTable.readyForFlush for one implementation which needs to check whether
	// there are any outstanding write references.
	readyForFlush() bool
}

// flushableEntry wraps a flushable and adds additional metadata and
// functionality that is common to all flushables.
type flushableEntry struct {
	flushable
	// Channel which is closed when the flushable has been flushed.
	flushed chan struct{}
	// flushForced indicates whether a flush was forced on this memtable (either
	// manual, or due to ingestion). Protected by DB.mu.
	flushForced bool
	// delayedFlushForcedAt indicates whether a timer has been set to force a
	// flush on this memtable at some point in the future. Protected by DB.mu.
	// Holds the timestamp of when the flush will be issued.
	delayedFlushForcedAt time.Time
	// logNum corresponds to the WAL that contains the records present in the
	// receiver.
	logNum FileNum
	// logSize is the size in bytes of the associated WAL. Protected by DB.mu.
	logSize uint64
	// The current logSeqNum at the time the memtable was created. This is
	// guaranteed to be less than or equal to any seqnum stored in the memtable.
	logSeqNum uint64
	// readerRefs tracks the read references on the flushable. The two sources of
	// reader references are DB.mu.mem.queue and readState.memtables. The memory
	// reserved by the flushable in the cache is released when the reader refs
	// drop to zero. If the flushable is a memTable, when the reader refs drops
	// to zero, the writer refs will already be zero because the memtable will
	// have been flushed and that only occurs once the writer refs drops to zero.
	readerRefs int32
	// Closure to invoke to release memory accounting.
	releaseMemAccounting func()
	// unrefFiles, if not nil, should be invoked to decrease the ref count of
	// files which are backing the flushable.
	unrefFiles func() []*fileMetadata
	// DeleteFnLocked should be called by readerUnrefLocked if unrefing the
	// memtable produces obsolete files. DeleteFnLocked is a reference to
	// vs.addObsoleteLocked, and therefore DB.mu must be held while calling.
	DeleteFnLocked func(obsolete []*fileMetadata, skipZombieCheck bool)
	// DeleteFn should be called by readerUnref if unrefing the memtable
	// produces obsolete files. DeleteFnLocked is a reference to
	// vs.addObsolete, and therefore DB.mu must not be held while calling.
	DeleteFn func(obsolete []*fileMetadata, skipZombieCheck bool)
}

func (e *flushableEntry) readerRef() {
	switch v := atomic.AddInt32(&e.readerRefs, 1); {
	case v <= 1:
		panic(fmt.Sprintf("pebble: inconsistent reference count: %d", v))
	}
}

// DB.mu must not be held when this is called.
func (e *flushableEntry) readerUnref() {
	switch v := atomic.AddInt32(&e.readerRefs, -1); {
	case v < 0:
		panic(fmt.Sprintf("pebble: inconsistent reference count: %d", v))
	case v == 0:
		if e.releaseMemAccounting == nil {
			panic("pebble: memtable reservation already released")
		}
		e.releaseMemAccounting()
		e.releaseMemAccounting = nil
		if e.unrefFiles != nil {
			obsolete := e.unrefFiles()
			e.unrefFiles = nil
			if len(obsolete) > 0 {
				e.DeleteFn(obsolete, true)
			}
		}
	}
}

// DB.mu must be held while this is called.
func (e *flushableEntry) readerUnrefLocked() {
	switch v := atomic.AddInt32(&e.readerRefs, -1); {
	case v < 0:
		panic(fmt.Sprintf("pebble: inconsistent reference count: %d", v))
	case v == 0:
		if e.releaseMemAccounting == nil {
			panic("pebble: memtable reservation already released")
		}
		e.releaseMemAccounting()
		e.releaseMemAccounting = nil
		if e.unrefFiles != nil {
			obsolete := e.unrefFiles()
			e.unrefFiles = nil
			if len(obsolete) > 0 {
				e.DeleteFnLocked(obsolete, true)
			}
		}
	}
}

type flushableList []*flushableEntry

// ingestedSSTable is the implementation of the flushable interface for the
// ingesting sstables which are added to the flushable list.
type ingestedSSTable struct {
	files            []*fileMetadata
	cmp              Compare
	split            Split
	newIters         tableNewIters
	newRangeKeyIters keyspan.TableNewSpanIter

	// Since the level slice is immutable, we construct and set it once. It
	// should be safe to read from slice in future reads.
	slice manifest.LevelSlice
	// hasRangeKeys is set on ingestedSSTable construction.
	hasRangeKeys bool
}

func newIngestedSSTableFlushable(
	files []*fileMetadata,
	cmp Compare,
	split Split,
	newIters tableNewIters,
	newRangeKeyIters keyspan.TableNewSpanIter,
) *ingestedSSTable {
	ret := &ingestedSSTable{
		files:            files,
		cmp:              cmp,
		split:            split,
		newIters:         newIters,
		newRangeKeyIters: newRangeKeyIters,
		// slice is immutable and can be set once and used many times.
		slice: manifest.NewLevelSliceKeySorted(cmp, files),
	}

	for _, f := range files {
		if f.HasRangeKeys {
			ret.hasRangeKeys = true
			break
		}
	}

	return ret
}

func (s *ingestedSSTable) newIter(o *IterOptions) internalIterator {
	var opts IterOptions
	if o != nil {
		opts = *o
	}
	// TODO(bananabrick): The manifest.Level in newLevelIter is only used for
	// logging. Update the manifest.Level encoding to account for levels which
	// aren't truly levels in the lsm. Right now, the encoding only supports
	// L0 sublevels, and the rest of the levels in the lsm.
	return newLevelIter(
		opts, s.cmp, s.split, s.newIters, s.slice.Iter(), manifest.Level(0), nil,
	)
}

func (s *ingestedSSTable) newFlushIter(o *IterOptions, bytesFlushed *uint64) internalIterator {
	// newFlushIter is only used for writing memtables to disk as sstables.
	// Since ingested sstables are already present on disk, they don't need to
	// make use of a flush iter.
	panic("pebble: not implemented")
}

func (s *ingestedSSTable) newRangeDelIter(_ *IterOptions) keyspan.FragmentIterator {
	return keyspan.NewLevelIter(
		keyspan.SpanIterOptions{}, s.cmp,
		s.newRangeKeyIters, s.slice.Iter(), manifest.Level(0),
		manifest.KeyTypePoint,
	)
}

func (s *ingestedSSTable) newRangeKeyIter(o *IterOptions) keyspan.FragmentIterator {
	if !s.containsRangeKeys() {
		return nil
	}

	return keyspan.NewLevelIter(
		keyspan.SpanIterOptions{}, s.cmp, s.newRangeKeyIters, s.slice.Iter(),
		manifest.Level(0), manifest.KeyTypeRange,
	)
}

func (s *ingestedSSTable) containsRangeKeys() bool {
	return s.hasRangeKeys
}

func (s *ingestedSSTable) inuseBytes() uint64 {
	// inuseBytes is only used when memtables are flushed to disk as sstables.
	panic("pebble: not implemented")
}

func (s *ingestedSSTable) totalBytes() uint64 {
	// We don't allocate additional bytes for the ingestedSSTable flushable.
	return 0
}

func (s *ingestedSSTable) readyForFlush() bool {
	// ingestedSSTable should always be ready to flush. However, note that
	// memtables beneath the ingested sstables in the memtable queue must
	// be flushed before an ingestedSSTable flushable can be flushed. This is
	// because the ingested sstables need an updated view of the Version to
	// determine where to place the files in the lsm.
	return true
}
