// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/keyspan/keyspanimpl"
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
	// computePossibleOverlaps determines whether the flushable's keys overlap
	// with the bounds of any of the provided bounded items. If an item overlaps
	// or might overlap but it's not possible to determine overlap cheaply,
	// computePossibleOverlaps invokes the provided function with the object
	// that might overlap. computePossibleOverlaps must not perform any I/O and
	// implementations should invoke the provided function for items that would
	// require I/O to determine overlap.
	computePossibleOverlaps(overlaps func(bounded) shouldContinue, bounded ...bounded)
}

type shouldContinue bool

const (
	continueIteration shouldContinue = true
	stopIteration                    = false
)

type bounded interface {
	// InternalKeyBounds returns a start key and an end key. Both bounds are
	// inclusive.
	InternalKeyBounds() (InternalKey, InternalKey)
}

func sliceAsBounded[B bounded](s []B) []bounded {
	ret := make([]bounded, len(s))
	for i := 0; i < len(s); i++ {
		ret[i] = s[i]
	}
	return ret
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
	logNum base.DiskFileNum
	// logSize is the size in bytes of the associated WAL. Protected by DB.mu.
	logSize uint64
	// The current logSeqNum at the time the memtable was created. This is
	// guaranteed to be less than or equal to any seqnum stored in the memtable.
	logSeqNum uint64
	// readerRefs tracks the read references on the flushable. The two sources of
	// reader references are DB.mu.mem.queue and readState.memtables. The memory
	// reserved by the flushable in the cache is released when the reader refs
	// drop to zero. If the flushable is referencing sstables, then the file
	// refount is also decreased once the reader refs drops to 0. If the
	// flushable is a memTable, when the reader refs drops to zero, the writer
	// refs will already be zero because the memtable will have been flushed and
	// that only occurs once the writer refs drops to zero.
	readerRefs atomic.Int32
	// Closure to invoke to release memory accounting.
	releaseMemAccounting func()
	// unrefFiles, if not nil, should be invoked to decrease the ref count of
	// files which are backing the flushable.
	unrefFiles func() []*fileBacking
	// deleteFnLocked should be called if the caller is holding DB.mu.
	deleteFnLocked func(obsolete []*fileBacking)
	// deleteFn should be called if the caller is not holding DB.mu.
	deleteFn func(obsolete []*fileBacking)
}

func (e *flushableEntry) readerRef() {
	switch v := e.readerRefs.Add(1); {
	case v <= 1:
		panic(fmt.Sprintf("pebble: inconsistent reference count: %d", v))
	}
}

// db.mu must not be held when this is called.
func (e *flushableEntry) readerUnref(deleteFiles bool) {
	e.readerUnrefHelper(deleteFiles, e.deleteFn)
}

// db.mu must be held when this is called.
func (e *flushableEntry) readerUnrefLocked(deleteFiles bool) {
	e.readerUnrefHelper(deleteFiles, e.deleteFnLocked)
}

func (e *flushableEntry) readerUnrefHelper(
	deleteFiles bool, deleteFn func(obsolete []*fileBacking),
) {
	switch v := e.readerRefs.Add(-1); {
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
			if deleteFiles {
				deleteFn(obsolete)
			}
		}
	}
}

type flushableList []*flushableEntry

// ingestedFlushable is the implementation of the flushable interface for the
// ingesting sstables which are added to the flushable list.
type ingestedFlushable struct {
	files            []physicalMeta
	comparer         *Comparer
	newIters         tableNewIters
	newRangeKeyIters keyspanimpl.TableNewSpanIter

	// Since the level slice is immutable, we construct and set it once. It
	// should be safe to read from slice in future reads.
	slice manifest.LevelSlice
	// hasRangeKeys is set on ingestedFlushable construction.
	hasRangeKeys bool
	// exciseSpan is populated if an excise operation should be performed during
	// flush.
	exciseSpan KeyRange
}

func newIngestedFlushable(
	files []*fileMetadata,
	comparer *Comparer,
	newIters tableNewIters,
	newRangeKeyIters keyspanimpl.TableNewSpanIter,
	exciseSpan KeyRange,
) *ingestedFlushable {
	var physicalFiles []physicalMeta
	var hasRangeKeys bool
	for _, f := range files {
		if f.HasRangeKeys {
			hasRangeKeys = true
		}
		physicalFiles = append(physicalFiles, f.PhysicalMeta())
	}

	ret := &ingestedFlushable{
		files:            physicalFiles,
		comparer:         comparer,
		newIters:         newIters,
		newRangeKeyIters: newRangeKeyIters,
		// slice is immutable and can be set once and used many times.
		slice:        manifest.NewLevelSliceKeySorted(comparer.Compare, files),
		hasRangeKeys: hasRangeKeys,
		exciseSpan:   exciseSpan,
	}

	return ret
}

// TODO(sumeer): ingestedFlushable iters also need to plumb context for
// tracing.

// newIter is part of the flushable interface.
func (s *ingestedFlushable) newIter(o *IterOptions) internalIterator {
	var opts IterOptions
	if o != nil {
		opts = *o
	}
	// TODO(bananabrick): The manifest.Level in newLevelIter is only used for
	// logging. Update the manifest.Level encoding to account for levels which
	// aren't truly levels in the lsm. Right now, the encoding only supports
	// L0 sublevels, and the rest of the levels in the lsm.
	return newLevelIter(
		context.Background(), opts, s.comparer, s.newIters, s.slice.Iter(), manifest.Level(0),
		internalIterOpts{},
	)
}

// newFlushIter is part of the flushable interface.
func (s *ingestedFlushable) newFlushIter(o *IterOptions, bytesFlushed *uint64) internalIterator {
	// newFlushIter is only used for writing memtables to disk as sstables.
	// Since ingested sstables are already present on disk, they don't need to
	// make use of a flush iter.
	panic("pebble: not implemented")
}

func (s *ingestedFlushable) constructRangeDelIter(
	file *manifest.FileMetadata, _ keyspan.SpanIterOptions,
) (keyspan.FragmentIterator, error) {
	// Note that the keyspan level iter expects a non-nil iterator to be
	// returned even if there is an error. So, we return the emptyKeyspanIter.
	iter, rangeDelIter, err := s.newIters.TODO(context.Background(), file, nil, internalIterOpts{})
	if err != nil {
		return emptyKeyspanIter, err
	}
	iter.Close()
	if rangeDelIter == nil {
		return emptyKeyspanIter, nil
	}
	return rangeDelIter, nil
}

// newRangeDelIter is part of the flushable interface.
// TODO(bananabrick): Using a level iter instead of a keyspan level iter to
// surface range deletes is more efficient.
//
// TODO(sumeer): *IterOptions are being ignored, so the index block load for
// the point iterator in constructRangeDeIter is not tracked.
func (s *ingestedFlushable) newRangeDelIter(_ *IterOptions) keyspan.FragmentIterator {
	return keyspanimpl.NewLevelIter(
		keyspan.SpanIterOptions{}, s.comparer.Compare,
		s.constructRangeDelIter, s.slice.Iter(), manifest.Level(0),
		manifest.KeyTypePoint,
	)
}

// newRangeKeyIter is part of the flushable interface.
func (s *ingestedFlushable) newRangeKeyIter(o *IterOptions) keyspan.FragmentIterator {
	if !s.containsRangeKeys() {
		return nil
	}

	return keyspanimpl.NewLevelIter(
		keyspan.SpanIterOptions{}, s.comparer.Compare, s.newRangeKeyIters,
		s.slice.Iter(), manifest.Level(0), manifest.KeyTypeRange,
	)
}

// containsRangeKeys is part of the flushable interface.
func (s *ingestedFlushable) containsRangeKeys() bool {
	return s.hasRangeKeys
}

// inuseBytes is part of the flushable interface.
func (s *ingestedFlushable) inuseBytes() uint64 {
	// inuseBytes is only used when memtables are flushed to disk as sstables.
	panic("pebble: not implemented")
}

// totalBytes is part of the flushable interface.
func (s *ingestedFlushable) totalBytes() uint64 {
	// We don't allocate additional bytes for the ingestedFlushable.
	return 0
}

// readyForFlush is part of the flushable interface.
func (s *ingestedFlushable) readyForFlush() bool {
	// ingestedFlushable should always be ready to flush. However, note that
	// memtables before the ingested sstables in the memtable queue must be
	// flushed before an ingestedFlushable can be flushed. This is because the
	// ingested sstables need an updated view of the Version to
	// determine where to place the files in the lsm.
	return true
}

// computePossibleOverlaps is part of the flushable interface.
func (s *ingestedFlushable) computePossibleOverlaps(
	fn func(bounded) shouldContinue, bounded ...bounded,
) {
	for i := range bounded {
		smallest, largest := bounded[i].InternalKeyBounds()
		for j := 0; j < len(s.files); j++ {
			if sstableKeyCompare(s.comparer.Compare, s.files[j].Largest, smallest) >= 0 {
				// This file's largest key is larger than smallest. Either the
				// file overlaps the bounds, or it lies strictly after the
				// bounds. Either way we can stop iterating since the files are
				// sorted. But first, determine if there's overlap and call fn
				// if necessary.
				if sstableKeyCompare(s.comparer.Compare, s.files[j].Smallest, largest) <= 0 {
					// The file overlaps in key boundaries. The file doesn't necessarily
					// contain any keys within the key range, but we would need to
					// perform I/O to know for sure. The flushable interface dictates
					// that we're not permitted to perform I/O here, so err towards
					// assuming overlap.
					if !fn(bounded[i]) {
						return
					}
				}
				break
			}
		}
	}
}

// computePossibleOverlapsGenericImpl is an implemention of the flushable
// interface's computePossibleOverlaps function for flushable implementations
// with only in-memory state that do not have special requirements and should
// read through the ordinary flushable iterators.
//
// This function must only be used with implementations that are infallible (eg,
// memtable iterators) and will panic if an error is encountered.
func computePossibleOverlapsGenericImpl[F flushable](
	f F, cmp Compare, fn func(bounded) shouldContinue, bounded []bounded,
) {
	iter := f.newIter(nil)
	rangeDelIter := f.newRangeDelIter(nil)
	rkeyIter := f.newRangeKeyIter(nil)
	for _, b := range bounded {
		s, l := b.InternalKeyBounds()
		kr := internalKeyRange{s, l}
		if overlapWithIterator(iter, &rangeDelIter, rkeyIter, kr, cmp) {
			if !fn(b) {
				break
			}
		}
	}

	for _, c := range [3]io.Closer{iter, rangeDelIter, rkeyIter} {
		if c != nil {
			if err := c.Close(); err != nil {
				// This implementation must be used in circumstances where
				// reading through the iterator is infallible.
				panic(err)
			}
		}
	}
}
