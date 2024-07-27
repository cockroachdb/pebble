// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/keyspan/keyspanimpl"
	"github.com/cockroachdb/pebble/internal/manifest"
)

// flushable defines the interface for immutable memtables.
type flushable interface {
	newIter(o *IterOptions) internalIterator
	newFlushIter(o *IterOptions) internalIterator
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
	UserKeyBounds() base.UserKeyBounds
}

var _ bounded = (*fileMetadata)(nil)
var _ bounded = KeyRange{}

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
	logSeqNum base.SeqNum
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
	// files are non-overlapping and ordered (according to their bounds).
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
	if invariants.Enabled {
		for i := 1; i < len(files); i++ {
			prev := files[i-1].UserKeyBounds()
			this := files[i].UserKeyBounds()
			if prev.End.IsUpperBoundFor(comparer.Compare, this.Start) {
				panic(errors.AssertionFailedf("ingested flushable files overlap: %s %s", prev, this))
			}
		}
	}
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
	return newLevelIter(
		context.Background(), opts, s.comparer, s.newIters, s.slice.Iter(), manifest.FlushableIngestsLayer(),
		internalIterOpts{},
	)
}

// newFlushIter is part of the flushable interface.
func (s *ingestedFlushable) newFlushIter(*IterOptions) internalIterator {
	// newFlushIter is only used for writing memtables to disk as sstables.
	// Since ingested sstables are already present on disk, they don't need to
	// make use of a flush iter.
	panic("pebble: not implemented")
}

func (s *ingestedFlushable) constructRangeDelIter(
	ctx context.Context, file *manifest.FileMetadata, _ keyspan.SpanIterOptions,
) (keyspan.FragmentIterator, error) {
	iters, err := s.newIters(ctx, file, nil, internalIterOpts{}, iterRangeDeletions)
	if err != nil {
		return nil, err
	}
	return iters.RangeDeletion(), nil
}

// newRangeDelIter is part of the flushable interface.
// TODO(bananabrick): Using a level iter instead of a keyspan level iter to
// surface range deletes is more efficient.
//
// TODO(sumeer): *IterOptions are being ignored, so the index block load for
// the point iterator in constructRangeDeIter is not tracked.
func (s *ingestedFlushable) newRangeDelIter(_ *IterOptions) keyspan.FragmentIterator {
	return keyspanimpl.NewLevelIter(
		context.TODO(),
		keyspan.SpanIterOptions{}, s.comparer.Compare,
		s.constructRangeDelIter, s.slice.Iter(), manifest.FlushableIngestsLayer(),
		manifest.KeyTypePoint,
	)
}

// newRangeKeyIter is part of the flushable interface.
func (s *ingestedFlushable) newRangeKeyIter(o *IterOptions) keyspan.FragmentIterator {
	if !s.containsRangeKeys() {
		return nil
	}

	return keyspanimpl.NewLevelIter(
		context.TODO(),
		keyspan.SpanIterOptions{}, s.comparer.Compare, s.newRangeKeyIters,
		s.slice.Iter(), manifest.FlushableIngestsLayer(), manifest.KeyTypeRange,
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
	for _, b := range bounded {
		if s.anyFileOverlaps(b.UserKeyBounds()) {
			// Some file overlaps in key boundaries. The file doesn't necessarily
			// contain any keys within the key range, but we would need to perform I/O
			// to know for sure. The flushable interface dictates that we're not
			// permitted to perform I/O here, so err towards assuming overlap.
			if !fn(b) {
				return
			}
		}
	}
}

// anyFileBoundsOverlap returns true if there is at least a file in s.files with
// bounds that overlap the given bounds.
func (s *ingestedFlushable) anyFileOverlaps(bounds base.UserKeyBounds) bool {
	// Note that s.files are non-overlapping and sorted.
	for _, f := range s.files {
		fileBounds := f.UserKeyBounds()
		if !fileBounds.End.IsUpperBoundFor(s.comparer.Compare, bounds.Start) {
			// The file ends before the bounds start. Go to the next file.
			continue
		}
		if !bounds.End.IsUpperBoundFor(s.comparer.Compare, fileBounds.Start) {
			// The file starts after the bounds end. There is no overlap, and
			// further files will not overlap either (the files are sorted).
			return false
		}
		// There is overlap. Note that UserKeyBounds.Overlaps() performs exactly the
		// checks above.
		return true
	}
	return false
}

// computePossibleOverlapsGenericImpl is an implementation of the flushable
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
	rangeKeyIter := f.newRangeKeyIter(nil)
	for _, b := range bounded {
		overlap, err := determineOverlapAllIters(cmp, b.UserKeyBounds(), iter, rangeDelIter, rangeKeyIter)
		if invariants.Enabled && err != nil {
			panic(errors.AssertionFailedf("expected iterator to be infallible: %v", err))
		}
		if overlap {
			if !fn(b) {
				break
			}
		}
	}

	if iter != nil {
		if err := iter.Close(); err != nil {
			// This implementation must be used in circumstances where
			// reading through the iterator is infallible.
			panic(err)
		}
	}
	if rangeDelIter != nil {
		rangeDelIter.Close()
	}
	if rangeKeyIter != nil {
		rangeKeyIter.Close()
	}
}

// determineOverlapAllIters checks for overlap in a point iterator, range
// deletion iterator and range key iterator.
func determineOverlapAllIters(
	cmp base.Compare,
	bounds base.UserKeyBounds,
	pointIter base.InternalIterator,
	rangeDelIter, rangeKeyIter keyspan.FragmentIterator,
) (bool, error) {
	if pointIter != nil {
		if pointOverlap, err := determineOverlapPointIterator(cmp, bounds, pointIter); pointOverlap || err != nil {
			return pointOverlap, err
		}
	}
	if rangeDelIter != nil {
		if rangeDelOverlap, err := determineOverlapKeyspanIterator(cmp, bounds, rangeDelIter); rangeDelOverlap || err != nil {
			return rangeDelOverlap, err
		}
	}
	if rangeKeyIter != nil {
		return determineOverlapKeyspanIterator(cmp, bounds, rangeKeyIter)
	}
	return false, nil
}

func determineOverlapPointIterator(
	cmp base.Compare, bounds base.UserKeyBounds, iter internalIterator,
) (bool, error) {
	kv := iter.SeekGE(bounds.Start, base.SeekGEFlagsNone)
	if kv == nil {
		return false, iter.Error()
	}
	return bounds.End.IsUpperBoundForInternalKey(cmp, kv.K), nil
}

func determineOverlapKeyspanIterator(
	cmp base.Compare, bounds base.UserKeyBounds, iter keyspan.FragmentIterator,
) (bool, error) {
	// NB: The spans surfaced by the fragment iterator are non-overlapping.
	span, err := iter.SeekGE(bounds.Start)
	if err != nil {
		return false, err
	}
	for ; span != nil; span, err = iter.Next() {
		if !bounds.End.IsUpperBoundFor(cmp, span.Start) {
			// The span starts after our bounds.
			return false, nil
		}
		if !span.Empty() {
			return true, nil
		}
	}
	return false, err
}
