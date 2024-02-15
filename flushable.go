// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"slices"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/keyspan/keyspanimpl"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
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
	//
	// TODO(aadityas,jackson): We'll need to do something about this (and
	// logSize) for entries corresponding to bufferedSSTables since there may be
	// multiple associated log nums.
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
}

func newIngestedFlushable(
	files []*fileMetadata,
	comparer *Comparer,
	newIters tableNewIters,
	newRangeKeyIters keyspanimpl.TableNewSpanIter,
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

func newFlushableBufferedSSTables(
	comparer *base.Comparer,
	metas []*fileMetadata,
	ro sstable.ReaderOptions,
	b *bufferedSSTables,
	extraReaderOpts ...sstable.ReaderOption,
) (*flushableBufferedSSTables, error) {
	if len(metas) != len(b.finished) {
		panic(errors.AssertionFailedf("metadata for %d files provided, but buffered %d files", len(metas), len(b.finished)))
	}
	f := &flushableBufferedSSTables{
		comparer: comparer,
		ls:       manifest.NewLevelSliceKeySorted(comparer.Compare, metas),
		metas:    metas,
		readers:  make([]*sstable.Reader, len(b.finished)),
	}
	// NB: metas and b.finished are parallel slices.
	for i := range b.finished {
		if b.finished[i].fileNum != base.PhysicalTableDiskFileNum(metas[i].FileNum) {
			panic(errors.AssertionFailedf("file metas and file buffers in mismatched order"))
		}
		f.anyRangeKeys = f.anyRangeKeys || metas[i].HasRangeKeys
		f.size += metas[i].Size
		readable := objstorageprovider.BytesReadable(b.finished[i].buf)
		var err error
		f.readers[i], err = sstable.NewReader(readable, ro, extraReaderOpts...)
		if err != nil {
			return nil, err
		}
	}
	return f, nil
}

// flushableBufferedSSTables holds a set of in-memory sstables produced by a
// flush. Buffering flushed state reduces write amplification by making it more
// likely that we're able to drop KVs before they reach disk.
type flushableBufferedSSTables struct {
	comparer *base.Comparer
	ls       manifest.LevelSlice
	metas    []*fileMetadata
	readers  []*sstable.Reader
	// size is the size, in bytes, of all the buffered sstables.
	size uint64
	// anyRangeKeys indicates whether any of the sstables contain any range
	// keys.
	anyRangeKeys bool
}

var (
	// Assert that *flushableBufferedSSTables implements the flushable
	// interface.
	_ flushable = (*flushableBufferedSSTables)(nil)
)

// newIters implements the tableNewIters function signature. Ordinarily this
// function is provided by the table cache. Flushable buffered sstables are not
// opened through the table cache since they're not present on the real
// filesystem and do not require use of file descriptors. Instead, the
// flushableBufferedSSTables keeps sstable.Readers for all the buffered sstables
// open, and this newIters func uses them to construct iterators.
func (b *flushableBufferedSSTables) newIters(
	ctx context.Context,
	file *manifest.FileMetadata,
	opts *IterOptions,
	internalOpts internalIterOpts,
	kinds iterKinds,
) (iterSet, error) {
	var r *sstable.Reader
	for i := range b.metas {
		if b.metas[i].FileNum == file.FileNum {
			r = b.readers[i]
			break
		}
	}
	if r == nil {
		return iterSet{}, errors.Newf("file %s not found among flushable buffered sstables", file.FileNum)
	}
	var iters iterSet
	var err error
	if kinds.RangeKey() && file.HasRangeKeys {
		iters.rangeKey, err = r.NewRawRangeKeyIter()
	}
	if kinds.RangeDeletion() && file.HasPointKeys && err == nil {
		iters.rangeDeletion, err = r.NewRawRangeDelIter()
	}
	if kinds.Point() && err == nil {
		// TODO(aadityas,jackson): We could support block-property filtering
		// within the in-memory sstables, but it's unclear it's worth the code
		// complexity. We expect these blocks to be hit frequently, and the cost
		// of loading them is less since they're already in main memory.
		var tableFormat sstable.TableFormat
		if tableFormat, err = r.TableFormat(); err != nil {
			return iterSet{}, err
		}
		var rp sstable.ReaderProvider
		if tableFormat >= sstable.TableFormatPebblev3 && r.Properties.NumValueBlocks > 0 {
			// NB: We can return a fixedReaderProvider because the Readers for
			// these in-memory sstables are guaranteed to be open until the
			// readState is obsolete which will only occur when all iterators
			// have closed.
			rp = &fixedReaderProvider{r}
		}
		var categoryAndQoS sstable.CategoryAndQoS
		if internalOpts.bytesIterated != nil {
			iters.point, err = r.NewCompactionIter(
				internalOpts.bytesIterated, categoryAndQoS, nil /* statsCollector */, rp,
				internalOpts.bufferPool)
		} else {
			iters.point, err = r.NewIterWithBlockPropertyFiltersAndContextEtc(
				ctx, opts.GetLowerBound(), opts.GetUpperBound(),
				nil /* filterer */, false /* hideObsoletePoints */, true, /* useFilter */
				internalOpts.stats, categoryAndQoS, nil /* stats collector */, rp)
		}
	}
	if err != nil {
		iters.CloseAll()
		return iterSet{}, err
	}
	return iters, nil
}

// newIter is part of the flushable interface.
func (b *flushableBufferedSSTables) newIter(o *IterOptions) internalIterator {
	var opts IterOptions
	if o != nil {
		opts = *o
	}
	// TODO(jackson): The manifest.Level in newLevelIter is only used for
	// logging. Update the manifest.Level encoding to account for levels which
	// aren't truly levels in the lsm. Right now, the encoding only supports
	// L0 sublevels, and the rest of the levels in the lsm.
	return newLevelIter(
		context.Background(), opts, b.comparer, b.newIters, b.ls.Iter(), manifest.Level(0),
		internalIterOpts{},
	)
}

// newFlushIter is part of the flushable interface.
func (b *flushableBufferedSSTables) newFlushIter(
	o *IterOptions, bytesFlushed *uint64,
) internalIterator {
	var opts IterOptions
	if o != nil {
		opts = *o
	}
	// TODO(jackson): The manifest.Level in newLevelIter is only used for
	// logging. Update the manifest.Level encoding to account for levels which
	// aren't truly levels in the lsm. Right now, the encoding only supports
	// L0 sublevels, and the rest of the levels in the lsm.
	return newLevelIter(
		context.Background(), opts, b.comparer, b.newIters, b.ls.Iter(), manifest.Level(0),
		internalIterOpts{bytesIterated: bytesFlushed},
	)
}

// constructRangeDelIter implements keyspanimpl.TableNewSpanIter.
func (b *flushableBufferedSSTables) constructRangeDelIter(
	file *manifest.FileMetadata, _ keyspan.SpanIterOptions,
) (keyspan.FragmentIterator, error) {
	iters, err := b.newIters(context.Background(), file, nil, internalIterOpts{}, iterRangeDeletions)
	return iters.RangeDeletion(), err
}

// newRangeDelIter is part of the flushable interface.
func (b *flushableBufferedSSTables) newRangeDelIter(_ *IterOptions) keyspan.FragmentIterator {
	return keyspanimpl.NewLevelIter(
		keyspan.SpanIterOptions{}, b.comparer.Compare,
		b.constructRangeDelIter, b.ls.Iter(), manifest.Level(0),
		manifest.KeyTypePoint,
	)
}

// constructRangeKeyIter implements keyspanimpl.TableNewSpanIter.
func (b *flushableBufferedSSTables) constructRangeKeyIter(
	file *manifest.FileMetadata, _ keyspan.SpanIterOptions,
) (keyspan.FragmentIterator, error) {
	iters, err := b.newIters(context.Background(), file, nil, internalIterOpts{}, iterRangeKeys)
	if err != nil {
		return nil, err
	}
	return iters.RangeKey(), nil
}

// newRangeKeyIter is part of the flushable interface.
func (b *flushableBufferedSSTables) newRangeKeyIter(o *IterOptions) keyspan.FragmentIterator {
	if !b.containsRangeKeys() {
		return nil
	}
	return keyspanimpl.NewLevelIter(
		keyspan.SpanIterOptions{}, b.comparer.Compare, b.constructRangeKeyIter,
		b.ls.Iter(), manifest.Level(0), manifest.KeyTypeRange,
	)
}

// containsRangeKeys is part of the flushable interface.
func (b *flushableBufferedSSTables) containsRangeKeys() bool { return b.anyRangeKeys }

// inuseBytes is part of the flushable interface.
func (b *flushableBufferedSSTables) inuseBytes() uint64 { return b.size }

// totalBytes is part of the flushable interface.
func (b *flushableBufferedSSTables) totalBytes() uint64 { return b.size }

// readyForFlush is part of the flushable interface.
func (b *flushableBufferedSSTables) readyForFlush() bool {
	// Buffered sstables are always ready for flush; they're immutable.
	return true
}

// computePossibleOverlaps is part of the flushable interface.
func (b *flushableBufferedSSTables) computePossibleOverlaps(
	fn func(bounded) shouldContinue, bounded ...bounded,
) {
	computePossibleOverlapsGenericImpl[*flushableBufferedSSTables](b, b.comparer.Compare, fn, bounded)
}

// bufferedSSTables implements the objectCreator interface and is used by a
// flush to buffer sstables into memory. When the flush is complete, the
// buffered sstables are either flushed to durable storage or moved into a
// flushableBufferedSSTables that's linked into the flushable queue.
//
// The bufferedSSTables implementation of objectCreator requires that only one
// created object may be open at a time. If violated, Create will panic.
type bufferedSSTables struct {
	// curr is a byte buffer used to accumulate the writes of the current
	// sstable when *bufferedSSTables is used as a writable.
	curr bytes.Buffer
	// currFileNum holds the file number assigned to the sstable being
	// constructed in curr.
	currFileNum base.DiskFileNum
	// finished holds the set of previously written and finished sstables.
	finished []bufferedSSTable
	// objectIsOpen is true if the bufferedSSTables is currently being used as a
	// Writable.
	objectIsOpen bool
}

// A bufferedSSTable holds a single, serialized sstable and a corresponding file
// number.
type bufferedSSTable struct {
	fileNum base.DiskFileNum
	buf     []byte
}

// init initializes the bufferedSSTables.
func (b *bufferedSSTables) init(targetFileSize int) {
	b.curr.Grow(targetFileSize)
}

// Assert that *bufferedSSTables implements the objectCreator interface.
var _ objectCreator = (*bufferedSSTables)(nil)

// Create implements the objectCreator interface.
func (b *bufferedSSTables) Create(
	ctx context.Context,
	fileType base.FileType,
	fileNum base.DiskFileNum,
	opts objstorage.CreateOptions,
) (w objstorage.Writable, meta objstorage.ObjectMetadata, err error) {
	// The bufferedSSTables implementation depends on only one writable being
	// open at a time. The *bufferedSSTables itself is used as the
	// implementation of both the objectCreator interface and the
	// objstorage.Writable interface. We guard against misuse by verifying that
	// there is no object currently open.
	if b.objectIsOpen {
		panic("bufferedSSTables used with concurrent open files")
	}
	b.objectIsOpen = true
	b.currFileNum = fileNum
	return b, objstorage.ObjectMetadata{
		DiskFileNum: fileNum,
		FileType:    fileType,
	}, nil
}

// Remove implements the objectCreator interface.
func (b *bufferedSSTables) Remove(fileType base.FileType, FileNum base.DiskFileNum) error {
	// Remove is called when a flush/compaction fails. We could zero out the
	// buffers to reclaim the memory marinally sooner, but it doesn't seem worth
	// the added complexity. Just do nothing; the bufferedSSTables object itself
	// should be reclaimed soon enough.
	return nil
}

// Sync implements the objectCreator interface.
func (b *bufferedSSTables) Sync() error {
	return nil
}

// Assert that bufferedSSTables implements objstorage.Writable.
//
// A flush writes files sequentially, so the bufferedSSTables type implements
// Writable directly, serving as the destination for writes across all sstables
// written by the flush.
var _ objstorage.Writable = (*bufferedSSTables)(nil)

// Finish implements objstorage.Writable.
func (b *bufferedSSTables) Write(p []byte) error {
	_, err := b.curr.Write(p)
	b.curr.Reset()
	return err
}

// Finish implements objstorage.Writable.
func (b *bufferedSSTables) Finish() error {
	if !b.objectIsOpen {
		panic("bufferedSSTables.Finish() invoked when no object is open")
	}
	b.finished = append(b.finished, bufferedSSTable{
		fileNum: b.currFileNum,
		buf:     slices.Clone(b.curr.Bytes()),
	})
	b.curr.Reset()
	b.objectIsOpen = false
	return nil
}

// Abort implements objstorage.Writable.
func (b *bufferedSSTables) Abort() {
	b.curr.Reset()
	b.objectIsOpen = false
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

type fixedReaderProvider struct {
	*sstable.Reader
}

var _ sstable.ReaderProvider = (*fixedReaderProvider)(nil)

// GetReader implements sstable.ReaderProvider.
//
// Note that currently the Reader returned here is only used to read value
// blocks.
func (p *fixedReaderProvider) GetReader() (*sstable.Reader, error) {
	return p.Reader, nil
}

// Close implements sstable.ReaderProvider.
func (p *fixedReaderProvider) Close() {}
