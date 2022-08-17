// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"sort"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/sstable"
)

// ExternalIterOption provide an interface to specify open-time options to
// NewExternalIter.
type ExternalIterOption interface {
	// iterApply is called on the iterator during opening in order to set internal
	// parameters.
	iterApply(*Iterator)
	// readerOptions returns any reader options added by this iter option.
	readerOptions() []sstable.ReaderOption
}

type externalIterReaderOptions struct {
	opts []sstable.ReaderOption
}

func (e *externalIterReaderOptions) iterApply(iterator *Iterator) {
	// Do nothing.
}

func (e *externalIterReaderOptions) readerOptions() []sstable.ReaderOption {
	return e.opts
}

// ExternalIterReaderOptions returns an ExternalIterOption that specifies
// sstable.ReaderOptions to be applied on sstable readers in NewExternalIter.
func ExternalIterReaderOptions(opts ...sstable.ReaderOption) ExternalIterOption {
	return &externalIterReaderOptions{opts: opts}
}

// ExternalIterForwardOnly is an ExternalIterOption that specifies this iterator
// will only be used for forward positioning operations (First, SeekGE, Next).
// This could enable optimizations that take advantage of this invariant.
// Behaviour when a reverse positioning operation is done on an iterator
// opened with this option is unpredictable, though in most cases it should.
type ExternalIterForwardOnly struct{}

func (e ExternalIterForwardOnly) iterApply(iter *Iterator) {
	iter.forwardOnly = true
}

func (e ExternalIterForwardOnly) readerOptions() []sstable.ReaderOption {
	return nil
}

// NewExternalIter takes an input 2d array of sstable files which may overlap
// across subarrays but not within a subarray (at least as far as points are
// concerned; range keys are allowed to overlap arbitrarily even within a
// subarray), and returns an Iterator over the merged contents of the sstables.
// Input sstables may contain point keys, range keys, range deletions, etc. The
// input files slice must be sorted in reverse chronological ordering. A key in a
// file at a lower index subarray will shadow a key with an identical user key
// contained within a file at a higher index subarray. Each subarray must be
// sorted in internal key order, where lower index files contain keys that sort
// left of files with higher indexes.
//
// Input sstables must only contain keys with the zero sequence number.
//
// Iterators constructed through NewExternalIter do not support all iterator
// options, including block-property and table filters. NewExternalIter errors
// if an incompatible option is set.
func NewExternalIter(
	o *Options,
	iterOpts *IterOptions,
	files [][]sstable.ReadableFile,
	extraOpts ...ExternalIterOption,
) (it *Iterator, err error) {
	if iterOpts != nil {
		if err := validateExternalIterOpts(iterOpts); err != nil {
			return nil, err
		}
	}

	var readers [][]*sstable.Reader

	// Ensure we close all the opened readers if we error out.
	defer func() {
		if err != nil {
			for i := range readers {
				for j := range readers[i] {
					_ = readers[i][j].Close()
				}
			}
		}
	}()
	seqNumOffset := 0
	var extraReaderOpts []sstable.ReaderOption
	for i := range extraOpts {
		extraReaderOpts = append(extraReaderOpts, extraOpts[i].readerOptions()...)
	}
	for _, levelFiles := range files {
		seqNumOffset += len(levelFiles)
	}
	for _, levelFiles := range files {
		var subReaders []*sstable.Reader
		seqNumOffset -= len(levelFiles)
		subReaders, err = openExternalTables(o, levelFiles, seqNumOffset, o.MakeReaderOptions(), extraReaderOpts...)
		readers = append(readers, subReaders)
	}
	if err != nil {
		return nil, err
	}

	buf := iterAllocPool.Get().(*iterAlloc)
	dbi := &buf.dbi
	*dbi = Iterator{
		alloc:               buf,
		cmp:                 o.Comparer.Compare,
		equal:               o.equal(),
		merge:               o.Merger.Merge,
		split:               o.Comparer.Split,
		comparer:            o.Comparer,
		readState:           nil,
		keyBuf:              buf.keyBuf,
		prefixOrFullSeekKey: buf.prefixOrFullSeekKey,
		boundsBuf:           buf.boundsBuf,
		batch:               nil,
		// Add the readers to the Iterator so that Close closes them, and
		// SetOptions can re-construct iterators from them.
		externalReaders: readers,
		newIters: func(f *manifest.FileMetadata, opts *IterOptions, internalOpts internalIterOpts) (internalIterator, keyspan.FragmentIterator, error) {
			// NB: External iterators are currently constructed without any
			// `levelIters`. newIters should never be called. When we support
			// organizing multiple non-overlapping files into a single level
			// (see TODO below), we'll need to adjust this tableNewIters
			// implementation to open iterators by looking up f in a map
			// of readers indexed by *fileMetadata.
			panic("unreachable")
		},
		seqNum: base.InternalKeySeqNumMax,
	}
	if iterOpts != nil {
		dbi.opts = *iterOpts
		dbi.saveBounds(iterOpts.LowerBound, iterOpts.UpperBound)
	}
	for i := range extraOpts {
		extraOpts[i].iterApply(dbi)
	}
	finishInitializingExternal(dbi)
	return dbi, nil
}

func validateExternalIterOpts(iterOpts *IterOptions) error {
	switch {
	case iterOpts.TableFilter != nil:
		return errors.Errorf("pebble: external iterator: TableFilter unsupported")
	case iterOpts.PointKeyFilters != nil:
		return errors.Errorf("pebble: external iterator: PointKeyFilters unsupported")
	case iterOpts.RangeKeyFilters != nil:
		return errors.Errorf("pebble: external iterator: RangeKeyFilters unsupported")
	case iterOpts.OnlyReadGuaranteedDurable:
		return errors.Errorf("pebble: external iterator: OnlyReadGuaranteedDurable unsupported")
	case iterOpts.UseL6Filters:
		return errors.Errorf("pebble: external iterator: UseL6Filters unsupported")
	}
	return nil
}

func finishInitializingExternal(it *Iterator) {
	// TODO(jackson): In some instances we could generate fewer levels by using
	// L0Sublevels code to organize nonoverlapping files into the same level.
	// This would allow us to use levelIters and keep a smaller set of data and
	// files in-memory. However, it would also require us to identify the bounds
	// of all the files upfront.

	mlevels := it.alloc.mlevels[:0]
	if !it.opts.pointKeys() {
		it.pointIter = emptyIter
	} else if it.pointIter == nil {
		if len(it.externalReaders) > cap(mlevels) {
			mlevels = make([]mergingIterLevel, 0, len(it.externalReaders))
		}
		for _, readers := range it.externalReaders {
			var combinedIters []internalIterator
			for _, r := range readers {
				var (
					rangeDelIter keyspan.FragmentIterator
					pointIter    internalIterator
					err          error
				)
				pointIter, err = r.NewIter(it.opts.LowerBound, it.opts.UpperBound)
				if err == nil {
					rangeDelIter, err = r.NewRawRangeDelIter()
				}
				if err != nil {
					pointIter = &errorIter{err: err}
					rangeDelIter = &errorKeyspanIter{err: err}
				}
				if err == nil && rangeDelIter == nil && pointIter != nil && it.forwardOnly {
					// TODO(bilal): Consider implementing range key pausing in
					// simpleLevelIter so we can reduce mergingIterLevels even more by
					// sending all sstable iterators to combinedIters, not just those
					// corresponding to sstables without range deletes.
					combinedIters = append(combinedIters, pointIter)
					continue
				}
				mlevels = append(mlevels, mergingIterLevel{
					iter:         base.WrapIterWithStats(pointIter),
					rangeDelIter: rangeDelIter,
				})
			}
			if len(combinedIters) > 0 {
				sli := &simpleLevelIter{
					cmp:   it.cmp,
					iters: combinedIters,
				}
				sli.init(it.opts)
				mlevels = append(mlevels, mergingIterLevel{
					iter:         base.WrapIterWithStats(sli),
					rangeDelIter: nil,
				})
			}
		}
		it.alloc.merging.init(&it.opts, it.cmp, it.split, mlevels...)
		it.alloc.merging.snapshot = base.InternalKeySeqNumMax
		it.alloc.merging.elideRangeTombstones = true
		it.pointIter = &it.alloc.merging
	}
	it.iter = it.pointIter

	if it.opts.rangeKeys() {
		it.rangeKeyMasking.init(it, it.cmp, it.split)
		if it.rangeKey == nil {
			it.rangeKey = iterRangeKeyStateAllocPool.Get().(*iteratorRangeKeyState)
			it.rangeKey.init(it.cmp, it.split, &it.opts)
			it.rangeKey.rangeKeyIter = it.rangeKey.iterConfig.Init(
				it.comparer,
				base.InternalKeySeqNumMax,
				it.opts.LowerBound, it.opts.UpperBound,
				&it.hasPrefix, &it.prefixOrFullSeekKey,
			)
			// We could take advantage of the lack of overlaps in range keys within
			// each slice in it.externalReaders, and generate keyspan.LevelIters
			// out of those. However, since range keys are expected to be sparse to
			// begin with, the performance gain might not be significant enough to
			// warrant it.
			//
			// TODO(bilal): Explore adding a simpleRangeKeyLevelIter that does not
			// operate on FileMetadatas (similar to simpleLevelIter), and implements
			// this optimization.
			for _, readers := range it.externalReaders {
				for _, r := range readers {
					if rki, err := r.NewRawRangeKeyIter(); err != nil {
						it.rangeKey.iterConfig.AddLevel(&errorKeyspanIter{err: err})
					} else if rki != nil {
						it.rangeKey.iterConfig.AddLevel(rki)
					}
				}
			}
		}
		it.rangeKey.iiter.Init(it.comparer, it.iter, it.rangeKey.rangeKeyIter, &it.rangeKeyMasking,
			it.opts.LowerBound, it.opts.UpperBound)
		it.iter = &it.rangeKey.iiter
	}
}

func openExternalTables(
	o *Options,
	files []sstable.ReadableFile,
	seqNumOffset int,
	readerOpts sstable.ReaderOptions,
	extraReaderOpts ...sstable.ReaderOption,
) (readers []*sstable.Reader, err error) {
	readers = make([]*sstable.Reader, 0, len(files))
	for i := range files {
		r, err := sstable.NewReader(files[i], readerOpts, extraReaderOpts...)
		if err != nil {
			return readers, err
		}
		// Use the index of the file in files as the sequence number for all of
		// its keys.
		r.Properties.GlobalSeqNum = uint64(len(files) - i + seqNumOffset)
		readers = append(readers, r)
	}
	return readers, err
}

// simpleLevelIter is similar to a levelIter in that it merges the points
// from multiple point iterators that are non-overlapping in the key ranges
// they return. It is only expected to support forward iteration and forward
// regular seeking; reverse iteration and prefix seeking is not supported.
// Intended to be a low-overhead, non-FileMetadata dependent option for
// NewExternalIter. To optimize seeking and forward iteration, it maintains
// two slices of child iterators; one of all iterators, and a subset of it that
// contains just the iterators that contain point keys within the current
// bounds.
//
// Note that this levelIter does not support pausing at file boundaries
// in case of range tombstones in this file that could apply to points outside
// of this file (and outside of this level). This is sufficient for optimizing
// the main use cases of NewExternalIter, however for completeness it would make
// sense to build this pausing functionality in.
type simpleLevelIter struct {
	cmp          Compare
	lowerBound   []byte
	iters        []internalIterator
	filtered     []internalIterator
	firstKeys    [][]byte
	firstKeysBuf []byte
	currentIdx   int
}

// init initializes this simpleLevelIter.
func (s *simpleLevelIter) init(opts IterOptions) {
	s.currentIdx = 0
	s.lowerBound = opts.LowerBound
	s.resetFilteredIters()
}

func (s *simpleLevelIter) resetFilteredIters() {
	s.filtered = s.filtered[:0]
	s.firstKeys = s.firstKeys[:0]
	s.firstKeysBuf = s.firstKeysBuf[:0]
	for i := range s.iters {
		var iterKey *base.InternalKey
		if s.lowerBound != nil {
			iterKey, _ = s.iters[i].SeekGE(s.lowerBound, base.SeekGEFlagsNone)
		} else {
			iterKey, _ = s.iters[i].First()
		}
		if iterKey != nil {
			s.filtered = append(s.filtered, s.iters[i])
			bufStart := len(s.firstKeysBuf)
			s.firstKeysBuf = append(s.firstKeysBuf, iterKey.UserKey...)
			s.firstKeys = append(s.firstKeys, s.firstKeysBuf[bufStart:bufStart+len(iterKey.UserKey)])
		}
	}
}

func (s *simpleLevelIter) SeekGE(key []byte, flags base.SeekGEFlags) (*base.InternalKey, []byte) {
	// Find the first file that is entirely >= key. The file before that could
	// contain the key we're looking for.
	n := sort.Search(len(s.firstKeys), func(i int) bool {
		return s.cmp(key, s.firstKeys[i]) <= 0
	})
	if n > 0 {
		s.currentIdx = n - 1
	} else {
		s.currentIdx = n
	}
	if s.currentIdx < len(s.filtered) {
		if iterKey, val := s.filtered[s.currentIdx].SeekGE(key, flags); iterKey != nil {
			return iterKey, val
		}
		s.currentIdx++
	}
	return s.skipEmptyFileForward(key, flags)
}

func (s *simpleLevelIter) skipEmptyFileForward(
	seekKey []byte, flags base.SeekGEFlags,
) (*base.InternalKey, []byte) {
	var iterKey *base.InternalKey
	var val []byte
	for s.currentIdx >= 0 && s.currentIdx < len(s.filtered) {
		if seekKey != nil {
			iterKey, val = s.filtered[s.currentIdx].SeekGE(seekKey, flags)
		} else if s.lowerBound != nil {
			iterKey, val = s.filtered[s.currentIdx].SeekGE(s.lowerBound, flags)
		} else {
			iterKey, val = s.filtered[s.currentIdx].First()
		}
		if iterKey != nil {
			return iterKey, val
		}
		s.currentIdx++
	}
	return nil, nil
}

func (s *simpleLevelIter) SeekPrefixGE(
	prefix, key []byte, flags base.SeekGEFlags,
) (*base.InternalKey, []byte) {
	panic("unimplemented")
}

func (s *simpleLevelIter) SeekLT(key []byte, flags base.SeekLTFlags) (*base.InternalKey, []byte) {
	panic("unimplemented")
}

func (s *simpleLevelIter) First() (*base.InternalKey, []byte) {
	s.currentIdx = 0
	return s.skipEmptyFileForward(nil /* seekKey */, base.SeekGEFlagsNone)
}

func (s *simpleLevelIter) Last() (*base.InternalKey, []byte) {
	panic("unimplemented")
}

func (s *simpleLevelIter) Next() (*base.InternalKey, []byte) {
	if s.currentIdx < 0 || s.currentIdx >= len(s.filtered) {
		return nil, nil
	}
	if iterKey, val := s.filtered[s.currentIdx].Next(); iterKey != nil {
		return iterKey, val
	}
	s.currentIdx++
	return s.skipEmptyFileForward(nil /* seekKey */, base.SeekGEFlagsNone)
}

func (s *simpleLevelIter) Prev() (*base.InternalKey, []byte) {
	panic("unimplemented")
}

func (s *simpleLevelIter) Error() error {
	return nil
}

func (s *simpleLevelIter) Close() error {
	var err error
	for i := range s.iters {
		err = firstError(err, s.iters[i].Close())
	}
	return err
}

func (s *simpleLevelIter) SetBounds(lower, upper []byte) {
	s.currentIdx = -1
	s.lowerBound = lower
	for i := range s.iters {
		s.iters[i].SetBounds(lower, upper)
	}
	s.resetFilteredIters()
}

func (s *simpleLevelIter) String() string {
	if s.currentIdx < 0 || s.currentIdx >= len(s.filtered) {
		return "simpleLevelIter: current=<nil>"
	}
	return fmt.Sprintf("simpleLevelIter: current=%s", s.filtered[s.currentIdx])
}

var _ internalIterator = &simpleLevelIter{}
