// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/sstable"
)

// NewExternalIter takes an input set of sstable files which may overlap
// arbitrarily and returns an Iterator over the merged contents of the sstables.
// Input sstables may contain point keys, range keys, range deletions, etc. The
// input files slice must be sorted in reverse chronological ordering. A key in
// a file at a lower index will shadow a key with an identical user key
// contained within a file at a higher index.
//
// Input sstables must only contain keys with the zero sequence number.
//
// Iterators constructed through NewExternalIter do not support all iterator
// options, including block-property and table filters. NewExternalIter errors
// if an incompatible option is set.
func NewExternalIter(
	o *Options,
	iterOpts *IterOptions,
	files []sstable.ReadableFile,
	extraReaderOpts ...sstable.ReaderOption,
) (it *Iterator, err error) {
	if iterOpts != nil {
		if err := validateExternalIterOpts(iterOpts); err != nil {
			return nil, err
		}
	}

	var readers []*sstable.Reader

	// Ensure we close all the opened readers if we error out.
	defer func() {
		if err != nil {
			for i := range readers {
				_ = readers[i].Close()
			}
		}
	}()
	readers, err = openExternalTables(o, files, o.MakeReaderOptions(), extraReaderOpts...)
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
		readState:           nil,
		keyBuf:              buf.keyBuf,
		prefixOrFullSeekKey: buf.prefixOrFullSeekKey,
		boundsBuf:           buf.boundsBuf,
		batch:               nil,
		// Add the readers to the Iterator so that Close closes them, and
		// SetOptions can re-construct iterators from them.
		externalReaders: readers,
		newIters: func(f *manifest.FileMetadata, opts *IterOptions, bytesIterated *uint64) (internalIterator, keyspan.FragmentIterator, error) {
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
		for _, r := range it.externalReaders {
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
			mlevels = append(mlevels, mergingIterLevel{
				iter:         base.WrapIterWithStats(pointIter),
				rangeDelIter: rangeDelIter,
			})
		}
		it.alloc.merging.init(&it.opts, it.cmp, it.split, mlevels...)
		it.alloc.merging.snapshot = base.InternalKeySeqNumMax
		it.alloc.merging.elideRangeTombstones = true
		it.pointIter = &it.alloc.merging
	}
	it.iter = it.pointIter

	if it.opts.rangeKeys() {
		if it.rangeKey == nil {
			it.rangeKey = iterRangeKeyStateAllocPool.Get().(*iteratorRangeKeyState)
			it.rangeKey.init(it.cmp, it.split, &it.opts)
			it.rangeKey.rangeKeyIter = it.rangeKey.iterConfig.Init(
				it.cmp,
				base.InternalKeySeqNumMax,
			)
			for _, r := range it.externalReaders {
				if rki, err := r.NewRawRangeKeyIter(); err != nil {
					it.rangeKey.iterConfig.AddLevel(&errorKeyspanIter{err: err})
				} else if rki != nil {
					it.rangeKey.iterConfig.AddLevel(rki)
				}
			}
		}
		it.rangeKey.iiter.Init(it.cmp, it.iter, it.rangeKey.rangeKeyIter, it.rangeKey,
			it.opts.LowerBound, it.opts.UpperBound)
		it.iter = &it.rangeKey.iiter
	}
}

func openExternalTables(
	o *Options,
	files []sstable.ReadableFile,
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
		r.Properties.GlobalSeqNum = uint64(len(files) - i)
		readers = append(readers, r)
	}
	return readers, err
}
