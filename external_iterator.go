// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/rangekey"
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
func NewExternalIter(
	o *Options,
	iterOpts *IterOptions,
	files []sstable.ReadableFile,
	extraReaderOpts ...sstable.ReaderOption,
) (it *Iterator, err error) {
	var readers []*sstable.Reader

	// Ensure we close all the opened readers if we error out.
	closeReaders := func() {
		for i := range readers {
			_ = readers[i].Close()
		}
	}
	defer func() {
		if err != nil {
			closeReaders()
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

	// TODO(jackson): In some instances we could generate fewer levels by using
	// L0Sublevels code to organize nonoverlapping files into the same level.
	// This would allow us to use levelIters and keep a smaller set of data and
	// files in-memory. However, it would also require us to identify the bounds
	// of all the files upfront.

	// Ensure we close all iters if error out early.
	mlevels := buf.mlevels[:0]
	var rangeKeyIters []keyspan.FragmentIterator
	defer func() {
		if err != nil {
			for i := range rangeKeyIters {
				_ = rangeKeyIters[i].Close()
			}
			for i := range mlevels {
				if mlevels[i].iter != nil {
					_ = mlevels[i].iter.Close()
				}
				if mlevels[i].rangeDelIter != nil {
					_ = mlevels[i].rangeDelIter.Close()
				}
			}
		}
	}()
	if iterOpts.pointKeys() {
		if len(files) > cap(mlevels) {
			mlevels = make([]mergingIterLevel, 0, len(files))
		}
		for _, r := range readers {
			pointIter, err := r.NewIter(dbi.opts.LowerBound, dbi.opts.UpperBound)
			if err != nil {
				return nil, err
			}
			rangeDelIter, err := r.NewRawRangeDelIter()
			if err != nil {
				_ = pointIter.Close()
				return nil, err
			}
			mlevels = append(mlevels, mergingIterLevel{
				iter:         base.WrapIterWithStats(pointIter),
				rangeDelIter: rangeDelIter,
			})
		}
	}
	buf.merging.init(&dbi.opts, dbi.cmp, dbi.split, mlevels...)
	buf.merging.snapshot = base.InternalKeySeqNumMax
	buf.merging.elideRangeTombstones = true
	dbi.pointIter = &buf.merging
	dbi.iter = dbi.pointIter

	if dbi.opts.rangeKeys() {
		for _, r := range readers {
			rki, err := r.NewRawRangeKeyIter()
			if err != nil {
				return nil, err
			}
			if rki != nil {
				rangeKeyIters = append(rangeKeyIters, rki)
			}
		}

		dbi.rangeKey = iterRangeKeyStateAllocPool.Get().(*iteratorRangeKeyState)
		dbi.rangeKey.rangeKeyIter = rangekey.InitUserIteration(
			o.Comparer.Compare,
			base.InternalKeySeqNumMax,
			&dbi.rangeKey.alloc.merging,
			&dbi.rangeKey.alloc.defraging,
			rangeKeyIters...,
		)

		dbi.rangeKey.iter.Init(dbi.cmp, &buf.merging, dbi.rangeKey.rangeKeyIter, keyspan.Hooks{
			SpanChanged: dbi.rangeKeySpanChanged,
			SkipPoint:   dbi.rangeKeySkipPoint,
		}, dbi.opts.LowerBound, dbi.opts.UpperBound)
		dbi.iter = &dbi.rangeKey.iter
	}

	// Close all the opened sstable.Readers when the Iterator is closed.
	dbi.closeHook = closeReaders
	return dbi, nil
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
