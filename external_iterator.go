// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/block"
)

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
// Input sstables must only contain keys with the zero sequence number and must
// not contain references to values in external blob files.
//
// Iterators constructed through NewExternalIter do not support all iterator
// options, including block-property and table filters. NewExternalIter errors
// if an incompatible option is set.
func NewExternalIter(
	o *Options, iterOpts *IterOptions, files [][]sstable.ReadableFile,
) (it *Iterator, err error) {
	return NewExternalIterWithContext(context.Background(), o, iterOpts, files)
}

// NewExternalIterWithContext is like NewExternalIter, and additionally
// accepts a context for tracing.
func NewExternalIterWithContext(
	ctx context.Context, o *Options, iterOpts *IterOptions, files [][]sstable.ReadableFile,
) (it *Iterator, err error) {
	if iterOpts != nil {
		if err := validateExternalIterOpts(iterOpts); err != nil {
			return nil, err
		}
	}

	ro := o.MakeReaderOptions()
	var readers [][]*sstable.Reader
	for _, levelFiles := range files {
		subReaders, err := openExternalTables(ctx, levelFiles, ro)
		readers = append(readers, subReaders)
		if err != nil {
			// Close all the opened readers.
			for i := range readers {
				for j := range readers[i] {
					_ = readers[i][j].Close()
				}
			}
			return nil, err
		}
	}

	buf := iterAllocPool.Get().(*iterAlloc)
	dbi := &buf.dbi
	*dbi = Iterator{
		ctx:                 ctx,
		alloc:               buf,
		merge:               o.Merger.Merge,
		comparer:            *o.Comparer,
		readState:           nil,
		keyBuf:              buf.keyBuf,
		prefixOrFullSeekKey: buf.prefixOrFullSeekKey,
		boundsBuf:           buf.boundsBuf,
		batch:               nil,
		// Add the external iter state to the Iterator so that Close closes it,
		// and SetOptions can re-construct iterators using its state.
		externalIter: &externalIterState{readers: readers},
		newIters: func(context.Context, *manifest.TableMetadata, *IterOptions,
			internalIterOpts, iterKinds) (iterSet, error) {
			// NB: External iterators are currently constructed without any
			// `levelIters`. newIters should never be called. When we support
			// organizing multiple non-overlapping files into a single level
			// (see TODO below), we'll need to adjust this tableNewIters
			// implementation to open iterators by looking up f in a map
			// of readers indexed by *fileMetadata.
			panic("unreachable")
		},
		seqNum: base.SeqNumMax,
	}
	dbi.externalIter.bufferPool.Init(2)

	if iterOpts != nil {
		dbi.opts = *iterOpts
		dbi.processBounds(iterOpts.LowerBound, iterOpts.UpperBound)
	}
	if err := finishInitializingExternal(ctx, dbi); err != nil {
		_ = dbi.Close()
		return nil, err
	}
	return dbi, nil
}

// externalIterState encapsulates state that is specific to external iterators.
// An external *pebble.Iterator maintains a pointer to the externalIterState and
// calls Close when the Iterator is Closed, providing an opportuntity for the
// external iterator to release resources particular to external iterators.
type externalIterState struct {
	bufferPool block.BufferPool
	readers    [][]*sstable.Reader
}

func (e *externalIterState) Close() (err error) {
	for _, readers := range e.readers {
		for _, r := range readers {
			err = firstError(err, r.Close())
		}
	}
	e.bufferPool.Release()
	return err
}

func validateExternalIterOpts(iterOpts *IterOptions) error {
	switch {
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

func createExternalPointIter(
	ctx context.Context, it *Iterator, readEnv sstable.ReadEnv,
) (topLevelIterator, error) {
	// TODO(jackson): In some instances we could generate fewer levels by using
	// L0Sublevels code to organize nonoverlapping files into the same level.
	// This would allow us to use levelIters and keep a smaller set of data and
	// files in-memory. However, it would also require us to identify the bounds
	// of all the files upfront.

	if !it.opts.pointKeys() {
		return emptyIter, nil
	} else if it.pointIter != nil {
		return it.pointIter, nil
	}
	mlevels := it.alloc.mlevels[:0]

	if len(it.externalIter.readers) > cap(mlevels) {
		mlevels = make([]mergingIterLevel, 0, len(it.externalIter.readers))
	}
	// We set a synthetic sequence number, with lower levels having higer numbers.
	seqNum := 0
	for _, readers := range it.externalIter.readers {
		seqNum += len(readers)
	}
	for _, readers := range it.externalIter.readers {
		for _, r := range readers {
			var (
				rangeDelIter keyspan.FragmentIterator
				pointIter    internalIterator
				err          error
			)
			// We could set hideObsoletePoints=true, since we are reading at
			// InternalKeySeqNumMax, but we don't bother since these sstables should
			// not have obsolete points (so the performance optimization is
			// unnecessary), and we don't want to bother constructing a
			// BlockPropertiesFilterer that includes obsoleteKeyBlockPropertyFilter.
			transforms := sstable.IterTransforms{SyntheticSeqNum: sstable.SyntheticSeqNum(seqNum)}
			seqNum--
			pointIter, err = r.NewPointIter(ctx, sstable.IterOptions{
				Lower:                it.opts.LowerBound,
				Upper:                it.opts.UpperBound,
				Transforms:           transforms,
				FilterBlockSizeLimit: sstable.NeverUseFilterBlock,
				Env:                  readEnv,
				ReaderProvider:       sstable.MakeTrivialReaderProvider(r),
			})
			if err == nil {
				rangeDelIter, err = r.NewRawRangeDelIter(ctx, sstable.FragmentIterTransforms{
					SyntheticSeqNum: sstable.SyntheticSeqNum(seqNum),
				}, readEnv)
			}
			if err != nil {
				if pointIter != nil {
					_ = pointIter.Close()
				}
				for i := range mlevels {
					_ = mlevels[i].iter.Close()
					if mlevels[i].rangeDelIter != nil {
						mlevels[i].rangeDelIter.Close()
					}
				}
				return nil, err
			}
			mlevels = append(mlevels, mergingIterLevel{
				iter:         pointIter,
				rangeDelIter: rangeDelIter,
			})
		}
	}

	it.alloc.merging.init(&it.opts, &it.stats.InternalStats, it.comparer.Compare, it.comparer.Split, mlevels...)
	it.alloc.merging.snapshot = base.SeqNumMax
	if len(mlevels) <= cap(it.alloc.levelsPositioned) {
		it.alloc.merging.levelsPositioned = it.alloc.levelsPositioned[:len(mlevels)]
	}
	return &it.alloc.merging, nil
}

func finishInitializingExternal(ctx context.Context, it *Iterator) error {
	readEnv := sstable.ReadEnv{
		Block: block.ReadEnv{
			Stats: &it.stats.InternalStats,
			// TODO(jackson): External iterators never provide categorized iterator
			// stats today because they exist outside the context of a *DB. If the
			// sstables being read are on the physical filesystem, we may still want to
			// thread a CategoryStatsCollector through so that we collect their stats.
			IterStats:  nil,
			BufferPool: &it.externalIter.bufferPool,
		},
	}
	pointIter, err := createExternalPointIter(ctx, it, readEnv)
	if err != nil {
		return err
	}
	it.pointIter = pointIter
	it.iter = it.pointIter

	if it.opts.rangeKeys() {
		it.rangeKeyMasking.init(it, &it.comparer)
		var rangeKeyIters []keyspan.FragmentIterator
		if it.rangeKey == nil {
			// We could take advantage of the lack of overlaps in range keys within
			// each slice in it.externalReaders, and generate keyspanimpl.LevelIters
			// out of those. However, since range keys are expected to be sparse to
			// begin with, the performance gain might not be significant enough to
			// warrant it.
			//
			// TODO(bilal): Explore adding a simpleRangeKeyLevelIter that does not
			// operate on TableMetadatas (similar to simpleLevelIter), and implements
			// this optimization.
			// We set a synthetic sequence number, with lower levels having higer numbers.
			seqNum := 0
			for _, readers := range it.externalIter.readers {
				seqNum += len(readers)
			}
			for _, readers := range it.externalIter.readers {
				for _, r := range readers {
					transforms := sstable.FragmentIterTransforms{SyntheticSeqNum: sstable.SyntheticSeqNum(seqNum)}
					seqNum--
					rki, err := r.NewRawRangeKeyIter(ctx, transforms, readEnv)
					if err != nil {
						for _, iter := range rangeKeyIters {
							iter.Close()
						}
						return err
					}
					if rki != nil {
						rangeKeyIters = append(rangeKeyIters, rki)
					}
				}
			}
			if len(rangeKeyIters) > 0 {
				it.rangeKey = iterRangeKeyStateAllocPool.Get().(*iteratorRangeKeyState)
				it.rangeKey.rangeKeyIter = it.rangeKey.iterConfig.Init(
					&it.comparer,
					base.SeqNumMax,
					it.opts.LowerBound, it.opts.UpperBound,
					&it.hasPrefix, &it.prefixOrFullSeekKey,
					false /* internalKeys */, &it.rangeKey.internal,
				)
				for i := range rangeKeyIters {
					it.rangeKey.iterConfig.AddLevel(rangeKeyIters[i])
				}
			}
		}
		if it.rangeKey != nil {
			it.rangeKey.iiter.Init(&it.comparer, it.iter, it.rangeKey.rangeKeyIter,
				keyspan.InterleavingIterOpts{
					Mask:       &it.rangeKeyMasking,
					LowerBound: it.opts.LowerBound,
					UpperBound: it.opts.UpperBound,
				})
			it.iter = &it.rangeKey.iiter
		}
	}
	return nil
}

func openExternalTables(
	ctx context.Context, files []sstable.ReadableFile, readerOpts sstable.ReaderOptions,
) (readers []*sstable.Reader, err error) {
	readers = make([]*sstable.Reader, 0, len(files))
	for i := range files {
		readable, err := sstable.NewSimpleReadable(files[i])
		if err != nil {
			return readers, err
		}
		r, err := sstable.NewReader(ctx, readable, readerOpts)
		if err != nil {
			return readers, errors.CombineErrors(err, readable.Close())
		}
		if r.Attributes.Has(sstable.AttributeBlobValues) {
			return readers, errors.Newf("pebble: NewExternalIter does not support blob references")
		}
		readers = append(readers, r)
	}
	return readers, err
}
