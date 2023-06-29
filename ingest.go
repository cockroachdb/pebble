// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"sort"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/private"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable"
)

func sstableKeyCompare(userCmp Compare, a, b InternalKey) int {
	c := userCmp(a.UserKey, b.UserKey)
	if c != 0 {
		return c
	}
	if a.IsExclusiveSentinel() {
		if !b.IsExclusiveSentinel() {
			return -1
		}
	} else if b.IsExclusiveSentinel() {
		return +1
	}
	return 0
}

// KeyRange encodes a key range in user key space. A KeyRange's Start is
// inclusive while its End is exclusive.
type KeyRange struct {
	Start, End []byte
}

// Valid returns true if the KeyRange is defined.
func (k *KeyRange) Valid() bool {
	return k.Start != nil && k.End != nil
}

// Contains returns whether the specified key exists in the KeyRange.
func (k *KeyRange) Contains(cmp base.Compare, key InternalKey) bool {
	v := cmp(key.UserKey, k.End)
	return (v < 0 || (v == 0 && key.IsExclusiveSentinel())) && cmp(k.Start, key.UserKey) <= 0
}

// OverlapsInternalKeyRange checks if the specified internal key range has an
// overlap with the KeyRange. Note that we aren't checking for full containment
// of smallest-largest within k, rather just that there's some intersection
// between the two ranges.
func (k *KeyRange) OverlapsInternalKeyRange(cmp base.Compare, smallest, largest InternalKey) bool {
	v := cmp(k.Start, largest.UserKey)
	return v <= 0 && !(largest.IsExclusiveSentinel() && v == 0) &&
		cmp(k.End, smallest.UserKey) > 0
}

// Overlaps checks if the specified file has an overlap with the KeyRange.
// Note that we aren't checking for full containment of m within k, rather just
// that there's some intersection between m and k's bounds.
func (k *KeyRange) Overlaps(cmp base.Compare, m *fileMetadata) bool {
	return k.OverlapsInternalKeyRange(cmp, m.Smallest, m.Largest)
}

func ingestValidateKey(opts *Options, key *InternalKey) error {
	if key.Kind() == InternalKeyKindInvalid {
		return base.CorruptionErrorf("pebble: external sstable has corrupted key: %s",
			key.Pretty(opts.Comparer.FormatKey))
	}
	if key.SeqNum() != 0 {
		return base.CorruptionErrorf("pebble: external sstable has non-zero seqnum: %s",
			key.Pretty(opts.Comparer.FormatKey))
	}
	return nil
}

// ingestLoad1Shared loads the fileMetadata for one shared sstable owned or
// shared by another node. It also sets the sequence numbers for a shared sstable.
func ingestLoad1Shared(
	opts *Options, sm SharedSSTMeta, fileNum base.DiskFileNum,
) (*fileMetadata, error) {
	if sm.Size == 0 {
		// Disallow 0 file sizes
		return nil, errors.New("pebble: cannot ingest shared file with size 0")
	}
	// Don't load table stats. Doing a round trip to shared storage, one SST
	// at a time is not worth it as it slows down ingestion.
	meta := &fileMetadata{}
	meta.FileNum = fileNum.FileNum()
	meta.CreationTime = time.Now().Unix()
	meta.Virtual = true
	meta.Size = sm.Size
	meta.InitProviderBacking(fileNum)
	// Set the underlying FileBacking's size to the same size as the virtualized
	// view of the sstable. This ensures that we don't over-prioritize this
	// sstable for compaction just yet, as we do not have a clear sense of
	// what parts of this sstable are referenced by other nodes.
	meta.FileBacking.Size = sm.Size
	seqNum := base.SeqNumForLevel(int(sm.Level))
	if sm.LargestRangeKey.Valid() && sm.LargestRangeKey.UserKey != nil {
		meta.HasRangeKeys = true
		meta.SmallestRangeKey = sm.SmallestRangeKey
		meta.LargestRangeKey = sm.LargestRangeKey
		meta.SmallestRangeKey.SetSeqNum(seqNum)
		if !meta.LargestRangeKey.IsExclusiveSentinel() {
			meta.LargestRangeKey.SetSeqNum(seqNum)
		}
		meta.SmallestSeqNum = seqNum
		meta.LargestSeqNum = seqNum
		// Initialize meta.{Smallest,Largest} and others by calling this.
		meta.ExtendRangeKeyBounds(opts.Comparer.Compare, meta.SmallestRangeKey, meta.LargestRangeKey)
	}
	if sm.LargestPointKey.Valid() && sm.LargestPointKey.UserKey != nil {
		meta.HasPointKeys = true
		meta.SmallestPointKey = sm.SmallestPointKey
		meta.LargestPointKey = sm.LargestPointKey
		meta.SmallestPointKey.SetSeqNum(seqNum)
		if !meta.LargestPointKey.IsExclusiveSentinel() {
			meta.LargestPointKey.SetSeqNum(seqNum)
		}
		meta.SmallestSeqNum = seqNum
		meta.LargestSeqNum = seqNum
		// Initialize meta.{Smallest,Largest} and others by calling this.
		meta.ExtendPointKeyBounds(opts.Comparer.Compare, meta.SmallestPointKey, meta.LargestPointKey)
	}

	if err := meta.Validate(opts.Comparer.Compare, opts.Comparer.FormatKey); err != nil {
		return nil, err
	}
	return meta, nil
}

// ingestLoad1 creates the FileMetadata for one file. This file will be owned
// by this store.
func ingestLoad1(
	opts *Options,
	fmv FormatMajorVersion,
	readable objstorage.Readable,
	cacheID uint64,
	fileNum base.DiskFileNum,
) (*fileMetadata, error) {
	cacheOpts := private.SSTableCacheOpts(cacheID, fileNum).(sstable.ReaderOption)
	r, err := sstable.NewReader(readable, opts.MakeReaderOptions(), cacheOpts)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	// Avoid ingesting tables with format versions this DB doesn't support.
	tf, err := r.TableFormat()
	if err != nil {
		return nil, err
	}
	if tf < fmv.MinTableFormat() || tf > fmv.MaxTableFormat() {
		return nil, errors.Newf(
			"pebble: table format %s is not within range supported at DB format major version %d, (%s,%s)",
			tf, fmv, fmv.MinTableFormat(), fmv.MaxTableFormat(),
		)
	}

	meta := &fileMetadata{}
	meta.FileNum = fileNum.FileNum()
	meta.Size = uint64(readable.Size())
	meta.CreationTime = time.Now().Unix()
	meta.InitPhysicalBacking()

	// Avoid loading into the table cache for collecting stats if we
	// don't need to. If there are no range deletions, we have all the
	// information to compute the stats here.
	//
	// This is helpful in tests for avoiding awkwardness around deletion of
	// ingested files from MemFS. MemFS implements the Windows semantics of
	// disallowing removal of an open file. Under MemFS, if we don't populate
	// meta.Stats here, the file will be loaded into the table cache for
	// calculating stats before we can remove the original link.
	maybeSetStatsFromProperties(meta.PhysicalMeta(), &r.Properties)

	{
		iter, err := r.NewIter(nil /* lower */, nil /* upper */)
		if err != nil {
			return nil, err
		}
		defer iter.Close()
		var smallest InternalKey
		if key, _ := iter.First(); key != nil {
			if err := ingestValidateKey(opts, key); err != nil {
				return nil, err
			}
			smallest = (*key).Clone()
		}
		if err := iter.Error(); err != nil {
			return nil, err
		}
		if key, _ := iter.Last(); key != nil {
			if err := ingestValidateKey(opts, key); err != nil {
				return nil, err
			}
			meta.ExtendPointKeyBounds(opts.Comparer.Compare, smallest, key.Clone())
		}
		if err := iter.Error(); err != nil {
			return nil, err
		}
	}

	iter, err := r.NewRawRangeDelIter()
	if err != nil {
		return nil, err
	}
	if iter != nil {
		defer iter.Close()
		var smallest InternalKey
		if s := iter.First(); s != nil {
			key := s.SmallestKey()
			if err := ingestValidateKey(opts, &key); err != nil {
				return nil, err
			}
			smallest = key.Clone()
		}
		if err := iter.Error(); err != nil {
			return nil, err
		}
		if s := iter.Last(); s != nil {
			k := s.SmallestKey()
			if err := ingestValidateKey(opts, &k); err != nil {
				return nil, err
			}
			largest := s.LargestKey().Clone()
			meta.ExtendPointKeyBounds(opts.Comparer.Compare, smallest, largest)
		}
	}

	// Update the range-key bounds for the table.
	{
		iter, err := r.NewRawRangeKeyIter()
		if err != nil {
			return nil, err
		}
		if iter != nil {
			defer iter.Close()
			var smallest InternalKey
			if s := iter.First(); s != nil {
				key := s.SmallestKey()
				if err := ingestValidateKey(opts, &key); err != nil {
					return nil, err
				}
				smallest = key.Clone()
			}
			if err := iter.Error(); err != nil {
				return nil, err
			}
			if s := iter.Last(); s != nil {
				k := s.SmallestKey()
				if err := ingestValidateKey(opts, &k); err != nil {
					return nil, err
				}
				// As range keys are fragmented, the end key of the last range key in
				// the table provides the upper bound for the table.
				largest := s.LargestKey().Clone()
				meta.ExtendRangeKeyBounds(opts.Comparer.Compare, smallest, largest)
			}
			if err := iter.Error(); err != nil {
				return nil, err
			}
		}
	}

	if !meta.HasPointKeys && !meta.HasRangeKeys {
		return nil, nil
	}

	// Sanity check that the various bounds on the file were set consistently.
	if err := meta.Validate(opts.Comparer.Compare, opts.Comparer.FormatKey); err != nil {
		return nil, err
	}

	return meta, nil
}

type ingestLoadResult struct {
	localMeta, sharedMeta []*fileMetadata
	localPaths            []string
	sharedLevels          []uint8
}

func ingestLoad(
	opts *Options,
	fmv FormatMajorVersion,
	paths []string,
	shared []SharedSSTMeta,
	cacheID uint64,
	pending []base.DiskFileNum,
) (ingestLoadResult, error) {
	meta := make([]*fileMetadata, 0, len(paths))
	newPaths := make([]string, 0, len(paths))
	for i := range paths {
		f, err := opts.FS.Open(paths[i])
		if err != nil {
			return ingestLoadResult{}, err
		}

		readable, err := sstable.NewSimpleReadable(f)
		if err != nil {
			return ingestLoadResult{}, err
		}
		m, err := ingestLoad1(opts, fmv, readable, cacheID, pending[i])
		if err != nil {
			return ingestLoadResult{}, err
		}
		if m != nil {
			meta = append(meta, m)
			newPaths = append(newPaths, paths[i])
		}
	}
	if len(shared) == 0 {
		return ingestLoadResult{localMeta: meta, localPaths: newPaths}, nil
	}
	sharedMeta := make([]*fileMetadata, 0, len(shared))
	levels := make([]uint8, 0, len(shared))
	for i := range shared {
		m, err := ingestLoad1Shared(opts, shared[i], pending[len(paths)+i])
		if err != nil {
			return ingestLoadResult{}, err
		}
		if shared[i].Level < sharedLevelsStart {
			return ingestLoadResult{}, errors.New("cannot ingest shared file in level below sharedLevelsStart")
		}
		sharedMeta = append(sharedMeta, m)
		levels = append(levels, shared[i].Level)
	}
	result := ingestLoadResult{
		localMeta:    meta,
		sharedMeta:   sharedMeta,
		localPaths:   newPaths,
		sharedLevels: levels,
	}
	return result, nil
}

// Struct for sorting metadatas by smallest user keys, while ensuring the
// matching path also gets swapped to the same index. For use in
// ingestSortAndVerify.
type metaAndPaths struct {
	meta  []*fileMetadata
	paths []string
	cmp   Compare
}

func (m metaAndPaths) Len() int {
	return len(m.meta)
}

func (m metaAndPaths) Less(i, j int) bool {
	return m.cmp(m.meta[i].Smallest.UserKey, m.meta[j].Smallest.UserKey) < 0
}

func (m metaAndPaths) Swap(i, j int) {
	m.meta[i], m.meta[j] = m.meta[j], m.meta[i]
	m.paths[i], m.paths[j] = m.paths[j], m.paths[i]
}

func ingestSortAndVerify(cmp Compare, lr ingestLoadResult, exciseSpan KeyRange) error {
	// Verify that all the shared files (i.e. files in sharedMeta)
	// fit within the exciseSpan.
	for i := range lr.sharedMeta {
		f := lr.sharedMeta[i]
		if !exciseSpan.Contains(cmp, f.Smallest) || !exciseSpan.Contains(cmp, f.Largest) {
			return errors.AssertionFailedf("pebble: shared file outside of excise span, span [%s-%s), file = %s", exciseSpan.Start, exciseSpan.End, f.String())
		}
	}
	if len(lr.localMeta) <= 1 || len(lr.localPaths) <= 1 {
		return nil
	}

	sort.Sort(&metaAndPaths{
		meta:  lr.localMeta,
		paths: lr.localPaths,
		cmp:   cmp,
	})

	for i := 1; i < len(lr.localPaths); i++ {
		if sstableKeyCompare(cmp, lr.localMeta[i-1].Largest, lr.localMeta[i].Smallest) >= 0 {
			return errors.AssertionFailedf("pebble: external sstables have overlapping ranges")
		}
	}
	if len(lr.sharedMeta) == 0 {
		return nil
	}
	filesInLevel := make([]*fileMetadata, 0, len(lr.sharedMeta))
	for l := sharedLevelsStart; l < numLevels; l++ {
		filesInLevel = filesInLevel[:0]
		for i := range lr.sharedMeta {
			if lr.sharedLevels[i] == uint8(l) {
				filesInLevel = append(filesInLevel, lr.sharedMeta[i])
			}
		}
		sort.Slice(filesInLevel, func(i, j int) bool {
			return cmp(filesInLevel[i].Smallest.UserKey, filesInLevel[j].Smallest.UserKey) < 0
		})
		for i := 1; i < len(filesInLevel); i++ {
			if sstableKeyCompare(cmp, filesInLevel[i-1].Largest, filesInLevel[i].Smallest) >= 0 {
				return errors.AssertionFailedf("pebble: external shared sstables have overlapping ranges")
			}
		}
	}
	return nil
}

func ingestCleanup(objProvider objstorage.Provider, meta []*fileMetadata) error {
	var firstErr error
	for i := range meta {
		if err := objProvider.Remove(fileTypeTable, meta[i].FileBacking.DiskFileNum); err != nil {
			firstErr = firstError(firstErr, err)
		}
	}
	return firstErr
}

// ingestLink creates new objects which are backed by either hardlinks to or
// copies of the ingested files. It also attaches shared objects to the provider.
func ingestLink(
	jobID int,
	opts *Options,
	objProvider objstorage.Provider,
	lr ingestLoadResult,
	shared []SharedSSTMeta,
) error {
	for i := range lr.localPaths {
		objMeta, err := objProvider.LinkOrCopyFromLocal(
			context.TODO(), opts.FS, lr.localPaths[i], fileTypeTable, lr.localMeta[i].FileBacking.DiskFileNum,
			objstorage.CreateOptions{PreferSharedStorage: true},
		)
		if err != nil {
			if err2 := ingestCleanup(objProvider, lr.localMeta[:i]); err2 != nil {
				opts.Logger.Infof("ingest cleanup failed: %v", err2)
			}
			return err
		}
		if opts.EventListener.TableCreated != nil {
			opts.EventListener.TableCreated(TableCreateInfo{
				JobID:   jobID,
				Reason:  "ingesting",
				Path:    objProvider.Path(objMeta),
				FileNum: lr.localMeta[i].FileNum,
			})
		}
	}
	sharedObjs := make([]objstorage.SharedObjectToAttach, 0, len(shared))
	for i := range shared {
		backing, err := shared[i].Backing.Get()
		if err != nil {
			return err
		}
		sharedObjs = append(sharedObjs, objstorage.SharedObjectToAttach{
			FileNum:  lr.sharedMeta[i].FileBacking.DiskFileNum,
			FileType: fileTypeTable,
			Backing:  backing,
		})
	}
	sharedObjMetas, err := objProvider.AttachSharedObjects(sharedObjs)
	if err != nil {
		return err
	}
	for i := range sharedObjMetas {
		// One corner case around file sizes we need to be mindful of, is that
		// if one of the shareObjs was initially created by us (and has boomeranged
		// back from another node), we'll need to update the FileBacking's size
		// to be the true underlying size. Otherwise, we could hit errors when we
		// open the db again after a crash/restart (see checkConsistency in open.go),
		// plus it more accurately allows us to prioritize compactions of files
		// that were originally created by us.
		if !objProvider.IsForeign(sharedObjMetas[i]) {
			size, err := objProvider.Size(sharedObjMetas[i])
			if err != nil {
				return err
			}
			lr.sharedMeta[i].FileBacking.Size = uint64(size)
		}
		if opts.EventListener.TableCreated != nil {
			opts.EventListener.TableCreated(TableCreateInfo{
				JobID:   jobID,
				Reason:  "ingesting",
				Path:    objProvider.Path(sharedObjMetas[i]),
				FileNum: lr.sharedMeta[i].FileNum,
			})
		}
	}

	return nil
}

func ingestMemtableOverlaps(cmp Compare, mem flushable, meta []*fileMetadata) bool {
	iter := mem.newIter(nil)
	rangeDelIter := mem.newRangeDelIter(nil)
	rkeyIter := mem.newRangeKeyIter(nil)

	closeIters := func() error {
		err := iter.Close()
		if rangeDelIter != nil {
			err = firstError(err, rangeDelIter.Close())
		}
		if rkeyIter != nil {
			err = firstError(err, rkeyIter.Close())
		}
		return err
	}

	for _, m := range meta {
		kr := internalKeyRange{smallest: m.Smallest, largest: m.Largest}
		if overlapWithIterator(iter, &rangeDelIter, rkeyIter, kr, cmp) {
			closeIters()
			return true
		}
	}

	// Assume overlap if any iterator errored out.
	return closeIters() != nil
}

func ingestUpdateSeqNum(
	cmp Compare, format base.FormatKey, seqNum uint64, meta []*fileMetadata,
) error {
	setSeqFn := func(k base.InternalKey) base.InternalKey {
		return base.MakeInternalKey(k.UserKey, seqNum, k.Kind())
	}
	for _, m := range meta {
		// NB: we set the fields directly here, rather than via their Extend*
		// methods, as we are updating sequence numbers.
		if m.HasPointKeys {
			m.SmallestPointKey = setSeqFn(m.SmallestPointKey)
		}
		if m.HasRangeKeys {
			m.SmallestRangeKey = setSeqFn(m.SmallestRangeKey)
		}
		m.Smallest = setSeqFn(m.Smallest)
		// Only update the seqnum for the largest key if that key is not an
		// "exclusive sentinel" (i.e. a range deletion sentinel or a range key
		// boundary), as doing so effectively drops the exclusive sentinel (by
		// lowering the seqnum from the max value), and extends the bounds of the
		// table.
		// NB: as the largest range key is always an exclusive sentinel, it is never
		// updated.
		if m.HasPointKeys && !m.LargestPointKey.IsExclusiveSentinel() {
			m.LargestPointKey = setSeqFn(m.LargestPointKey)
		}
		if !m.Largest.IsExclusiveSentinel() {
			m.Largest = setSeqFn(m.Largest)
		}
		// Setting smallestSeqNum == largestSeqNum triggers the setting of
		// Properties.GlobalSeqNum when an sstable is loaded.
		m.SmallestSeqNum = seqNum
		m.LargestSeqNum = seqNum
		// Ensure the new bounds are consistent.
		if err := m.Validate(cmp, format); err != nil {
			return err
		}
		seqNum++
	}
	return nil
}

type internalKeyRange struct {
	smallest, largest InternalKey
}

func overlapWithIterator(
	iter internalIterator,
	rangeDelIter *keyspan.FragmentIterator,
	rkeyIter keyspan.FragmentIterator,
	keyRange internalKeyRange,
	cmp Compare,
) bool {
	// Check overlap with point operations.
	//
	// When using levelIter, it seeks to the SST whose boundaries
	// contain keyRange.smallest.UserKey(S).
	// It then tries to find a point in that SST that is >= S.
	// If there's no such point it means the SST ends in a tombstone in which case
	// levelIter.SeekGE generates a boundary range del sentinel.
	// The comparison of this boundary with keyRange.largest(L) below
	// is subtle but maintains correctness.
	// 1) boundary < L,
	//    since boundary is also > S (initial seek),
	//    whatever the boundary's start key may be, we're always overlapping.
	// 2) boundary > L,
	//    overlap with boundary cannot be determined since we don't know boundary's start key.
	//    We require checking for overlap with rangeDelIter.
	// 3) boundary == L and L is not sentinel,
	//    means boundary < L and hence is similar to 1).
	// 4) boundary == L and L is sentinel,
	//    we'll always overlap since for any values of i,j ranges [i, k) and [j, k) always overlap.
	key, _ := iter.SeekGE(keyRange.smallest.UserKey, base.SeekGEFlagsNone)
	if key != nil {
		c := sstableKeyCompare(cmp, *key, keyRange.largest)
		if c <= 0 {
			return true
		}
	}
	// Assume overlap if iterator errored.
	if err := iter.Error(); err != nil {
		return true
	}

	computeOverlapWithSpans := func(rIter keyspan.FragmentIterator) bool {
		// NB: The spans surfaced by the fragment iterator are non-overlapping.
		span := rIter.SeekLT(keyRange.smallest.UserKey)
		if span == nil {
			span = rIter.Next()
		}
		for ; span != nil; span = rIter.Next() {
			if span.Empty() {
				continue
			}
			key := span.SmallestKey()
			c := sstableKeyCompare(cmp, key, keyRange.largest)
			if c > 0 {
				// The start of the span is after the largest key in the
				// ingested table.
				return false
			}
			if cmp(span.End, keyRange.smallest.UserKey) > 0 {
				// The end of the span is greater than the smallest in the
				// table. Note that the span end key is exclusive, thus ">0"
				// instead of ">=0".
				return true
			}
		}
		// Assume overlap if iterator errored.
		if err := rIter.Error(); err != nil {
			return true
		}
		return false
	}

	// rkeyIter is either a range key level iter, or a range key iterator
	// over a single file.
	if rkeyIter != nil {
		if computeOverlapWithSpans(rkeyIter) {
			return true
		}
	}

	// Check overlap with range deletions.
	if rangeDelIter == nil || *rangeDelIter == nil {
		return false
	}
	return computeOverlapWithSpans(*rangeDelIter)
}

func ingestTargetLevel(
	newIters tableNewIters,
	newRangeKeyIter keyspan.TableNewSpanIter,
	iterOps IterOptions,
	cmp Compare,
	v *version,
	baseLevel int,
	compactions map[*compaction]struct{},
	meta *fileMetadata,
) (int, error) {
	// Find the lowest level which does not have any files which overlap meta. We
	// search from L0 to L6 looking for whether there are any files in the level
	// which overlap meta. We want the "lowest" level (where lower means
	// increasing level number) in order to reduce write amplification.
	//
	// There are 2 kinds of overlap we need to check for: file boundary overlap
	// and data overlap. Data overlap implies file boundary overlap. Note that it
	// is always possible to ingest into L0.
	//
	// To place meta at level i where i > 0:
	// - there must not be any data overlap with levels <= i, since that will
	//   violate the sequence number invariant.
	// - no file boundary overlap with level i, since that will violate the
	//   invariant that files do not overlap in levels i > 0.
	//
	// The file boundary overlap check is simpler to conceptualize. Consider the
	// following example, in which the ingested file lies completely before or
	// after the file being considered.
	//
	//   |--|           |--|  ingested file: [a,b] or [f,g]
	//         |-----|        existing file: [c,e]
	//  _____________________
	//   a  b  c  d  e  f  g
	//
	// In both cases the ingested file can move to considering the next level.
	//
	// File boundary overlap does not necessarily imply data overlap. The check
	// for data overlap is a little more nuanced. Consider the following examples:
	//
	//  1. No data overlap:
	//
	//          |-|   |--|    ingested file: [cc-d] or [ee-ff]
	//  |*--*--*----*------*| existing file: [a-g], points: [a, b, c, dd, g]
	//  _____________________
	//   a  b  c  d  e  f  g
	//
	// In this case the ingested files can "fall through" this level. The checks
	// continue at the next level.
	//
	//  2. Data overlap:
	//
	//            |--|        ingested file: [d-e]
	//  |*--*--*----*------*| existing file: [a-g], points: [a, b, c, dd, g]
	//  _____________________
	//   a  b  c  d  e  f  g
	//
	// In this case the file cannot be ingested into this level as the point 'dd'
	// is in the way.
	//
	// It is worth noting that the check for data overlap is only approximate. In
	// the previous example, the ingested table [d-e] could contain only the
	// points 'd' and 'e', in which case the table would be eligible for
	// considering lower levels. However, such a fine-grained check would need to
	// be exhaustive (comparing points and ranges in both the ingested existing
	// tables) and such a check is prohibitively expensive. Thus Pebble treats any
	// existing point that falls within the ingested table bounds as being "data
	// overlap".

	targetLevel := 0

	// This assertion implicitly checks that we have the current version of
	// the metadata.
	if v.L0Sublevels == nil {
		return 0, errors.AssertionFailedf("could not read L0 sublevels")
	}
	// Check for overlap over the keys of L0 by iterating over the sublevels.
	for subLevel := 0; subLevel < len(v.L0SublevelFiles); subLevel++ {
		iter := newLevelIter(iterOps, cmp, nil /* split */, newIters,
			v.L0Sublevels.Levels[subLevel].Iter(), manifest.Level(0), internalIterOpts{})

		var rangeDelIter keyspan.FragmentIterator
		// Pass in a non-nil pointer to rangeDelIter so that levelIter.findFileGE
		// sets it up for the target file.
		iter.initRangeDel(&rangeDelIter)

		levelIter := keyspan.LevelIter{}
		levelIter.Init(
			keyspan.SpanIterOptions{}, cmp, newRangeKeyIter,
			v.L0Sublevels.Levels[subLevel].Iter(), manifest.Level(0), manifest.KeyTypeRange,
		)

		kr := internalKeyRange{
			smallest: meta.Smallest,
			largest:  meta.Largest,
		}
		overlap := overlapWithIterator(iter, &rangeDelIter, &levelIter, kr, cmp)
		err := iter.Close() // Closes range del iter as well.
		err = firstError(err, levelIter.Close())
		if err != nil {
			return 0, err
		}
		if overlap {
			return targetLevel, nil
		}
	}

	level := baseLevel
	for ; level < numLevels; level++ {
		levelIter := newLevelIter(iterOps, cmp, nil /* split */, newIters,
			v.Levels[level].Iter(), manifest.Level(level), internalIterOpts{})
		var rangeDelIter keyspan.FragmentIterator
		// Pass in a non-nil pointer to rangeDelIter so that levelIter.findFileGE
		// sets it up for the target file.
		levelIter.initRangeDel(&rangeDelIter)

		rkeyLevelIter := &keyspan.LevelIter{}
		rkeyLevelIter.Init(
			keyspan.SpanIterOptions{}, cmp, newRangeKeyIter,
			v.Levels[level].Iter(), manifest.Level(level), manifest.KeyTypeRange,
		)

		kr := internalKeyRange{
			smallest: meta.Smallest,
			largest:  meta.Largest,
		}
		overlap := overlapWithIterator(levelIter, &rangeDelIter, rkeyLevelIter, kr, cmp)
		err := levelIter.Close() // Closes range del iter as well.
		err = firstError(err, rkeyLevelIter.Close())
		if err != nil {
			return 0, err
		}
		if overlap {
			return targetLevel, nil
		}

		// Check boundary overlap.
		boundaryOverlaps := v.Overlaps(level, cmp, meta.Smallest.UserKey,
			meta.Largest.UserKey, meta.Largest.IsExclusiveSentinel())
		if !boundaryOverlaps.Empty() {
			continue
		}

		// Check boundary overlap with any ongoing compactions.
		//
		// We cannot check for data overlap with the new SSTs compaction will
		// produce since compaction hasn't been done yet. However, there's no need
		// to check since all keys in them will either be from c.startLevel or
		// c.outputLevel, both levels having their data overlap already tested
		// negative (else we'd have returned earlier).
		overlaps := false
		for c := range compactions {
			if c.outputLevel == nil || level != c.outputLevel.level {
				continue
			}
			if cmp(meta.Smallest.UserKey, c.largest.UserKey) <= 0 &&
				cmp(meta.Largest.UserKey, c.smallest.UserKey) >= 0 {
				overlaps = true
				break
			}
		}
		if !overlaps {
			targetLevel = level
		}
	}
	return targetLevel, nil
}

// Ingest ingests a set of sstables into the DB. Ingestion of the files is
// atomic and semantically equivalent to creating a single batch containing all
// of the mutations in the sstables. Ingestion may require the memtable to be
// flushed. The ingested sstable files are moved into the DB and must reside on
// the same filesystem as the DB. Sstables can be created for ingestion using
// sstable.Writer. On success, Ingest removes the input paths.
//
// Two types of sstables are accepted for ingestion(s): one is sstables present
// in the instance's vfs.FS and can be referenced locally. The other is sstables
// present in shared.Storage, referred to as shared or foreign sstables. These
// shared sstables can be linked through objstorageprovider.Provider, and do not
// need to already be present on the local vfs.FS. Foreign sstables must all fit
// in an excise span, and are destined for a level specified in SharedSSTMeta.
//
// All sstables *must* be Sync()'d by the caller after all bytes are written
// and before its file handle is closed; failure to do so could violate
// durability or lead to corrupted on-disk state. This method cannot, in a
// platform-and-FS-agnostic way, ensure that all sstables in the input are
// properly synced to disk. Opening new file handles and Sync()-ing them
// does not always guarantee durability; see the discussion here on that:
// https://github.com/cockroachdb/pebble/pull/835#issuecomment-663075379
//
// Ingestion loads each sstable into the lowest level of the LSM which it
// doesn't overlap (see ingestTargetLevel). If an sstable overlaps a memtable,
// ingestion forces the memtable to flush, and then waits for the flush to
// occur. In some cases, such as with no foreign sstables and no excise span,
// ingestion that gets blocked on a memtable can join the flushable queue and
// finish even before the memtable has been flushed.
//
// The steps for ingestion are:
//
//  1. Allocate file numbers for every sstable being ingested.
//  2. Load the metadata for all sstables being ingested.
//  3. Sort the sstables by smallest key, verifying non overlap (for local
//     sstables).
//  4. Hard link (or copy) the local sstables into the DB directory.
//  5. Allocate a sequence number to use for all of the entries in the
//     local sstables. This is the step where overlap with memtables is
//     determined. If there is overlap, we remember the most recent memtable
//     that overlaps.
//  6. Update the sequence number in the ingested local sstables. (Shared
//     sstables get fixed sequence numbers that were determined at load time.)
//  7. Wait for the most recent memtable that overlaps to flush (if any).
//  8. Add the ingested sstables to the version (DB.ingestApply).
//     8.1.  If an excise span was specified, figure out what sstables in the
//     current version overlap with the excise span, and create new virtual
//     sstables out of those sstables that exclude the excised span (DB.excise).
//  9. Publish the ingestion sequence number.
//
// Note that if the mutable memtable overlaps with ingestion, a flush of the
// memtable is forced equivalent to DB.Flush. Additionally, subsequent
// mutations that get sequence numbers larger than the ingestion sequence
// number get queued up behind the ingestion waiting for it to complete. This
// can produce a noticeable hiccup in performance. See
// https://github.com/cockroachdb/pebble/issues/25 for an idea for how to fix
// this hiccup.
func (d *DB) Ingest(paths []string) error {
	if err := d.closed.Load(); err != nil {
		panic(err)
	}
	if d.opts.ReadOnly {
		return ErrReadOnly
	}
	_, err := d.ingest(paths, ingestTargetLevel, nil /* shared */, KeyRange{})
	return err
}

// IngestOperationStats provides some information about where in the LSM the
// bytes were ingested.
type IngestOperationStats struct {
	// Bytes is the total bytes in the ingested sstables.
	Bytes uint64
	// ApproxIngestedIntoL0Bytes is the approximate number of bytes ingested
	// into L0. This value is approximate when flushable ingests are active and
	// an ingest overlaps an entry in the flushable queue. Currently, this
	// approximation is very rough, only including tables that overlapped the
	// memtable. This estimate may be improved with #2112.
	ApproxIngestedIntoL0Bytes uint64
	// MemtableOverlappingFiles is the count of ingested sstables
	// that overlapped keys in the memtables.
	MemtableOverlappingFiles int
}

// IngestWithStats does the same as Ingest, and additionally returns
// IngestOperationStats.
func (d *DB) IngestWithStats(paths []string) (IngestOperationStats, error) {
	if err := d.closed.Load(); err != nil {
		panic(err)
	}
	if d.opts.ReadOnly {
		return IngestOperationStats{}, ErrReadOnly
	}
	return d.ingest(paths, ingestTargetLevel, nil /* shared */, KeyRange{})
}

// IngestAndExcise does the same as IngestWithStats, and additionally accepts a
// list of shared files to ingest that can be read from a shared.Storage through
// a Provider. All the shared files must live within exciseSpan, and any existing
// keys in exciseSpan are deleted by turning existing sstables into virtual
// sstables (if not virtual already) and shrinking their spans to exclude
// exciseSpan. See the comment at Ingest for a more complete picture of the
// ingestion process.
//
// Panics if this DB instance was not instantiated with a shared.Storage and
// shared sstables are present.
func (d *DB) IngestAndExcise(
	paths []string, shared []SharedSSTMeta, exciseSpan KeyRange,
) (IngestOperationStats, error) {
	if err := d.closed.Load(); err != nil {
		panic(err)
	}
	if d.opts.ReadOnly {
		return IngestOperationStats{}, ErrReadOnly
	}
	return d.ingest(paths, ingestTargetLevel, shared, exciseSpan)
}

// Both DB.mu and commitPipeline.mu must be held while this is called.
func (d *DB) newIngestedFlushableEntry(
	meta []*fileMetadata, seqNum uint64, logNum FileNum,
) (*flushableEntry, error) {
	// Update the sequence number for all of the sstables in the
	// metadata. Writing the metadata to the manifest when the
	// version edit is applied is the mechanism that persists the
	// sequence number. The sstables themselves are left unmodified.
	// In this case, a version edit will only be written to the manifest
	// when the flushable is eventually flushed. If Pebble restarts in that
	// time, then we'll lose the ingest sequence number information. But this
	// information will also be reconstructed on node restart.
	if err := ingestUpdateSeqNum(
		d.cmp, d.opts.Comparer.FormatKey, seqNum, meta,
	); err != nil {
		return nil, err
	}

	f := newIngestedFlushable(meta, d.cmp, d.split, d.newIters, d.tableNewRangeKeyIter)

	// NB: The logNum/seqNum are the WAL number which we're writing this entry
	// to and the sequence number within the WAL which we'll write this entry
	// to.
	entry := d.newFlushableEntry(f, logNum, seqNum)
	// The flushable entry starts off with a single reader ref, so increment
	// the FileMetadata.Refs.
	for _, file := range f.files {
		file.Ref()
	}
	entry.unrefFiles = func() []*fileBacking {
		var obsolete []*fileBacking
		for _, file := range f.files {
			if file.Unref() == 0 {
				obsolete = append(obsolete, file.FileMetadata.FileBacking)
			}
		}
		return obsolete
	}

	entry.flushForced = true
	entry.releaseMemAccounting = func() {}
	return entry, nil
}

// Both DB.mu and commitPipeline.mu must be held while this is called. Since
// we're holding both locks, the order in which we rotate the memtable or
// recycle the WAL in this function is irrelevant as long as the correct log
// numbers are assigned to the appropriate flushable.
func (d *DB) handleIngestAsFlushable(meta []*fileMetadata, seqNum uint64) error {
	b := d.NewBatch()
	for _, m := range meta {
		b.ingestSST(m.FileNum)
	}
	b.setSeqNum(seqNum)

	// If the WAL is disabled, then the logNum used to create the flushable
	// entry doesn't matter. We just use the logNum assigned to the current
	// mutable memtable. If the WAL is enabled, then this logNum will be
	// overwritten by the logNum of the log which will contain the log entry
	// for the ingestedFlushable.
	logNum := d.mu.mem.queue[len(d.mu.mem.queue)-1].logNum
	if !d.opts.DisableWAL {
		// We create a new WAL for the flushable instead of reusing the end of
		// the previous WAL. This simplifies the increment of the minimum
		// unflushed log number, and also simplifies WAL replay.
		logNum, _ = d.recycleWAL()
		d.mu.Unlock()
		err := d.commit.directWrite(b)
		if err != nil {
			d.opts.Logger.Fatalf("%v", err)
		}
		d.mu.Lock()
	}

	entry, err := d.newIngestedFlushableEntry(meta, seqNum, logNum)
	if err != nil {
		return err
	}
	nextSeqNum := seqNum + uint64(b.Count())

	// Set newLogNum to the logNum of the previous flushable. This value is
	// irrelevant if the WAL is disabled. If the WAL is enabled, then we set
	// the appropriate value below.
	newLogNum := d.mu.mem.queue[len(d.mu.mem.queue)-1].logNum
	if !d.opts.DisableWAL {
		// This is WAL num of the next mutable memtable which comes after the
		// ingestedFlushable in the flushable queue. The mutable memtable
		// will be created below.
		newLogNum, _ = d.recycleWAL()
		if err != nil {
			return err
		}
	}

	currMem := d.mu.mem.mutable
	// NB: Placing ingested sstables above the current memtables
	// requires rotating of the existing memtables/WAL. There is
	// some concern of churning through tiny memtables due to
	// ingested sstables being placed on top of them, but those
	// memtables would have to be flushed anyways.
	d.mu.mem.queue = append(d.mu.mem.queue, entry)
	d.rotateMemtable(newLogNum, nextSeqNum, currMem)
	d.updateReadStateLocked(d.opts.DebugCheck)
	d.maybeScheduleFlush()
	return nil
}

// See comment at Ingest() for details on how this works.
func (d *DB) ingest(
	paths []string,
	targetLevelFunc ingestTargetLevelFunc,
	shared []SharedSSTMeta,
	exciseSpan KeyRange,
) (IngestOperationStats, error) {
	if len(shared) > 0 && d.opts.Experimental.SharedStorage == nil {
		panic("cannot ingest shared sstables with nil SharedStorage")
	}
	if (exciseSpan.Valid() || len(shared) > 0) && d.opts.FormatMajorVersion < ExperimentalFormatVirtualSSTables {
		return IngestOperationStats{}, errors.New("pebble: format major version too old for excise or shared sstable ingestion")
	}
	// Allocate file numbers for all of the files being ingested and mark them as
	// pending in order to prevent them from being deleted. Note that this causes
	// the file number ordering to be out of alignment with sequence number
	// ordering. The sorting of L0 tables by sequence number avoids relying on
	// that (busted) invariant.
	d.mu.Lock()
	pendingOutputs := make([]base.DiskFileNum, len(paths)+len(shared))
	for i := range paths {
		pendingOutputs[i] = d.mu.versions.getNextFileNum().DiskFileNum()
	}
	for i := range shared {
		pendingOutputs[len(paths)+i] = d.mu.versions.getNextFileNum().DiskFileNum()
	}
	jobID := d.mu.nextJobID
	d.mu.nextJobID++
	d.mu.Unlock()

	// Load the metadata for all of the files being ingested. This step detects
	// and elides empty sstables.
	loadResult, err := ingestLoad(d.opts, d.FormatMajorVersion(), paths, shared, d.cacheID, pendingOutputs)
	if err != nil {
		return IngestOperationStats{}, err
	}

	if len(loadResult.localMeta) == 0 && len(loadResult.sharedMeta) == 0 {
		// All of the sstables to be ingested were empty. Nothing to do.
		return IngestOperationStats{}, nil
	}

	// Verify the sstables do not overlap.
	if err := ingestSortAndVerify(d.cmp, loadResult, exciseSpan); err != nil {
		return IngestOperationStats{}, err
	}

	// Hard link the sstables into the DB directory. Since the sstables aren't
	// referenced by a version, they won't be used. If the hard linking fails
	// (e.g. because the files reside on a different filesystem), ingestLink will
	// fall back to copying, and if that fails we undo our work and return an
	// error.
	if err := ingestLink(jobID, d.opts, d.objProvider, loadResult, shared); err != nil {
		return IngestOperationStats{}, err
	}
	for i, sharedMeta := range loadResult.sharedMeta {
		d.checkVirtualBounds(sharedMeta, &IterOptions{level: manifest.Level(loadResult.sharedLevels[i])})
	}
	// Make the new tables durable. We need to do this at some point before we
	// update the MANIFEST (via logAndApply), otherwise a crash can have the
	// tables referenced in the MANIFEST, but not present in the provider.
	if err := d.objProvider.Sync(); err != nil {
		return IngestOperationStats{}, err
	}

	// metaFlushableOverlaps is a slice parallel to meta indicating which of the
	// ingested sstables overlap some table in the flushable queue. It's used to
	// approximate ingest-into-L0 stats when using flushable ingests.
	metaFlushableOverlaps := make([]bool, len(loadResult.localMeta)+len(loadResult.sharedMeta))
	var mem *flushableEntry
	var mut *memTable
	// asFlushable indicates whether the sstable was ingested as a flushable.
	var asFlushable bool
	prepare := func(seqNum uint64) {
		// Note that d.commit.mu is held by commitPipeline when calling prepare.

		d.mu.Lock()
		defer d.mu.Unlock()

		// Check to see if any files overlap with any of the memtables. The queue
		// is ordered from oldest to newest with the mutable memtable being the
		// last element in the slice. We want to wait for the newest table that
		// overlaps.

		for i := len(d.mu.mem.queue) - 1; i >= 0; i-- {
			m := d.mu.mem.queue[i]
			iter := m.newIter(nil)
			rangeDelIter := m.newRangeDelIter(nil)
			rkeyIter := m.newRangeKeyIter(nil)

			checkForOverlap := func(i int, meta *fileMetadata) {
				if metaFlushableOverlaps[i] {
					// This table already overlapped a more recent flushable.
					return
				}
				kr := internalKeyRange{
					smallest: meta.Smallest,
					largest:  meta.Largest,
				}
				if overlapWithIterator(iter, &rangeDelIter, rkeyIter, kr, d.cmp) {
					// If this is the first table to overlap a flushable, save
					// the flushable. This ingest must be ingested or flushed
					// after it.
					if mem == nil {
						mem = m
					}
					metaFlushableOverlaps[i] = true
				}
			}
			for i := range loadResult.localMeta {
				checkForOverlap(i, loadResult.localMeta[i])
			}
			for i := range loadResult.sharedMeta {
				checkForOverlap(len(loadResult.localMeta)+i, loadResult.sharedMeta[i])
			}
			if exciseSpan.Valid() {
				kr := internalKeyRange{
					smallest: base.MakeInternalKey(exciseSpan.Start, InternalKeySeqNumMax, InternalKeyKindMax),
					largest:  base.MakeExclusiveSentinelKey(InternalKeyKindRangeDelete, exciseSpan.End),
				}
				if overlapWithIterator(iter, &rangeDelIter, rkeyIter, kr, d.cmp) {
					if mem == nil {
						mem = m
					}
				}
			}
			err := iter.Close()
			if rangeDelIter != nil {
				err = firstError(err, rangeDelIter.Close())
			}
			if rkeyIter != nil {
				err = firstError(err, rkeyIter.Close())
			}
			if err != nil {
				d.opts.Logger.Infof("ingest error reading flushable for log %s: %s", m.logNum, err)
			}
		}

		if mem == nil {
			// No overlap with any of the queued flushables, so no need to queue
			// after them.

			// New writes with higher sequence numbers may be concurrently
			// committed. We must ensure they don't flush before this ingest
			// completes. To do that, we ref the mutable memtable as a writer,
			// preventing its flushing (and the flushing of all subsequent
			// flushables in the queue). Once we've acquired the manifest lock
			// to add the ingested sstables to the LSM, we can unref as we're
			// guaranteed that the flush won't edit the LSM before this ingest.
			mut = d.mu.mem.mutable
			mut.writerRef()
			return
		}
		// The ingestion overlaps with some entry in the flushable queue.
		if d.FormatMajorVersion() < FormatFlushableIngest ||
			d.opts.Experimental.DisableIngestAsFlushable() ||
			len(shared) > 0 || exciseSpan.Valid() ||
			(len(d.mu.mem.queue) > d.opts.MemTableStopWritesThreshold-1) {
			// We're not able to ingest as a flushable,
			// so we must synchronously flush.
			//
			// TODO(bilal): Currently, if any of the files being ingested are shared or
			// there's an excise span present, we cannot use flushable ingests and need
			// to wait synchronously. Either remove this caveat by fleshing out
			// flushable ingest logic to also account for these cases, or remove this
			// comment. Tracking issue: https://github.com/cockroachdb/pebble/issues/2676
			if mem.flushable == d.mu.mem.mutable {
				err = d.makeRoomForWrite(nil)
			}
			// New writes with higher sequence numbers may be concurrently
			// committed. We must ensure they don't flush before this ingest
			// completes. To do that, we ref the mutable memtable as a writer,
			// preventing its flushing (and the flushing of all subsequent
			// flushables in the queue). Once we've acquired the manifest lock
			// to add the ingested sstables to the LSM, we can unref as we're
			// guaranteed that the flush won't edit the LSM before this ingest.
			mut = d.mu.mem.mutable
			mut.writerRef()
			mem.flushForced = true
			d.maybeScheduleFlush()
			return
		}
		// Since there aren't too many memtables already queued up, we can
		// slide the ingested sstables on top of the existing memtables.
		asFlushable = true
		err = d.handleIngestAsFlushable(loadResult.localMeta, seqNum)
	}

	var ve *versionEdit
	apply := func(seqNum uint64) {
		if err != nil || asFlushable {
			// An error occurred during prepare.
			if mut != nil {
				if mut.writerUnref() {
					d.mu.Lock()
					d.maybeScheduleFlush()
					d.mu.Unlock()
				}
			}
			return
		}

		// Update the sequence number for all local sstables in the
		// metadata. Writing the metadata to the manifest when the
		// version edit is applied is the mechanism that persists the
		// sequence number. The sstables themselves are left unmodified.
		//
		// For shared sstables, we do not need to update sequence numbers. These
		// sequence numbers are already set in ingestLoad.
		if err = ingestUpdateSeqNum(
			d.cmp, d.opts.Comparer.FormatKey, seqNum, loadResult.localMeta,
		); err != nil {
			if mut != nil {
				if mut.writerUnref() {
					d.mu.Lock()
					d.maybeScheduleFlush()
					d.mu.Unlock()
				}
			}
			return
		}

		// If we overlapped with a memtable in prepare wait for the flush to
		// finish.
		if mem != nil {
			<-mem.flushed
		}

		// Assign the sstables to the correct level in the LSM and apply the
		// version edit.
		ve, err = d.ingestApply(jobID, loadResult, targetLevelFunc, mut, exciseSpan)
	}

	// Only one ingest can occur at a time because if not, one would block waiting
	// for the other to finish applying. This blocking would happen while holding
	// the commit mutex which would prevent unrelated batches from writing their
	// changes to the WAL and memtable. This will cause a bigger commit hiccup
	// during ingestion.
	d.commit.ingestSem <- struct{}{}
	d.commit.AllocateSeqNum(len(loadResult.localPaths), prepare, apply)
	<-d.commit.ingestSem

	if err != nil {
		if err2 := ingestCleanup(d.objProvider, loadResult.localMeta); err2 != nil {
			d.opts.Logger.Infof("ingest cleanup failed: %v", err2)
		}
	} else {
		// Since we either created a hard link to the ingesting files, or copied
		// them over, it is safe to remove the originals paths.
		for _, path := range loadResult.localPaths {
			if err2 := d.opts.FS.Remove(path); err2 != nil {
				d.opts.Logger.Infof("ingest failed to remove original file: %s", err2)
			}
		}
	}

	// NB: Shared-sstable-only ingestions do not assign a sequence number to
	// any sstables.
	globalSeqNum := uint64(0)
	if len(loadResult.localMeta) > 0 {
		globalSeqNum = loadResult.localMeta[0].SmallestSeqNum
	}
	info := TableIngestInfo{
		JobID:        jobID,
		GlobalSeqNum: globalSeqNum,
		Err:          err,
		flushable:    asFlushable,
	}
	var stats IngestOperationStats
	if ve != nil {
		info.Tables = make([]struct {
			TableInfo
			Level int
		}, len(ve.NewFiles))
		for i := range ve.NewFiles {
			e := &ve.NewFiles[i]
			info.Tables[i].Level = e.Level
			info.Tables[i].TableInfo = e.Meta.TableInfo()
			stats.Bytes += e.Meta.Size
			if e.Level == 0 {
				stats.ApproxIngestedIntoL0Bytes += e.Meta.Size
			}
			if i < len(metaFlushableOverlaps) && metaFlushableOverlaps[i] {
				stats.MemtableOverlappingFiles++
			}
		}
	} else if asFlushable {
		// NB: If asFlushable == true, there are no shared sstables.
		info.Tables = make([]struct {
			TableInfo
			Level int
		}, len(loadResult.localMeta))
		for i, f := range loadResult.localMeta {
			info.Tables[i].Level = -1
			info.Tables[i].TableInfo = f.TableInfo()
			stats.Bytes += f.Size
			// We don't have exact stats on which files will be ingested into
			// L0, because actual ingestion into the LSM has been deferred until
			// flush time. Instead, we infer based on memtable overlap.
			//
			// TODO(jackson): If we optimistically compute data overlap (#2112)
			// before entering the commit pipeline, we can use that overlap to
			// improve our approximation by incorporating overlap with L0, not
			// just memtables.
			if metaFlushableOverlaps[i] {
				stats.ApproxIngestedIntoL0Bytes += f.Size
				stats.MemtableOverlappingFiles++
			}
		}
	}
	d.opts.EventListener.TableIngested(info)

	return stats, err
}

// excise updates ve to include a replacement of the file m with new virtual
// sstables that exclude exciseSpan, returning a slice of newly-created files if
// any. If the entirety of m is deleted by exciseSpan, no new sstables are added
// and m is deleted. Note that ve is updated in-place.
//
// The manifest lock must be held when calling this method.
func (d *DB) excise(
	exciseSpan KeyRange, m *fileMetadata, ve *versionEdit, level int,
) ([]manifest.NewFileEntry, error) {
	numCreatedFiles := 0
	// Check if there's actually an overlap between m and exciseSpan.
	if !exciseSpan.Overlaps(d.cmp, m) {
		return nil, nil
	}
	ve.DeletedFiles[deletedFileEntry{
		Level:   level,
		FileNum: m.FileNum,
	}] = m
	// Fast path: m sits entirely within the exciseSpan, so just delete it.
	if exciseSpan.Contains(d.cmp, m.Smallest) && exciseSpan.Contains(d.cmp, m.Largest) {
		return nil, nil
	}
	var iter internalIterator
	var rangeDelIter keyspan.FragmentIterator
	var rangeKeyIter keyspan.FragmentIterator
	backingTableCreated := false
	// Create a file to the left of the excise span, if necessary.
	// The bounds of this file will be [m.Smallest, lastKeyBefore(exciseSpan.Start)].
	//
	// We create bounds that are tight on user keys, and we make the effort to find
	// the last key in the original sstable that's smaller than exciseSpan.Start
	// even though it requires some sstable reads. We could choose to create
	// virtual sstables on loose userKey bounds, in which case we could just set
	// leftFile.Largest to an exclusive sentinel at exciseSpan.Start. The biggest
	// issue with that approach would be that it'd lead to lots of small virtual
	// sstables in the LSM that have no guarantee on containing even a single user
	// key within the file bounds. This has the potential to increase both read and
	// write-amp as we will be opening up these sstables only to find no relevant
	// keys in the read path, and compacting sstables on top of them instead of
	// directly into the space occupied by them. We choose to incur the cost of
	// calculating tight bounds at this time instead of creating more work in the
	// future.
	//
	// TODO(bilal): Some of this work can happen without grabbing the manifest
	// lock; we could grab one currentVersion, release the lock, calculate excised
	// files, then grab the lock again and recalculate for just the files that
	// have changed since our previous calculation. Do this optimiaztino as part of
	// https://github.com/cockroachdb/pebble/issues/2112 .
	if d.cmp(m.Smallest.UserKey, exciseSpan.Start) < 0 {
		leftFile := &fileMetadata{
			Virtual:     true,
			FileBacking: m.FileBacking,
			FileNum:     d.mu.versions.getNextFileNum(),
			// Note that these are loose bounds for smallest/largest seqnums, but they're
			// sufficient for maintaining correctness.
			SmallestSeqNum: m.SmallestSeqNum,
			LargestSeqNum:  m.LargestSeqNum,
		}
		if m.HasPointKeys && !exciseSpan.Contains(d.cmp, m.SmallestPointKey) {
			// This file will contain point keys
			smallestPointKey := m.SmallestPointKey
			var err error
			iter, rangeDelIter, err = d.newIters(context.TODO(), m, &IterOptions{level: manifest.Level(level)}, internalIterOpts{})
			if err != nil {
				return nil, err
			}
			var key *InternalKey
			if iter != nil {
				defer iter.Close()
				key, _ = iter.SeekLT(exciseSpan.Start, base.SeekLTFlagsNone)
			} else {
				iter = emptyIter
			}
			if key != nil {
				leftFile.ExtendPointKeyBounds(d.cmp, smallestPointKey, key.Clone())
			}
			// Store the min of (exciseSpan.Start, rdel.End) in lastRangeDel. This
			// needs to be a copy if the key is owned by the range del iter.
			var lastRangeDel []byte
			if rangeDelIter != nil {
				defer rangeDelIter.Close()
				rdel := rangeDelIter.SeekLT(exciseSpan.Start)
				if rdel != nil {
					lastRangeDel = append(lastRangeDel[:0], rdel.End...)
					if d.cmp(lastRangeDel, exciseSpan.Start) > 0 {
						lastRangeDel = exciseSpan.Start
					}
				}
			} else {
				rangeDelIter = emptyKeyspanIter
			}
			if lastRangeDel != nil {
				leftFile.ExtendPointKeyBounds(d.cmp, smallestPointKey, base.MakeExclusiveSentinelKey(InternalKeyKindRangeDelete, lastRangeDel))
			}
		}
		if m.HasRangeKeys && !exciseSpan.Contains(d.cmp, m.SmallestRangeKey) {
			// This file will contain range keys
			var err error
			smallestRangeKey := m.SmallestRangeKey
			rangeKeyIter, err = d.tableNewRangeKeyIter(m, keyspan.SpanIterOptions{})
			if err != nil {
				return nil, err
			}
			// Store the min of (exciseSpan.Start, rkey.End) in lastRangeKey. This
			// needs to be a copy if the key is owned by the range key iter.
			var lastRangeKey []byte
			var lastRangeKeyKind InternalKeyKind
			defer rangeKeyIter.Close()
			rkey := rangeKeyIter.SeekLT(exciseSpan.Start)
			if rkey != nil {
				lastRangeKey = append(lastRangeKey[:0], rkey.End...)
				if d.cmp(lastRangeKey, exciseSpan.Start) > 0 {
					lastRangeKey = exciseSpan.Start
				}
				lastRangeKeyKind = rkey.Keys[0].Kind()
			}
			if lastRangeKey != nil {
				leftFile.ExtendRangeKeyBounds(d.cmp, smallestRangeKey, base.MakeExclusiveSentinelKey(lastRangeKeyKind, lastRangeKey))
			}
		}
		if leftFile.HasRangeKeys || leftFile.HasPointKeys {
			var err error
			leftFile.Size, err = d.tableCache.estimateSize(m, leftFile.Smallest.UserKey, leftFile.Largest.UserKey)
			if err != nil {
				return nil, err
			}
			if leftFile.Size == 0 {
				// On occasion, estimateSize gives us a low estimate, i.e. a 0 file size,
				// such as if the excised file only has range keys/dels and no point
				// keys. This can cause panics in places where we divide by file sizes.
				// Correct for it here.
				leftFile.Size = 1
			}
			if err := leftFile.Validate(d.cmp, d.opts.Comparer.FormatKey); err != nil {
				return nil, err
			}
			leftFile.ValidateVirtual(m)
			d.checkVirtualBounds(leftFile, nil /* iterOptions */)
			ve.NewFiles = append(ve.NewFiles, newFileEntry{Level: level, Meta: leftFile})
			ve.CreatedBackingTables = append(ve.CreatedBackingTables, leftFile.FileBacking)
			backingTableCreated = true
			numCreatedFiles++
		}
	}
	// Create a file to the right, if necessary.
	if exciseSpan.Contains(d.cmp, m.Largest) {
		// No key exists to the right of the excise span in this file.
		return ve.NewFiles[len(ve.NewFiles)-numCreatedFiles:], nil
	}
	// Create a new file, rightFile, between [firstKeyAfter(exciseSpan.End), m.Largest].
	//
	// See comment before the definition of leftFile for the motivation behind
	// calculating tight user-key bounds.
	rightFile := &fileMetadata{
		Virtual:     true,
		FileBacking: m.FileBacking,
		FileNum:     d.mu.versions.getNextFileNum(),
		// Note that these are loose bounds for smallest/largest seqnums, but they're
		// sufficient for maintaining correctness.
		SmallestSeqNum: m.SmallestSeqNum,
		LargestSeqNum:  m.LargestSeqNum,
	}
	if m.HasPointKeys && !exciseSpan.Contains(d.cmp, m.LargestPointKey) {
		// This file will contain point keys
		largestPointKey := m.LargestPointKey
		var err error
		if iter == nil && rangeDelIter == nil {
			iter, rangeDelIter, err = d.newIters(context.TODO(), m, &IterOptions{level: manifest.Level(level)}, internalIterOpts{})
			if err != nil {
				return nil, err
			}
			if iter != nil {
				defer iter.Close()
			} else {
				iter = emptyIter
			}
			if rangeDelIter != nil {
				defer rangeDelIter.Close()
			} else {
				rangeDelIter = emptyKeyspanIter
			}
		}
		key, _ := iter.SeekGE(exciseSpan.End, base.SeekGEFlagsNone)
		if key != nil {
			rightFile.ExtendPointKeyBounds(d.cmp, key.Clone(), largestPointKey)
		}
		// Store the max of (exciseSpan.End, rdel.Start) in firstRangeDel. This
		// needs to be a copy if the key is owned by the range del iter.
		var firstRangeDel []byte
		rdel := rangeDelIter.SeekGE(exciseSpan.End)
		if rdel != nil {
			firstRangeDel = append(firstRangeDel[:0], rdel.Start...)
			if d.cmp(firstRangeDel, exciseSpan.End) < 0 {
				firstRangeDel = exciseSpan.End
			}
		}
		if firstRangeDel != nil {
			smallestPointKey := rdel.SmallestKey()
			smallestPointKey.UserKey = firstRangeDel
			rightFile.ExtendPointKeyBounds(d.cmp, smallestPointKey, largestPointKey)
		}
	} else if m.HasRangeKeys && !exciseSpan.Contains(d.cmp, m.LargestRangeKey) {
		// This file will contain range keys.
		largestRangeKey := m.LargestRangeKey
		if rangeKeyIter == nil {
			var err error
			rangeKeyIter, err = d.tableNewRangeKeyIter(m, keyspan.SpanIterOptions{})
			if err != nil {
				return nil, err
			}
			defer rangeKeyIter.Close()
		}
		// Store the max of (exciseSpan.End, rkey.Start) in firstRangeKey. This
		// needs to be a copy if the key is owned by the range key iter.
		var firstRangeKey []byte
		rkey := rangeKeyIter.SeekGE(exciseSpan.End)
		if rkey != nil {
			firstRangeKey = append(firstRangeKey[:0], rkey.Start...)
			if d.cmp(firstRangeKey, exciseSpan.End) < 0 {
				firstRangeKey = exciseSpan.End
			}
		}
		if firstRangeKey != nil {
			smallestRangeKey := rkey.SmallestKey()
			smallestRangeKey.UserKey = firstRangeKey
			// We call ExtendRangeKeyBounds so any internal boundType fields are
			// set correctly. Note that this is mildly wasteful as we'll be comparing
			// rightFile.{Smallest,Largest}RangeKey with themselves, which can be
			// avoided if we exported ExtendOverallKeyBounds or so.
			rightFile.ExtendRangeKeyBounds(d.cmp, smallestRangeKey, largestRangeKey)
		}
	}
	if rightFile.HasRangeKeys || rightFile.HasPointKeys {
		var err error
		rightFile.Size, err = d.tableCache.estimateSize(m, rightFile.Smallest.UserKey, rightFile.Largest.UserKey)
		if err != nil {
			return nil, err
		}
		if rightFile.Size == 0 {
			// On occasion, estimateSize gives us a low estimate, i.e. a 0 file size,
			// such as if the excised file only has range keys/dels and no point keys.
			// This can cause panics in places where we divide by file sizes. Correct
			// for it here.
			rightFile.Size = 1
		}
		rightFile.ValidateVirtual(m)
		d.checkVirtualBounds(rightFile, nil /* iterOptions */)
		ve.NewFiles = append(ve.NewFiles, newFileEntry{Level: level, Meta: rightFile})
		if !backingTableCreated {
			ve.CreatedBackingTables = append(ve.CreatedBackingTables, rightFile.FileBacking)
			backingTableCreated = true
		}
		numCreatedFiles++
	}

	if err := rightFile.Validate(d.cmp, d.opts.Comparer.FormatKey); err != nil {
		return nil, err
	}
	return ve.NewFiles[len(ve.NewFiles)-numCreatedFiles:], nil
}

type ingestTargetLevelFunc func(
	newIters tableNewIters,
	newRangeKeyIter keyspan.TableNewSpanIter,
	iterOps IterOptions,
	cmp Compare,
	v *version,
	baseLevel int,
	compactions map[*compaction]struct{},
	meta *fileMetadata,
) (int, error)

func (d *DB) ingestApply(
	jobID int,
	lr ingestLoadResult,
	findTargetLevel ingestTargetLevelFunc,
	mut *memTable,
	exciseSpan KeyRange,
) (*versionEdit, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	ve := &versionEdit{
		NewFiles: make([]newFileEntry, len(lr.localMeta)+len(lr.sharedMeta)),
	}
	if exciseSpan.Valid() {
		ve.DeletedFiles = map[manifest.DeletedFileEntry]*manifest.FileMetadata{}
	}
	metrics := make(map[int]*LevelMetrics)

	// Lock the manifest for writing before we use the current version to
	// determine the target level. This prevents two concurrent ingestion jobs
	// from using the same version to determine the target level, and also
	// provides serialization with concurrent compaction and flush jobs.
	// logAndApply unconditionally releases the manifest lock, but any earlier
	// returns must unlock the manifest.
	d.mu.versions.logLock()

	if mut != nil {
		// Unref the mutable memtable to allows its flush to proceed. Now that we've
		// acquired the manifest lock, we can be certain that if the mutable
		// memtable has received more recent conflicting writes, the flush won't
		// beat us to applying to the manifest resulting in sequence number
		// inversion. Even though we call maybeScheduleFlush right now, this flush
		// will apply after our ingestion.
		if mut.writerUnref() {
			d.maybeScheduleFlush()
		}
	}

	current := d.mu.versions.currentVersion()
	baseLevel := d.mu.versions.picker.getBaseLevel()
	iterOps := IterOptions{logger: d.opts.Logger}
	for i := 0; i < len(lr.localMeta)+len(lr.sharedMeta); i++ {
		// Determine the lowest level in the LSM for which the sstable doesn't
		// overlap any existing files in the level.
		var m *fileMetadata
		sharedIdx := -1
		sharedLevel := -1
		if i < len(lr.localMeta) {
			m = lr.localMeta[i]
		} else {
			sharedIdx = i - len(lr.localMeta)
			m = lr.sharedMeta[sharedIdx]
			sharedLevel = int(lr.sharedLevels[sharedIdx])
		}
		f := &ve.NewFiles[i]
		var err error
		if sharedIdx >= 0 {
			f.Level = sharedLevel
			if f.Level < sharedLevelsStart {
				panic("cannot slot a shared file higher than the highest shared level")
			}
			ve.CreatedBackingTables = append(ve.CreatedBackingTables, m.FileBacking)
		} else {
			if exciseSpan.Valid() && exciseSpan.Contains(d.cmp, m.Smallest) && exciseSpan.Contains(d.cmp, m.Largest) {
				// This file fits perfectly within the excise span. We can slot it at
				// L6, or sharedLevelsStart - 1 if we have shared files.
				if len(lr.sharedMeta) > 0 {
					f.Level = sharedLevelsStart - 1
					if baseLevel > f.Level {
						f.Level = 0
					}
				} else {
					f.Level = 6
				}
			} else {
				f.Level, err = findTargetLevel(d.newIters, d.tableNewRangeKeyIter, iterOps, d.cmp, current, baseLevel, d.mu.compact.inProgress, m)
			}
		}
		if err != nil {
			d.mu.versions.logUnlock()
			return nil, err
		}
		f.Meta = m
		levelMetrics := metrics[f.Level]
		if levelMetrics == nil {
			levelMetrics = &LevelMetrics{}
			metrics[f.Level] = levelMetrics
		}
		levelMetrics.NumFiles++
		levelMetrics.Size += int64(m.Size)
		levelMetrics.BytesIngested += m.Size
		levelMetrics.TablesIngested++
	}
	if exciseSpan.Valid() {
		// Iterate through all levels and find files that intersect with exciseSpan.
		//
		// TODO(bilal): We could drop the DB mutex here as we don't need it for
		// excises; we only need to hold the version lock which we already are
		// holding. However releasing the DB mutex could mess with the
		// ingestTargetLevel calculation that happened above, as it assumed that it
		// had a complete view of in-progress compactions that wouldn't change
		// until logAndApply is called. If we were to drop the mutex now, we could
		// schedule another in-progress compaction that would go into the chosen target
		// level and lead to file overlap within level (which would panic in
		// logAndApply). We should drop the db mutex here, do the excise, then
		// re-grab the DB mutex and rerun just the in-progress compaction check to
		// see if any new compactions are conflicting with our chosen target levels
		// for files, and if they are, we should signal those compactions to error
		// out.
		for level := range current.Levels {
			overlaps := current.Overlaps(level, d.cmp, exciseSpan.Start, exciseSpan.End, true /* exclusiveEnd */)
			iter := overlaps.Iter()

			for m := iter.First(); m != nil; m = iter.Next() {
				excised, err := d.excise(exciseSpan, m, ve, level)
				if err != nil {
					return nil, err
				}

				if _, ok := ve.DeletedFiles[deletedFileEntry{
					Level:   level,
					FileNum: m.FileNum,
				}]; !ok {
					// We did not excise this file.
					continue
				}
				levelMetrics := metrics[level]
				if levelMetrics == nil {
					levelMetrics = &LevelMetrics{}
					metrics[level] = levelMetrics
				}
				levelMetrics.NumFiles--
				levelMetrics.Size -= int64(m.Size)
				for i := range excised {
					levelMetrics.NumFiles++
					levelMetrics.Size += int64(excised[i].Meta.Size)
				}
			}
		}
	}
	for c := range d.mu.compact.inProgress {
		if c.versionEditApplied {
			continue
		}
		// Check if this compaction overlaps with the excise span. Note that just
		// checking if the inputs individually overlap with the excise span
		// isn't sufficient; for instance, a compaction could have [a,b] and [e,f]
		// as inputs and write it all out as [a,b,e,f] in one sstable. If we're
		// doing a [c,d) excise at the same time as this compaction, we will have
		// to error out the whole compaction as we can't guarantee it hasn't/won't
		// write a file overlapping with the excise span.
		if exciseSpan.OverlapsInternalKeyRange(d.cmp, c.smallest, c.largest) {
			c.cancel.Store(true)
		}
	}
	if err := d.mu.versions.logAndApply(jobID, ve, metrics, false /* forceRotation */, func() []compactionInfo {
		return d.getInProgressCompactionInfoLocked(nil)
	}); err != nil {
		return nil, err
	}

	d.mu.versions.metrics.Ingest.Count++

	d.updateReadStateLocked(d.opts.DebugCheck)
	// updateReadStateLocked could have generated obsolete tables, schedule a
	// cleanup job if necessary.
	d.deleteObsoleteFiles(jobID)
	d.updateTableStatsLocked(ve.NewFiles)
	// The ingestion may have pushed a level over the threshold for compaction,
	// so check to see if one is necessary and schedule it.
	d.maybeScheduleCompaction()
	d.maybeValidateSSTablesLocked(ve.NewFiles)
	return ve, nil
}

// maybeValidateSSTablesLocked adds the slice of newFileEntrys to the pending
// queue of files to be validated, when the feature is enabled.
// DB.mu must be locked when calling.
//
// TODO(bananabrick): Make sure that the ingestion step only passes in the
// physical sstables for validation here.
func (d *DB) maybeValidateSSTablesLocked(newFiles []newFileEntry) {
	// Only add to the validation queue when the feature is enabled.
	if !d.opts.Experimental.ValidateOnIngest {
		return
	}

	for _, f := range newFiles {
		if f.Meta.Virtual {
			panic("pebble: invalid call to maybeValidateSSTablesLocked")
		}
	}

	d.mu.tableValidation.pending = append(d.mu.tableValidation.pending, newFiles...)
	if d.shouldValidateSSTablesLocked() {
		go d.validateSSTables()
	}
}

// shouldValidateSSTablesLocked returns true if SSTable validation should run.
// DB.mu must be locked when calling.
func (d *DB) shouldValidateSSTablesLocked() bool {
	return !d.mu.tableValidation.validating &&
		d.closed.Load() == nil &&
		d.opts.Experimental.ValidateOnIngest &&
		len(d.mu.tableValidation.pending) > 0
}

// validateSSTables runs a round of validation on the tables in the pending
// queue.
func (d *DB) validateSSTables() {
	d.mu.Lock()
	if !d.shouldValidateSSTablesLocked() {
		d.mu.Unlock()
		return
	}

	pending := d.mu.tableValidation.pending
	d.mu.tableValidation.pending = nil
	d.mu.tableValidation.validating = true
	jobID := d.mu.nextJobID
	d.mu.nextJobID++
	rs := d.loadReadState()

	// Drop DB.mu before performing IO.
	d.mu.Unlock()

	// Validate all tables in the pending queue. This could lead to a situation
	// where we are starving IO from other tasks due to having to page through
	// all the blocks in all the sstables in the queue.
	// TODO(travers): Add some form of pacing to avoid IO starvation.
	for _, f := range pending {
		// The file may have been moved or deleted since it was ingested, in
		// which case we skip.
		if !rs.current.Contains(f.Level, d.cmp, f.Meta) {
			// Assume the file was moved to a lower level. It is rare enough
			// that a table is moved or deleted between the time it was ingested
			// and the time the validation routine runs that the overall cost of
			// this inner loop is tolerably low, when amortized over all
			// ingested tables.
			found := false
			for i := f.Level + 1; i < numLevels; i++ {
				if rs.current.Contains(i, d.cmp, f.Meta) {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		err := d.tableCache.withReader(
			f.Meta.PhysicalMeta(), func(r *sstable.Reader) error {
				return r.ValidateBlockChecksums()
			})
		if err != nil {
			// TODO(travers): Hook into the corruption reporting pipeline, once
			// available. See pebble#1192.
			d.opts.Logger.Fatalf("pebble: encountered corruption during ingestion: %s", err)
		}

		d.opts.EventListener.TableValidated(TableValidatedInfo{
			JobID: jobID,
			Meta:  f.Meta,
		})
	}
	rs.unref()

	d.mu.Lock()
	defer d.mu.Unlock()
	d.mu.tableValidation.validating = false
	d.mu.tableValidation.cond.Broadcast()
	if d.shouldValidateSSTablesLocked() {
		go d.validateSSTables()
	}
}
