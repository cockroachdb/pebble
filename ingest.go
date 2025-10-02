// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/overlap"
	"github.com/cockroachdb/pebble/internal/sstableinternal"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/remote"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/block"
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

func ingestValidateKey(opts *Options, key *InternalKey) error {
	if key.Kind() == InternalKeyKindInvalid {
		return base.CorruptionErrorf("pebble: external sstable has corrupted key: %s",
			key.Pretty(opts.Comparer.FormatKey))
	}
	if key.SeqNum() != 0 {
		return base.CorruptionErrorf("pebble: external sstable has non-zero seqnum: %s",
			key.Pretty(opts.Comparer.FormatKey))
	}
	if err := opts.Comparer.ValidateKey.Validate(key.UserKey); err != nil {
		return base.CorruptionErrorf("pebble: external sstable has corrupted key: %s, %w",
			key.Pretty(opts.Comparer.FormatKey), err)
	}
	return nil
}

// ingestSynthesizeShared constructs a fileMetadata for one shared sstable owned
// or shared by another node.
func ingestSynthesizeShared(
	opts *Options, sm SharedSSTMeta, tableNum base.TableNum,
) (*manifest.TableMetadata, error) {
	if sm.Size == 0 {
		// Disallow 0 file sizes
		return nil, errors.New("pebble: cannot ingest shared file with size 0")
	}
	// Don't load table stats. Doing a round trip to shared storage, one SST
	// at a time is not worth it as it slows down ingestion.
	meta := &manifest.TableMetadata{
		TableNum:     tableNum,
		CreationTime: time.Now().Unix(),
		Virtual:      true,
		Size:         sm.Size,
	}
	if sm.LargestPointKey.Valid() && sm.LargestPointKey.UserKey != nil {
		// Initialize meta.{HasPointKeys,Smallest,Largest}, etc.
		//
		// NB: We create new internal keys and pass them into ExtendPointKeyBounds
		// so that we can sub a zero sequence number into the bounds. We can set
		// the sequence number to anything here; it'll be reset in ingestUpdateSeqNum
		// anyway. However, we do need to use the same sequence number across all
		// bound keys at this step so that we end up with bounds that are consistent
		// across point/range keys.
		//
		// Because of the sequence number rewriting, we cannot use the Kind of
		// sm.SmallestPointKey. For example, the original SST might start with
		// a.SET.2 and a.RANGEDEL.1 (with a.SET.2 being the smallest key); after
		// rewriting the sequence numbers, these keys become a.SET.100 and
		// a.RANGEDEL.100, with a.RANGEDEL.100 being the smallest key. To create a
		// correct bound, we just use the maximum key kind (which sorts first).
		// Similarly, we use the smallest key kind for the largest key.
		smallestPointKey := base.MakeInternalKey(sm.SmallestPointKey.UserKey, 0, base.InternalKeyKindMaxForSSTable)
		largestPointKey := base.MakeInternalKey(sm.LargestPointKey.UserKey, 0, 0)
		if sm.LargestPointKey.IsExclusiveSentinel() {
			largestPointKey = base.MakeRangeDeleteSentinelKey(sm.LargestPointKey.UserKey)
		}
		if opts.Comparer.Equal(smallestPointKey.UserKey, largestPointKey.UserKey) &&
			smallestPointKey.Trailer < largestPointKey.Trailer {
			// We get kinds from the sender, however we substitute our own sequence
			// numbers. This can result in cases where an sstable [b#5,SET-b#4,DELSIZED]
			// becomes [b#0,SET-b#0,DELSIZED] when we synthesize it here, but the
			// kinds need to be reversed now because DelSized > Set.
			smallestPointKey, largestPointKey = largestPointKey, smallestPointKey
		}
		meta.ExtendPointKeyBounds(opts.Comparer.Compare, smallestPointKey, largestPointKey)
	}
	if sm.LargestRangeKey.Valid() && sm.LargestRangeKey.UserKey != nil {
		// Initialize meta.{HasRangeKeys,Smallest,Largest}, etc.
		//
		// See comment above on why we use a zero sequence number and these key
		// kinds here.
		smallestRangeKey := base.MakeInternalKey(sm.SmallestRangeKey.UserKey, 0, base.InternalKeyKindRangeKeyMax)
		largestRangeKey := base.MakeExclusiveSentinelKey(base.InternalKeyKindRangeKeyMin, sm.LargestRangeKey.UserKey)
		meta.ExtendRangeKeyBounds(opts.Comparer.Compare, smallestRangeKey, largestRangeKey)
	}

	// For simplicity, we use the same number for both the FileNum and the
	// DiskFileNum (even though this is a virtual sstable). Pass the underlying
	// TableBacking's size to the same size as the virtualized view of the sstable.
	// This ensures that we don't over-prioritize this sstable for compaction just
	// yet, as we do not have a clear sense of what parts of this sstable are
	// referenced by other nodes.
	meta.InitVirtualBacking(base.DiskFileNum(tableNum), sm.Size)

	if err := meta.Validate(opts.Comparer.Compare, opts.Comparer.FormatKey); err != nil {
		return nil, err
	}
	return meta, nil
}

// ingestLoad1External loads the fileMetadata for one external sstable.
// Sequence number and target level calculation happens during prepare/apply.
func ingestLoad1External(
	opts *Options, e ExternalFile, tableNum base.TableNum,
) (*manifest.TableMetadata, error) {
	if e.Size == 0 {
		return nil, errors.New("pebble: cannot ingest external file with size 0")
	}
	if !e.HasRangeKey && !e.HasPointKey {
		return nil, errors.New("pebble: cannot ingest external file with no point or range keys")
	}

	if opts.Comparer.Compare(e.StartKey, e.EndKey) > 0 {
		return nil, errors.Newf("pebble: external file bounds [%q, %q) are invalid", e.StartKey, e.EndKey)
	}
	if opts.Comparer.Compare(e.StartKey, e.EndKey) == 0 && !e.EndKeyIsInclusive {
		return nil, errors.Newf("pebble: external file bounds [%q, %q) are invalid", e.StartKey, e.EndKey)
	}
	if n := opts.Comparer.Split(e.StartKey); n != len(e.StartKey) {
		return nil, errors.Newf("pebble: external file bounds start key %q has suffix", e.StartKey)
	}
	if n := opts.Comparer.Split(e.EndKey); n != len(e.EndKey) {
		return nil, errors.Newf("pebble: external file bounds end key %q has suffix", e.EndKey)
	}

	// Don't load table stats. Doing a round trip to shared storage, one SST
	// at a time is not worth it as it slows down ingestion.
	meta := &manifest.TableMetadata{
		TableNum:     tableNum,
		CreationTime: time.Now().Unix(),
		Size:         e.Size,
		Virtual:      true,
	}
	// In the name of keeping this ingestion as fast as possible, we avoid *all*
	// existence checks and synthesize a table metadata with smallest/largest
	// keys that overlap whatever the passed-in span was.
	smallestCopy := slices.Clone(e.StartKey)
	largestCopy := slices.Clone(e.EndKey)
	if e.HasPointKey {
		// Sequence numbers are updated later by
		// ingestUpdateSeqNum, applying a squence number that
		// is applied to all keys in the sstable.
		if e.EndKeyIsInclusive {
			meta.ExtendPointKeyBounds(
				opts.Comparer.Compare,
				base.MakeInternalKey(smallestCopy, 0, base.InternalKeyKindMaxForSSTable),
				base.MakeInternalKey(largestCopy, 0, 0))
		} else {
			meta.ExtendPointKeyBounds(
				opts.Comparer.Compare,
				base.MakeInternalKey(smallestCopy, 0, base.InternalKeyKindMaxForSSTable),
				base.MakeRangeDeleteSentinelKey(largestCopy))
		}
	}
	if e.HasRangeKey {
		meta.ExtendRangeKeyBounds(
			opts.Comparer.Compare,
			base.MakeInternalKey(smallestCopy, 0, InternalKeyKindRangeKeyMax),
			base.MakeExclusiveSentinelKey(InternalKeyKindRangeKeyMin, largestCopy),
		)
	}

	meta.SyntheticPrefixAndSuffix = sstable.MakeSyntheticPrefixAndSuffix(e.SyntheticPrefix, e.SyntheticSuffix)

	return meta, nil
}

type rangeKeyIngestValidator struct {
	// lastRangeKey is the last range key seen in the previous file.
	lastRangeKey keyspan.Span
	// comparer, if unset, disables range key validation.
	comparer *base.Comparer
}

func disableRangeKeyChecks() rangeKeyIngestValidator {
	return rangeKeyIngestValidator{}
}

func validateSuffixedBoundaries(
	cmp *base.Comparer, lastRangeKey keyspan.Span,
) rangeKeyIngestValidator {
	return rangeKeyIngestValidator{
		lastRangeKey: lastRangeKey,
		comparer:     cmp,
	}
}

// Validate valides if the stored state of this rangeKeyIngestValidator allows for
// a file with the given nextFileSmallestKey to be ingested, such that the stored
// last file's largest range key defragments cleanly with the next file's smallest
// key if it was suffixed. If a value of nil is passed in for nextFileSmallestKey,
// that denotes the next file does not have a range key or there is no next file.
func (r *rangeKeyIngestValidator) Validate(nextFileSmallestKey *keyspan.Span) error {
	if r.comparer == nil {
		return nil
	}
	if r.lastRangeKey.Valid() {
		if r.comparer.Split.HasSuffix(r.lastRangeKey.End) {
			if nextFileSmallestKey == nil || !r.comparer.Equal(r.lastRangeKey.End, nextFileSmallestKey.Start) {
				// The last range key has a suffix, and it doesn't defragment cleanly with this range key.
				return errors.AssertionFailedf("pebble: ingest sstable has suffixed largest range key that does not match the start key of the next sstable: %s",
					r.comparer.FormatKey(r.lastRangeKey.End))
			} else if !keyspan.DefragmentInternal.ShouldDefragment(r.comparer.CompareRangeSuffixes, &r.lastRangeKey, nextFileSmallestKey) {
				// The last range key has a suffix, and it doesn't defragment cleanly with this range key.
				return errors.AssertionFailedf("pebble: ingest sstable has suffixed range key that won't defragment with next sstable: %s",
					r.comparer.FormatKey(r.lastRangeKey.End))
			}
		}
	} else if nextFileSmallestKey != nil && r.comparer.Split.HasSuffix(nextFileSmallestKey.Start) {
		return errors.Newf("pebble: ingest sstable has suffixed range key start that won't defragment: %s",
			r.comparer.FormatKey(nextFileSmallestKey.Start))
	}
	return nil
}

// ingestLoad1 creates the TableMetadata for one file. This file will be owned
// by this store.
//
// prevLastRangeKey is the last range key from the previous file. It is used to
// ensure that the range keys defragment cleanly across files. These checks
// are disabled if disableRangeKeyChecks is true.
func ingestLoad1(
	ctx context.Context,
	opts *Options,
	fmv FormatMajorVersion,
	readable objstorage.Readable,
	cacheHandle *cache.Handle,
	tableNum base.TableNum,
	rangeKeyValidator rangeKeyIngestValidator,
) (
	meta *manifest.TableMetadata,
	lastRangeKey keyspan.Span,
	blockReadStats base.BlockReadStats,
	err error,
) {
	o := opts.MakeReaderOptions()
	o.CacheOpts = sstableinternal.CacheOptions{
		CacheHandle: cacheHandle,
		FileNum:     base.PhysicalTableDiskFileNum(tableNum),
	}
	r, err := sstable.NewReader(ctx, readable, o)
	if err != nil {
		return nil, keyspan.Span{}, base.BlockReadStats{}, errors.CombineErrors(err, readable.Close())
	}
	defer func() { _ = r.Close() }()

	// Avoid ingesting tables with format versions this DB doesn't support.
	tf, err := r.TableFormat()
	if err != nil {
		return nil, keyspan.Span{}, base.BlockReadStats{}, err
	}
	if tf < fmv.MinTableFormat() || tf > fmv.MaxTableFormat() {
		return nil, keyspan.Span{}, base.BlockReadStats{}, errors.Newf(
			"pebble: table format %s is not within range supported at DB format major version %d, (%s,%s)",
			tf, fmv, fmv.MinTableFormat(), fmv.MaxTableFormat(),
		)
	}

	if r.Attributes.Has(sstable.AttributeBlobValues) {
		return nil, keyspan.Span{}, base.BlockReadStats{}, errors.Newf(
			"pebble: ingesting tables with blob references is not supported")
	}

	props, err := r.ReadPropertiesBlock(ctx, nil /* buffer pool */)
	if err != nil {
		return nil, keyspan.Span{}, base.BlockReadStats{}, err
	}

	// If this is a columnar block, read key schema name from properties block.
	if tf.BlockColumnar() {
		if _, ok := opts.KeySchemas[props.KeySchemaName]; !ok {
			return nil, keyspan.Span{}, base.BlockReadStats{}, errors.Newf(
				"pebble: table uses key schema %q unknown to the database",
				props.KeySchemaName)
		}
	}

	meta = &manifest.TableMetadata{}
	meta.TableNum = tableNum
	meta.Size = max(uint64(readable.Size()), 1)
	meta.CreationTime = time.Now().Unix()
	meta.InitPhysicalBacking()

	// Avoid loading into the file cache for collecting stats if we
	// don't need to. If there are no range deletions, we have all the
	// information to compute the stats here.
	//
	// This is helpful in tests for avoiding awkwardness around deletion of
	// ingested files from MemFS. MemFS implements the Windows semantics of
	// disallowing removal of an open file. Under MemFS, if we don't populate
	// meta.Stats here, the file will be loaded into the file cache for
	// calculating stats before we can remove the original link.
	maybeSetStatsFromProperties(meta.PhysicalMeta(), &props)

	var iterStats base.InternalIteratorStats
	env := sstable.ReadEnv{
		Block: block.ReadEnv{
			Stats: &iterStats,
		},
	}
	{
		iterOpts := sstable.IterOptions{
			Lower:                nil,
			Upper:                nil,
			Transforms:           sstable.NoTransforms,
			Filterer:             nil,
			FilterBlockSizeLimit: sstable.AlwaysUseFilterBlock,
			Env:                  env,
			ReaderProvider:       sstable.MakeTrivialReaderProvider(r),
			BlobContext:          sstable.AssertNoBlobHandles,
		}
		iter, err := r.NewPointIter(ctx, iterOpts)
		if err != nil {
			return nil, keyspan.Span{}, base.BlockReadStats{}, err
		}
		defer func() { _ = iter.Close() }()
		var smallest InternalKey
		if kv := iter.First(); kv != nil {
			if err := ingestValidateKey(opts, &kv.K); err != nil {
				return nil, keyspan.Span{}, base.BlockReadStats{}, err
			}
			smallest = kv.K.Clone()
		}
		if err := iter.Error(); err != nil {
			return nil, keyspan.Span{}, base.BlockReadStats{}, err
		}
		if kv := iter.Last(); kv != nil {
			if err := ingestValidateKey(opts, &kv.K); err != nil {
				return nil, keyspan.Span{}, base.BlockReadStats{}, err
			}
			meta.ExtendPointKeyBounds(opts.Comparer.Compare, smallest, kv.K.Clone())
		}
		if err := iter.Error(); err != nil {
			return nil, keyspan.Span{}, base.BlockReadStats{}, err
		}
	}

	iter, err := r.NewRawRangeDelIter(ctx, sstable.NoFragmentTransforms, env)
	if err != nil {
		return nil, keyspan.Span{}, base.BlockReadStats{}, err
	}
	if iter != nil {
		defer iter.Close()
		var smallest InternalKey
		if s, err := iter.First(); err != nil {
			return nil, keyspan.Span{}, base.BlockReadStats{}, err
		} else if s != nil {
			key := s.SmallestKey()
			if err := ingestValidateKey(opts, &key); err != nil {
				return nil, keyspan.Span{}, base.BlockReadStats{}, err
			}
			smallest = key.Clone()
		}
		if s, err := iter.Last(); err != nil {
			return nil, keyspan.Span{}, base.BlockReadStats{}, err
		} else if s != nil {
			k := s.SmallestKey()
			if err := ingestValidateKey(opts, &k); err != nil {
				return nil, keyspan.Span{}, base.BlockReadStats{}, err
			}
			largest := s.LargestKey().Clone()
			meta.ExtendPointKeyBounds(opts.Comparer.Compare, smallest, largest)
		}
	}

	// Update the range-key bounds for the table.
	{
		iter, err := r.NewRawRangeKeyIter(ctx, sstable.NoFragmentTransforms, env)
		if err != nil {
			return nil, keyspan.Span{}, base.BlockReadStats{}, err
		}
		if iter != nil {
			defer iter.Close()
			var smallest InternalKey
			if s, err := iter.First(); err != nil {
				return nil, keyspan.Span{}, base.BlockReadStats{}, err
			} else if s != nil {
				key := s.SmallestKey()
				if err := ingestValidateKey(opts, &key); err != nil {
					return nil, keyspan.Span{}, base.BlockReadStats{}, err
				}
				smallest = key.Clone()
				// Range keys need some additional validation as we need to ensure they
				// defragment cleanly with the lastRangeKey from the previous file.
				if err := rangeKeyValidator.Validate(s); err != nil {
					return nil, keyspan.Span{}, base.BlockReadStats{}, err
				}
			}
			lastRangeKey = keyspan.Span{}
			if s, err := iter.Last(); err != nil {
				return nil, keyspan.Span{}, base.BlockReadStats{}, err
			} else if s != nil {
				k := s.SmallestKey()
				if err := ingestValidateKey(opts, &k); err != nil {
					return nil, keyspan.Span{}, base.BlockReadStats{}, err
				}
				// As range keys are fragmented, the end key of the last range key in
				// the table provides the upper bound for the table.
				largest := s.LargestKey().Clone()
				meta.ExtendRangeKeyBounds(opts.Comparer.Compare, smallest, largest)
				lastRangeKey = s.Clone()
			} else {
				// s == nil.
				if err := rangeKeyValidator.Validate(nil /* nextFileSmallestKey */); err != nil {
					return nil, keyspan.Span{}, base.BlockReadStats{}, err
				}
			}
		} else {
			if err := rangeKeyValidator.Validate(nil /* nextFileSmallestKey */); err != nil {
				return nil, keyspan.Span{}, base.BlockReadStats{}, err
			}
			lastRangeKey = keyspan.Span{}
		}
	}

	if !meta.HasPointKeys && !meta.HasRangeKeys {
		return nil, keyspan.Span{}, base.BlockReadStats{}, nil
	}

	// Sanity check that the various bounds on the file were set consistently.
	if err := meta.Validate(opts.Comparer.Compare, opts.Comparer.FormatKey); err != nil {
		return nil, keyspan.Span{}, base.BlockReadStats{}, err
	}

	return meta, lastRangeKey, iterStats.TotalBlockReads(), nil
}

type ingestLoadResult struct {
	local    []ingestLocalMeta
	shared   []ingestSharedMeta
	external []ingestExternalMeta

	externalFilesHaveLevel bool
	blockReadStats         base.BlockReadStats
}

type ingestLocalMeta struct {
	*manifest.TableMetadata
	path string
}

type ingestSharedMeta struct {
	*manifest.TableMetadata
	shared SharedSSTMeta
}

type ingestExternalMeta struct {
	*manifest.TableMetadata
	external ExternalFile
	// usedExistingBacking is true if the external file is reusing a backing
	// that existed before this ingestion. In this case, we called
	// VirtualBackings.Protect() on that backing; we will need to call
	// Unprotect() after the ingestion.
	usedExistingBacking bool
}

func (r *ingestLoadResult) fileCount() int {
	return len(r.local) + len(r.shared) + len(r.external)
}

func ingestLoad(
	ctx context.Context,
	opts *Options,
	fmv FormatMajorVersion,
	paths []string,
	shared []SharedSSTMeta,
	external []ExternalFile,
	cacheHandle *cache.Handle,
	pending []base.TableNum,
) (ingestLoadResult, error) {
	localFileNums := pending[:len(paths)]
	sharedFileNums := pending[len(paths) : len(paths)+len(shared)]
	externalFileNums := pending[len(paths)+len(shared) : len(paths)+len(shared)+len(external)]

	var result ingestLoadResult
	result.local = make([]ingestLocalMeta, 0, len(paths))
	var lastRangeKey keyspan.Span
	var blockReadStats base.BlockReadStats
	// NB: we disable range key boundary assertions if we have shared or external files
	// present in this ingestion. This is because a suffixed range key in a local file
	// can possibly defragment with a suffixed range key in a shared or external file.
	// We also disable range key boundary assertions if we have CreateOnShared set to
	// true, as that means we could have suffixed RangeKeyDels or Unsets in the local
	// files that won't ever be surfaced, even if there are no shared or external files
	// in the ingestion.
	shouldDisableRangeKeyChecks := len(shared) > 0 || len(external) > 0 || opts.Experimental.CreateOnShared != remote.CreateOnSharedNone
	for i := range paths {
		f, err := opts.FS.Open(paths[i])
		if err != nil {
			return ingestLoadResult{}, err
		}

		readable, err := sstable.NewSimpleReadable(f)
		if err != nil {
			return ingestLoadResult{}, err
		}
		var m *manifest.TableMetadata
		rangeKeyValidator := disableRangeKeyChecks()
		if !shouldDisableRangeKeyChecks {
			rangeKeyValidator = validateSuffixedBoundaries(opts.Comparer, lastRangeKey)
		}
		m, lastRangeKey, blockReadStats, err = ingestLoad1(ctx, opts, fmv, readable, cacheHandle, localFileNums[i], rangeKeyValidator)
		if err != nil {
			return ingestLoadResult{}, err
		}
		if m != nil {
			result.local = append(result.local, ingestLocalMeta{
				TableMetadata: m,
				path:          paths[i],
			})
			result.blockReadStats = blockReadStats
		}
	}

	if !shouldDisableRangeKeyChecks {
		rangeKeyValidator := validateSuffixedBoundaries(opts.Comparer, lastRangeKey)
		if err := rangeKeyValidator.Validate(nil /* nextFileSmallestKey */); err != nil {
			return ingestLoadResult{}, err
		}
	}

	// Sort the shared files according to level.
	sort.Sort(sharedByLevel(shared))

	result.shared = make([]ingestSharedMeta, 0, len(shared))
	for i := range shared {
		m, err := ingestSynthesizeShared(opts, shared[i], sharedFileNums[i])
		if err != nil {
			return ingestLoadResult{}, err
		}
		if shared[i].Level < sharedLevelsStart {
			return ingestLoadResult{}, errors.New("cannot ingest shared file in level below sharedLevelsStart")
		}
		result.shared = append(result.shared, ingestSharedMeta{
			TableMetadata: m,
			shared:        shared[i],
		})
	}
	result.external = make([]ingestExternalMeta, 0, len(external))
	for i := range external {
		m, err := ingestLoad1External(opts, external[i], externalFileNums[i])
		if err != nil {
			return ingestLoadResult{}, err
		}
		result.external = append(result.external, ingestExternalMeta{
			TableMetadata: m,
			external:      external[i],
		})
		if external[i].Level > 0 {
			if i != 0 && !result.externalFilesHaveLevel {
				return ingestLoadResult{}, base.AssertionFailedf("pebble: external sstables must all have level set or unset")
			}
			result.externalFilesHaveLevel = true
		} else if result.externalFilesHaveLevel {
			return ingestLoadResult{}, base.AssertionFailedf("pebble: external sstables must all have level set or unset")
		}
	}
	return result, nil
}

func ingestSortAndVerify(cmp Compare, lr ingestLoadResult, exciseSpan KeyRange) error {
	// Verify that all the shared files (i.e. files in sharedMeta)
	// fit within the exciseSpan.
	for _, f := range lr.shared {
		if !exciseSpan.Contains(cmp, f.Smallest()) || !exciseSpan.Contains(cmp, f.Largest()) {
			return errors.Newf("pebble: shared file outside of excise span, span [%s-%s), file = %s", exciseSpan.Start, exciseSpan.End, f.String())
		}
	}

	if lr.externalFilesHaveLevel {
		for _, f := range lr.external {
			if !exciseSpan.Contains(cmp, f.Smallest()) || !exciseSpan.Contains(cmp, f.Largest()) {
				return base.AssertionFailedf("pebble: external file outside of excise span, span [%s-%s), file = %s", exciseSpan.Start, exciseSpan.End, f.String())
			}
		}
	}

	if len(lr.external) > 0 {
		if len(lr.shared) > 0 {
			// If external files are present alongside shared files,
			// return an error.
			return base.AssertionFailedf("pebble: external files cannot be ingested atomically alongside shared files")
		}

		// Sort according to the smallest key.
		slices.SortFunc(lr.external, func(a, b ingestExternalMeta) int {
			return cmp(a.Smallest().UserKey, b.Smallest().UserKey)
		})
		for i := 1; i < len(lr.external); i++ {
			if sstableKeyCompare(cmp, lr.external[i-1].Largest(), lr.external[i].Smallest()) >= 0 {
				return errors.Newf("pebble: external sstables have overlapping ranges")
			}
		}
		return nil
	}
	if len(lr.local) <= 1 {
		return nil
	}

	// Sort according to the smallest key.
	slices.SortFunc(lr.local, func(a, b ingestLocalMeta) int {
		return cmp(a.Smallest().UserKey, b.Smallest().UserKey)
	})

	for i := 1; i < len(lr.local); i++ {
		if sstableKeyCompare(cmp, lr.local[i-1].Largest(), lr.local[i].Smallest()) >= 0 {
			return errors.Newf("pebble: local ingestion sstables have overlapping ranges")
		}
	}
	if len(lr.shared) == 0 {
		return nil
	}
	filesInLevel := make([]*manifest.TableMetadata, 0, len(lr.shared))
	for l := sharedLevelsStart; l < numLevels; l++ {
		filesInLevel = filesInLevel[:0]
		for i := range lr.shared {
			if lr.shared[i].shared.Level == uint8(l) {
				filesInLevel = append(filesInLevel, lr.shared[i].TableMetadata)
			}
		}
		for i := range lr.external {
			if lr.external[i].external.Level == uint8(l) {
				filesInLevel = append(filesInLevel, lr.external[i].TableMetadata)
			}
		}
		slices.SortFunc(filesInLevel, func(a, b *manifest.TableMetadata) int {
			return cmp(a.Smallest().UserKey, b.Smallest().UserKey)
		})
		for i := 1; i < len(filesInLevel); i++ {
			if sstableKeyCompare(cmp, filesInLevel[i-1].Largest(), filesInLevel[i].Smallest()) >= 0 {
				return base.AssertionFailedf("pebble: external shared sstables have overlapping ranges")
			}
		}
	}
	return nil
}

func ingestCleanup(objProvider objstorage.Provider, meta []ingestLocalMeta) error {
	var firstErr error
	for i := range meta {
		if err := objProvider.Remove(base.FileTypeTable, meta[i].TableBacking.DiskFileNum); err != nil {
			firstErr = firstError(firstErr, err)
		}
	}
	return firstErr
}

// ingestLinkLocal creates new objects which are backed by either hardlinks to or
// copies of the ingested files.
func ingestLinkLocal(
	ctx context.Context,
	jobID JobID,
	opts *Options,
	objProvider objstorage.Provider,
	localMetas []ingestLocalMeta,
) error {
	for i := range localMetas {
		objMeta, err := objProvider.LinkOrCopyFromLocal(
			ctx, opts.FS, localMetas[i].path, base.FileTypeTable, localMetas[i].TableBacking.DiskFileNum,
			objstorage.CreateOptions{PreferSharedStorage: true},
		)
		if err != nil {
			if err2 := ingestCleanup(objProvider, localMetas[:i]); err2 != nil {
				opts.Logger.Errorf("ingest cleanup failed: %v", err2)
			}
			return err
		}
		if opts.EventListener.TableCreated != nil {
			opts.EventListener.TableCreated(TableCreateInfo{
				JobID:   int(jobID),
				Reason:  "ingesting",
				Path:    objProvider.Path(objMeta),
				FileNum: base.PhysicalTableDiskFileNum(localMetas[i].TableNum),
			})
		}
	}
	return nil
}

// ingestAttachRemote attaches remote objects to the storage provider.
//
// For external objects, we reuse existing FileBackings from the current version
// when possible.
//
// ingestUnprotectExternalBackings() must be called after this function (even in
// error cases).
func (d *DB) ingestAttachRemote(jobID JobID, lr ingestLoadResult) error {
	remoteObjs := make([]objstorage.RemoteObjectToAttach, 0, len(lr.shared)+len(lr.external))
	for i := range lr.shared {
		backing, err := lr.shared[i].shared.Backing.Get()
		if err != nil {
			return err
		}
		remoteObjs = append(remoteObjs, objstorage.RemoteObjectToAttach{
			FileNum:  lr.shared[i].TableBacking.DiskFileNum,
			FileType: base.FileTypeTable,
			Backing:  backing,
		})
	}

	d.findExistingBackingsForExternalObjects(lr.external)

	newTableBackings := make(map[remote.ObjectKey]*manifest.TableBacking, len(lr.external))
	for i := range lr.external {
		meta := lr.external[i].TableMetadata
		if meta.TableBacking != nil {
			// The backing was filled in by findExistingBackingsForExternalObjects().
			continue
		}
		key := remote.MakeObjectKey(lr.external[i].external.Locator, lr.external[i].external.ObjName)
		if backing, ok := newTableBackings[key]; ok {
			// We already created the same backing in this loop. Update its size.
			backing.Size += lr.external[i].external.Size
			meta.AttachVirtualBacking(backing)
			continue
		}
		providerBacking, err := d.objProvider.CreateExternalObjectBacking(key.Locator, key.ObjectName)
		if err != nil {
			return err
		}
		// We have to attach the remote object (and assign it a DiskFileNum). For
		// simplicity, we use the same number for both the FileNum and the
		// DiskFileNum (even though this is a virtual sstable).
		size := max(lr.external[i].external.Size, 1)
		meta.InitVirtualBacking(base.DiskFileNum(meta.TableNum), size)

		// Set the underlying TableBacking's size to the same size as the virtualized
		// view of the sstable. This ensures that we don't over-prioritize this
		// sstable for compaction just yet, as we do not have a clear sense of
		// what parts of this sstable are referenced by other nodes.
		meta.TableBacking.Size = size
		newTableBackings[key] = meta.TableBacking

		remoteObjs = append(remoteObjs, objstorage.RemoteObjectToAttach{
			FileNum:  meta.TableBacking.DiskFileNum,
			FileType: base.FileTypeTable,
			Backing:  providerBacking,
		})
	}

	for i := range lr.external {
		if err := lr.external[i].Validate(d.opts.Comparer.Compare, d.opts.Comparer.FormatKey); err != nil {
			return err
		}
	}

	remoteObjMetas, err := d.objProvider.AttachRemoteObjects(remoteObjs)
	if err != nil {
		return err
	}

	for i := range lr.shared {
		// One corner case around file sizes we need to be mindful of, is that
		// if one of the shareObjs was initially created by us (and has boomeranged
		// back from another node), we'll need to update the TableBacking's size
		// to be the true underlying size. Otherwise, we could hit errors when we
		// open the db again after a crash/restart (see checkConsistency in open.go),
		// plus it more accurately allows us to prioritize compactions of files
		// that were originally created by us.
		if remoteObjMetas[i].IsShared() && !d.objProvider.IsSharedForeign(remoteObjMetas[i]) {
			size, err := d.objProvider.Size(remoteObjMetas[i])
			if err != nil {
				return err
			}
			lr.shared[i].TableBacking.Size = max(uint64(size), 1)
		}
	}

	if d.opts.EventListener.TableCreated != nil {
		for i := range remoteObjMetas {
			d.opts.EventListener.TableCreated(TableCreateInfo{
				JobID:   int(jobID),
				Reason:  "ingesting",
				Path:    d.objProvider.Path(remoteObjMetas[i]),
				FileNum: remoteObjMetas[i].DiskFileNum,
			})
		}
	}

	return nil
}

// findExistingBackingsForExternalObjects populates the TableBacking for external
// files which are already in use by the current version.
//
// We take a Ref and LatestRef on populated backings.
func (d *DB) findExistingBackingsForExternalObjects(metas []ingestExternalMeta) {
	d.mu.Lock()
	defer d.mu.Unlock()

	for i := range metas {
		diskFileNums := d.objProvider.GetExternalObjects(metas[i].external.Locator, metas[i].external.ObjName)
		// We cross-check against fileBackings in the current version because it is
		// possible that the external object is referenced by an sstable which only
		// exists in a previous version. In that case, that object could be removed
		// at any time so we cannot reuse it.
		for _, n := range diskFileNums {
			if backing, ok := d.mu.versions.latest.virtualBackings.Get(n); ok {
				// Protect this backing from being removed from the latest version. We
				// will unprotect in ingestUnprotectExternalBackings.
				d.mu.versions.latest.virtualBackings.Protect(n)
				metas[i].usedExistingBacking = true
				metas[i].AttachVirtualBacking(backing)

				// We can't update the size of the backing here, so make sure the
				// virtual size is sane.
				// TODO(radu): investigate what would it take to update the backing size.
				metas[i].Size = min(metas[i].Size, backing.Size)
				break
			}
		}
	}
}

// ingestUnprotectExternalBackings unprotects the file backings that were reused
// for external objects when the ingestion fails.
func (d *DB) ingestUnprotectExternalBackings(lr ingestLoadResult) {
	d.mu.Lock()
	defer d.mu.Unlock()

	for _, meta := range lr.external {
		if meta.usedExistingBacking {
			// If the backing is not use anywhere else and the ingest failed (or the
			// ingested tables were already compacted away), this call will cause in
			// the next version update to remove the backing.
			d.mu.versions.latest.virtualBackings.Unprotect(meta.TableBacking.DiskFileNum)
		}
	}
}

func setSeqNumInMetadata(
	m *manifest.TableMetadata, seqNum base.SeqNum, cmp Compare, format base.FormatKey,
) error {
	setSeqFn := func(k base.InternalKey) base.InternalKey {
		return base.MakeInternalKey(k.UserKey, seqNum, k.Kind())
	}
	// NB: we set the fields directly here, rather than via their Extend*
	// methods, as we are updating sequence numbers.
	if m.HasPointKeys {
		m.PointKeyBounds.SetSmallest(setSeqFn(m.PointKeyBounds.Smallest()))
	}
	if m.HasRangeKeys {
		m.RangeKeyBounds.SetSmallest(setSeqFn(m.RangeKeyBounds.Smallest()))
	}
	// Only update the seqnum for the largest key if that key is not an
	// "exclusive sentinel" (i.e. a range deletion sentinel or a range key
	// boundary), as doing so effectively drops the exclusive sentinel (by
	// lowering the seqnum from the max value), and extends the bounds of the
	// table.
	// NB: as the largest range key is always an exclusive sentinel, it is never
	// updated.
	if m.HasPointKeys && !m.PointKeyBounds.Largest().IsExclusiveSentinel() {
		m.PointKeyBounds.SetLargest(setSeqFn(m.PointKeyBounds.Largest()))
	}
	// Setting smallestSeqNum == largestSeqNum triggers the setting of
	// Properties.GlobalSeqNum when an sstable is loaded.
	m.SmallestSeqNum = seqNum
	m.LargestSeqNum = seqNum
	m.LargestSeqNumAbsolute = seqNum
	// Ensure the new bounds are consistent.
	if err := m.Validate(cmp, format); err != nil {
		return err
	}
	return nil
}

func ingestUpdateSeqNum(
	cmp Compare, format base.FormatKey, seqNum base.SeqNum, loadResult ingestLoadResult,
) error {
	// Shared sstables are required to be sorted by level ascending. We then
	// iterate the shared sstables in reverse, assigning the lower sequence
	// numbers to the shared sstables that will be ingested into the lower
	// (larger numbered) levels first. This ensures sequence number shadowing is
	// correct.
	for i := len(loadResult.shared) - 1; i >= 0; i-- {
		if i-1 >= 0 && loadResult.shared[i-1].shared.Level > loadResult.shared[i].shared.Level {
			panic(errors.AssertionFailedf("shared files %s, %s out of order", loadResult.shared[i-1], loadResult.shared[i]))
		}
		if err := setSeqNumInMetadata(loadResult.shared[i].TableMetadata, seqNum, cmp, format); err != nil {
			return err
		}
		seqNum++
	}
	for i := range loadResult.external {
		if err := setSeqNumInMetadata(loadResult.external[i].TableMetadata, seqNum, cmp, format); err != nil {
			return err
		}
		seqNum++
	}
	for i := range loadResult.local {
		if err := setSeqNumInMetadata(loadResult.local[i].TableMetadata, seqNum, cmp, format); err != nil {
			return err
		}
		seqNum++
	}
	return nil
}

// ingestTargetLevel returns the target level for a file being ingested.
// If suggestSplit is true, it accounts for ingest-time splitting as part of
// its target level calculation, and if a split candidate is found, that file
// is returned as the splitFile.
func ingestTargetLevel(
	ctx context.Context,
	cmp base.Compare,
	lsmOverlap overlap.WithLSM,
	baseLevel int,
	compactions map[compaction]struct{},
	meta *manifest.TableMetadata,
	suggestSplit bool,
) (targetLevel int, splitFile *manifest.TableMetadata, err error) {
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
	//   - if there is only a file overlap at a given level, and no data overlap,
	//     we can still slot a file at that level. We return the fileMetadata with
	//     which we have file boundary overlap (must be only one file, as sstable
	//     bounds are usually tight on user keys) and the caller is expected to split
	//     that sstable into two virtual sstables, allowing this file to go into that
	//     level. Note that if we have file boundary overlap with two files, which
	//     should only happen on rare occasions, we treat it as data overlap and
	//     don't use this optimization.
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

	if lsmOverlap[0].Result == overlap.Data {
		return 0, nil, nil
	}
	targetLevel = 0
	splitFile = nil
	metaBounds := meta.UserKeyBounds()
	for level := baseLevel; level < numLevels; level++ {
		var candidateSplitFile *manifest.TableMetadata
		switch lsmOverlap[level].Result {
		case overlap.Data:
			// We cannot ingest into or under this level; return the best target level
			// so far.
			return targetLevel, splitFile, nil

		case overlap.OnlyBoundary:
			if !suggestSplit || lsmOverlap[level].SplitFile == nil {
				// We can ingest under this level, but not into this level.
				continue
			}
			// We can ingest into this level if we split this file.
			candidateSplitFile = lsmOverlap[level].SplitFile

		case overlap.None:
		// We can ingest into this level.

		default:
			return 0, nil, base.AssertionFailedf("unexpected WithLevel.Result: %v", lsmOverlap[level].Result)
		}

		// Check boundary overlap with any ongoing compactions. We consider an
		// overlapping compaction that's writing files to an output level as
		// equivalent to boundary overlap with files in that output level.
		//
		// We cannot check for data overlap with the new SSTs compaction will produce
		// since compaction hasn't been done yet. However, there's no need to check
		// since all keys in them will be from levels in [c.startLevel,
		// c.outputLevel], and all those levels have already had their data overlap
		// tested negative (else we'd have returned earlier).
		//
		// An alternative approach would be to cancel these compactions and proceed
		// with an ingest-time split on this level if necessary. However, compaction
		// cancellation can result in significant wasted effort and is best avoided
		// unless necessary.
		overlaps := false
		for c := range compactions {
			tblCompaction, ok := c.(*tableCompaction)
			if !ok {
				continue
			}
			if tblCompaction.outputLevel == nil || level != tblCompaction.outputLevel.level {
				continue
			}
			bounds := tblCompaction.Bounds()
			if bounds != nil && metaBounds.Overlaps(cmp, bounds) {
				overlaps = true
				break
			}
		}
		if !overlaps {
			targetLevel = level
			splitFile = candidateSplitFile
		}
	}
	return targetLevel, splitFile, nil
}

// Ingest ingests a set of sstables into the DB. Ingestion of the files is
// atomic and semantically equivalent to creating a single batch containing all
// of the mutations in the sstables. Ingestion may require the memtable to be
// flushed. The ingested sstable files are moved into the DB and must reside on
// the same filesystem as the DB. Sstables can be created for ingestion using
// sstable.Writer. On success, Ingest removes the input paths.
//
// Ingested sstables must have been created with a known KeySchema (when written
// with columnar blocks) and Comparer. They must not contain any references to
// external blob files.
//
// Two types of sstables are accepted for ingestion(s): one is sstables present
// in the instance's vfs.FS and can be referenced locally. The other is sstables
// present in remote.Storage, referred to as shared or foreign sstables. These
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
//  1. Allocate table numbers for every sstable being ingested.
//  2. Load the metadata for all sstables being ingested.
//  3. Sort the sstables by smallest key, verifying non overlap (for local
//     sstables).
//  4. Hard link (or copy) the local sstables into the DB directory.
//  5. Allocate a sequence number to use for all of the entries in the
//     local sstables. This is the step where overlap with memtables is
//     determined. If there is overlap, we remember the most recent memtable
//     that overlaps.
//  6. Update the sequence number in the ingested local sstables. (Remote
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
func (d *DB) Ingest(ctx context.Context, paths []string) error {
	if err := d.closed.Load(); err != nil {
		panic(err)
	}
	if d.opts.ReadOnly {
		return ErrReadOnly
	}
	_, err := d.ingest(ctx, ingestArgs{Local: paths})
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

// ExternalFile are external sstables that can be referenced through
// objprovider and ingested as remote files that will not be refcounted or
// cleaned up. For use with online restore. Note that the underlying sstable
// could contain keys outside the [Smallest,Largest) bounds; however Pebble
// is expected to only read the keys within those bounds.
type ExternalFile struct {
	// Locator is the shared.Locator that can be used with objProvider to
	// resolve a reference to this external sstable.
	Locator remote.Locator

	// ObjName is the unique name of this sstable on Locator.
	ObjName string

	// Size of the referenced proportion of the virtualized sstable. An estimate
	// is acceptable in lieu of the backing file size.
	Size uint64

	// StartKey and EndKey define the bounds of the sstable; the ingestion
	// of this file will only result in keys within [StartKey, EndKey) if
	// EndKeyIsInclusive is false or [StartKey, EndKey] if it is true.
	// These bounds are loose i.e. it's possible for keys to not span the
	// entirety of this range.
	//
	// StartKey and EndKey user keys must not have suffixes.
	//
	// Multiple ExternalFiles in one ingestion must all have non-overlapping
	// bounds.
	StartKey, EndKey []byte

	// EndKeyIsInclusive is true if EndKey should be treated as inclusive.
	EndKeyIsInclusive bool

	// HasPointKey and HasRangeKey denote whether this file contains point keys
	// or range keys. If both structs are false, an error is returned during
	// ingestion.
	HasPointKey, HasRangeKey bool

	// SyntheticPrefix will prepend this prefix to all keys in the file during
	// iteration. Note that the backing file itself is not modified.
	//
	// SyntheticPrefix must be a prefix of both Bounds.Start and Bounds.End.
	SyntheticPrefix []byte

	// SyntheticSuffix will replace the suffix of every key in the file during
	// iteration. Note that the file itself is not modified, rather, every key
	// returned by an iterator will have the synthetic suffix.
	//
	// SyntheticSuffix can only be used under the following conditions:
	//  - the synthetic suffix must sort before any non-empty suffixes in the
	//    backing sst (the entire sst, not just the part restricted to Bounds).
	//  - the backing sst must not contain multiple keys with the same prefix.
	SyntheticSuffix []byte

	// Level denotes the level at which this file was present at read time
	// if the external file was returned by a scan of an existing Pebble
	// instance. If Level is 0, this field is ignored.
	Level uint8
}

// IngestWithStats does the same as Ingest, and additionally returns
// IngestOperationStats.
func (d *DB) IngestWithStats(ctx context.Context, paths []string) (IngestOperationStats, error) {
	if err := d.closed.Load(); err != nil {
		panic(err)
	}
	if d.opts.ReadOnly {
		return IngestOperationStats{}, ErrReadOnly
	}
	return d.ingest(ctx, ingestArgs{Local: paths})
}

// IngestExternalFiles does the same as IngestWithStats, and additionally
// accepts external files (with locator info that can be resolved using
// d.opts.SharedStorage). These files must also be non-overlapping with
// each other, and must be resolvable through d.objProvider.
func (d *DB) IngestExternalFiles(
	ctx context.Context, external []ExternalFile,
) (IngestOperationStats, error) {
	if err := d.closed.Load(); err != nil {
		panic(err)
	}

	if d.opts.ReadOnly {
		return IngestOperationStats{}, ErrReadOnly
	}
	if d.opts.Experimental.RemoteStorage == nil {
		return IngestOperationStats{}, errors.New("pebble: cannot ingest external files without shared storage configured")
	}
	return d.ingest(ctx, ingestArgs{External: external})
}

// IngestAndExcise does the same as IngestWithStats, and additionally accepts a
// list of shared files to ingest that can be read from a remote.Storage through
// a Provider. All the shared files must live within exciseSpan, and any existing
// keys in exciseSpan are deleted by turning existing sstables into virtual
// sstables (if not virtual already) and shrinking their spans to exclude
// exciseSpan. See the comment at Ingest for a more complete picture of the
// ingestion process.
//
// Panics if this DB instance was not instantiated with a remote.Storage and
// shared sstables are present.
func (d *DB) IngestAndExcise(
	ctx context.Context,
	paths []string,
	shared []SharedSSTMeta,
	external []ExternalFile,
	exciseSpan KeyRange,
) (IngestOperationStats, error) {
	if err := d.closed.Load(); err != nil {
		panic(err)
	}
	if d.opts.ReadOnly {
		return IngestOperationStats{}, ErrReadOnly
	}
	// Excise is only supported on prefix keys.
	if d.opts.Comparer.Split(exciseSpan.Start) != len(exciseSpan.Start) {
		return IngestOperationStats{}, errors.New("IngestAndExcise called with suffixed start key")
	}
	if d.opts.Comparer.Split(exciseSpan.End) != len(exciseSpan.End) {
		return IngestOperationStats{}, errors.New("IngestAndExcise called with suffixed end key")
	}
	if v := d.FormatMajorVersion(); v < FormatMinForSharedObjects {
		return IngestOperationStats{}, errors.Newf(
			"store has format major version %d; IngestAndExcise requires at least %d",
			v, FormatMinForSharedObjects,
		)
	}
	args := ingestArgs{
		Local:              paths,
		Shared:             shared,
		External:           external,
		ExciseSpan:         exciseSpan,
		ExciseBoundsPolicy: tightExciseBounds,
	}
	return d.ingest(ctx, args)
}

// Both DB.mu and commitPipeline.mu must be held while this is called.
func (d *DB) newIngestedFlushableEntry(
	meta []*manifest.TableMetadata, seqNum base.SeqNum, logNum base.DiskFileNum, exciseSpan KeyRange,
) (*flushableEntry, error) {
	// If there's an excise being done atomically with the same ingest, we
	// assign the lowest sequence number in the set of sequence numbers for this
	// ingestion to the excise. Note that we've already allocated fileCount+1
	// sequence numbers in this case.
	//
	// This mimics the behaviour in the non-flushable ingest case (see the callsite
	// for ingestUpdateSeqNum).
	fileSeqNumStart := seqNum
	if exciseSpan.Valid() {
		fileSeqNumStart = seqNum + 1 // the first seqNum is reserved for the excise.
		// The excise span will be retained by the flushable, outliving the
		// caller's ingestion call. Copy it.
		exciseSpan = KeyRange{
			Start: slices.Clone(exciseSpan.Start),
			End:   slices.Clone(exciseSpan.End),
		}
	}
	// Update the sequence number for all of the sstables in the
	// metadata. Writing the metadata to the manifest when the
	// version edit is applied is the mechanism that persists the
	// sequence number. The sstables themselves are left unmodified.
	// In this case, a version edit will only be written to the manifest
	// when the flushable is eventually flushed. If Pebble restarts in that
	// time, then we'll lose the ingest sequence number information. But this
	// information will also be reconstructed on node restart.
	for i, m := range meta {
		if err := setSeqNumInMetadata(m, fileSeqNumStart+base.SeqNum(i), d.cmp, d.opts.Comparer.FormatKey); err != nil {
			return nil, err
		}
	}

	f := newIngestedFlushable(meta, d.opts.Comparer, d.newIters, d.tableNewRangeKeyIter, exciseSpan, seqNum)

	// NB: The logNum/seqNum are the WAL number which we're writing this entry
	// to and the sequence number within the WAL which we'll write this entry
	// to.
	entry := d.newFlushableEntry(f, logNum, seqNum)
	// The flushable entry starts off with a single reader ref, so increment
	// the TableMetadata.Refs.
	for _, file := range f.files {
		file.Ref()
	}
	entry.unrefFiles = func(of *manifest.ObsoleteFiles) {
		// Invoke Unref on each table. If any files become obsolete, they'll be
		// added to the set of obsolete files.
		for _, file := range f.files {
			file.Unref(of)
		}
	}

	entry.flushForced = true
	entry.releaseMemAccounting = func() {}
	return entry, nil
}

// Both DB.mu and commitPipeline.mu must be held while this is called. Since
// we're holding both locks, the order in which we rotate the memtable or
// recycle the WAL in this function is irrelevant as long as the correct log
// numbers are assigned to the appropriate flushable.
func (d *DB) handleIngestAsFlushable(
	meta []*manifest.TableMetadata, seqNum base.SeqNum, exciseSpan KeyRange,
) error {
	b := d.NewBatch()
	if exciseSpan.Valid() {
		b.excise(exciseSpan.Start, exciseSpan.End)
	}
	for _, m := range meta {
		b.ingestSST(m.TableNum)
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
		var prevLogSize uint64
		logNum, prevLogSize = d.rotateWAL()
		// As the rotator of the WAL, we're responsible for updating the
		// previous flushable queue tail's log size.
		d.mu.mem.queue[len(d.mu.mem.queue)-1].logSize = prevLogSize

		d.mu.Unlock()
		err := d.commit.directWrite(b)
		if err != nil {
			d.opts.Logger.Fatalf("%v", err)
		}
		d.mu.Lock()
	}

	entry, err := d.newIngestedFlushableEntry(meta, seqNum, logNum, exciseSpan)
	if err != nil {
		return err
	}
	nextSeqNum := seqNum + base.SeqNum(b.Count())

	// Set newLogNum to the logNum of the previous flushable. This value is
	// irrelevant if the WAL is disabled. If the WAL is enabled, then we set
	// the appropriate value below.
	newLogNum := d.mu.mem.queue[len(d.mu.mem.queue)-1].logNum
	if !d.opts.DisableWAL {
		// newLogNum will be the WAL num of the next mutable memtable which
		// comes after the ingestedFlushable in the flushable queue. The mutable
		// memtable will be created below.
		//
		// The prevLogSize returned by rotateWAL is the WAL to which the
		// flushable ingest keys were appended. This intermediary WAL is only
		// used to record the flushable ingest and nothing else.
		newLogNum, entry.logSize = d.rotateWAL()
	}

	d.mu.versions.metrics.Ingest.Count++
	currMem := d.mu.mem.mutable
	// NB: Placing ingested sstables above the current memtables
	// requires rotating of the existing memtables/WAL. There is
	// some concern of churning through tiny memtables due to
	// ingested sstables being placed on top of them, but those
	// memtables would have to be flushed anyways.
	d.mu.mem.queue = append(d.mu.mem.queue, entry)
	d.rotateMemtable(newLogNum, nextSeqNum, currMem, 0 /* minSize */)
	d.updateReadStateLocked(d.opts.DebugCheck)
	// TODO(aaditya): is this necessary? we call this already in rotateMemtable above
	d.maybeScheduleFlush()
	return nil
}

type ingestArgs struct {
	// Local sstables to ingest.
	Local []string
	// Shared sstables to ingest.
	Shared []SharedSSTMeta
	// External sstables to ingest.
	External []ExternalFile
	// ExciseSpan (unset if not excising).
	ExciseSpan         KeyRange
	ExciseBoundsPolicy exciseBoundsPolicy
}

// See comment at Ingest() for details on how this works.
func (d *DB) ingest(ctx context.Context, args ingestArgs) (IngestOperationStats, error) {
	paths := args.Local
	shared := args.Shared
	external := args.External
	if len(shared) > 0 && d.opts.Experimental.RemoteStorage == nil {
		panic("cannot ingest shared sstables with nil SharedStorage")
	}
	if (args.ExciseSpan.Valid() || len(shared) > 0 || len(external) > 0) && d.FormatMajorVersion() < FormatVirtualSSTables {
		return IngestOperationStats{}, errors.New("pebble: format major version too old for excise, shared or external sstable ingestion")
	}
	if len(external) > 0 && d.FormatMajorVersion() < FormatSyntheticPrefixSuffix {
		for i := range external {
			if len(external[i].SyntheticPrefix) > 0 {
				return IngestOperationStats{}, errors.New("pebble: format major version too old for synthetic prefix ingestion")
			}
			if len(external[i].SyntheticSuffix) > 0 {
				return IngestOperationStats{}, errors.New("pebble: format major version too old for synthetic suffix ingestion")
			}
		}
	}
	// Allocate table numbers for all files being ingested and mark them as
	// pending in order to prevent them from being deleted. Note that this causes
	// the file number ordering to be out of alignment with sequence number
	// ordering. The sorting of L0 tables by sequence number avoids relying on
	// that (busted) invariant.
	pendingOutputs := make([]base.TableNum, len(paths)+len(shared)+len(external))
	for i := 0; i < len(paths)+len(shared)+len(external); i++ {
		pendingOutputs[i] = d.mu.versions.getNextTableNum()
	}

	jobID := d.newJobID()

	// Load the metadata for all the files being ingested. This step detects
	// and elides empty sstables.
	loadResult, err := ingestLoad(ctx, d.opts, d.FormatMajorVersion(), paths, shared, external,
		d.cacheHandle, pendingOutputs)
	if err != nil {
		return IngestOperationStats{}, err
	}

	if loadResult.fileCount() == 0 && !args.ExciseSpan.Valid() {
		// All of the sstables to be ingested were empty. Nothing to do.
		return IngestOperationStats{}, nil
	}

	// Verify the sstables do not overlap.
	if err := ingestSortAndVerify(d.cmp, loadResult, args.ExciseSpan); err != nil {
		return IngestOperationStats{}, err
	}

	// Hard link the sstables into the DB directory. Since the sstables aren't
	// referenced by a version, they won't be used. If the hard linking fails
	// (e.g. because the files reside on a different filesystem), ingestLinkLocal
	// will fall back to copying, and if that fails we undo our work and return an
	// error.
	if err := ingestLinkLocal(ctx, jobID, d.opts, d.objProvider, loadResult.local); err != nil {
		return IngestOperationStats{}, err
	}

	err = d.ingestAttachRemote(jobID, loadResult)
	defer d.ingestUnprotectExternalBackings(loadResult)
	if err != nil {
		return IngestOperationStats{}, err
	}

	// Make the new tables durable. We need to do this at some point before we
	// update the MANIFEST (via UpdateVersionLocked), otherwise a crash can have
	// the tables referenced in the MANIFEST, but not present in the provider.
	if err := d.objProvider.Sync(); err != nil {
		return IngestOperationStats{}, err
	}

	// metaFlushableOverlaps is a map indicating which of the ingested sstables
	// overlap some table in the flushable queue. It's used to approximate
	// ingest-into-L0 stats when using flushable ingests.
	metaFlushableOverlaps := make(map[base.TableNum]bool, loadResult.fileCount())
	var mem *flushableEntry
	var mut *memTable
	// asFlushable indicates whether the sstable was ingested as a flushable.
	var asFlushable bool
	var waitFlushStart crtime.Mono
	var removeFromOngoingExcises bool
	var assignedSeqNum SeqNum
	prepare := func(seqNum base.SeqNum) {
		assignedSeqNum = seqNum
		// Note that d.commit.mu is held by commitPipeline when calling prepare.

		// Determine the set of bounds we care about for the purpose of checking
		// for overlap among the flushables. If there's an excise span, we need
		// to check for overlap with its bounds as well.
		overlapBounds := make([]bounded, 0, loadResult.fileCount()+1)
		for _, m := range loadResult.local {
			overlapBounds = append(overlapBounds, m.TableMetadata)
		}
		for _, m := range loadResult.shared {
			overlapBounds = append(overlapBounds, m.TableMetadata)
		}
		for _, m := range loadResult.external {
			overlapBounds = append(overlapBounds, m.TableMetadata)
		}
		if args.ExciseSpan.Valid() {
			overlapBounds = append(overlapBounds, &args.ExciseSpan)
		}

		addToOngoingExcises := false
		d.mu.Lock()
		defer func() {
			if addToOngoingExcises {
				_, ok := d.mu.snapshots.ongoingExcises[seqNum]
				if ok {
					panic(errors.AssertionFailedf("pebble: excise with seqnum %s already in map", seqNum))
				}
				d.mu.snapshots.ongoingExcises[seqNum] = args.ExciseSpan
				removeFromOngoingExcises = true
			}
			d.mu.Unlock()
		}()
		if args.ExciseSpan.Valid() {
			// Check if any of the currently-open EventuallyFileOnlySnapshots
			// overlap in key ranges with the excise span. If so, we need to
			// check for memtable overlaps with all bounds of that
			// EventuallyFileOnlySnapshot in addition to the ingestion's own
			// bounds too.
			overlapBounds = append(overlapBounds, exciseOverlapBounds(
				d.cmp, &d.mu.snapshots.snapshotList, args.ExciseSpan, seqNum)...)
			addToOngoingExcises = true
		}

		// Check to see if any files overlap with any of the memtables. The queue
		// is ordered from oldest to newest with the mutable memtable being the
		// last element in the slice. We want to wait for the newest table that
		// overlaps.

		for i := len(d.mu.mem.queue) - 1; i >= 0; i-- {
			m := d.mu.mem.queue[i]
			m.computePossibleOverlaps(func(b bounded) shouldContinue {
				// If this is the first table to overlap a flushable, save
				// the flushable. This ingest must be ingested or flushed
				// after it.
				if mem == nil {
					mem = m
				}

				switch v := b.(type) {
				case *manifest.TableMetadata:
					// NB: False positives are possible if `m` is a flushable
					// ingest that overlaps the file `v` in bounds but doesn't
					// contain overlapping data. This is considered acceptable
					// because it's rare (in CockroachDB a bound overlap likely
					// indicates a data overlap), and blocking the commit
					// pipeline while we perform I/O to check for overlap may be
					// more disruptive than enqueueing this ingestion on the
					// flushable queue and switching to a new memtable.
					metaFlushableOverlaps[v.TableNum] = true
				case *KeyRange:
					// An excise span or an EventuallyFileOnlySnapshot protected range;
					// not a file.
				default:
					panic("unreachable")
				}
				return continueIteration
			}, overlapBounds...)
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

		// The ingestion overlaps with some entry in the flushable queue. If the
		// pre-conditions are met below, we can treat this ingestion as a flushable
		// ingest, otherwise we wait on the memtable flush before ingestion.
		//
		// TODO(aaditya): We should make flushableIngest compatible with remote
		// files.
		hasRemoteFiles := len(shared) > 0 || len(external) > 0
		canIngestFlushable := d.FormatMajorVersion() >= FormatFlushableIngest &&
			// We require that either the queue of flushables is below the
			// stop-writes threshold (note that this is typically a conservative
			// check, since not every element of this queue will contribute the full
			// memtable memory size that could result in a write stall), or WAL
			// failover is permitting an unlimited queue without causing a write
			// stall. The latter condition is important to avoid delays in
			// visibility of concurrent writes that happen to get a sequence number
			// after this ingest and then must wait for this ingest that is itself
			// waiting on a large flush. See
			// https://github.com/cockroachdb/pebble/issues/4944 for an illustration
			// of this problem.
			(len(d.mu.mem.queue) < d.opts.MemTableStopWritesThreshold ||
				d.mu.log.manager.ElevateWriteStallThresholdForFailover()) &&
			!d.opts.Experimental.DisableIngestAsFlushable() && !hasRemoteFiles &&
			(!args.ExciseSpan.Valid() || d.FormatMajorVersion() >= FormatFlushableIngestExcises)
		if !canIngestFlushable {
			// We're not able to ingest as a flushable,
			// so we must synchronously flush.
			//
			// TODO(bilal): Currently, if any of the files being ingested are shared,
			// we cannot use flushable ingests and need
			// to wait synchronously.
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
			waitFlushStart = crtime.NowMono()
			d.maybeScheduleFlush()
			return
		}
		// Since there aren't too many memtables already queued up, we can
		// slide the ingested sstables on top of the existing memtables.
		asFlushable = true
		fileMetas := make([]*manifest.TableMetadata, len(loadResult.local))
		for i := range fileMetas {
			fileMetas[i] = loadResult.local[i].TableMetadata
		}
		err = d.handleIngestAsFlushable(fileMetas, seqNum, args.ExciseSpan)
		if addToOngoingExcises {
			// Ingested flushables do not destructively excise, so cannot harm a
			// concurrent EFOS creation.
			addToOngoingExcises = false
		}
	}

	var ve *manifest.VersionEdit
	var waitFlushDuration time.Duration
	var manifestUpdateDuration time.Duration
	apply := func(seqNum base.SeqNum) {
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

		// If there's an excise being done atomically with the same ingest, we
		// assign the lowest sequence number in the set of sequence numbers for this
		// ingestion to the excise. Note that we've already allocated fileCount+1
		// sequence numbers in this case.
		if args.ExciseSpan.Valid() {
			seqNum++ // the first seqNum is reserved for the excise.
		}
		// Update the sequence numbers for all ingested sstables'
		// metadata. When the version edit is applied, the metadata is
		// written to the manifest, persisting the sequence number.
		// The sstables themselves are left unmodified.
		if err = ingestUpdateSeqNum(
			d.cmp, d.opts.Comparer.FormatKey, seqNum, loadResult,
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
			waitFlushDuration = waitFlushStart.Elapsed()
		}

		if d.opts.private.testingBeforeIngestApplyFunc != nil {
			d.opts.private.testingBeforeIngestApplyFunc()
		}
		// Assign the sstables to the correct level in the LSM and apply the
		// version edit.
		ve, manifestUpdateDuration, err = d.ingestApply(ctx, jobID, loadResult, mut, args.ExciseSpan, args.ExciseBoundsPolicy, seqNum)
	}

	// Only one ingest can occur at a time because if not, one would block waiting
	// for the other to finish applying. This blocking would happen while holding
	// the commit mutex which would prevent unrelated batches from writing their
	// changes to the WAL and memtable. This will cause a bigger commit hiccup
	// during ingestion.
	seqNumCount := loadResult.fileCount()
	if args.ExciseSpan.Valid() {
		seqNumCount++
	}
	d.commit.ingestSem <- struct{}{}
	d.commit.AllocateSeqNum(seqNumCount, prepare, apply)
	<-d.commit.ingestSem
	if removeFromOngoingExcises {
		// NB: this must happen after the assignedSeqNum has become visible, so
		// that any concurrent EFOS creation that acquires d.mu after the removal
		// of this excise gets a visible seqnum after the excise. The
		// assignedSeqNum becomes visible in AllocateSeqNum.
		d.removeFromOngoingExcises(assignedSeqNum)
	}

	if err != nil {
		if err2 := ingestCleanup(d.objProvider, loadResult.local); err2 != nil {
			d.opts.Logger.Errorf("ingest cleanup failed: %v", err2)
		}
	} else {
		// Since we either created a hard link to the ingesting files, or copied
		// them over, it is safe to remove the originals paths.
		for i := range loadResult.local {
			path := loadResult.local[i].path
			if err2 := d.opts.FS.Remove(path); err2 != nil {
				d.opts.Logger.Errorf("ingest failed to remove original file: %s", err2)
			}
		}
	}

	// TODO(jackson): Refactor this so that the case where there are no files
	// but a valid excise span is not so exceptional.

	var stats IngestOperationStats
	if loadResult.fileCount() > 0 {
		info := TableIngestInfo{
			JobID:                  int(jobID),
			Err:                    err,
			flushable:              asFlushable,
			WaitFlushDuration:      waitFlushDuration,
			ManifestUpdateDuration: manifestUpdateDuration,
			BlockReadDuration:      loadResult.blockReadStats.BlockReadDuration,
			BlockReadBytes:         loadResult.blockReadStats.BlockBytes - loadResult.blockReadStats.BlockBytesInCache,
		}
		if len(loadResult.local) > 0 {
			info.GlobalSeqNum = loadResult.local[0].SmallestSeqNum
		} else if len(loadResult.shared) > 0 {
			info.GlobalSeqNum = loadResult.shared[0].SmallestSeqNum
		} else {
			info.GlobalSeqNum = loadResult.external[0].SmallestSeqNum
		}
		if ve != nil {
			info.Tables = make([]struct {
				TableInfo
				Level int
			}, len(ve.NewTables))
			for i := range ve.NewTables {
				e := &ve.NewTables[i]
				info.Tables[i].Level = e.Level
				info.Tables[i].TableInfo = e.Meta.TableInfo()
				stats.Bytes += e.Meta.Size
				if e.Level == 0 {
					stats.ApproxIngestedIntoL0Bytes += e.Meta.Size
				}
				if metaFlushableOverlaps[e.Meta.TableNum] {
					stats.MemtableOverlappingFiles++
				}
			}
		} else if asFlushable {
			// NB: If asFlushable == true, there are no shared sstables.
			info.Tables = make([]struct {
				TableInfo
				Level int
			}, len(loadResult.local))
			for i, f := range loadResult.local {
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
				if metaFlushableOverlaps[f.TableNum] {
					stats.ApproxIngestedIntoL0Bytes += f.Size
					stats.MemtableOverlappingFiles++
				}
			}
		}
		d.opts.EventListener.TableIngested(info)
	}

	return stats, err
}

type ingestSplitFile struct {
	// ingestFile is the file being ingested.
	ingestFile *manifest.TableMetadata
	// splitFile is the file that needs to be split to allow ingestFile to slot
	// into `level` level.
	splitFile *manifest.TableMetadata
	// The level where ingestFile will go (and where splitFile already is).
	level int
}

// ingestSplit splits files specified in `files` and updates ve in-place to
// account for existing files getting split into two virtual sstables. The map
// `replacedFiles` contains an in-progress map of all files that have been
// replaced with new virtual sstables in this version edit so far, which is also
// updated in-place.
//
// d.mu as well as the manifest lock must be held when calling this method.
func (d *DB) ingestSplit(
	ctx context.Context,
	ve *manifest.VersionEdit,
	updateMetrics func(*manifest.TableMetadata, int, []manifest.NewTableEntry),
	files []ingestSplitFile,
	replacedTables map[base.TableNum][]manifest.NewTableEntry,
) error {
	for _, s := range files {
		ingestFileBounds := s.ingestFile.UserKeyBounds()
		// replacedFiles can be thought of as a tree, where we start iterating with
		// s.splitFile and run its fileNum through replacedFiles, then find which of
		// the replaced files overlaps with s.ingestFile, which becomes the new
		// splitFile, then we check splitFile's replacements in replacedFiles again
		// for overlap with s.ingestFile, and so on until we either can't find the
		// current splitFile in replacedFiles (i.e. that's the file that now needs to
		// be split), or we don't find a file that overlaps with s.ingestFile, which
		// means a prior ingest split already produced enough room for s.ingestFile
		// to go into this level without necessitating another ingest split.
		splitFile := s.splitFile
		for splitFile != nil {
			replaced, ok := replacedTables[splitFile.TableNum]
			if !ok {
				break
			}
			updatedSplitFile := false
			for i := range replaced {
				if replaced[i].Meta.Overlaps(d.cmp, &ingestFileBounds) {
					if updatedSplitFile {
						// This should never happen because the earlier ingestTargetLevel
						// function only finds split file candidates that are guaranteed to
						// have no data overlap, only boundary overlap. See the comments
						// in that method to see the definitions of data vs boundary
						// overlap. That, plus the fact that files in `replaced` are
						// guaranteed to have file bounds that are tight on user keys
						// (as that's what `d.excise` produces), means that the only case
						// where we overlap with two or more files in `replaced` is if we
						// actually had data overlap all along, or if the ingestion files
						// were overlapping, either of which is an invariant violation.
						panic("updated with two files in ingestSplit")
					}
					splitFile = replaced[i].Meta
					updatedSplitFile = true
				}
			}
			if !updatedSplitFile {
				// None of the replaced files overlapped with the file being ingested.
				// This can happen if we've already excised a span overlapping with
				// this file, or if we have consecutive ingested files that can slide
				// within the same gap between keys in an existing file. For instance,
				// if an existing file has keys a and g and we're ingesting b-c, d-e,
				// the first loop iteration will split the existing file into one that
				// ends in a and another that starts at g, and the second iteration will
				// fall into this case and require no splitting.
				//
				// No splitting necessary.
				splitFile = nil
			}
		}
		if splitFile == nil {
			continue
		}
		// NB: excise operates on [start, end). We're splitting at [start, end]
		// (assuming !s.ingestFile.Largest.IsExclusiveSentinel()). The conflation
		// of exclusive vs inclusive end bounds should not make a difference here
		// as we're guaranteed to not have any data overlap between splitFile and
		// s.ingestFile. d.excise will return an error if we pass an inclusive user
		// key bound _and_ we end up seeing data overlap at the end key.
		exciseBounds := base.UserKeyBoundsFromInternal(s.ingestFile.Smallest(), s.ingestFile.Largest())
		leftTable, rightTable, err := d.exciseTable(ctx, exciseBounds, splitFile, s.level, tightExciseBounds)
		if err != nil {
			return err
		}
		added := applyExciseToVersionEdit(ve, splitFile, leftTable, rightTable, s.level)
		replacedTables[splitFile.TableNum] = added
		for i := range added {
			addedBounds := added[i].Meta.UserKeyBounds()
			if s.ingestFile.Overlaps(d.cmp, &addedBounds) {
				panic("ingest-time split produced a file that overlaps with ingested file")
			}
		}
		updateMetrics(splitFile, s.level, added)
	}
	// Flatten the version edit by removing any entries from ve.NewFiles that
	// are also in ve.DeletedFiles.
	newNewFiles := ve.NewTables[:0]
	for i := range ve.NewTables {
		fn := ve.NewTables[i].Meta.TableNum
		deEntry := manifest.DeletedTableEntry{Level: ve.NewTables[i].Level, FileNum: fn}
		if _, ok := ve.DeletedTables[deEntry]; ok {
			delete(ve.DeletedTables, deEntry)
		} else {
			newNewFiles = append(newNewFiles, ve.NewTables[i])
		}
	}
	ve.NewTables = newNewFiles
	return nil
}

func (d *DB) ingestApply(
	ctx context.Context,
	jobID JobID,
	lr ingestLoadResult,
	mut *memTable,
	exciseSpan KeyRange,
	exciseBoundsPolicy exciseBoundsPolicy,
	exciseSeqNum base.SeqNum,
) (*manifest.VersionEdit, time.Duration, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	ve := &manifest.VersionEdit{
		NewTables: make([]manifest.NewTableEntry, lr.fileCount()),
	}
	if exciseSpan.Valid() || (d.opts.Experimental.IngestSplit != nil && d.opts.Experimental.IngestSplit()) {
		ve.DeletedTables = map[manifest.DeletedTableEntry]*manifest.TableMetadata{}
	}
	var metrics levelMetricsDelta

	// Determine the target level inside UpdateVersionLocked. This prevents two
	// concurrent ingestion jobs from using the same version to determine the
	// target level, and also provides serialization with concurrent compaction
	// and flush jobs.
	manifestUpdateDuration, err := d.mu.versions.UpdateVersionLocked(func() (versionUpdate, error) {
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
		overlapChecker := &overlapChecker{
			comparer: d.opts.Comparer,
			newIters: d.newIters,
			opts: IterOptions{
				logger:   d.opts.Logger,
				Category: categoryIngest,
			},
			v: current,
		}
		shouldIngestSplit := d.opts.Experimental.IngestSplit != nil &&
			d.opts.Experimental.IngestSplit() && d.FormatMajorVersion() >= FormatVirtualSSTables
		baseLevel := d.mu.versions.picker.getBaseLevel()
		// filesToSplit is a list where each element is a pair consisting of a file
		// being ingested and a file being split to make room for an ingestion into
		// that level. Each ingested file will appear at most once in this list. It
		// is possible for split files to appear twice in this list.
		filesToSplit := make([]ingestSplitFile, 0)
		checkCompactions := false
		for i := 0; i < lr.fileCount(); i++ {
			// Determine the lowest level in the LSM for which the sstable doesn't
			// overlap any existing files in the level.
			var m *manifest.TableMetadata
			specifiedLevel := -1
			isShared := false
			isExternal := false
			if i < len(lr.local) {
				// local file.
				m = lr.local[i].TableMetadata
			} else if (i - len(lr.local)) < len(lr.shared) {
				// shared file.
				isShared = true
				sharedIdx := i - len(lr.local)
				m = lr.shared[sharedIdx].TableMetadata
				specifiedLevel = int(lr.shared[sharedIdx].shared.Level)
			} else {
				// external file.
				isExternal = true
				externalIdx := i - (len(lr.local) + len(lr.shared))
				m = lr.external[externalIdx].TableMetadata
				if lr.externalFilesHaveLevel {
					specifiedLevel = int(lr.external[externalIdx].external.Level)
				}
			}

			// Add to CreatedBackingTables if this is a new backing.
			//
			// Shared files always have a new backing. External files have new backings
			// iff the backing disk file num and the file num match (see ingestAttachRemote).
			if isShared || (isExternal && m.TableBacking.DiskFileNum == base.DiskFileNum(m.TableNum)) {
				ve.CreatedBackingTables = append(ve.CreatedBackingTables, m.TableBacking)
			}

			f := &ve.NewTables[i]
			var err error
			if specifiedLevel != -1 {
				f.Level = specifiedLevel
			} else {
				var splitTable *manifest.TableMetadata
				if exciseSpan.Valid() && exciseSpan.Contains(d.cmp, m.Smallest()) && exciseSpan.Contains(d.cmp, m.Largest()) {
					// This file fits perfectly within the excise span. We can slot it at
					// L6, or sharedLevelsStart - 1 if we have shared files.
					if len(lr.shared) > 0 || lr.externalFilesHaveLevel {
						f.Level = sharedLevelsStart - 1
						if baseLevel > f.Level {
							f.Level = 0
						}
					} else {
						f.Level = 6
					}
				} else {
					// We check overlap against the LSM without holding DB.mu. Note that we
					// are still holding the log lock, so the version cannot change.
					// TODO(radu): perform this check optimistically outside of the log lock.
					var lsmOverlap overlap.WithLSM
					lsmOverlap, err = func() (overlap.WithLSM, error) {
						d.mu.Unlock()
						defer d.mu.Lock()
						return overlapChecker.DetermineLSMOverlap(ctx, m.UserKeyBounds())
					}()
					if err == nil {
						f.Level, splitTable, err = ingestTargetLevel(
							ctx, d.cmp, lsmOverlap, baseLevel, d.mu.compact.inProgress, m, shouldIngestSplit,
						)
					}
				}

				if splitTable != nil {
					if invariants.Enabled {
						if lf := current.Levels[f.Level].Find(d.cmp, splitTable); lf.Empty() {
							panic("splitFile returned is not in level it should be")
						}
					}
					// We take advantage of the fact that we won't drop the db mutex
					// between now and the call to UpdateVersionLocked. So, no files should
					// get added to a new in-progress compaction at this point. We can
					// avoid having to iterate on in-progress compactions to cancel them
					// if none of the files being split have a compacting state.
					if splitTable.IsCompacting() {
						checkCompactions = true
					}
					filesToSplit = append(filesToSplit, ingestSplitFile{ingestFile: m, splitFile: splitTable, level: f.Level})
				}
			}
			if err != nil {
				return versionUpdate{}, err
			}
			if isShared && f.Level < sharedLevelsStart {
				panic(fmt.Sprintf("cannot slot a shared file higher than the highest shared level: %d < %d",
					f.Level, sharedLevelsStart))
			}
			f.Meta = m
			levelMetrics := metrics[f.Level]
			if levelMetrics == nil {
				levelMetrics = &LevelMetrics{}
				metrics[f.Level] = levelMetrics
			}
			levelMetrics.TablesCount++
			levelMetrics.TablesSize += int64(m.Size)
			levelMetrics.EstimatedReferencesSize += m.EstimatedReferenceSize()
			levelMetrics.TableBytesIngested += m.Size
			levelMetrics.TablesIngested++
		}
		// replacedTables maps files excised due to exciseSpan (or splitFiles returned
		// by ingestTargetLevel), to files that were created to replace it. This map
		// is used to resolve references to split files in filesToSplit, as it is
		// possible for a file that we want to split to no longer exist or have a
		// newer fileMetadata due to a split induced by another ingestion file, or an
		// excise.
		replacedTables := make(map[base.TableNum][]manifest.NewTableEntry)
		updateLevelMetricsOnExcise := func(m *manifest.TableMetadata, level int, added []manifest.NewTableEntry) {
			levelMetrics := metrics[level]
			if levelMetrics == nil {
				levelMetrics = &LevelMetrics{}
				metrics[level] = levelMetrics
			}
			levelMetrics.TablesCount--
			levelMetrics.TablesSize -= int64(m.Size)
			levelMetrics.EstimatedReferencesSize -= m.EstimatedReferenceSize()
			for i := range added {
				levelMetrics.TablesCount++
				levelMetrics.TablesSize += int64(added[i].Meta.Size)
				levelMetrics.EstimatedReferencesSize += added[i].Meta.EstimatedReferenceSize()
			}
		}
		var exciseBounds base.UserKeyBounds
		if exciseSpan.Valid() {
			exciseBounds = exciseSpan.UserKeyBounds()
			d.mu.versions.metrics.Ingest.ExciseIngestCount++
			// Iterate through all levels and find files that intersect with exciseSpan.
			//
			// TODO(bilal): We could drop the DB mutex here as we don't need it for
			// excises; we only need to hold the version lock which we already are
			// holding. However releasing the DB mutex could mess with the
			// ingestTargetLevel calculation that happened above, as it assumed that it
			// had a complete view of in-progress compactions that wouldn't change
			// until UpdateVersionLocked is called. If we were to drop the mutex now,
			// we could schedule another in-progress compaction that would go into the
			// chosen target level and lead to file overlap within level (which would
			// panic in UpdateVersionLocked). We should drop the db mutex here, do the
			// excise, then re-grab the DB mutex and rerun just the in-progress
			// compaction check to see if any new compactions are conflicting with our
			// chosen target levels for files, and if they are, we should signal those
			// compactions to error out.
			for layer, ls := range current.AllLevelsAndSublevels() {
				for m := range ls.Overlaps(d.cmp, exciseSpan.UserKeyBounds()).All() {
					leftTable, rightTable, err := d.exciseTable(ctx, exciseBounds, m, layer.Level(), exciseBoundsPolicy)
					if err != nil {
						return versionUpdate{}, err
					}
					newFiles := applyExciseToVersionEdit(ve, m, leftTable, rightTable, layer.Level())
					replacedTables[m.TableNum] = newFiles
					updateLevelMetricsOnExcise(m, layer.Level(), newFiles)
				}
			}
			if d.FormatMajorVersion() >= FormatExciseBoundsRecord {
				ve.ExciseBoundsRecord = append(ve.ExciseBoundsRecord, manifest.ExciseOpEntry{
					Bounds: exciseBounds,
					SeqNum: exciseSeqNum,
				})
			}
		}
		if len(filesToSplit) > 0 {
			// For the same reasons as the above call to excise, we hold the db mutex
			// while calling this method.
			if err := d.ingestSplit(ctx, ve, updateLevelMetricsOnExcise, filesToSplit, replacedTables); err != nil {
				return versionUpdate{}, err
			}
		}
		if len(filesToSplit) > 0 || exciseSpan.Valid() {
			for c := range d.mu.compact.inProgress {
				if c.VersionEditApplied() {
					continue
				}
				// Check if this compaction overlaps with the excise span. Note that just
				// checking if the inputs individually overlap with the excise span
				// isn't sufficient; for instance, a compaction could have [a,b] and [e,f]
				// as inputs and write it all out as [a,b,e,f] in one sstable. If we're
				// doing a [c,d) excise at the same time as this compaction, we will have
				// to error out the whole compaction as we can't guarantee it hasn't/won't
				// write a file overlapping with the excise span.
				bounds := c.Bounds()
				if bounds != nil && bounds.Overlaps(d.cmp, &exciseBounds) {
					c.Cancel()
				}
				// Check if this compaction's inputs have been replaced due to an
				// ingest-time split. In that case, cancel the compaction as a newly picked
				// compaction would need to include any new files that slid in between
				// previously-existing files. Note that we cancel any compaction that has a
				// file that was ingest-split as an input, even if it started before this
				// ingestion.
				if checkCompactions {
					for _, table := range c.Tables() {
						if _, ok := replacedTables[table.TableNum]; ok {
							c.Cancel()
							break
						}
					}
				}
			}
		}

		return versionUpdate{
			VE:                      ve,
			JobID:                   jobID,
			Metrics:                 metrics,
			InProgressCompactionsFn: func() []compactionInfo { return d.getInProgressCompactionInfoLocked(nil) },
		}, nil
	})
	if err != nil {
		return nil, 0, err
	}
	// Check for any EventuallyFileOnlySnapshots that could be watching for
	// an excise on this span. There should be none as the
	// computePossibleOverlaps steps should have forced these EFOS to transition
	// to file-only snapshots by now. If we see any that conflict with this
	// excise, panic.
	if exciseSpan.Valid() {
		for s := d.mu.snapshots.root.next; s != &d.mu.snapshots.root; s = s.next {
			// Skip non-EFOS snapshots, and also skip any EFOS that were created
			// *after* the excise.
			if s.efos == nil || base.Visible(exciseSeqNum, s.efos.seqNum, base.SeqNumMax) {
				continue
			}
			efos := s.efos
			// TODO(bilal): We can make this faster by taking advantage of the sorted
			// nature of protectedRanges to do a sort.Search, or even maintaining a
			// global list of all protected ranges instead of having to peer into every
			// snapshot.
			for i := range efos.protectedRanges {
				if efos.protectedRanges[i].OverlapsKeyRange(d.cmp, exciseSpan) {
					panic("unexpected excise of an EventuallyFileOnlySnapshot's bounds")
				}
			}
		}
	}

	d.mu.versions.metrics.Ingest.Count++

	d.updateReadStateLocked(d.opts.DebugCheck)
	// updateReadStateLocked could have generated obsolete tables, schedule a
	// cleanup job if necessary.
	d.deleteObsoleteFiles(jobID)
	d.updateTableStatsLocked(ve.NewTables)
	// The ingestion may have pushed a level over the threshold for compaction,
	// so check to see if one is necessary and schedule it.
	d.maybeScheduleCompaction()
	var toValidate []manifest.NewTableEntry
	dedup := make(map[base.DiskFileNum]struct{})
	for _, entry := range ve.NewTables {
		if _, ok := dedup[entry.Meta.TableBacking.DiskFileNum]; !ok {
			toValidate = append(toValidate, entry)
			dedup[entry.Meta.TableBacking.DiskFileNum] = struct{}{}
		}
	}
	d.maybeValidateSSTablesLocked(toValidate)

	return ve, manifestUpdateDuration, nil
}

// maybeValidateSSTablesLocked adds the slice of newTableEntrys to the pending
// queue of files to be validated, when the feature is enabled.
//
// Note that if two entries with the same backing file are added twice, then the
// block checksums for the backing file will be validated twice.
//
// DB.mu must be locked when calling.
func (d *DB) maybeValidateSSTablesLocked(newFiles []manifest.NewTableEntry) {
	// Only add to the validation queue when the feature is enabled.
	if !d.opts.Experimental.ValidateOnIngest {
		return
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
	jobID := d.newJobIDLocked()
	rs := d.loadReadState()

	// Drop DB.mu before performing IO.
	d.mu.Unlock()

	// Validate all tables in the pending queue. This could lead to a situation
	// where we are starving IO from other tasks due to having to page through
	// all the blocks in all the sstables in the queue.
	// TODO(travers): Add some form of pacing to avoid IO starvation.

	// If we fail to validate any files due to reasons other than uncovered
	// corruption, accumulate them and re-queue them for another attempt.
	var retry []manifest.NewTableEntry

	for _, f := range pending {
		// The file may have been moved or deleted since it was ingested, in
		// which case we skip.
		if !rs.current.Contains(f.Level, f.Meta) {
			// Assume the file was moved to a lower level. It is rare enough
			// that a table is moved or deleted between the time it was ingested
			// and the time the validation routine runs that the overall cost of
			// this inner loop is tolerably low, when amortized over all
			// ingested tables.
			found := false
			for i := f.Level + 1; i < numLevels; i++ {
				if rs.current.Contains(i, f.Meta) {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		// TOOD(radu): plumb a ReadEnv with a CategoryIngest stats collector through
		// to ValidateBlockChecksums.
		err := d.fileCache.withReader(context.TODO(), block.NoReadEnv,
			f.Meta, func(r *sstable.Reader, _ sstable.ReadEnv) error {
				return r.ValidateBlockChecksums()
			})

		if err != nil {
			if IsCorruptionError(err) {
				// TODO(travers): Hook into the corruption reporting pipeline, once
				// available. See pebble#1192.
				d.opts.Logger.Fatalf("pebble: encountered corruption during ingestion: %s", err)
			} else {
				// If there was some other, possibly transient, error that
				// caused table validation to fail inform the EventListener and
				// move on. We remember the table so that we can retry it in a
				// subsequent table validation job.
				//
				// TODO(jackson): If the error is not transient, this will retry
				// validation indefinitely. While not great, it's the same
				// behavior as erroring flushes and compactions. We should
				// address this as a part of #270.
				d.opts.EventListener.BackgroundError(err)
				retry = append(retry, f)
				continue
			}
		}

		d.opts.EventListener.TableValidated(TableValidatedInfo{
			JobID: int(jobID),
			Meta:  f.Meta,
		})
	}
	rs.unref()
	d.mu.Lock()
	defer d.mu.Unlock()
	d.mu.tableValidation.pending = append(d.mu.tableValidation.pending, retry...)
	d.mu.tableValidation.validating = false
	d.mu.tableValidation.cond.Broadcast()
	if d.shouldValidateSSTablesLocked() {
		go d.validateSSTables()
	}
}
