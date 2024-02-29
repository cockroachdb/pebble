// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package compact implements the mechanics around compactions.
package compact

import (
	"context"
	"slices"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/private"
	"github.com/cockroachdb/pebble/internal/rangedel"
	"github.com/cockroachdb/pebble/internal/rangekey"
	"github.com/cockroachdb/pebble/sstable"
)

// Funcs encapsulates a set of funcs invoked by the compaction runner while
// executing a compaction.
type Funcs struct {
	// NewTable allocates a new file number and creates a corresponding
	// sstable.Writer.
	NewTable func(extraOpts ...sstable.WriterOption) (*manifest.FileMetadata, *sstable.Writer, error)
	// TableFinished is invoked by the compaction whenever a new output SSTable
	// has been finished. It's provided with the file's computed fileMetadata
	// and the writer metadata returned by the sstable writer.
	TableFinished func(meta *manifest.FileMetadata, writerMeta *sstable.WriterMetadata) error
	// ElideTombstone is invoked by a compaction to determine if a tombstone at
	// the provided key were to fall within the final snapshot stripe, should it
	// be elided? The implementation is responsible for ensuring that it only
	// returns true when there are no instances of the key in levels lower than
	// the compaction input sstables.
	//
	// ElideTombsotne may be invoked by a compaction for keys that are NOT in
	// the final snapshot stripe in order to compute statistics.
	ElideTombstone func(key []byte) bool
	// ElideRangeTombstone is invoked by a compaction to determine if a range
	// tombstone with the provided key range were to fall within the final
	// snapshot stripe, should it be elided? The implementation is responsible
	// for ensuring that it only return true if there are no instances of any
	// overlapping key in levels lower than the compaction input sstables.
	//
	// ElideRangeTombsotne may be invoked by a compaction for tombstones that
	// are NOT in the final snapshot stripe in order to compute statistics.
	ElideRangeTombstone func(start, end []byte) bool
	// IneffectualSingleDelete is an optional func. When provided, if a single
	// delete is observed to have been ineffectual (eg, it was elided without
	// ever causing the elision of another key), the compaction invokes the func
	// passing the single delete's user key.
	IneffectualSingleDelete func(userKey []byte)
	// SingleDeleteInvariantViolation is an optional func. When provided, if
	// single delete invariants are observed to have been violated, the
	// compaction invokes the func passing the single delete's user key. False
	// positives are possible in the presence of deletion-only compactions.
	SingleDeleteInvariantViolation func(userKey []byte)
}

// Stats describes stats collected during the compaction.
type Stats struct {
	CumulativePinnedKeys uint64
	CumulativePinnedSize uint64
	CountMissizedDels    uint64
}

// Output describes all the output state constructed by the compaction,
// including the metadata of constructed files and related statistics.
type Output struct {
	FileMetadatas   []*manifest.FileMetadata
	WriterMetadatas []sstable.WriterMetadata
	Stats           Stats
}

// Run runs a single compaction, reading keys from the provided input iterator
// and writing to sstables created through funcs.NewTable.
func Run(
	ctx context.Context,
	comparer *base.Comparer,
	merge base.Merge,
	splitter OutputSplitter,
	snapshots []uint64,
	allowZeroSeqNum bool,
	inputIterator base.InternalIterator,
	rangeDelInterleaving *keyspan.InterleavingIter,
	rangeKeyInterleaving *keyspan.InterleavingIter,
	funcs Funcs,
) (Output, error) {
	r := new(runner)
	*r = runner{
		comparer: comparer,
		funcs:    funcs,
		iter: newIter(
			comparer.Compare,
			comparer.Equal,
			comparer.FormatKey,
			merge,
			inputIterator,
			snapshots,
			&r.rangeDelFrag,
			&r.rangeKeyFrag,
			allowZeroSeqNum,
			funcs.ElideTombstone,
			funcs.ElideRangeTombstone,
			funcs.IneffectualSingleDelete,
			funcs.SingleDeleteInvariantViolation,
		),
		rangeDelInterleaving: rangeDelInterleaving,
		rangeKeyInterleaving: rangeKeyInterleaving,
		splitter:             splitter,
	}
	return r.run(ctx)
}

// A runner executes a single compaction.
type runner struct {
	comparer             *base.Comparer
	funcs                Funcs
	iter                 *compactionIter
	rangeDelInterleaving *keyspan.InterleavingIter
	rangeKeyInterleaving *keyspan.InterleavingIter
	splitter             OutputSplitter

	rangeDelFrag keyspan.Fragmenter
	rangeKeyFrag keyspan.Fragmenter
}

// Run executes a compaction. Run iterates over the input iterator, applying
// internal key semantics to compact keys and outputting the results to new
// sstables.
func (r *runner) run(ctx context.Context) (out Output, err error) {
	var (
		tw              *sstable.Writer
		pinnedKeySize   uint64
		pinnedValueSize uint64
		pinnedCount     uint64
	)
	defer func() {
		if tw != nil {
			err = errors.CombineErrors(err, tw.Close())
		}
	}()

	// prevPointKey is a sstable.WriterOption that provides access to
	// the last point key written to a writer's sstable. When a new
	// output begins in newOutput, prevPointKey is updated to point to
	// the new output's sstable.Writer. This allows the compaction loop
	// to access the last written point key without requiring the
	// compaction loop to make a copy of each key ahead of time. Users
	// must be careful, because the byte slice returned by UnsafeKey
	// points directly into the Writer's block buffer.
	var prevPointKey sstable.PreviousPointKeyOpt
	var currentMeta *manifest.FileMetadata

	// finishOutput is called with the a user key up to which all tombstones
	// should be flushed. Typically, this is the first key of the next
	// sstable or an empty key if this output is the final sstable.
	finishOutput := func(splitKey []byte) error {
		// If we haven't output any point records to the sstable (tw == nil) then the
		// sstable will only contain range tombstones and/or range keys. The smallest
		// key in the sstable will be the start key of the first range tombstone or
		// range key added. We need to ensure that this start key is distinct from
		// the splitKey passed to finishOutput (if set), otherwise we would generate
		// an sstable where the largest key is smaller than the smallest key due to
		// how the largest key boundary is set below. NB: It is permissible for the
		// range tombstone / range key start key to be the empty string.
		//
		// TODO: It is unfortunate that we have to do this check here rather than
		// when we decide to finish the sstable in the runCompaction loop. A better
		// structure currently eludes us.
		if tw == nil {
			startKey := r.rangeDelFrag.Start()
			if len(r.iter.tombstones) > 0 {
				startKey = r.iter.tombstones[0].Start
			}
			if startKey == nil {
				startKey = r.rangeKeyFrag.Start()
				if len(r.iter.rangeKeys) > 0 {
					startKey = r.iter.rangeKeys[0].Start
				}
			}
			if splitKey != nil && r.comparer.Equal(startKey, splitKey) {
				return nil
			}
		}

		// NB: clone the key because the data can be held on to by the call to
		// compactionIter.Tombstones via keyspan.Fragmenter.FlushTo, and by the
		// WriterMetadata.LargestRangeDel.UserKey.
		splitKey = slices.Clone(splitKey)
		for _, v := range r.iter.Tombstones(splitKey) {
			// Create a new output file if the previous one has been closed.
			if tw == nil {
				currentMeta, tw, err = r.funcs.NewTable(&prevPointKey)
				if err != nil {
					return err
				}
			}
			// The tombstone being added could be completely outside the
			// eventual bounds of the sstable. Consider this example (bounds
			// in square brackets next to table filename):
			//
			// ./000240.sst   [tmgc#391,MERGE-tmgc#391,MERGE]
			// tmgc#391,MERGE [786e627a]
			// tmgc-udkatvs#331,RANGEDEL
			//
			// ./000241.sst   [tmgc#384,MERGE-tmgc#384,MERGE]
			// tmgc#384,MERGE [666c7070]
			// tmgc-tvsalezade#383,RANGEDEL
			// tmgc-tvsalezade#331,RANGEDEL
			//
			// ./000242.sst   [tmgc#383,RANGEDEL-tvsalezade#72057594037927935,RANGEDEL]
			// tmgc-tvsalezade#383,RANGEDEL
			// tmgc#375,SET [72646c78766965616c72776865676e79]
			// tmgc-tvsalezade#356,RANGEDEL
			//
			// Note that both of the top two SSTables have range tombstones
			// that start after the file's end keys. Since the file bound
			// computation happens well after all range tombstones have been
			// added to the writer, eliding out-of-file range tombstones based
			// on sequence number at this stage is difficult, and necessitates
			// read-time logic to ignore range tombstones outside file bounds.
			if err := rangedel.Encode(&v, tw.Add); err != nil {
				return err
			}
		}
		for _, v := range r.iter.RangeKeys(splitKey) {
			// Same logic as for range tombstones, except added using tw.AddRangeKey.
			if tw == nil {
				currentMeta, tw, err = r.funcs.NewTable(&prevPointKey)
				if err != nil {
					return err
				}
			}
			if err := rangekey.Encode(&v, tw.AddRangeKey); err != nil {
				return err
			}
		}

		if tw == nil {
			return nil
		}
		{
			// Set internal sstable properties.
			p := getInternalWriterProperties(tw)
			// Set the external sst version to 0. This is what RocksDB expects for
			// db-internal sstables; otherwise, it could apply a global sequence number.
			p.ExternalFormatVersion = 0
			// Set the snapshot pinned totals.
			p.SnapshotPinnedKeys = pinnedCount
			p.SnapshotPinnedKeySize = pinnedKeySize
			p.SnapshotPinnedValueSize = pinnedValueSize
			out.Stats.CumulativePinnedKeys += pinnedCount
			out.Stats.CumulativePinnedSize += pinnedKeySize + pinnedValueSize
			pinnedCount = 0
			pinnedKeySize = 0
			pinnedValueSize = 0
		}
		if err := tw.Close(); err != nil {
			tw = nil
			return err
		}
		writerMeta, err := tw.Metadata()
		if err != nil {
			tw = nil
			return err
		}
		tw = nil
		currentMeta.Size = writerMeta.Size
		currentMeta.SmallestSeqNum = writerMeta.SmallestSeqNum
		currentMeta.LargestSeqNum = writerMeta.LargestSeqNum
		currentMeta.InitPhysicalBacking()
		if writerMeta.HasPointKeys {
			currentMeta.ExtendPointKeyBounds(r.comparer.Compare, writerMeta.SmallestPoint, writerMeta.LargestPoint)
		}
		if writerMeta.HasRangeDelKeys {
			currentMeta.ExtendPointKeyBounds(r.comparer.Compare, writerMeta.SmallestRangeDel, writerMeta.LargestRangeDel)
		}
		if writerMeta.HasRangeKeys {
			currentMeta.ExtendRangeKeyBounds(r.comparer.Compare, writerMeta.SmallestRangeKey, writerMeta.LargestRangeKey)
		}
		out.WriterMetadatas = append(out.WriterMetadatas, *writerMeta)

		// Verify that all range deletions outputted to the sstable are
		// truncated to split key.
		if splitKey != nil && writerMeta.LargestRangeDel.UserKey != nil &&
			r.comparer.Compare(writerMeta.LargestRangeDel.UserKey, splitKey) > 0 {
			return errors.Errorf(
				"pebble: invariant violation: rangedel largest key %q extends beyond split key %q",
				writerMeta.LargestRangeDel.Pretty(r.comparer.FormatKey),
				r.comparer.FormatKey(splitKey),
			)
		}

		// Let the caller know that we've completed an output; the caller may
		// want to perform additional verification, or issue event
		// notifications.
		if err := r.funcs.TableFinished(currentMeta, writerMeta); err != nil {
			return err
		}
		currentMeta = nil
		return nil
	}

	// Each outer loop iteration produces one output file. An iteration that
	// produces a file containing point keys (and optionally range tombstones)
	// guarantees that the input iterator advanced. An iteration that produces
	// a file containing only range tombstones guarantees the limit passed to
	// `finishOutput()` advanced to a strictly greater user key corresponding
	// to a grandparent file largest key, or nil. Taken together, these
	// progress guarantees ensure that eventually the input iterator will be
	// exhausted and the range tombstone fragments will all be flushed.
	for key, val := r.iter.First(); key != nil || !r.rangeDelFrag.Empty() || !r.rangeKeyFrag.Empty(); {
		var firstKey []byte
		if key != nil {
			firstKey = key.UserKey
		} else if startKey := r.rangeDelFrag.Start(); startKey != nil {
			// Pass the start key of the first pending tombstone to find the
			// next limit. All pending tombstones have the same start key. We
			// use this as opposed to the end key of the last written sstable to
			// effectively handle cases like these:
			//
			// a.SET.3
			// (lf.limit at b)
			// d.RANGEDEL.4:f
			//
			// In this case, the partition after b has only range deletions, so
			// if we were to find the limit after the last written key at the
			// split point (key a), we'd get the limit b again, and
			// finishOutput() would not advance any further because the next
			// range tombstone to write does not start until after the L0 split
			// point.
			firstKey = startKey
		}
		splitterSuggestion := r.splitter.OnNewOutput(firstKey)

		// Each inner loop iteration processes one key from the input iterator.
		for ; key != nil; key, val = r.iter.Next() {
			if split := r.splitter.ShouldSplitBefore(key, tw); split == SplitNow {
				break
			}

			switch key.Kind() {
			case base.InternalKeyKindRangeDelete:
				// Range tombstones are handled specially. They are fragmented,
				// and they're not written until later during `finishOutput()`.
				// We add them to the `Fragmenter` now to make them visible to
				// `compactionIter` so covered keys in the same snapshot stripe
				// can be elided.

				// The interleaved range deletion might only be one of many with
				// these bounds. Some fragmenting is performed ahead of time by
				// keyspanimpl.MergingIter.
				if s := r.rangeDelInterleaving.Span(); !s.Empty() {
					// The memory management here is subtle. Range deletions blocks do NOT
					// use prefix compression, which ensures that range deletion spans'
					// memory is available as long we keep the iterator open. However, the
					// keyspanimpl.MergingIter that merges spans across levels only
					// guarantees the lifetime of the [start, end) bounds until the next
					// positioning method is called.
					//
					// Additionally, the Span.Keys slice is owned by the the range
					// deletion iterator stack, and it may be overwritten when we advance.
					//
					// Clone the Keys slice and the start and end keys.
					//
					// TODO(jackson): Avoid the clone by removing r.rangeDelFrag and
					// performing explicit truncation of the pending rangedel span as
					// necessary.
					clone := keyspan.Span{
						Start: r.iter.cloneKey(s.Start),
						End:   r.iter.cloneKey(s.End),
						Keys:  make([]keyspan.Key, len(s.Keys)),
					}
					copy(clone.Keys, s.Keys)
					r.rangeDelFrag.Add(clone)
				}
				continue
			case base.InternalKeyKindRangeKeySet, base.InternalKeyKindRangeKeyUnset, base.InternalKeyKindRangeKeyDelete:
				// Range keys are handled in the same way as range tombstones, except
				// with a dedicated fragmenter.
				if s := r.rangeKeyInterleaving.Span(); !s.Empty() {
					clone := keyspan.Span{
						Start: r.iter.cloneKey(s.Start),
						End:   r.iter.cloneKey(s.End),
						Keys:  make([]keyspan.Key, len(s.Keys)),
					}
					// Since the keys' Suffix and Value fields are not deep cloned, the
					// underlying blockIter must be kept open for the lifetime of the
					// compaction.
					copy(clone.Keys, s.Keys)
					r.rangeKeyFrag.Add(clone)
				}
				continue
			}
			if tw == nil {
				currentMeta, tw, err = r.funcs.NewTable(&prevPointKey)
				if err != nil {
					return out, err
				}
			}
			if err := tw.AddWithForceObsolete(*key, val, r.iter.forceObsoleteDueToRangeDel); err != nil {
				return out, err
			}
			if r.iter.snapshotPinned {
				// The kv pair we just added to the sstable was only surfaced by
				// the compaction iterator because an open snapshot prevented
				// its elision. Increment the stats.
				pinnedCount++
				pinnedKeySize += uint64(len(key.UserKey)) + base.InternalTrailerLen
				pinnedValueSize += uint64(len(val))
			}
		}

		// A splitter requested a split, and we're ready to finish the output.
		// We need to choose the key at which to split any pending range
		// tombstones. There are two options:
		// 1. splitterSuggestion — The key suggested by the splitter. This key
		//    is guaranteed to be greater than the last key written to the
		//    current output.
		// 2. key.UserKey — the first key of the next sstable output. This user
		//     key is also guaranteed to be greater than the last user key
		//     written to the current output (see userKeyChangeSplitter).
		//
		// Use whichever is smaller. Using the smaller of the two limits
		// overlap with grandparents. Consider the case where the
		// grandparent limit is calculated to be 'b', key is 'x', and
		// there exist many sstables between 'b' and 'x'. If the range
		// deletion fragmenter has a pending tombstone [a,x), splitting
		// at 'x' would cause the output table to overlap many
		// grandparents well beyond the calculated grandparent limit
		// 'b'. Splitting at the smaller `splitterSuggestion` avoids
		// this unbounded overlap with grandparent tables.
		splitKey := splitterSuggestion
		if key != nil && (splitKey == nil || r.comparer.Compare(splitKey, key.UserKey) > 0) {
			splitKey = key.UserKey
		}
		if err := finishOutput(splitKey); err != nil {
			return out, err
		}
	}

	// The compaction iterator keeps track of a count of the number of DELSIZED
	// keys that encoded an incorrect size. Propagate it up as a part of
	// compactStats.
	out.Stats.CountMissizedDels = r.iter.stats.countMissizedDels
	return out, nil
}

// getInternalWriterProperties accesses a private variable (in the
// internal/private package) initialized by the sstable Writer. This indirection
// is necessary to ensure non-Pebble users constructing sstables for ingestion
// are unable to set internal-only properties.
var getInternalWriterProperties = private.SSTableInternalProperties.(func(*sstable.Writer) *sstable.Properties)
