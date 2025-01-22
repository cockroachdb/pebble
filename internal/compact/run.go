// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package compact

import (
	"sort"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable"
)

// Result stores the result of a compaction - more specifically, the "data" part
// where we use the compaction iterator to write output tables.
type Result struct {
	// Err is the result of the compaction. On success, Err is nil and Tables
	// stores the output tables. On failure, Err is set and Tables stores the
	// tables created so far (and which need to be cleaned up).
	Err    error
	Tables []OutputTable
	Stats  Stats
}

// WithError returns a modified Result which has the Err field set.
func (r Result) WithError(err error) Result {
	return Result{
		Err:    errors.CombineErrors(r.Err, err),
		Tables: r.Tables,
		Stats:  r.Stats,
	}
}

// OutputTable contains metadata about a table that was created during a compaction.
type OutputTable struct {
	CreationTime time.Time
	// ObjMeta is metadata for the object backing the table.
	ObjMeta objstorage.ObjectMetadata
	// WriterMeta is populated once the table is fully written. On compaction
	// failure (see Result), WriterMeta might not be set.
	WriterMeta sstable.WriterMetadata
}

// Stats describes stats collected during the compaction.
type Stats struct {
	CumulativePinnedKeys  uint64
	CumulativePinnedSize  uint64
	CumulativeWrittenSize uint64
	CountMissizedDels     uint64
}

// RunnerConfig contains the parameters needed for the Runner.
type RunnerConfig struct {
	// CompactionBounds are the bounds containing all the input tables. All output
	// tables must fall within these bounds as well.
	CompactionBounds base.UserKeyBounds

	// L0SplitKeys is only set for flushes and it contains the flush split keys
	// (see L0Sublevels.FlushSplitKeys). These are split points enforced for the
	// output tables.
	L0SplitKeys [][]byte

	// Grandparents are the tables in level+2 that overlap with the files being
	// compacted. Used to determine output table boundaries. Do not assume that
	// the actual files in the grandparent when this compaction finishes will be
	// the same.
	Grandparents manifest.LevelSlice

	// MaxGrandparentOverlapBytes is the maximum number of bytes of overlap
	// allowed for a single output table with the tables in the grandparent level.
	MaxGrandparentOverlapBytes uint64

	// TargetOutputFileSize is the desired size of an individual table created
	// during compaction. In practice, the sizes can vary between 50%-200% of this
	// value.
	TargetOutputFileSize uint64

	// GrantHandle is used to perform accounting of resource consumption by the
	// CompactionScheduler.
	GrantHandle base.CompactionGrantHandle
}

// Runner is a helper for running the "data" part of a compaction (where we use
// the compaction iterator to write output tables).
//
// Sample usage:
//
//	r := NewRunner(cfg, iter)
//	for r.MoreDataToWrite() {
//	  objMeta, tw := ... // Create object and table writer.
//	  r.WriteTable(objMeta, tw)
//	}
//	result := r.Finish()
type Runner struct {
	cmp  base.Compare
	cfg  RunnerConfig
	iter *Iter

	tables []OutputTable
	// Stores any error encountered.
	err error
	// Last key/value returned by the compaction iterator.
	kv *base.InternalKV
	// Last RANGEDEL span (or portion of it) that was not yet written to a table.
	lastRangeDelSpan keyspan.Span
	// Last range key span (or portion of it) that was not yet written to a table.
	lastRangeKeySpan keyspan.Span
	stats            Stats
}

// NewRunner creates a new Runner.
func NewRunner(cfg RunnerConfig, iter *Iter) *Runner {
	r := &Runner{
		cmp:  iter.cmp,
		cfg:  cfg,
		iter: iter,
	}
	r.kv = r.iter.First()
	return r
}

// MoreDataToWrite returns true if there is more data to be written.
func (r *Runner) MoreDataToWrite() bool {
	if r.err != nil {
		return false
	}
	return r.kv != nil || !r.lastRangeDelSpan.Empty() || !r.lastRangeKeySpan.Empty()
}

// WriteTable writes a new output table. This table will be part of
// Result.Tables. Should only be called if MoreDataToWrite() returned true.
//
// WriteTable always closes the Writer.
func (r *Runner) WriteTable(objMeta objstorage.ObjectMetadata, tw sstable.RawWriter) {
	if r.err != nil {
		panic("error already encountered")
	}
	r.tables = append(r.tables, OutputTable{
		CreationTime: time.Now(),
		ObjMeta:      objMeta,
	})
	splitKey, err := r.writeKeysToTable(tw)
	err = errors.CombineErrors(err, tw.Close())
	if err != nil {
		r.err = err
		r.kv = nil
		return
	}
	writerMeta, err := tw.Metadata()
	if err != nil {
		r.err = err
		return
	}
	if err := r.validateWriterMeta(writerMeta, splitKey); err != nil {
		r.err = err
		return
	}
	r.tables[len(r.tables)-1].WriterMeta = *writerMeta
	r.stats.CumulativeWrittenSize += writerMeta.Size
}

func (r *Runner) writeKeysToTable(tw sstable.RawWriter) (splitKey []byte, _ error) {
	const updateGrantHandleEveryNKeys = 128
	firstKey := base.MinUserKey(r.cmp, spanStartOrNil(&r.lastRangeDelSpan), spanStartOrNil(&r.lastRangeKeySpan))
	if r.kv != nil && firstKey == nil {
		firstKey = r.kv.K.UserKey
	}
	if firstKey == nil {
		return nil, base.AssertionFailedf("no data to write")
	}
	splitter := NewOutputSplitter(
		r.cmp, firstKey, r.TableSplitLimit(firstKey),
		r.cfg.TargetOutputFileSize, r.cfg.Grandparents.Iter(), r.iter.Frontiers(),
	)
	equalPrev := func(k []byte) bool {
		return tw.ComparePrev(k) == 0
	}
	var pinnedKeySize, pinnedValueSize, pinnedCount uint64
	var iteratedKeys uint64
	kv := r.kv
	for ; kv != nil; kv = r.iter.Next() {
		iteratedKeys++
		if iteratedKeys%updateGrantHandleEveryNKeys == 0 {
			r.cfg.GrantHandle.CumulativeStats(base.CompactionGrantHandleStats{
				CumWriteBytes: r.stats.CumulativeWrittenSize + tw.EstimatedSize(),
			})
			// TODO(sumeer): give the GrantHandle to the writer so it can account on
			// all its goroutines.
			r.cfg.GrantHandle.MeasureCPU(0)
		}
		if splitter.ShouldSplitBefore(kv.K.UserKey, tw.EstimatedSize(), equalPrev) {
			break
		}

		switch kv.K.Kind() {
		case base.InternalKeyKindRangeDelete:
			// The previous span (if any) must end at or before this key, since the
			// spans we receive are non-overlapping.
			if err := tw.EncodeSpan(r.lastRangeDelSpan); r.err != nil {
				return nil, err
			}
			r.lastRangeDelSpan.CopyFrom(r.iter.Span())
			continue

		case base.InternalKeyKindRangeKeySet, base.InternalKeyKindRangeKeyUnset, base.InternalKeyKindRangeKeyDelete:
			// The previous span (if any) must end at or before this key, since the
			// spans we receive are non-overlapping.
			if err := tw.EncodeSpan(r.lastRangeKeySpan); err != nil {
				return nil, err
			}
			r.lastRangeKeySpan.CopyFrom(r.iter.Span())
			continue
		}
		v, _, err := kv.Value(nil)
		if err != nil {
			return nil, err
		}
		if err := tw.AddWithForceObsolete(kv.K, v, r.iter.ForceObsoleteDueToRangeDel()); err != nil {
			return nil, err
		}
		if r.iter.SnapshotPinned() {
			// The kv pair we just added to the sstable was only surfaced by
			// the compaction iterator because an open snapshot prevented
			// its elision. Increment the stats.
			pinnedCount++
			pinnedKeySize += uint64(len(kv.K.UserKey)) + base.InternalTrailerLen
			pinnedValueSize += uint64(len(v))
		}
	}
	r.kv = kv
	splitKey = splitter.SplitKey()
	if err := SplitAndEncodeSpan(r.cmp, &r.lastRangeDelSpan, splitKey, tw); err != nil {
		return nil, err
	}
	if err := SplitAndEncodeSpan(r.cmp, &r.lastRangeKeySpan, splitKey, tw); err != nil {
		return nil, err
	}
	// Set internal sstable properties.
	tw.SetSnapshotPinnedProperties(pinnedCount, pinnedKeySize, pinnedValueSize)
	r.stats.CumulativePinnedKeys += pinnedCount
	r.stats.CumulativePinnedSize += pinnedKeySize + pinnedValueSize
	r.cfg.GrantHandle.CumulativeStats(base.CompactionGrantHandleStats{
		CumWriteBytes: r.stats.CumulativeWrittenSize + tw.EstimatedSize(),
	})
	r.cfg.GrantHandle.MeasureCPU(0)
	return splitKey, nil
}

// Finish closes the compaction iterator and returns the result of the
// compaction.
func (r *Runner) Finish() Result {
	r.err = errors.CombineErrors(r.err, r.iter.Close())
	// The compaction iterator keeps track of a count of the number of DELSIZED
	// keys that encoded an incorrect size.
	r.stats.CountMissizedDels = r.iter.Stats().CountMissizedDels
	return Result{
		Err:    r.err,
		Tables: r.tables,
		Stats:  r.stats,
	}
}

// TableSplitLimit returns a hard split limit for an output table that starts at
// startKey (which must be strictly greater than startKey), or nil if there is
// no limit.
func (r *Runner) TableSplitLimit(startKey []byte) []byte {
	var limitKey []byte

	// Enforce the MaxGrandparentOverlapBytes limit: find the user key to which
	// that table can extend without excessively overlapping the grandparent
	// level. If no limit is needed considering the grandparent, limitKey stays
	// nil.
	//
	// This is done in order to prevent a table at level N from overlapping too
	// much data at level N+1. We want to avoid such large overlaps because they
	// translate into large compactions. The current heuristic stops output of a
	// table if the addition of another key would cause the table to overlap more
	// than 10x the target file size at level N. See
	// compaction.maxGrandparentOverlapBytes.
	iter := r.cfg.Grandparents.Iter()
	var overlappedBytes uint64
	f := iter.SeekGE(r.cmp, startKey)
	// Handle an overlapping table.
	if f != nil && r.cmp(f.Smallest.UserKey, startKey) <= 0 {
		overlappedBytes += f.Size
		f = iter.Next()
	}
	for ; f != nil; f = iter.Next() {
		overlappedBytes += f.Size
		if overlappedBytes > r.cfg.MaxGrandparentOverlapBytes {
			limitKey = f.Smallest.UserKey
			break
		}
	}

	if len(r.cfg.L0SplitKeys) != 0 {
		// Find the first split key that is greater than startKey.
		index := sort.Search(len(r.cfg.L0SplitKeys), func(i int) bool {
			return r.cmp(r.cfg.L0SplitKeys[i], startKey) > 0
		})
		if index < len(r.cfg.L0SplitKeys) {
			limitKey = base.MinUserKey(r.cmp, limitKey, r.cfg.L0SplitKeys[index])
		}
	}

	return limitKey
}

// validateWriterMeta runs some sanity cehcks on the WriterMetadata on an output
// table that was just finished. splitKey is the key where the table must have
// ended (or nil).
func (r *Runner) validateWriterMeta(meta *sstable.WriterMetadata, splitKey []byte) error {
	if !meta.HasPointKeys && !meta.HasRangeDelKeys && !meta.HasRangeKeys {
		return base.AssertionFailedf("output table has no keys")
	}

	var err error
	checkBounds := func(smallest, largest base.InternalKey, description string) {
		bounds := base.UserKeyBoundsFromInternal(smallest, largest)
		if !r.cfg.CompactionBounds.ContainsBounds(r.cmp, &bounds) {
			err = errors.CombineErrors(err, base.AssertionFailedf(
				"output table %s bounds %s extend beyond compaction bounds %s",
				description, bounds, r.cfg.CompactionBounds,
			))
		}
		if splitKey != nil && bounds.End.IsUpperBoundFor(r.cmp, splitKey) {
			err = errors.CombineErrors(err, base.AssertionFailedf(
				"output table %s bounds %s extend beyond split key %s",
				description, bounds, splitKey,
			))
		}
	}

	if meta.HasPointKeys {
		checkBounds(meta.SmallestPoint, meta.LargestPoint, "point key")
	}
	if meta.HasRangeDelKeys {
		checkBounds(meta.SmallestRangeDel, meta.LargestRangeDel, "range del")
	}
	if meta.HasRangeKeys {
		checkBounds(meta.SmallestRangeKey, meta.LargestRangeKey, "range key")
	}
	return err
}

func spanStartOrNil(s *keyspan.Span) []byte {
	if s.Empty() {
		return nil
	}
	return s.Start
}
