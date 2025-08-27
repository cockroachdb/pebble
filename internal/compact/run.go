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
	"github.com/cockroachdb/pebble/sstable/blob"
)

// Result stores the result of a compaction - more specifically, the "data" part
// where we use the compaction iterator to write output tables.
type Result struct {
	// Err is the result of the compaction. On failure, Err is set, Tables/Blobs
	// stores the tables/blobs created so far (and which need to be cleaned up).
	Err    error
	Tables []OutputTable
	Blobs  []OutputBlob
	Stats  Stats
}

// WithError returns a modified Result which has the Err field set.
func (r Result) WithError(err error) Result {
	return Result{
		Err:    errors.CombineErrors(r.Err, err),
		Tables: r.Tables,
		Blobs:  r.Blobs,
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
	// BlobReferences is the list of blob references for the table.
	BlobReferences manifest.BlobReferences
	// BlobReferenceDepth is the depth of the blob references for the table.
	BlobReferenceDepth manifest.BlobReferenceDepth
}

// OutputBlob contains metadata about a blob file that was created during a
// compaction.
type OutputBlob struct {
	Stats blob.FileWriterStats
	// ObjMeta is metadata for the object backing the blob file.
	ObjMeta objstorage.ObjectMetadata
	// Metadata is metadata for the blob file.
	Metadata *manifest.PhysicalBlobFile
}

// Stats describes stats collected during the compaction.
type Stats struct {
	CumulativePinnedKeys uint64
	CumulativePinnedSize uint64
	// CumulativeWrittenSize is the total size of all data written to output
	// objects.
	CumulativeWrittenSize uint64
	// CumulativeBlobReferenceSize is the total size of all blob references
	// written to output objects.
	CumulativeBlobReferenceSize uint64
	// CumulativeBlobFileSize is the total size of all data written to blob
	// output objects specifically.
	CumulativeBlobFileSize uint64
	CountMissizedDels      uint64
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

// ValueSeparationOverrideConfig is used to selectively override value
// separation for an individual compaction output.
type ValueSeparationOverrideConfig struct {
	// MinimumSize is the minimum size of a value that will be separated into a
	// blob file.
	MinimumSize int
}

// ValueSeparationOutputConfig is used to configure value separation for the
// next output sstable.
type ValueSeparationOutputConfig struct {
	// TieringSpanIDForNewColdBlobFile is only relevant when ValueSeparation is
	// permitted to create new cold blob files. It specifies the single
	// TieringSpanID that it is allowed to create cold blob files for.
	//
	// This is only necessary because we need to assign BlobReferenceIDs to blob
	// references as we write the sstable, and we need to bound the number of
	// new output blob files that can be created, so that we can pre-allocate
	// the BlobReferenceIDs for the new output blob files. This ensures that
	// reused references can have stable BlobReferenceIDs. An implication is
	// that we cannot create multiple cold blob files for this TieringSpanID
	// when writing the sstable. We could allow for multiple cold blob files (to
	// fatten the sstable), by reserving additional BlobReferenceIDs, with the
	// tradeoff that reserved slots may be unused, but occupy space in
	// FileMetadata.
	TieringSpanIDForNewColdBlobFile base.TieringSpanID
}

// ValueSeparation defines an interface for writing some values to separate blob
// files.
type ValueSeparation interface {
	// OverrideNextOutputConfig may be called when a compaction is starting a
	// new output sstable. It can be used to configure value separation
	// specifically for the next compaction output. If not called, the default
	// configuration that ValueSeparation was created with is used.
	OverrideNextOutputConfig(config ValueSeparationOverrideConfig)
	// StartOutput is called when a compaction is starting a new output sstable ...
	StartOutput(config ValueSeparationOutputConfig)
	// EstimatedFileSize returns an estimate of the disk space consumed by the
	// current, pending blob file if it were closed now. If no blob file has
	// been created, it returns 0.
	EstimatedFileSize() uint64
	// EstimatedHotReferenceSize returns an estimate of the disk space consumed
	// by the current output sstable's hot blob references so far.
	EstimatedHotReferenceSize() uint64
	// Add adds the provided key-value pair to the provided sstable writer,
	// possibly separating the value into a blob file.
	Add(tw sstable.RawWriter, kv *base.InternalKV, forceObsolete bool) error
	// FinishOutput is called when a compaction is finishing an output sstable.
	// It returns the table's blob references, which will be added to the
	// table's TableMetadata, and stats and metadata describing a newly
	// constructed blob file if any.
	FinishOutput() (ValueSeparationMetadata, error)
}

// ValueSeparationMetadata describes metadata about a table's blob references,
// and optionally a newly constructed blob file.
type ValueSeparationMetadata struct {
	BlobReferences     manifest.BlobReferences
	BlobReferenceSize  uint64
	BlobReferenceDepth manifest.BlobReferenceDepth
	NewBlobFiles       []NewBlobFileInfo
}

type NewBlobFileInfo struct {
	FileStats    blob.FileWriterStats
	FileObject   objstorage.ObjectMetadata
	FileMetadata *manifest.PhysicalBlobFile
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
	blobs  []OutputBlob
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

// FirstKey returns the first key that will be written; this can be a point key
// or the beginning of a range del or range key span.
//
// FirstKey can only be called right after MoreDataToWrite() was called and
// returned true.
func (r *Runner) FirstKey() []byte {
	firstKey := base.MinUserKey(r.cmp, spanStartOrNil(&r.lastRangeDelSpan), spanStartOrNil(&r.lastRangeKeySpan))
	// Note: if there was a r.lastRangeDelSpan or r.lastRangeKeySpan, it
	// necessarily starts before the first point key.
	if r.kv != nil && firstKey == nil {
		firstKey = r.kv.K.UserKey
	}
	return firstKey
}

// WriteTable writes a new output table. This table will be part of
// Result.Tables. Should only be called if MoreDataToWrite() returned true.
//
// limitKey (if non-empty) forces the sstable to be finished before reaching
// this key.
//
// WriteTable always closes the Writer.
func (r *Runner) WriteTable(
	objMeta objstorage.ObjectMetadata,
	tw sstable.RawWriter,
	limitKey []byte,
	valueSeparation ValueSeparation,
	tieringPolicy base.TieringPolicyAndExtractor,
) {
	if r.err != nil {
		panic("error already encountered")
	}
	r.tables = append(r.tables, OutputTable{
		CreationTime: time.Now(),
		ObjMeta:      objMeta,
	})
	splitKey, err := r.writeKeysToTable(tw, limitKey, valueSeparation, tieringPolicy)

	// Inform the value separation policy that the table is finished.
	valSepMeta, valSepErr := valueSeparation.FinishOutput()
	var newBlobsSize uint64
	if valSepErr != nil {
		r.err = errors.CombineErrors(r.err, valSepErr)
	} else {
		r.tables[len(r.tables)-1].BlobReferences = valSepMeta.BlobReferences
		r.tables[len(r.tables)-1].BlobReferenceDepth = valSepMeta.BlobReferenceDepth
		for _, nb := range valSepMeta.NewBlobFiles {
			r.blobs = append(r.blobs, OutputBlob{
				Stats:    nb.FileStats,
				ObjMeta:  nb.FileObject,
				Metadata: nb.FileMetadata,
			})
			newBlobsSize += nb.FileStats.FileLen
		}
	}

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
	r.stats.CumulativeWrittenSize += writerMeta.Size + newBlobsSize
	r.stats.CumulativeBlobReferenceSize += valSepMeta.BlobReferenceSize
	r.stats.CumulativeBlobFileSize += newBlobsSize
}

func (r *Runner) writeKeysToTable(
	tw sstable.RawWriter,
	limitKey []byte,
	valueSeparation ValueSeparation,
	tieringPolicy base.TieringPolicyAndExtractor,
) (splitKey []byte, _ error) {
	const updateGrantHandleEveryNKeys = 128
	firstKey := r.FirstKey()
	if firstKey == nil {
		return nil, base.AssertionFailedf("no data to write")
	}
	limitKey = base.MinUserKey(r.cmp, limitKey, r.TableSplitLimit(firstKey))
	splitter := NewOutputSplitter(
		r.cmp, firstKey, limitKey,
		r.cfg.TargetOutputFileSize, r.cfg.Grandparents.Iter(), r.iter.Frontiers(),
	)
	equalPrev := func(k []byte) bool {
		return tw.ComparePrev(k) == 0
	}
	var pinnedKeySize, pinnedValueSize, pinnedCount uint64
	var iteratedKeys uint64
	kv := r.kv
	var tieringSpanID base.TieringSpanID
	if tieringPolicy != nil {
		tieringSpanID = tieringPolicy.Policy().SpanID
	}
	for ; kv != nil; kv = r.iter.Next() {
		iteratedKeys++
		if iteratedKeys%updateGrantHandleEveryNKeys == 0 {
			r.cfg.GrantHandle.CumulativeStats(base.CompactionGrantHandleStats{
				CumWriteBytes: r.stats.CumulativeWrittenSize + tw.EstimatedSize() +
					valueSeparation.EstimatedFileSize(),
			})
			r.cfg.GrantHandle.MeasureCPU(base.CompactionGoroutinePrimary)
		}
		outputSize := tw.EstimatedSize()
		// NB: only hot references are added to sstable size when deciding to
		// split since we don't have to worry about huge compactions rewriting
		// cold blob files, since they are never rewritten as part of sstable
		// compactions.
		outputSize += valueSeparation.EstimatedHotReferenceSize()
		if splitter.ShouldSplitBefore(kv.K.UserKey, outputSize, equalPrev) {
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

		case base.InternalKeyKindSet, base.InternalKeyKindSetWithDelete:
			if tieringSpanID != 0 && kv.M.Tiering == (base.TieringMeta{}) {
				// Try to extract the TieringMeta.
				val, _, err := kv.V.Value(nil)
				if err != nil {
					return nil, err
				}
				attr, err := tieringPolicy.ExtractAttribute(kv.K.UserKey, val)
				if err != nil {
					// TODO(sumeer): log warning periodically.
					attr = 0
				}
				kv.M.Tiering = base.TieringMeta{SpanID: tieringSpanID, Attribute: attr}
			}
		}

		valueLen := kv.V.Len()
		// Add the value to the sstable, possibly separating its value into a
		// blob file. The ValueSeparation implementation is responsible for
		// writing the KV to the sstable.
		if err := valueSeparation.Add(tw, kv, r.iter.ForceObsoleteDueToRangeDel()); err != nil {
			return nil, err
		}
		if r.iter.SnapshotPinned() {
			// The kv pair we just added to the sstable was only surfaced by
			// the compaction iterator because an open snapshot prevented
			// its elision. Increment the stats.
			pinnedCount++
			pinnedKeySize += uint64(len(kv.K.UserKey)) + base.InternalTrailerLen
			pinnedValueSize += uint64(valueLen)
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

	// TODO(jackson): CumulativeStats may block if the compaction scheduler
	// wants to pace the compaction. We should thread through knowledge of
	// whether or not this is the final sstable of the compaction, in which case
	// all work has been completed and pacing would only needlessly delay the
	// installation of the version edit.
	r.cfg.GrantHandle.CumulativeStats(base.CompactionGrantHandleStats{
		CumWriteBytes: r.stats.CumulativeWrittenSize +
			tw.EstimatedSize() +
			valueSeparation.EstimatedFileSize(),
	})
	r.cfg.GrantHandle.MeasureCPU(base.CompactionGoroutinePrimary)
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
		Blobs:  r.blobs,
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
	if f != nil && r.cmp(f.Smallest().UserKey, startKey) <= 0 {
		overlappedBytes += f.Size
		f = iter.Next()
	}
	for ; f != nil; f = iter.Next() {
		overlappedBytes += f.Size
		if overlappedBytes > r.cfg.MaxGrandparentOverlapBytes {
			limitKey = f.Smallest().UserKey
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

// NeverSeparateValues is a ValueSeparation implementation that never separates
// values into external blob files. It is the default value if no
// ValueSeparation implementation is explicitly provided.
type NeverSeparateValues struct{}

// Assert that NeverSeparateValues implements the ValueSeparation interface.
var _ ValueSeparation = NeverSeparateValues{}

// OverrideNextOutputConfig implements the ValueSeparation interface.
func (NeverSeparateValues) OverrideNextOutputConfig(config ValueSeparationOverrideConfig) {}

// StartOutput implements the ValueSeparation interface.
func (NeverSeparateValues) StartOutput(config ValueSeparationOutputConfig) {}

// EstimatedFileSize implements the ValueSeparation interface.
func (NeverSeparateValues) EstimatedFileSize() uint64 { return 0 }

// EstimatedHotReferenceSize implements the ValueSeparation interface.
func (NeverSeparateValues) EstimatedHotReferenceSize() uint64 { return 0 }

// Add implements the ValueSeparation interface.
func (NeverSeparateValues) Add(
	tw sstable.RawWriter, kv *base.InternalKV, forceObsolete bool,
) error {
	v, _, err := kv.Value(nil)
	if err != nil {
		return err
	}
	return tw.Add(kv.K, v, forceObsolete, kv.M)
}

// FinishOutput implements the ValueSeparation interface.
func (NeverSeparateValues) FinishOutput() (ValueSeparationMetadata, error) {
	return ValueSeparationMetadata{}, nil
}
