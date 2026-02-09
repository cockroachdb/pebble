// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/crlib/crmath"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/keyspan/keyspanimpl"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/tombspan"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/redact"
)

// In-memory statistics about tables help inform compaction picking, but may
// be expensive to calculate or load from disk. Every time a database is
// opened, these statistics must be reloaded or recalculated. To minimize
// impact on user activity and compactions, we load these statistics
// asynchronously in the background and store loaded statistics in each
// table's *TableMetadata.
//
// This file implements the asynchronous loading of statistics by maintaining
// a list of files that require statistics, alongside their LSM levels.
// Whenever new files are added to the LSM, the files are appended to
// d.mu.tableStats.pending. If a stats collection job is not currently
// running, one is started in a separate goroutine.
//
// The stats collection job grabs and clears the pending list, computes table
// statistics relative to the current readState and updates the tables' file
// metadata. New pending files may accumulate during a stats collection job,
// so a completing job triggers a new job if necessary. Only one job runs at a
// time.
//
// When an existing database is opened, all files lack in-memory statistics.
// These files' stats are loaded incrementally whenever the pending list is
// empty by scanning a current readState for files missing statistics. Once a
// job completes a scan without finding any remaining files without
// statistics, it flips a `loadedInitial` flag. From then on, the stats
// collection job only needs to load statistics for new files appended to the
// pending list.

func (d *DB) maybeCollectTableStatsLocked() {
	if d.shouldCollectTableStatsLocked() {
		go d.collectTableStats()
	}
}

// updateTableStatsLocked is called when new files are introduced, after the
// read state has been updated. It may trigger a new stat collection.
// DB.mu must be locked when calling.
func (d *DB) updateTableStatsLocked(newTables []manifest.NewTableEntry) {
	var needStats bool
	for _, nf := range newTables {
		if _, statsValid := nf.Meta.Stats(); !statsValid {
			needStats = true
			break
		}
	}
	if !needStats {
		return
	}

	d.mu.tableStats.pending = append(d.mu.tableStats.pending, newTables...)
	d.maybeCollectTableStatsLocked()
}

func (d *DB) shouldCollectTableStatsLocked() bool {
	return !d.mu.tableStats.loading &&
		d.closed.Load() == nil &&
		!d.opts.DisableTableStats &&
		(len(d.mu.tableStats.pending) > 0 || !d.mu.tableStats.loadedInitial)
}

// collectTableStats runs a table stats collection job, returning true if the
// invocation did the collection work, false otherwise (e.g. if another job was
// already running).
func (d *DB) collectTableStats() bool {
	const maxTableStatsPerScan = 50

	d.mu.Lock()
	if !d.shouldCollectTableStatsLocked() {
		d.mu.Unlock()
		return false
	}
	ctx := context.Background()

	pending := d.mu.tableStats.pending
	d.mu.tableStats.pending = nil
	d.mu.tableStats.loading = true
	jobID := d.newJobIDLocked()
	// Drop DB.mu before performing IO.
	d.mu.Unlock()

	// Every run of collectTableStats either collects stats from the pending
	// list (if non-empty) or from scanning the version (loadedInitial is
	// false). This job only runs if at least one of those conditions holds.

	// Grab a read state to scan for tables.
	rs := d.loadReadState()
	var collected []collectedStats
	var wideTombstones []tombspan.WideTombstone
	initialLoadCompleted := false
	if len(pending) > 0 {
		collected, wideTombstones = d.loadNewFileStats(ctx, rs, pending)
	} else {
		var moreRemain bool
		var buf [maxTableStatsPerScan]collectedStats
		collected, wideTombstones, moreRemain = d.scanReadStateTableStats(ctx, rs.current, buf[:0])
		if !moreRemain {
			// Once we're done with table stats, load blob file properties.
			moreRemain = d.scanBlobFileProperties(ctx, rs.current, maxTableStatsPerScan-len(collected))
			if !moreRemain {
				initialLoadCompleted = true
			}
		}
	}
	rs.unref()

	// Update the TableMetadata with the loaded stats while holding d.mu.
	d.mu.Lock()
	defer d.mu.Unlock()
	d.mu.tableStats.loading = false
	if initialLoadCompleted && !d.mu.tableStats.loadedInitial {
		d.mu.tableStats.loadedInitial = true
		d.opts.EventListener.TableStatsLoaded(TableStatsInfo{
			JobID: int(jobID),
		})
	}

	maybeCompact := false
	for _, c := range collected {
		sanityCheckStats(c.TableMetadata, &c.TableStats, d.opts.Logger, "collected stats")
		c.TableMetadata.PopulateStats(&c.TableStats)
		maybeCompact = maybeCompact || tableTombstoneCompensation(c.TableMetadata) > 0
	}

	d.mu.tableStats.cond.Broadcast()
	d.maybeCollectTableStatsLocked()
	if !d.opts.private.disableDeleteOnlyCompactions {
		d.mu.compact.wideTombstones.AddTombstones(wideTombstones...)
		d.mu.compact.wideTombstones.UpdateWithEarliestSnapshot(d.mu.snapshots.earliest())
	}
	if maybeCompact {
		d.maybeScheduleCompaction()
	}
	return true
}

type collectedStats struct {
	*manifest.TableMetadata
	manifest.TableStats
}

func (d *DB) loadNewFileStats(
	ctx context.Context, rs *readState, pending []manifest.NewTableEntry,
) (collected []collectedStats, wideTombstones []tombspan.WideTombstone) {
	collected = make([]collectedStats, 0, len(pending))
	for _, nf := range pending {
		if err := d.closed.Load(); err != nil {
			// Returning an incomplete result is okay since we're closing so it likely
			// is being ignroed anyway, or, at worst, we'd just have some incomplete
			// stats while we close.
			break
		}
		// A file's stats might have been populated by an earlier call to
		// loadNewFileStats if the file was moved.
		// NB: Only collectTableStats updates f.Stats for active files, and we
		// ensure only one goroutine runs it at a time through
		// d.mu.tableStats.loading.
		if _, ok := nf.Meta.Stats(); ok {
			continue
		}

		// The file isn't guaranteed to still be live in the readState's
		// version. It may have been deleted or moved. Skip it if it's not in
		// the expected level.
		if !rs.current.Contains(nf.Level, nf.Meta) {
			continue
		}

		stats, newWideTombstones, err := d.loadTableStats(ctx, rs.current, nf.Level, nf.Meta)
		if err != nil {
			d.opts.EventListener.BackgroundError(err)
			continue
		}
		// NB: We don't update the TableMetadata yet, because we aren't holding
		// DB.mu. We'll copy it to the TableMetadata after we're finished with
		// IO.
		collected = append(collected, collectedStats{
			TableMetadata: nf.Meta,
			TableStats:    stats,
		})
		wideTombstones = append(wideTombstones, newWideTombstones...)
	}
	return collected, wideTombstones
}

// scanReadStateTableStats is run by an active stat collection job when there
// are no pending new files, but there might be files that existed at Open for
// which we haven't loaded table stats.
func (d *DB) scanReadStateTableStats(
	ctx context.Context, version *manifest.Version, fill []collectedStats,
) (_ []collectedStats, _ []tombspan.WideTombstone, moreRemain bool) {
	var wideTombstones []tombspan.WideTombstone
	sizesChecked := make(map[base.DiskFileNum]struct{})
	// TODO(radu): an O(#tables) scan every time could be problematic.
	for l, levelMetadata := range version.Levels {
		for f := range levelMetadata.All() {
			// NB: Only the active stats collection job updates f.Stats for active
			// files, and we ensure only one goroutine runs it at a time through
			// d.mu.tableStats.loading.
			if _, ok := f.Stats(); ok {
				continue
			}

			// Limit how much work we do per read state. The older the read
			// state is, the higher the likelihood files are no longer being
			// used in the current version. If we've exhausted our allowance,
			// return true for the last return value to signal there's more
			// work to do.
			if len(fill) == cap(fill) {
				return fill, wideTombstones, true
			}

			// If the file is remote and not SharedForeign, we should check if its size
			// matches. This is because checkConsistency skips over remote files.
			//
			// SharedForeign and External files are skipped as their sizes are allowed
			// to have a mismatch; the size stored in the TableBacking is just the part
			// of the file that is referenced by this Pebble instance, not the size of
			// the whole object.
			objMeta, err := d.objProvider.Lookup(base.FileTypeTable, f.TableBacking.DiskFileNum)
			if err != nil {
				// Set `moreRemain` so we'll try again.
				moreRemain = true
				d.opts.EventListener.BackgroundError(err)
				continue
			}

			shouldCheckSize := objMeta.IsRemote() &&
				!d.objProvider.IsSharedForeign(objMeta) &&
				!objMeta.IsExternal()
			if _, ok := sizesChecked[f.TableBacking.DiskFileNum]; !ok && shouldCheckSize {
				size, err := d.objProvider.Size(objMeta)
				fileSize := f.TableBacking.Size
				if err != nil {
					moreRemain = true
					d.opts.EventListener.BackgroundError(err)
					continue
				}
				if size != int64(fileSize) {
					err := errors.Errorf(
						"during consistency check in loadTableStats: L%d: %s: object size mismatch (%s): %d (provider) != %d (MANIFEST)",
						errors.Safe(l), f.TableNum, d.objProvider.Path(objMeta),
						errors.Safe(size), errors.Safe(fileSize))
					d.opts.EventListener.BackgroundError(err)
					d.opts.Logger.Fatalf("%s", err)
				}

				sizesChecked[f.TableBacking.DiskFileNum] = struct{}{}
			}

			stats, newWideTombstones, err := d.loadTableStats(ctx, version, l, f)
			if err != nil {
				// Set `moreRemain` so we'll try again.
				moreRemain = true
				d.opts.EventListener.BackgroundError(err)
				continue
			}
			fill = append(fill, collectedStats{
				TableMetadata: f,
				TableStats:    stats,
			})
			wideTombstones = append(wideTombstones, newWideTombstones...)
		}
	}
	return fill, wideTombstones, moreRemain
}

// populateBlobFileProperties reads at most maxNum blob file properties for blob
// files that don't have them populated. Returns false once all properties have
// been populated.
func (d *DB) scanBlobFileProperties(
	ctx context.Context, version *manifest.Version, maxNum int,
) (moreRemain bool) {
	// TODO(radu): an O(#files) scan every time could be problematic.
	// We could remember the last blob file ID and scan from there.
	for f := range version.BlobFiles.All() {
		if _, propsValid := f.Physical.Properties(); propsValid {
			// Properties are already populated.
			continue
		}
		if maxNum == 0 {
			// We've reached the limit for this scan, and there are more files
			// remaining.
			return true
		}
		maxNum--
		v, err := d.fileCache.findOrCreateBlob(ctx, f.Physical, block.InitFileReadStats{})
		if err != nil {
			// Set moreRemain so we'll try again.
			moreRemain = true
			continue
		}
		blobReader := v.Value().mustBlob()
		blobProps, err := blobReader.ReadProperties(ctx)
		v.Unref()
		if err != nil {
			// Set moreRemain so we'll try again.
			moreRemain = true
			continue
		}
		// It is ok to call PopulateProperties here because this function runs as
		// part of a table statistics job, and at most one goroutine runs this at a
		// time (see d.mu.tableStats.loading).
		f.Physical.PopulateProperties(&blobProps)
	}
	return moreRemain
}

func (d *DB) loadTableStats(
	ctx context.Context, v *manifest.Version, level int, meta *manifest.TableMetadata,
) (manifest.TableStats, []tombspan.WideTombstone, error) {
	var wideTombstones []tombspan.WideTombstone
	var rangeDeletionsBytesEstimate uint64

	backingProps, backingPropsOk := meta.TableBacking.Properties()

	blockReadEnv := block.ReadEnv{
		Level: base.MakeLevel(level),
	}
	// If the stats are already available (always the case other than after
	// initial startup), and there are no range deletions or range key deletions,
	// we avoid opening the table.
	if !backingPropsOk || backingProps.NumRangeDeletions > 0 || backingProps.NumRangeKeyDels > 0 {
		err := d.fileCache.withReader(
			ctx, blockReadEnv, meta, func(r *sstable.Reader, env sstable.ReadEnv) (err error) {
				if !backingPropsOk {
					loadedProps, err := r.ReadPropertiesBlock(ctx, nil /* buffer pool */)
					if err != nil {
						return err
					}
					backingProps = meta.TableBacking.PopulateProperties(&loadedProps)
				}
				if backingProps.NumRangeDeletions > 0 || backingProps.NumRangeKeyDels > 0 {
					wideTombstones, rangeDeletionsBytesEstimate, err = d.loadTableRangeDelStats(ctx, r, v, level, meta, env)
					if err != nil {
						return err
					}
				}
				return err
			})
		if err != nil {
			return manifest.TableStats{}, nil, err
		}
	}

	var stats manifest.TableStats
	stats.RangeDeletionsBytesEstimate = rangeDeletionsBytesEstimate

	if backingProps.NumPointDeletions() > 0 {
		var err error
		stats.PointDeletionsBytesEstimate, err = d.loadTablePointKeyStats(ctx, backingProps, v, level, meta)
		if err != nil {
			return stats, nil, err
		}
	}
	return stats, wideTombstones, nil
}

// loadTablePointKeyStats calculates the point key statistics for the given
// table.
//
// The backing props are scaled as necessary if the table is virtual.
func (d *DB) loadTablePointKeyStats(
	ctx context.Context,
	props *manifest.TableBackingProperties,
	v *manifest.Version,
	level int,
	meta *manifest.TableMetadata,
) (pointDeletionsBytes uint64, _ error) {
	// TODO(jackson): If the file has a wide keyspace, the average
	// value size beneath the entire file might not be representative
	// of the size of the keys beneath the point tombstones.
	// We could write the ranges of 'clusters' of point tombstones to
	// a sstable property and call averageValueSizeBeneath for each of
	// these narrower ranges to improve the estimate.
	avgValLogicalSize, compressionRatio, err := d.estimateSizesBeneath(ctx, v, level, meta, props)
	if err != nil {
		return 0, err
	}
	pointDeletionsBytes = pointDeletionsBytesEstimate(props, avgValLogicalSize, compressionRatio)
	pointDeletionsBytes = meta.ScaleStatistic(pointDeletionsBytes)
	return pointDeletionsBytes, nil
}

// loadTableRangeDelStats calculates the range deletion and range key deletion
// statistics for the given table. It also collects wide tombstones if it finds
// any eligible sstables.
func (d *DB) loadTableRangeDelStats(
	ctx context.Context,
	r *sstable.Reader,
	v *manifest.Version,
	level int,
	meta *manifest.TableMetadata,
	env sstable.ReadEnv,
) (_ []tombspan.WideTombstone, rangeDeletionsBytesEstimate uint64, _ error) {
	iter, err := newCombinedDeletionKeyspanIter(ctx, d.opts.Comparer, r, meta, env)
	if err != nil {
		return nil, 0, err
	}
	defer iter.Close()
	var wideTombstones []tombspan.WideTombstone
	// We iterate over the defragmented range tombstones and range key deletions,
	// which ensures we don't double count ranges deleted at different sequence
	// numbers. Also, merging abutting tombstones reduces the number of calls to
	// estimateReclaimedSizeBeneath which is costly, and improves the accuracy of
	// our overall estimate.
	s, err := iter.First()
	for ; s != nil; s, err = iter.Next() {
		// If the table is in the last level of the LSM, there is no data
		// beneath it. The fact that there is still a range tombstone in a
		// bottommost table indicates two possibilites:
		//   1. an open snapshot kept the tombstone around, and the data the
		//      tombstone deletes is contained within the table itself.
		//   2. the table was ingested.
		// In the first case, we'd like to estimate disk usage within the table
		// itself since compacting the table will drop that covered data. In the
		// second case, we expect that compacting the table will NOT drop any
		// data and rewriting the table is a waste of write bandwidth. We can
		// distinguish these cases by looking at the table metadata's sequence
		// numbers. A table's range deletions can only delete data within the
		// table at lower sequence numbers. All keys in an ingested sstable
		// adopt the same sequence number, preventing tombstones from deleting
		// keys within the same table. We check here if the largest RANGEDEL
		// sequence number is greater than the table's smallest sequence number.
		// If it is, the RANGEDEL could conceivably (although inconclusively)
		// delete data within the same table.
		//
		// Note that this heuristic is imperfect. If a table containing a range
		// deletion is ingested into L5 and subsequently compacted into L6 but
		// an open snapshot prevents elision of covered keys in L6, the
		// resulting RangeDeletionsBytesEstimate will incorrectly include all
		// covered keys.
		//
		// TODO(jackson): We could prevent the above error in the heuristic by
		// computing the table's RangeDeletionsBytesEstimate during the
		// compaction itself. It's unclear how common this is.
		//
		// NOTE: If the span `s` wholly contains a table containing range keys,
		// the returned size estimate will be slightly inflated by the range key
		// block. However, in practice, range keys are expected to be rare, and
		// the size of the range key block relative to the overall size of the
		// table is expected to be small.
		if level == numLevels-1 {
			var maxRangeDeleteSeqNum base.SeqNum
			for _, k := range s.Keys {
				if k.Kind() == base.InternalKeyKindRangeDelete && maxRangeDeleteSeqNum < k.SeqNum() {
					maxRangeDeleteSeqNum = k.SeqNum()
					break
				}
			}
			if meta.SeqNums.Low < maxRangeDeleteSeqNum {
				size, err := estimateDiskUsageInTableAndBlobReferences(r, s.Start, s.End, env, meta)
				if err != nil {
					return nil, 0, err
				}
				rangeDeletionsBytesEstimate += size
				// As the table is in the bottommost level, there is no need to
				// collect a wide tombstone.
				continue
			}
		}

		// While the size estimates for point keys should only be updated if
		// this span contains a range del, the sequence numbers are required for
		// the wide tombstone. Unconditionally descend, but conditionally update
		// the estimates.
		tombstoneKeyType := tombstoneKeyTypeFromKeys(s.Keys)
		tombBounds := base.UserKeyBoundsEndExclusive(s.Start, s.End)
		estimate, deletionCandidates, err := d.examineTablesBeneathTombstones(
			ctx, v, level, tombBounds, tombstoneKeyType)
		if err != nil {
			return nil, 0, err
		}
		rangeDeletionsBytesEstimate += estimate

		// NB: deletionCandidates is the number of tables that are determined
		// eligible for delete-only compactions based on the tombstones within
		// this sstable alone. It's not strictly guaranteed to uncover all files
		// that could be deleted due to tombstone fragmentation. See
		// newCombinedDeletionKeyspanIter for details on the defragmentation performed
		// while looking for deletion candidates.
		if deletionCandidates > 0 {
			wideTombstones = append(wideTombstones, tombspan.WideTombstone{
				PointSeqNums: seqNumRangeOfKind(s.Keys, base.InternalKeyKindRangeDelete),
				RangeSeqNums: seqNumRangeOfKind(s.Keys, base.InternalKeyKindRangeKeyDelete),
				Bounds:       tombBounds.Clone(),
				Level:        level,
				Table:        meta,
			})
		}
	}
	if err != nil {
		return nil, 0, err
	}
	return wideTombstones, rangeDeletionsBytesEstimate, nil
}

func seqNumRangeOfKind(keys []keyspan.Key, kind base.InternalKeyKind) base.SeqNumRange {
	var seqnums base.SeqNumRange
	for _, k := range keys {
		if k.Kind() != kind {
			continue
		}
		seq := k.SeqNum()
		if seqnums.High == 0 {
			seqnums.High, seqnums.Low = seq, seq
			continue
		}
		seqnums.Low = min(seqnums.Low, seq)
		seqnums.High = max(seqnums.High, seq)
	}
	return seqnums
}

// estimateSizesBeneath calculates two statistics describing the data in the LSM
// below the provided table metadata:
//
//  1. The average logical size of values: This is a precompression sum of
//     non-tombstone values. It's helpful for estimating how much data a DEL
//     might delete.
//  2. The compression ratio of the data beneath the table.
//
// estimateSizesBeneath walks the LSM table metadata for all tables beneath meta
// (plus the table itself), computing the above statistics.
func (d *DB) estimateSizesBeneath(
	ctx context.Context,
	v *manifest.Version,
	level int,
	meta *manifest.TableMetadata,
	fileProps *manifest.TableBackingProperties,
) (avgValueLogicalSize, compressionRatio float64, err error) {
	// Find all files in lower levels that overlap with meta,
	// summing their value sizes and entry counts.

	// Include the file itself. This is important because in some instances, the
	// computed compression ratio is applied to the tombstones contained within
	// `meta` itself. If there are no files beneath `meta` in the LSM, we would
	// calculate a compression ratio of 0 which is not accurate for the file's
	// own tombstones.
	var (
		fileSum       = meta.Size + meta.EstimatedReferenceSize()
		entryCount    = fileProps.NumEntries
		deletionCount = fileProps.NumDeletions
		keySum        = fileProps.RawKeySize
		valSum        = fileProps.RawValueSize
	)

	for l := level + 1; l < numLevels; l++ {
		for tableBeneath := range v.Overlaps(l, meta.UserKeyBounds()).All() {
			fileSum += tableBeneath.Size + tableBeneath.EstimatedReferenceSize()

			backingProps, ok := tableBeneath.TableBacking.Properties()
			if !ok {
				// If properties aren't available, we need to read the properties block.
				err := d.fileCache.withReader(ctx, block.NoReadEnv, tableBeneath, func(v *sstable.Reader, _ sstable.ReadEnv) (err error) {
					loadedProps, err := v.ReadPropertiesBlock(ctx, nil /* buffer pool */)
					if err != nil {
						return err
					}
					// It is ok to call PopulateProperties here because this function runs as part of
					// a table statistics job, and at most one goroutine runs this at a
					// time (see d.mu.tableStats.loading).
					backingProps = tableBeneath.TableBacking.PopulateProperties(&loadedProps)
					return nil
				})
				if err != nil {
					return 0, 0, err
				}
			}

			entryCount += tableBeneath.ScaleStatistic(backingProps.NumEntries)
			deletionCount += tableBeneath.ScaleStatistic(backingProps.NumDeletions)
			keySum += tableBeneath.ScaleStatistic(backingProps.RawKeySize)
			valSum += tableBeneath.ScaleStatistic(backingProps.RawValueSize)
			continue
		}
	}
	if entryCount == 0 {
		return 0, 0, nil
	}
	// RawKeySize and RawValueSize are uncompressed totals. We'll need to scale
	// the value sum according to the data size to account for compression,
	// index blocks and metadata overhead. Eg:
	//
	//    Compression rate        ×  Average uncompressed value size
	//
	//                            ↓
	//
	//         FileSize                    RawValueSize
	//   -----------------------  ×  -------------------------
	//   RawKeySize+RawValueSize     NumEntries - NumDeletions
	//
	// We return the average logical value size plus the compression ratio,
	// leaving the scaling to the caller. This allows the caller to perform
	// additional compression ratio scaling if necessary.
	uncompressedSum := float64(keySum + valSum)
	compressionRatio = float64(fileSum) / uncompressedSum
	if compressionRatio > 1 {
		// We can get huge compression ratios due to the fixed overhead of files
		// containing a tiny amount of data. By setting this to 1, we are ignoring
		// that overhead, but we accept that tradeoff since the total bytes in
		// such overhead is not large.
		compressionRatio = 1
	}
	// When calculating the average value size, we subtract the number of
	// deletions from the total number of entries.
	avgValueLogicalSize = float64(valSum) / float64(max(1, invariants.SafeSub(entryCount, deletionCount)))
	return avgValueLogicalSize, compressionRatio, nil
}

// examineTablesBeneathTombstones descends the LSM, visting sstables within the
// bounds of a set of tombstones. It does two things: determine the number of
// tables that are eligible for delete-only compactions, and estimates the
// amount of data that can be reclaimed.
//
// The type of tombstones is indicated by tombType, with RangeKeyOnly indicating
// a RANGEKEYDEL tombstone, PointKeyOnly indicating a RANGEDEL tombstone, and
// PointAndRangeKey indicating the presence of both with the same bounds.
//
// In order for a table to be eligible for a delete-only compaction, it must
// meet certain requirements with respect to the kinds of keys contained and the
// bounds of the table relative to the tombstones. If a table falls wholly
// within the provided bounds and only contains keys deleted by the tombstone
// (as indicated by tombType), it could be dropped outright by a delete-only
// compaction. Additionally, if the tombstone bounds overlap either the left or
// right boundary of the table, a delete-only compaction could excise the table,
// shortening its boundary to exclude the part of its keyspace that overlaps the
// tombstone. Both of these cases are considered deletion candidates.
func (d *DB) examineTablesBeneathTombstones(
	ctx context.Context,
	v *manifest.Version,
	level int,
	tombBounds base.UserKeyBounds,
	tombType manifest.KeyType,
) (estimate uint64, deletionCandidates int, err error) {
	// Find all tables in lower levels that overlap with tombBounds.
	//
	// An overlapping table might be completely contained by the tombstone(s),
	// in which case we can count the entire table size in our estimate without
	// doing any additional I/O. Otherwise, estimating the amount of data within
	// the table that falls within the tombstone bounds requires additional I/O
	// to read the table's index blocks.
	//
	// TODO(jackson): When there are multiple sub-levels in L0 and the RANGEDEL
	// is from a higher sub-level, we incorrectly skip the tables in the lower
	// sub-levels when estimating this overlap.
	for l := level + 1; l < numLevels; l++ {
		for tbl := range v.Overlaps(l, tombBounds).All() {
			// Determine whether we need to update size estimates and
			// WideTombstone seqnums based on the type of tombstone and the type
			// of keys in this file.
			var updateEstimates, updateWideTombstones bool
			switch tombType {
			case manifest.KeyTypePoint:
				// The range deletion byte estimates should only be updated if this
				// table contains point keys. This ends up being an overestimate in
				// the case that table also has range keys, but such keys are expected
				// to contribute a negligible amount of the table's overall size,
				// relative to point keys.
				if tbl.HasPointKeys {
					updateEstimates = true
				}
				// As the initiating span contained only range dels, wide
				// tombstone state can only be updated if this table does _not_
				// contain range keys.
				if !tbl.HasRangeKeys {
					updateWideTombstones = true
				}
			case manifest.KeyTypeRange:
				// The initiating span contained only range key dels. The estimates
				// apply only to point keys, and are therefore not updated.
				updateEstimates = false
				// As the initiating span contained only range key dels, wide
				// tombstones can only be updated if this table does _not_
				// contain point keys.
				if !tbl.HasPointKeys {
					updateWideTombstones = true
				}
			case manifest.KeyTypePointAndRange:
				// Always update the estimates and wide tombstones, as
				// tombstones deleting both points and ranges can drop a table,
				// irrespective of the table's mixture of keys. Similar to
				// above, the range del bytes estimates is an overestimate.
				updateEstimates, updateWideTombstones = true, true
			default:
				panic(errors.AssertionFailedf("pebble: unknown tombstone type %s", tombType))
			}

			if tombBounds.ContainsBounds(d.cmp, tbl.UserKeyBounds()) {
				// The range fully contains the file, so skip looking it up in
				// table cache/looking at its indexes and add the full file
				// size.
				if updateEstimates {
					estimate += tbl.Size + tbl.EstimatedReferenceSize()
				}
				if updateWideTombstones {
					deletionCandidates++
				}
				continue
			}
			// Partial overlap.
			if tombType == manifest.KeyTypeRange {
				// If the tombstone can only delete range keys, there is no need
				// to calculate disk usage, as the reclaimable space is expected
				// to be minimal relative to point keys and we have no way of
				// accurately calculating it.
				continue
			}
			var size uint64
			err := d.fileCache.withReader(ctx, block.NoReadEnv, tbl,
				func(r *sstable.Reader, env sstable.ReadEnv) (err error) {
					size, err = estimateDiskUsageInTableAndBlobReferences(
						r, tombBounds.Start, tombBounds.End.Key, env, tbl)
					return err
				})
			if err != nil {
				return 0, 0, err
			}
			estimate += size

			// If the format major version is past FormatVirtualSSTables,
			// deletion only compactions can also apply to partial overlaps with
			// sstables via excise.
			if !updateWideTombstones || d.FormatMajorVersion() < FormatVirtualSSTables {
				continue
			}
			tableBounds := tbl.UserKeyBounds()
			if d.cmp(tableBounds.Start, tombBounds.Start) < 0 &&
				tableBounds.End.CompareUpperBounds(d.cmp, tombBounds.End) > 0 {
				// The table's bounds completely contain the tombstone bounds.
				// An excise would need to split the table into two. Skip it.
				continue
			}
			// The table's bounds do not completely contain the tombstone
			// bounds, and vice versa. So the tombstone must overlap either the
			// left or right boundary of the table, and an excise would be able
			// to shorten the bound without increasing the number of tables.
			// Mark it as a candidate.
			deletionCandidates++
		}
	}
	return estimate, deletionCandidates, nil
}

var lastSanityCheckStatsLog crtime.AtomicMono

func sanityCheckStats(
	meta *manifest.TableMetadata, stats *manifest.TableStats, logger Logger, info string,
) {
	// Values for PointDeletionsBytesEstimate and RangeDeletionsBytesEstimate that
	// exceed this value are likely indicative of a bug (eg, underflow).
	const maxDeletionBytesEstimate = 1 << 50 // 1 PiB

	if stats.PointDeletionsBytesEstimate > maxDeletionBytesEstimate ||
		stats.RangeDeletionsBytesEstimate > maxDeletionBytesEstimate {
		if invariants.Enabled {
			panic(fmt.Sprintf("%s: table %s has extreme deletion bytes estimates: point=%d range=%d",
				info, meta.TableNum,
				redact.Safe(stats.PointDeletionsBytesEstimate),
				redact.Safe(stats.RangeDeletionsBytesEstimate),
			))
		}
		if v := lastSanityCheckStatsLog.Load(); v == 0 || v.Elapsed() > 30*time.Second {
			logger.Errorf("%s: table %s has extreme deletion bytes estimates: point=%d range=%d",
				info, meta.TableNum,
				redact.Safe(stats.PointDeletionsBytesEstimate),
				redact.Safe(stats.RangeDeletionsBytesEstimate),
			)
			lastSanityCheckStatsLog.Store(crtime.NowMono())
		}
	}
}

// estimateDiskUsageInTableAndBlobReferences estimates the disk usage within a
// sstable and its referenced values. The size of blob files is computed using
// linear interpolation.
func estimateDiskUsageInTableAndBlobReferences(
	r *sstable.Reader, start, end []byte, env sstable.ReadEnv, meta *manifest.TableMetadata,
) (uint64, error) {
	size, err := r.EstimateDiskUsage(start, end, env, meta.IterTransforms())
	if err != nil {
		return 0, err
	}

	estimatedTableSize := max(size, 1)
	originalTableSize := max(meta.Size, 1)
	referenceSize := crmath.ScaleUint64(meta.EstimatedReferenceSize(),
		estimatedTableSize, originalTableSize)
	return size + referenceSize, nil
}

// maybeSetStatsFromProperties sets the table backing properties and attempts to
// set the table stats from the properties, for a table that was created by an
// ingestion or compaction.
func maybeSetStatsFromProperties(meta *manifest.TableMetadata, props *sstable.Properties) bool {
	meta.TableBacking.PopulateProperties(props)
	if invariants.Enabled && meta.Virtual {
		panic("table expected to be physical")
	}
	// If a table contains any deletions, we defer the stats collection. There
	// are two main reasons for this:
	//
	//  1. Estimating the potential for reclaimed space due to a deletion
	//     requires scanning the LSM - a potentially expensive operation that
	//     should be deferred.
	//  2. Range deletions present an opportunity to find "wide" ranged
	//     tombstones, which also requires a scan of the LSM to compute tables
	//     that would be eligible for deletion.
	//
	// These two tasks are deferred to the table stats collector goroutine.
	//
	// Note that even if the point deletions are sized (DELSIZEDs), an accurate
	// compression ratio is necessary to calculate an accurate estimate of the
	// physical disk space they reclaim. To do that, we need to scan the LSM
	// beneath the file.
	if props.NumDeletions != 0 || props.NumRangeKeyDels != 0 {
		return false
	}
	meta.PopulateStats(new(manifest.TableStats))
	return true
}

// pointDeletionBytesEstimate returns an estimation of the total disk space that
// may be dropped by the physical table's point deletions by compacting them.
// The results should be scaled accordingly for virtual tables.
func pointDeletionsBytesEstimate(
	props *manifest.TableBackingProperties, avgValLogicalSize, compressionRatio float64,
) (estimate uint64) {
	if props.NumEntries == 0 {
		return 0
	}
	numPointDels := props.NumPointDeletions()
	if numPointDels == 0 {
		return 0
	}
	// Estimate the potential space to reclaim using the table's own properties.
	// There may or may not be keys covered by any individual point tombstone.
	// If not, compacting the point tombstone into L6 will at least allow us to
	// drop the point deletion key and will reclaim the tombstone's key bytes.
	// If there are covered key(s), we also get to drop key and value bytes for
	// each covered key.
	//
	// Some point tombstones (DELSIZEDs) carry a user-provided estimate of the
	// uncompressed size of entries that will be elided by fully compacting the
	// tombstone. For these tombstones, there's no guesswork—we use the
	// RawPointTombstoneValueSizeHint property which is the sum of all these
	// tombstones' encoded values.
	//
	// For un-sized point tombstones (DELs), we estimate assuming that each
	// point tombstone on average covers 1 key and using average value sizes.
	// This is almost certainly an overestimate, but that's probably okay
	// because point tombstones can slow range iterations even when they don't
	// cover a key.
	//
	// TODO(jackson): This logic doesn't directly incorporate fixed per-key
	// overhead (8-byte trailer, plus at least 1 byte encoding the length of the
	// key and 1 byte encoding the length of the value). This overhead is
	// indirectly incorporated through the compression ratios, but that results
	// in the overhead being smeared per key-byte and value-byte, rather than
	// per-entry. This per-key fixed overhead can be nontrivial, especially for
	// dense swaths of point tombstones. Give some thought as to whether we
	// should directly include fixed per-key overhead in the calculations.

	// Below, we calculate the tombstone contributions and the shadowed keys'
	// contributions separately.
	var tombstonesLogicalSize float64
	var shadowedLogicalSize float64

	// 1. Calculate the contribution of the tombstone keys themselves.
	if props.RawPointTombstoneKeySize > 0 {
		tombstonesLogicalSize += float64(props.RawPointTombstoneKeySize)
	} else {
		// This sstable predates the existence of the RawPointTombstoneKeySize
		// property. We can use the average key size within the file itself and
		// the count of point deletions to estimate the size.
		tombstonesLogicalSize += float64(numPointDels * props.RawKeySize / props.NumEntries)
	}

	// 2. Calculate the contribution of the keys shadowed by tombstones.
	//
	// 2a. First account for keys shadowed by DELSIZED tombstones. THE DELSIZED
	// tombstones encode the size of both the key and value of the shadowed KV
	// entries. These sizes are aggregated into a sstable property.
	shadowedLogicalSize += float64(props.RawPointTombstoneValueSize)

	// 2b. Calculate the contribution of the KV entries shadowed by ordinary DEL
	// keys.
	numUnsizedDels := invariants.SafeSub(numPointDels, props.NumSizedDeletions)
	{
		// The shadowed keys have the same exact user keys as the tombstones
		// themselves, so we can use the `tombstonesLogicalSize` we computed
		// earlier as an estimate. There's a complication that
		// `tombstonesLogicalSize` may include DELSIZED keys we already
		// accounted for.
		shadowedLogicalSize += tombstonesLogicalSize / float64(numPointDels) * float64(numUnsizedDels)

		// Calculate the contribution of the deleted values. The caller has
		// already computed an average logical size (possibly computed across
		// many sstables).
		shadowedLogicalSize += float64(numUnsizedDels) * avgValLogicalSize
	}

	// Scale both tombstone and shadowed totals by logical:physical ratios to
	// account for compression, metadata overhead, etc.
	//
	//      Physical             FileSize
	//     -----------  = -----------------------
	//      Logical       RawKeySize+RawValueSize
	//
	return uint64((tombstonesLogicalSize + shadowedLogicalSize) * compressionRatio)
}

// newCombinedDeletionKeyspanIter returns a keyspan.FragmentIterator that
// returns "ranged deletion" spans for a single table, providing a combined view
// of both range deletion and range key deletion spans. The
// tableRangedDeletionIter is intended for use in the specific case of computing
// the statistics and wide tombstones for a single table.
//
// As an example, consider the following set of spans from the range deletion
// and range key blocks of a table:
//
//		      |---------|     |---------|         |-------| RANGEKEYDELs
//		|-----------|-------------|           |-----|       RANGEDELs
//	  __________________________________________________________
//		a b c d e f g h i j k l m n o p q r s t u v w x y z
//
// The tableRangedDeletionIter produces the following set of output spans, where
// '1' indicates a span containing only range deletions, '2' is a span
// containing only range key deletions, and '3' is a span containing a mixture
// of both range deletions and range key deletions.
//
//		   1       3       1    3    2          1  3   2
//		|-----|---------|-----|---|-----|     |---|-|-----|
//	  __________________________________________________________
//		a b c d e f g h i j k l m n o p q r s t u v w x y z
//
// Algorithm.
//
// The iterator first defragments the range deletion and range key blocks
// separately. During this defragmentation, the range key block is also filtered
// so that keys other than range key deletes are ignored. The range delete and
// range key delete keyspaces are then merged.
//
// Note that the only fragmentation introduced by merging is from where a range
// del span overlaps with a range key del span. Within the bounds of any overlap
// there is guaranteed to be no further fragmentation, as the constituent spans
// have already been defragmented. To the left and right of any overlap, the
// same reasoning applies. For example,
//
//		         |--------|         |-------| RANGEKEYDEL
//		|---------------------------|         RANGEDEL
//		|----1---|----3---|----1----|---2---| Merged, fragmented spans.
//	  __________________________________________________________
//		a b c d e f g h i j k l m n o p q r s t u v w x y z
//
// Any fragmented abutting spans produced by the merging iter will be of
// differing types (i.e. a transition from a span with homogenous key kinds to a
// heterogeneous span, or a transition from a span with exclusively range dels
// to a span with exclusively range key dels). Therefore, further
// defragmentation is not required.
//
// Each span returned by the tableRangeDeletionIter will have at most four keys,
// corresponding to the largest and smallest sequence numbers encountered across
// the range deletes and range keys deletes that comprised the merged spans.
func newCombinedDeletionKeyspanIter(
	ctx context.Context,
	comparer *base.Comparer,
	r *sstable.Reader,
	m *manifest.TableMetadata,
	env sstable.ReadEnv,
) (keyspan.FragmentIterator, error) {
	// The range del iter and range key iter are each wrapped in their own
	// defragmenting iter. For each iter, abutting spans can always be merged.
	var equal = keyspan.DefragmentMethodFunc(func(_ base.CompareRangeSuffixes, a, b *keyspan.Span) bool { return true })
	// Reduce keys by maintaining a slice of at most length two, corresponding to
	// the largest and smallest keys in the defragmented span. This maintains the
	// contract that the emitted slice is sorted by (SeqNum, Kind) descending.
	reducer := func(current, incoming []keyspan.Key) []keyspan.Key {
		if len(current) == 0 && len(incoming) == 0 {
			// While this should never occur in practice, a defensive return is used
			// here to preserve correctness.
			return current
		}
		var largest, smallest keyspan.Key
		var set bool
		for _, keys := range [2][]keyspan.Key{current, incoming} {
			if len(keys) == 0 {
				continue
			}
			first, last := keys[0], keys[len(keys)-1]
			if !set {
				largest, smallest = first, last
				set = true
				continue
			}
			if first.Trailer > largest.Trailer {
				largest = first
			}
			if last.Trailer < smallest.Trailer {
				smallest = last
			}
		}
		if largest.Equal(comparer.CompareRangeSuffixes, smallest) {
			current = append(current[:0], largest)
		} else {
			current = append(current[:0], largest, smallest)
		}
		return current
	}

	// The separate iters for the range dels and range keys are wrapped in a
	// merging iter to join the keyspaces into a single keyspace. The separate
	// iters are only added if the particular key kind is present.
	mIter := &keyspanimpl.MergingIter{}
	var transform = keyspan.TransformerFunc(func(_ base.CompareRangeSuffixes, in keyspan.Span, out *keyspan.Span) error {
		if in.KeysOrder != keyspan.ByTrailerDesc {
			return base.AssertionFailedf("combined deletion iter encountered keys in non-trailer descending order")
		}
		out.Start, out.End = in.Start, in.End
		out.Keys = append(out.Keys[:0], in.Keys...)
		out.KeysOrder = keyspan.ByTrailerDesc
		// NB: The order of by-trailer descending may have been violated,
		// because we've layered rangekey and rangedel iterators from the same
		// sstable into the same keyspanimpl.MergingIter. The MergingIter will
		// return the keys in the order that the child iterators were provided.
		// Sort the keys to ensure they're sorted by trailer descending.
		keyspan.SortKeysByTrailer(out.Keys)
		return nil
	})
	mIter.Init(comparer, transform, new(keyspanimpl.MergingBuffers))
	iter, err := r.NewRawRangeDelIter(ctx, m.FragmentIterTransforms(), env)
	if err != nil {
		return nil, err
	}
	if iter != nil {
		// Assert expected bounds. In previous versions of Pebble, range
		// deletions persisted to sstables could exceed the bounds of the
		// containing files due to "split user keys." This required readers to
		// constrain the tombstones' bounds to the containing file at read time.
		// See docs/range_deletions.md for an extended discussion of the design
		// and invariants at that time.
		//
		// We've since compacted away all 'split user-keys' and in the process
		// eliminated all "untruncated range tombstones" for physical sstables.
		// We no longer need to perform truncation at read time for these
		// sstables.
		//
		// At the same time, we've also introduced the concept of "virtual
		// SSTables" where the table metadata's effective bounds can again be
		// reduced to be narrower than the contained tombstones. These virtual
		// SSTables handle truncation differently, performing it using
		// keyspan.Truncate when the sstable's range deletion iterator is
		// opened.
		//
		// Together, these mean that we should never see untruncated range
		// tombstones any more—and the merging iterator no longer accounts for
		// their existence. Since there's abundant subtlety that we're relying
		// on, we choose to be conservative and assert that these invariants
		// hold. We could (and previously did) choose to only validate these
		// bounds in invariants builds, but the most likely avenue for these
		// tombstones' existence is through a bug in a migration and old data
		// sitting around in an old store from long ago.
		//
		// The table stats collector will read all files' range deletions
		// asynchronously after Open, and provides a perfect opportunity to
		// validate our invariants without harming user latency. We also
		// previously performed truncation here which similarly required key
		// comparisons, so replacing those key comparisons with assertions
		// should be roughly similar in performance.
		//
		// TODO(jackson): Only use AssertBounds in invariants builds in the
		// following release.
		iter = keyspan.AssertBounds(
			iter, m.PointKeyBounds.Smallest(), m.PointKeyBounds.LargestUserKey(), comparer.Compare,
		)
		dIter := &keyspan.DefragmentingIter{}
		dIter.Init(comparer, iter, equal, reducer, new(keyspan.DefragmentingBuffers))
		iter = dIter
		mIter.AddLevel(iter)
	}

	iter, err = r.NewRawRangeKeyIter(ctx, m.FragmentIterTransforms(), env)
	if err != nil {
		return nil, err
	}
	if iter != nil {
		// Assert expected bounds in tests.
		if invariants.Sometimes(50) {
			if m.HasRangeKeys {
				iter = keyspan.AssertBounds(
					iter, m.RangeKeyBounds.Smallest(), m.RangeKeyBounds.LargestUserKey(), comparer.Compare,
				)
			}
		}
		// Wrap the range key iterator in a filter that elides keys other than range
		// key deletions.
		iter = keyspan.Filter(iter, func(in *keyspan.Span, buf []keyspan.Key) []keyspan.Key {
			keys := buf[:0]
			for _, k := range in.Keys {
				if k.Kind() != base.InternalKeyKindRangeKeyDelete {
					continue
				}
				keys = append(keys, k)
			}
			return keys
		}, comparer.Compare)
		dIter := &keyspan.DefragmentingIter{}
		dIter.Init(comparer, iter, equal, reducer, new(keyspan.DefragmentingBuffers))
		iter = dIter
		mIter.AddLevel(iter)
	}

	return mIter, nil
}

type deletionBytes struct {
	// PointDels contains a sum of TableStats.PointDeletionsBytesEstimate.
	PointDels uint64
	// RangeDels contains a sum of TableStats.RangeDeletionsBytesEstimate.
	RangeDels uint64
}

var deletionBytesAnnotator = manifest.MakeTableAnnotator[deletionBytes](
	manifest.NewTableAnnotationIdx(),
	manifest.TableAnnotatorFuncs[deletionBytes]{
		Merge: func(dst *deletionBytes, src deletionBytes) {
			dst.PointDels += src.PointDels
			dst.RangeDels += src.RangeDels
		},
		Table: func(t *manifest.TableMetadata) (v deletionBytes, cacheOK bool) {
			if stats, ok := t.Stats(); ok {
				return deletionBytes{
					PointDels: stats.PointDeletionsBytesEstimate,
					RangeDels: stats.RangeDeletionsBytesEstimate,
				}, true
			}
			return deletionBytes{}, false
		},
	},
)

// annotatedTableProps are properties derived from TableBackingProperties that
// are aggregated for metrics.
type aggregatedTableProps struct {
	// NumRangeKeySets is the sum of the tables' counts of range key fragments.
	NumRangeKeySets uint64
	// NumDeletions is the sum of the tables' counts of tombstones (DEL, SINGLEDEL
	// and RANGEDEL keys).
	NumDeletions uint64
	// ValueBlocksSize is the sum of the tables' Properties.ValueBlocksSize.
	ValueBlocksSize uint64

	CompressionMetrics CompressionMetrics
}

var tablePropsAnnotator = manifest.MakeTableAnnotator[aggregatedTableProps](
	manifest.NewTableAnnotationIdx(),
	manifest.TableAnnotatorFuncs[aggregatedTableProps]{
		Merge: func(dst *aggregatedTableProps, src aggregatedTableProps) {
			dst.NumRangeKeySets += src.NumRangeKeySets
			dst.NumDeletions += src.NumDeletions
			dst.ValueBlocksSize += src.ValueBlocksSize
			dst.CompressionMetrics.MergeWith(&src.CompressionMetrics)
		},
		Table: func(t *manifest.TableMetadata) (v aggregatedTableProps, cacheOK bool) {
			props, propsValid := t.TableBacking.Properties()
			if propsValid {
				v.NumRangeKeySets = props.NumRangeKeySets
				v.NumDeletions = props.NumDeletions
				v.ValueBlocksSize = props.ValueBlocksSize
			}
			if !propsValid || props.CompressionStats.IsEmpty() {
				v.CompressionMetrics.CompressedBytesWithoutStats = t.ScaleStatistic(t.Size)
			} else {
				compressionStats := props.CompressionStats
				if t.Virtual {
					// Scale the compression stats for virtual tables.
					compressionStats = compressionStats.Scale(t.Size, t.TableBacking.Size)
				}
				v.CompressionMetrics.Add(&compressionStats)
			}
			return v, propsValid
		},
	})

// compressionStatsAnnotator is a manifest.TableAnnotator that annotates B-tree nodes
// with the compression statistics for tables. Its annotation type is
// block.CompressionStats. The compression type may change once a table's stats
// are loaded asynchronously, so its values are marked as cacheable only if a
// file's stats have been loaded. Statistics for virtual tables are estimated
// from the physical table statistics, proportional to the estimated virtual
// table size.
var blobCompressionStatsAnnotator = manifest.MakeBlobFileAnnotator[CompressionMetrics](
	manifest.NewBlobAnnotationIdx(),
	manifest.BlobFileAnnotatorFuncs[CompressionMetrics]{
		Merge: func(dst *CompressionMetrics, src CompressionMetrics) {
			dst.MergeWith(&src)
		},
		BlobFile: func(f manifest.BlobFileMetadata) (v CompressionMetrics, cacheOK bool) {
			props, propsValid := f.Physical.Properties()
			if !propsValid || props.CompressionStats.IsEmpty() {
				v.CompressedBytesWithoutStats += f.Physical.Size
				return v, propsValid
			}
			compressionStats := props.CompressionStats
			v.Add(&compressionStats)
			return v, true
		},
	},
)
