// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"math"
	"sync/atomic"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/rangedel"
	"github.com/cockroachdb/pebble/sstable"
)

// In-memory statistics about tables help inform compaction picking, but may
// be expensive to calculate or load from disk. Every time a database is
// opened, these statistics must be reloaded or recalculated. To minimize
// impact on user activity and compactions, we load these statistics
// asynchronously in the background and store loaded statistics in each
// table's *FileMetadata.
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

func (d *DB) maybeCollectTableStats() {
	if d.shouldCollectTableStats() {
		go d.collectTableStats()
	}
}

// updateTableStatsLocked is called when new files are introduced, after the
// read state has been updated. It may trigger a new stat collection.
// DB.mu must be locked when calling.
func (d *DB) updateTableStatsLocked(newFiles []manifest.NewFileEntry) {
	var needStats bool
	for _, nf := range newFiles {
		if !nf.Meta.Stats.Valid {
			needStats = true
			break
		}
	}
	if !needStats {
		return
	}

	d.mu.tableStats.pending = append(d.mu.tableStats.pending, newFiles...)
	d.maybeCollectTableStats()
}

func (d *DB) shouldCollectTableStats() bool {
	ok := !d.mu.tableStats.loading
	ok = ok && atomic.LoadInt32(&d.closed) == 0
	ok = ok && !d.opts.private.disableTableStats
	ok = ok && (len(d.mu.tableStats.pending) > 0 || !d.mu.tableStats.loadedInitial)
	return ok
}

func (d *DB) collectTableStats() {
	const maxTableStatsPerScan = 50

	d.mu.Lock()
	if !d.shouldCollectTableStats() {
		d.mu.Unlock()
		return
	}

	pending := d.mu.tableStats.pending
	d.mu.tableStats.pending = nil
	d.mu.tableStats.loading = true
	jobID := d.mu.nextJobID
	d.mu.nextJobID++
	loadedInitial := d.mu.tableStats.loadedInitial
	// Drop DB.mu before performing IO.
	d.mu.Unlock()

	// Every run of collectTableStats either collects stats from the pending
	// list (if non-empty) or from scanning the version (loadedInitial is
	// false). This job only runs if at least one of those conditions holds.

	// Grab a read state to scan for tables.
	rs := d.loadReadState()
	var collected []collectedStats
	var hints []deleteCompactionHint
	if len(pending) > 0 {
		collected, hints = d.loadNewFileStats(rs, pending)
	} else {
		var moreRemain bool
		var buf [maxTableStatsPerScan]collectedStats
		collected, hints, moreRemain = d.scanReadStateTableStats(rs, buf[:0])
		loadedInitial = !moreRemain
	}
	rs.unref()

	// Update the FileMetadata with the loaded stats while holding d.mu.
	d.mu.Lock()
	defer d.mu.Unlock()
	d.mu.tableStats.loading = false
	if loadedInitial && !d.mu.tableStats.loadedInitial {
		d.mu.tableStats.loadedInitial = loadedInitial
		d.opts.EventListener.TableStatsLoaded(TableStatsInfo{
			JobID: jobID,
		})
	}

	maybeCompact := false
	for _, c := range collected {
		c.fileMetadata.Stats = c.TableStats
		maybeCompact = maybeCompact || c.fileMetadata.Stats.RangeDeletionsBytesEstimate > 0
	}
	d.mu.tableStats.cond.Broadcast()
	d.maybeCollectTableStats()
	if len(hints) > 0 {
		// Verify that all of the hint tombstones' files still exist in the
		// current version. Otherwise, the tombstone itself may have been
		// compacted into L6 and more recent keys may have had their sequence
		// numbers zeroed. See DB.maybeUpdateDeleteCompactionHints.
		v := d.mu.versions.currentVersion()
		i := 0
		for _, h := range hints {
			if v.Contains(h.tombstoneLevel, d.cmp, h.tombstoneFile) {
				hints[i] = h
				i++
			}
		}
		d.mu.compact.deletionHints = append(d.mu.compact.deletionHints, hints[:i]...)
	}
	if maybeCompact {
		d.maybeScheduleCompaction()
	}
}

type collectedStats struct {
	*fileMetadata
	manifest.TableStats
}

func (d *DB) loadNewFileStats(rs *readState, pending []manifest.NewFileEntry) ([]collectedStats, []deleteCompactionHint) {
	var hints []deleteCompactionHint
	collected := make([]collectedStats, 0, len(pending))
	for _, nf := range pending {
		// A file's stats might have been populated by an earlier call to
		// loadNewFileStats if the file was moved.
		// NB: We're not holding d.mu which protects f.Stats, but only
		// collectTableStats updates f.Stats for active files, and we
		// ensure only one goroutine runs it at a time through
		// d.mu.tableStats.loading.
		if nf.Meta.Stats.Valid {
			continue
		}

		// The file isn't guaranteed to still be live in the readState's
		// version. It may have been deleted or moved. Skip it if it's not in
		// the expected level.
		if !rs.current.Contains(nf.Level, d.cmp, nf.Meta) {
			continue
		}

		stats, newHints, err := d.loadTableStats(rs.current, nf.Level, nf.Meta)
		if err != nil {
			d.opts.EventListener.BackgroundError(err)
			continue
		}
		// NB: We don't update the FileMetadata yet, because we aren't
		// holding DB.mu. We'll copy it to the FileMetadata after we're
		// finished with IO.
		collected = append(collected, collectedStats{
			fileMetadata: nf.Meta,
			TableStats:   stats,
		})
		hints = append(hints, newHints...)
	}
	return collected, hints
}

// scanReadStateTableStats is run by an active stat collection job when there
// are no pending new files, but there might be files that existed at Open for
// which we haven't loaded table stats.
func (d *DB) scanReadStateTableStats(rs *readState, fill []collectedStats) ([]collectedStats, []deleteCompactionHint, bool) {
	moreRemain := false
	var hints []deleteCompactionHint
	for l, ff := range rs.current.Levels {
		for _, f := range ff {
			// NB: We're not holding d.mu which protects f.Stats, but only the
			// active stats collection job updates f.Stats for active files,
			// and we ensure only one goroutine runs it at a time through
			// d.mu.tableStats.loading. This makes it safe to read
			// f.Stats.Valid despite not holding d.mu.
			if f.Stats.Valid {
				continue
			}

			// Limit how much work we do per read state. The older the read
			// state is, the higher the likelihood files are no longer being
			// used in the current version. If we've exhausted our allowance,
			// return true for the second return value to signal there's more
			// work to do.
			if len(fill) == cap(fill) {
				moreRemain = true
				return fill, hints, moreRemain
			}

			stats, newHints, err := d.loadTableStats(rs.current, l, f)
			if err != nil {
				// Set `moreRemain` so we'll try again.
				moreRemain = true
				d.opts.EventListener.BackgroundError(err)
				continue
			}
			fill = append(fill, collectedStats{
				fileMetadata: f,
				TableStats:   stats,
			})
			hints = append(hints, newHints...)
		}
	}
	return fill, hints, moreRemain
}

func (d *DB) loadTableStats(
	v *version, level int, meta *fileMetadata,
) (manifest.TableStats, []deleteCompactionHint, error) {
	var totalRangeDeletionEstimate uint64
	var compactionHints []deleteCompactionHint
	err := d.tableCache.withReader(meta, func(r *sstable.Reader) (err error) {
		if r.Properties.NumRangeDeletions == 0 {
			return nil
		}

		// We iterate over the defragmented range tombstones, which ensures
		// we don't double count ranges deleted at different sequence numbers.
		// Also, merging abutting tombstones reduces the number of calls to
		// estimateSizeBeneath which is costly, and improves the accuracy of
		// our overall estimate.
		rangeDelIter, err := r.NewRawRangeDelIter()
		if err != nil {
			return err
		}
		defer rangeDelIter.Close()
		// Truncate tombstones to the containing file's bounds if necessary.
		// See docs/range_deletions.md for why this is necessary.
		rangeDelIter = rangedel.Truncate(d.cmp, rangeDelIter, meta.Smallest.UserKey, meta.Largest.UserKey)
		err = foreachDefragmentedTombstone(rangeDelIter, d.cmp,
			func(startUserKey, endUserKey []byte, smallestSeqNum, largestSeqNum uint64) error {
				estimate, hintSeqNum, err := d.estimateSizeBeneath(v, level, meta, startUserKey, endUserKey)
				if err != nil {
					return err
				}
				totalRangeDeletionEstimate += estimate

				// If any files were completely contained with the range,
				// hintSeqNum is the smallest sequence number contained in any
				// such file.
				if hintSeqNum == math.MaxUint64 {
					return nil
				}
				hint := deleteCompactionHint{
					start:                   make([]byte, len(startUserKey)),
					end:                     make([]byte, len(endUserKey)),
					tombstoneFile:           meta,
					tombstoneLevel:          level,
					tombstoneLargestSeqNum:  largestSeqNum,
					tombstoneSmallestSeqNum: smallestSeqNum,
					fileSmallestSeqNum:      hintSeqNum,
				}
				copy(hint.start, startUserKey)
				copy(hint.end, endUserKey)
				compactionHints = append(compactionHints, hint)
				return nil
			})
		return err
	})
	var stats manifest.TableStats
	if err != nil {
		return stats, nil, err
	}
	stats.Valid = true
	stats.RangeDeletionsBytesEstimate = totalRangeDeletionEstimate
	return stats, compactionHints, nil
}

func (d *DB) estimateSizeBeneath(
	v *version, level int, meta *fileMetadata, start, end []byte,
) (estimate uint64, hintSeqNum uint64, err error) {
	// Find all files in lower levels that overlap with the deleted range.
	//
	// An overlapping file might be completely contained by the range
	// tombstone, in which case we can count the entire file size in
	// our estimate without doing any additional I/O.
	//
	// Otherwise, estimating the range for the file requires
	// additional I/O to read the file's index blocks.
	hintSeqNum = math.MaxUint64
	for l := level + 1; l < numLevels; l++ {
		overlaps := v.Overlaps(l, d.cmp, start, end)

		for i, file := range overlaps {
			if i > 0 && i < len(overlaps)-1 {
				// The files to the left and the right at least
				// partially overlap with the range tombstone, which
				// means `file` is fully contained within the range.
				estimate += file.Size
				if file.SmallestSeqNum < hintSeqNum {
					hintSeqNum = file.SmallestSeqNum
				}
			} else if d.cmp(start, file.Smallest.UserKey) <= 0 &&
				d.cmp(file.Largest.UserKey, end) <= 0 {
				// The range fully contains the file, so skip looking it up in
				// table cache/looking at its indexes and add the full file size.
				estimate += file.Size
				if file.SmallestSeqNum < hintSeqNum {
					hintSeqNum = file.SmallestSeqNum
				}
			} else if d.cmp(file.Smallest.UserKey, end) <= 0 && d.cmp(start, file.Largest.UserKey) <= 0 {
				var size uint64
				err := d.tableCache.withReader(file, func(r *sstable.Reader) (err error) {
					size, err = r.EstimateDiskUsage(start, end)
					return err
				})
				if err != nil {
					return 0, hintSeqNum, err
				}
				estimate += size
			}
		}
	}
	return estimate, hintSeqNum, nil
}

func foreachDefragmentedTombstone(
	rangeDelIter base.InternalIterator, cmp base.Compare, fn func([]byte, []byte, uint64, uint64) error,
) error {
	var startUserKey, endUserKey []byte
	var smallestSeqNum, largestSeqNum uint64
	var initialized bool
	for start, end := rangeDelIter.First(); start != nil; start, end = rangeDelIter.Next() {

		// Range tombstones are fragmented such that any two tombstones
		// that share the same start key also share the same end key.
		// Multiple tombstones may exist at different sequence numbers.
		// If this tombstone starts or ends at the same point, it's a fragment
		// of the previous one.
		if cmp(startUserKey, start.UserKey) == 0 || cmp(endUserKey, end) == 0 {
			if start.SeqNum() < smallestSeqNum {
				smallestSeqNum = start.SeqNum()
			}
			if start.SeqNum() > largestSeqNum {
				largestSeqNum = start.SeqNum()
			}
			continue
		}

		// If this fragmented tombstone begins where the previous
		// tombstone ended, merge it and continue.
		if cmp(endUserKey, start.UserKey) == 0 {
			endUserKey = append(endUserKey[:0], end...)
			if start.SeqNum() < smallestSeqNum {
				smallestSeqNum = start.SeqNum()
			}
			if start.SeqNum() > largestSeqNum {
				largestSeqNum = start.SeqNum()
			}
			continue
		}

		// If this is the first iteration, continue so we have an
		// opportunity to merge subsequent abutting tombstones.
		if !initialized {
			startUserKey = append(startUserKey[:0], start.UserKey...)
			endUserKey = append(endUserKey[:0], end...)
			smallestSeqNum, largestSeqNum = start.SeqNum(), start.SeqNum()
			initialized = true
			continue
		}

		if err := fn(startUserKey, endUserKey, smallestSeqNum, largestSeqNum); err != nil {
			return err
		}
		startUserKey = append(startUserKey[:0], start.UserKey...)
		endUserKey = append(endUserKey[:0], end...)
		smallestSeqNum, largestSeqNum = start.SeqNum(), start.SeqNum()
	}
	if initialized {
		if err := fn(startUserKey, endUserKey, smallestSeqNum, largestSeqNum); err != nil {
			return err
		}
	}
	if err := rangeDelIter.Error(); err != nil {
		_ = rangeDelIter.Close()
		return err
	}
	return rangeDelIter.Close()
}
