// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"sync/atomic"

	"github.com/cockroachdb/pebble/internal/manifest"
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
// a list of files that require statistics, alongside a corresponding
// readState and the file levels. Whenever new files are added to the LSM, the
// files and readState are appended to d.mu.tableStats.pending. If a stats
// collection job is not currently running, one is started in a separate
// goroutine.
//
// The stats collection job grabs and clears the pending list, computes table
// statistics relative to the associated readState and updates the tables'
// file metadata. New pending files may accumulate during a stats collection
// job, so a completing job triggers a new job if necessary. Only one job runs
// at a time.
//
// When an existing database is opened, all files lack in-memory statistics.
// These files' stats are loaded incrementally whenever the pending list is
// empty by scanning a readState for files missing statistics. Once a job
// completes a scan without finding any remaining files without statistics, it
// flips a `loadedInitial` flag. From then on, the stats collection job only
// needs to load statistics for new files appended to the pending list.

// newFilesVersion stores a set of new files that do not yet have their table
// stats calculated, along with the readState corresponding to the first
// version the files appeared in.
type newFilesVersion struct {
	rs    *readState
	files []manifest.NewFileEntry
}

func (d *DB) maybeCollectTableStats() {
	if d.shouldCollectTableStats() {
		go d.collectTableStats()
	}
}

// updateTableStatsLocked is called when new files are introduced, after the
// read state has been updated. It may trigger a new stat collection.
// DB.mu must be locked when calling.
func (d *DB) updateTableStatsLocked(newFiles []manifest.NewFileEntry) {
	if d.opts.disableTableStats || len(newFiles) == 0 {
		return
	}

	d.mu.tableStats.pending = append(d.mu.tableStats.pending, newFilesVersion{
		rs:    d.loadReadState(),
		files: newFiles,
	})
	d.maybeCollectTableStats()
}

// clearPendingTableStatsLocked releases any readStates from pending table
// stats. It's called from DB.Close. DB.mu must be held while calling.
func (d *DB) clearPendingTableStatsLocked() {
	for _, nfv := range d.mu.tableStats.pending {
		nfv.rs.unrefLocked()
	}
	d.mu.tableStats.pending = nil

	// (*readState).unrefLocked does not try to clean up obsolete files.
	// Since we're closing the database, clean up any files made obsolete by
	// our unrefing of previous versions.
	jobID := d.mu.nextJobID
	d.mu.nextJobID++
	d.deleteObsoleteFiles(jobID)
}

func (d *DB) shouldCollectTableStats() bool {
	ok := !d.mu.tableStats.loading
	ok = ok && atomic.LoadInt32(&d.closed) == 0
	ok = ok && !d.opts.disableTableStats
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
	loadedInitial := d.mu.tableStats.loadedInitial
	// Drop DB.mu before performing IO.
	d.mu.Unlock()

	// Every run of collectTableStats either collects stats from the pending
	// list (if non-empty) or from the current readState (loadedInitial is
	// false). This job only runs if at least one of those conditions holds.
	var collected []collectedStats
	if len(pending) > 0 {
		collected = d.loadNewFileStats(pending)
	} else {
		var moreRemain bool
		var buf [maxTableStatsPerScan]collectedStats
		collected, moreRemain = d.scanReadStateTableStats(buf[:0])
		loadedInitial = !moreRemain
	}

	// Update the FileMetadata with the loaded stats while holding d.mu.
	d.mu.Lock()
	defer d.mu.Unlock()
	d.mu.tableStats.loading = false
	d.mu.tableStats.loadedInitial = loadedInitial

	maybeCompact := false
	for _, c := range collected {
		c.fileMetadata.Stats = c.TableStats
		maybeCompact = maybeCompact || c.fileMetadata.Stats.RangeDeletionsBytesEstimate > 0
	}
	d.mu.tableStats.cond.Broadcast()
	d.maybeCollectTableStats()
	if maybeCompact {
		d.maybeScheduleCompaction()
	}
}

type collectedStats struct {
	*fileMetadata
	manifest.TableStats
}

func (d *DB) loadNewFileStats(pending []newFilesVersion) []collectedStats {
	var collected []collectedStats
	for _, nfv := range pending {
		for _, nf := range nfv.files {
			// A file's stats might have been populated by an earlier call to
			// loadNewFileStats if the file was moved.
			// NB: We're not holding d.mu which protects f.Stats, but only
			// collectTableStats updates f.Stats for active files, and we
			// ensure only one goroutine runs it at a time through
			// d.mu.tableStats.loading.
			if nf.Meta.Stats.Valid {
				continue
			}

			// Limit how much work we do per job.
			stats, err := d.loadTableStats(nfv.rs.current, nf.Level, nf.Meta)
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
		}
		nfv.rs.unref()
	}
	return collected
}

// scanReadStateTableStats is run by an active stat collection job when there
// are no pending new files, but there might be files that existed at Open for
// which we haven't loaded table stats.
func (d *DB) scanReadStateTableStats(fill []collectedStats) ([]collectedStats, bool) {
	// Grab a read state to scan for tables.
	rs := d.loadReadState()
	defer rs.unref()

	moreRemain := false
	for l, ff := range rs.current.Files {
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
				return fill, moreRemain
			}

			stats, err := d.loadTableStats(rs.current, l, f)
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
		}
	}
	return fill, moreRemain
}

func (d *DB) loadTableStats(v *version, level int, meta *fileMetadata) (manifest.TableStats, error) {
	var stats manifest.TableStats
	err := d.tableCache.withReader(meta, func(r *sstable.Reader) (err error) {
		if r.Properties.NumRangeDeletions == 0 {
			return nil
		}

		rangeDelIter, err := r.NewRangeDelIter()
		if err != nil {
			return err
		}
		defer rangeDelIter.Close()
		var startUserKey []byte
		for start, end := rangeDelIter.First(); start != nil; start, end = rangeDelIter.Next() {
			// Range tombstones are fragmented such that any two tombstones
			// that share the same start key also share the same end key.
			// Multiple tombstones may exist at different sequence numbers.
			// When we estimate the disk usage, we ignore sequence numbers,
			// so we don't double count a key range.
			if d.cmp(startUserKey, start.UserKey) == 0 {
				continue
			}
			startUserKey = append(startUserKey[:0], start.UserKey...)

			// Find all files in lower levels that overlap with the deleted range.
			//
			// An overlapping file might be completely contained by the range
			// tombstone, in which case we can count the entire file size in
			// our estimate without doing any additional I/O.
			//
			// Otherwise, estimating the range for the file requires
			// additional I/O to read the file's index blocks.
			for l := level + 1; l < numLevels; l++ {
				overlaps := v.Overlaps(l, d.cmp, start.UserKey, end)

				for i, file := range overlaps {
					if i > 0 && i < len(overlaps)-1 {
						// The files to the left and the right at least
						// partially overlap with the range tombstone, which
						// means `file` is fully contained within the range.
						stats.RangeDeletionsBytesEstimate += file.Size
					} else if d.cmp(start.UserKey, file.Smallest.UserKey) <= 0 &&
						d.cmp(file.Largest.UserKey, end) <= 0 {
						// The range fully contains the file, so skip looking it up in
						// table cache/looking at its indexes and add the full file size.
						stats.RangeDeletionsBytesEstimate += file.Size
					} else if d.cmp(file.Smallest.UserKey, end) <= 0 && d.cmp(start.UserKey, file.Largest.UserKey) <= 0 {
						var size uint64
						err := d.tableCache.withReader(file, func(r *sstable.Reader) (err error) {
							size, err = r.EstimateDiskUsage(start.UserKey, end)
							return err
						})
						if err != nil {
							return err
						}
						stats.RangeDeletionsBytesEstimate += size
					}
				}
			}
		}
		if err := rangeDelIter.Error(); err != nil {
			_ = rangeDelIter.Close()
			return err
		}
		return rangeDelIter.Close()
	})
	if err != nil {
		return stats, err
	}
	stats.Valid = true
	return stats, nil
}
