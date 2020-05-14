// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"sync/atomic"

	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/sstable"
)

// updateTableStats is installed as a versionSet newVersionHook, called
// whenever a new version is installed. It may trigger a new stat collection.
func (d *DB) updateTableStats(added [numLevels][]*fileMetadata) {
	if d.mu.tableStats.unloaded {
		return
	}

	for _, ff := range added[:numLevels-1] {
		for _, f := range ff {
			if f.Stats.Valid {
				continue
			}

			// There's at least one new file without table statistics.
			d.mu.tableStats.unloaded = true
			d.maybeLoadMissingTableStats()
			return
		}
	}
}

type collectedStats struct {
	*fileMetadata
	manifest.TableStats
}

func (d *DB) maybeLoadMissingTableStats() {
	if d.mu.tableStats.loading || !d.mu.tableStats.unloaded || atomic.LoadInt32(&d.closed) != 0 {
		return
	}
	go d.loadMissingTableStats()
}

func (d *DB) loadMissingTableStats() {
	const maxTableStatsPerScan = 25
	var loadedStats [maxTableStatsPerScan]collectedStats

	d.mu.Lock()
	if d.mu.tableStats.loading || !d.mu.tableStats.unloaded || atomic.LoadInt32(&d.closed) != 0 {
		d.mu.Unlock()
		return
	}

	// Reset the unloaded flag. We'll set it again if the scan finds more
	// tables without without statistics.
	d.mu.tableStats.unloaded = false
	d.mu.tableStats.loading = true
	d.mu.Unlock()

	count, more := d.scanMissingTableStats(loadedStats[:0])

	// Update the FileMetadata with the loaded stats while holding d.mu.
	d.mu.Lock()
	defer d.mu.Unlock()
	d.mu.tableStats.loading = false
	d.mu.tableStats.unloaded = d.mu.tableStats.unloaded || more

	maybeCompact := false
	for _, f := range loadedStats[:count] {
		f.fileMetadata.Stats = f.TableStats
		f.fileMetadata.Stats.Valid = true
		if f.fileMetadata.Stats.RangeDeletionsBytesEstimate > 0 {
			maybeCompact = true
		}
	}
	d.mu.tableStats.cond.Broadcast()
	if maybeCompact {
		d.maybeScheduleCompaction()
	}
	d.maybeLoadMissingTableStats()
}

func (d *DB) scanMissingTableStats(fill []collectedStats) (int, bool) {
	// Grab a read state to scan for tables.
	rs := d.loadReadState()
	defer rs.unref()

	moreRemain := false
	for l, ff := range rs.current.Files {
		for _, f := range ff {
			// NB: We're not holding d.mu which protects f.Stats, but only
			// loadMissingTableStats updates f.Stats for active files, and we
			// ensure only one goroutine runs it at a time through
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
				return len(fill), moreRemain
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
	return len(fill), moreRemain
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
	return stats, nil
}
