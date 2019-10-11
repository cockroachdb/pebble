// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/humanize"
	"github.com/cockroachdb/pebble/sstable"
)

// CacheMetrics holds metrics for the block and table cache.
type CacheMetrics = cache.Metrics

// FilterMetrics holds metrics for the filter policy
type FilterMetrics = sstable.FilterMetrics

func formatCacheMetrics(buf *bytes.Buffer, m *CacheMetrics, name string) {
	fmt.Fprintf(buf, "%7s %9s %7s %6.1f%%  (score == hit-rate)\n",
		name,
		humanize.SI.Int64(m.Count),
		humanize.IEC.Int64(m.Size),
		hitRate(m.Hits, m.Misses))
}

// LevelMetrics holds per-level metrics such as the number of files and total
// size of the files, and compaction related metrics.
type LevelMetrics struct {
	// The total number of files in the level.
	NumFiles int64
	// The total size in bytes of the files in the level.
	Size uint64
	// The level's compaction score.
	Score float64
	// The number of incoming bytes from other levels read during
	// compactions. This excludes bytes moved and bytes ingested. For L0 this is
	// the bytes written to the WAL.
	BytesIn uint64
	// The number of bytes ingested. The sibling metric for tables is
	// TablesIngested.
	BytesIngested uint64
	// The number of bytes moved into the level by a "move" compaction. The
	// sibling metric for tables is TablesMoved.
	BytesMoved uint64
	// The number of bytes read for compactions at the level. This includes bytes
	// read from other levels (BytesIn), as well as bytes read for the level.
	BytesRead uint64
	// The number of bytes written during flushes and compactions. The sibling
	// metrics for tables are TablesCompacted and TablesFlushed.
	BytesWritten uint64
	// The number of sstables compacted to this level.
	TablesCompacted uint64
	// The number of sstables flushed to this level.
	TablesFlushed uint64
	// The number of sstables ingested into the level.
	TablesIngested uint64
	// The number of sstables moved to this level by a "move" compaction.
	TablesMoved uint64
}

// Add updates the counter metrics for the level.
func (m *LevelMetrics) Add(u *LevelMetrics) {
	m.BytesIn += u.BytesIn
	m.BytesIngested += u.BytesIngested
	m.BytesMoved += u.BytesMoved
	m.BytesRead += u.BytesRead
	m.BytesWritten += u.BytesWritten
	m.TablesCompacted += u.TablesCompacted
	m.TablesFlushed += u.TablesFlushed
	m.TablesIngested += u.TablesIngested
	m.TablesMoved += u.TablesMoved
}

// WriteAmp computes the write amplification for compactions at this
// level. Computed as BytesWritten / BytesIn.
func (m *LevelMetrics) WriteAmp() float64 {
	if m.BytesIn == 0 {
		return 0
	}
	return float64(m.BytesWritten) / float64(m.BytesIn)
}

// format generates a string of the receiver's metrics, formatting it into the
// supplied buffer.
func (m *LevelMetrics) format(buf *bytes.Buffer, score string) {
	fmt.Fprintf(buf, "%9d %7s %7s %7s %7s %7s %7s %7s %7s %7s %7s %7.1f\n",
		m.NumFiles,
		humanize.IEC.Uint64(m.Size),
		score,
		humanize.IEC.Uint64(m.BytesIn),
		humanize.IEC.Uint64(m.BytesIngested),
		humanize.SI.Uint64(m.TablesIngested),
		humanize.IEC.Uint64(m.BytesMoved),
		humanize.SI.Uint64(m.TablesMoved),
		humanize.IEC.Uint64(m.BytesWritten),
		humanize.SI.Uint64(m.TablesFlushed+m.TablesCompacted),
		humanize.IEC.Uint64(m.BytesRead),
		m.WriteAmp())
}

// Metrics holds metrics for various subsystems of the DB such as the Cache,
// Compactions, WAL, and per-Level metrics.
//
// TODO(peter): The testing of these metrics is relatively weak. There should
// be testing that performs various operations on a DB and verifies that the
// metrics reflect those operations.
type Metrics struct {
	BlockCache CacheMetrics

	Compact struct {
		// The total number of compactions.
		Count int64
		// An estimate of the number of bytes that need to be compacted for the LSM
		// to reach a stable state.
		EstimatedDebt uint64
	}

	Flush struct {
		// The total number of flushes.
		Count int64
	}

	Filter FilterMetrics

	Levels [numLevels]LevelMetrics

	MemTable struct {
		// The number of bytes allocated by memtables and large (flushable)
		// batches.
		Size uint64
		// The count of memtables.
		Count int64
	}

	TableCache CacheMetrics

	// Count of the number of open sstable iterators.
	TableIters int64

	WAL struct {
		// Number of live WAL files.
		Files int64
		// Number of obsolete WAL files.
		ObsoleteFiles int64
		// Size of the live data in the WAL files. Note that with WAL file
		// recycling this is less than the actual on-disk size of the WAL files.
		Size uint64
		// Number of logical bytes written to the WAL.
		BytesIn uint64
		// Number of bytes written to the WAL.
		BytesWritten uint64
	}
}

const notApplicable = "-"

func (m *Metrics) formatWAL(buf *bytes.Buffer) {
	var writeAmp float64
	if m.WAL.BytesIn > 0 {
		writeAmp = float64(m.WAL.BytesWritten) / float64(m.WAL.BytesIn)
	}
	fmt.Fprintf(buf, "    WAL %9d %7s %7s %7s %7s %7s %7s %7s %7s %7s %7s %7.1f\n",
		m.WAL.Files,
		humanize.Uint64(m.WAL.Size),
		notApplicable,
		humanize.Uint64(m.WAL.BytesIn),
		notApplicable,
		notApplicable,
		notApplicable,
		notApplicable,
		humanize.Uint64(m.WAL.BytesWritten),
		notApplicable,
		notApplicable,
		writeAmp)
}

// Pretty-print the metrics, showing a line for the WAL, a line per-level, and
// a total:
//
//   __level_____count____size___score______in__ingest(sz_cnt)____move(sz_cnt)___write(sz_cnt)____read___w-amp
//       WAL         1    27 B       -    48 B       -       -       -       -   108 B       -       -     2.2
//         0         2   1.6 K    0.50    81 B   825 B       1     0 B       0   2.4 K       3     0 B    30.6
//         1         0     0 B    0.00     0 B     0 B       0     0 B       0     0 B       0     0 B     0.0
//         2         0     0 B    0.00     0 B     0 B       0     0 B       0     0 B       0     0 B     0.0
//         3         0     0 B    0.00     0 B     0 B       0     0 B       0     0 B       0     0 B     0.0
//         4         0     0 B    0.00     0 B     0 B       0     0 B       0     0 B       0     0 B     0.0
//         5         0     0 B    0.00     0 B     0 B       0     0 B       0     0 B       0     0 B     0.0
//         6         1   825 B    0.00   1.6 K     0 B       0     0 B       0   825 B       1   1.6 K     0.5
//     total         3   2.4 K       -   933 B   825 B       1     0 B       0   4.1 K       4   1.6 K     4.5
//     flush         3
//   compact         1   1.6 K          (size == estimated-debt)
//    memtbl         1   4.0 M
//    bcache         4   752 B    7.7%  (score == hit-rate)
//    tcache         0     0 B    0.0%  (score == hit-rate)
//    titers         0
//    filter         -       -    0.0%  (score == utility)
//
// The WAL "in" metric is the size of the batches written to the WAL. The WAL
// "write" metric is the size of the physical data written to the WAL which
// includes record fragment overhead. Write amplification is computed as
// bytes-written / bytes-in, except for the total row where bytes-in is
// replaced with WAL-bytes-written + bytes-ingested.
func (m *Metrics) String() string {
	var buf bytes.Buffer
	var total LevelMetrics
	fmt.Fprintf(&buf, "__level_____count____size___score______in__ingest(sz_cnt)"+
		"____move(sz_cnt)___write(sz_cnt)____read___w-amp\n")
	m.formatWAL(&buf)
	for level := 0; level < numLevels; level++ {
		l := &m.Levels[level]
		fmt.Fprintf(&buf, "%7d ", level)
		l.format(&buf, fmt.Sprintf("%0.2f", l.Score))
		total.Add(l)
		total.NumFiles += l.NumFiles
		total.Size += l.Size
	}
	// Compute total bytes-in as the bytes written to the WAL + bytes ingested
	total.BytesIn = m.WAL.BytesWritten + total.BytesIngested
	// Add the total bytes-in to the total bytes-written. This is to account for
	// the bytes written to the log and bytes written externally and then
	// ingested.
	total.BytesWritten += total.BytesIn
	fmt.Fprintf(&buf, "  total ")
	total.format(&buf, "-")

	fmt.Fprintf(&buf, "  flush %9d\n", m.Flush.Count)
	fmt.Fprintf(&buf, "compact %9d %7s %7s  (size == estimated-debt)\n",
		m.Compact.Count,
		humanize.IEC.Uint64(m.Compact.EstimatedDebt),
		"")
	fmt.Fprintf(&buf, " memtbl %9d %7s\n",
		m.MemTable.Count,
		humanize.IEC.Uint64(m.MemTable.Size))
	formatCacheMetrics(&buf, &m.BlockCache, "bcache")
	formatCacheMetrics(&buf, &m.TableCache, "tcache")
	fmt.Fprintf(&buf, " titers %9d\n", m.TableIters)
	fmt.Fprintf(&buf, " filter %9s %7s %6.1f%%  (score == utility)\n",
		notApplicable,
		notApplicable,
		hitRate(m.Filter.Hits, m.Filter.Misses))
	return buf.String()
}

func hitRate(hits, misses int64) float64 {
	sum := hits + misses
	if sum == 0 {
		return 0
	}
	return 100 * float64(hits) / float64(sum)
}
