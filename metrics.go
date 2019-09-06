// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/pebble/internal/humanize"
)

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
	// The number of bytes ingested.
	BytesIngested uint64
	// The number of bytes moved into the level by a "move" compaction.
	BytesMoved uint64
	// The number of bytes read for compactions at the level. This includes bytes
	// read from other levels (BytesIn), as well as bytes read for the level.
	BytesRead uint64
	// The number of bytes written during compactions.
	BytesWritten uint64
}

// Add updates the counter metrics for the level.
func (m *LevelMetrics) Add(u *LevelMetrics) {
	m.BytesIn += u.BytesIn
	m.BytesIngested += u.BytesIngested
	m.BytesMoved += u.BytesMoved
	m.BytesRead += u.BytesRead
	m.BytesWritten += u.BytesWritten
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
func (m *LevelMetrics) format(buf *bytes.Buffer) {
	fmt.Fprintf(buf, "%6d %7s %7.2f %7s %7s %7s %7s %7s %7.1f\n",
		m.NumFiles,
		humanize.Uint64(m.Size),
		m.Score,
		humanize.Uint64(m.BytesIn),
		humanize.Uint64(m.BytesIngested),
		humanize.Uint64(m.BytesMoved),
		humanize.Uint64(m.BytesRead),
		humanize.Uint64(m.BytesWritten),
		m.WriteAmp(),
	)
}

// VersionMetrics holds metrics for each level.
type VersionMetrics struct {
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
	Levels [numLevels]LevelMetrics
}

func (m *VersionMetrics) formatWAL(buf *bytes.Buffer) {
	var writeAmp float64
	if m.WAL.BytesIn > 0 {
		writeAmp = float64(m.WAL.BytesWritten) / float64(m.WAL.BytesIn)
	}
	fmt.Fprintf(buf, "  WAL %6d %7s       - %7s       -       -       - %7s %7.1f\n",
		m.WAL.Files,
		humanize.Uint64(m.WAL.Size),
		humanize.Uint64(m.WAL.BytesIn),
		humanize.Uint64(m.WAL.BytesWritten),
		writeAmp)
}

// Pretty-print the metrics, showing a line for the WAL, a line per-level, and
// a total:
//
//   level__files____size___score______in__ingest____move____read___write___w-amp
//     WAL      1    53 M       -   744 M       -       -       -   765 M     1.0
//       0      6   285 M    3.00   712 M     0 B     0 B     0 B   707 M     1.0
//       1      0     0 B    0.00     0 B     0 B     0 B     0 B     0 B     0.0
//       2      0     0 B    0.00     0 B     0 B     0 B     0 B     0 B     0.0
//       3      0     0 B    0.00     0 B     0 B     0 B     0 B     0 B     0.0
//       4      0     0 B    0.00     0 B     0 B     0 B     0 B     0 B     0.0
//       5     80   312 M    1.09   328 M     0 B     0 B   580 M   580 M     1.8
//       6     23   110 M    0.35   110 M     0 B     0 B   146 M   146 M     1.3
//   total    109   706 M    0.00   765 M     0 B     0 B   726 M   2.1 G     2.9
//
// The WAL "in" metric is the size of the batches written to the WAL. The WAL
// "write" metric is the size of the physical data written to the WAL which
// includes record fragment overhead. Write amplification is computed as
// bytes-written / bytes-in, except for the total row where bytes-in is
// replaced with WAL-bytes-written + bytes-ingested.
func (m *VersionMetrics) String() string {
	var buf bytes.Buffer
	var total LevelMetrics
	fmt.Fprintf(&buf, "level__files____size___score______in__ingest____move____read___write___w-amp\n")
	m.formatWAL(&buf)
	for level := 0; level < numLevels; level++ {
		l := &m.Levels[level]
		fmt.Fprintf(&buf, "%5d ", level)
		l.format(&buf)
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
	fmt.Fprintf(&buf, "total ")
	total.format(&buf)
	return buf.String()
}
