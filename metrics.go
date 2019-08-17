// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"

	"github.com/petermattis/pebble/internal/humanize"
	"github.com/petermattis/pebble/internal/manifest"
)

// LevelMetrics exports the manifest.LevelMetrics type.
type LevelMetrics = manifest.LevelMetrics

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
		l.Format(&buf)
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
	total.Format(&buf)
	return buf.String()
}
