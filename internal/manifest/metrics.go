// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"bytes"
	"fmt"

	"github.com/petermattis/pebble/internal/humanize"
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

// Format generates a string of the receiver's metrics, formatting it into the
// supplied buffer.
func (m *LevelMetrics) Format(buf *bytes.Buffer) {
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
