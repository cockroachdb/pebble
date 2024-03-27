package pebble

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/humanize"
	"github.com/cockroachdb/pebble/internal/manifest"
)

// SpanLevelStats contains statistics for a span in a given level of the LSM.
type SpanLevelStats struct {
	NumFiles   int
	TotalBytes uint64
}

// SpanStats contains statistics for a span in the LSM.
type SpanStats struct {
	// Levels contains aggregate statistics for each level in the LSM. Level 0 is
	// aggregated across all the constituent sub-levels.
	Levels [numLevels]SpanLevelStats
	// L0SubLevels contains statistics for each sub-level in L0. The oldest
	// sublevel is the first element of the slice.
	L0Sublevels []SpanLevelStats
}

// TotalReadAmp returns the total read amplification for the span.
func (s SpanStats) TotalReadAmp() int {
	var rAmp int
	for i, level := range s.Levels {
		if level.NumFiles == 0 {
			continue
		}
		if i == 0 {
			rAmp += len(s.L0Sublevels)
		} else {
			rAmp++
		}
	}
	return rAmp
}

// TotalFiles returns the total number of files that overlap the span.
func (s SpanStats) TotalFiles() int {
	var nFiles int
	for _, level := range s.Levels {
		nFiles += level.NumFiles
	}
	return nFiles
}

// TotalBytes returns the total number of bytes across all files that overlap
// the span.
func (s SpanStats) TotalBytes() uint64 {
	var nBytes uint64
	for _, level := range s.Levels {
		nBytes += level.TotalBytes
	}
	return nBytes
}

// String implements fmt.Stringer, printing a summary of the span, one line for
// each level (and sub-level).
func (s SpanStats) String() string {
	var b bytes.Buffer
	_, _ = fmt.Fprintln(&b, "___level___files________size___r-amp")
	printLevelStats := func(levelIdx string, l SpanLevelStats) {
		var rAmp int
		if l.NumFiles > 0 {
			rAmp = 1
		}
		_, _ = fmt.Fprintf(
			&b, "%8s %7d %11s %7d\n",
			levelIdx, l.NumFiles, humanize.Uint64(l.TotalBytes), rAmp,
		)
	}
	// Print L0 sublevel individually, in reverse order (youngest first).
	for i := len(s.L0Sublevels) - 1; i >= 0; i-- {
		printLevelStats(fmt.Sprintf("0.%d", i), s.L0Sublevels[i])
	}
	// Print remaining levels.
	for i := 1; i < len(s.Levels); i++ {
		printLevelStats(strconv.Itoa(i), s.Levels[i])
	}
	_, _ = fmt.Fprintf(
		&b, "   total %7d %11s %7d\n",
		s.TotalFiles(), humanize.Uint64(s.TotalBytes()), s.TotalReadAmp(),
	)
	return b.String()
}

// DebugSpan returns a SpanStats for a span between the given start and end
// bounds (inclusive, and exclusive, respectively).
//
// Passing `nil` for a start and end bound will return a SpanStats covering the
// entire LSM, whose output should match that of Metrics.
//
// If `start` is non-nil, then `end > start`.
func (d *DB) DebugSpan(start, end []byte) (stats SpanStats, _ error) {
	if start != nil && d.cmp(start, end) >= 0 {
		return stats, errors.Errorf("debug span: end must be > start")
	}

	rs := d.loadReadState()
	defer rs.unref()
	v := rs.current

	levelStats := func(iter manifest.LevelIterator) (lStats SpanLevelStats) {
		iter.SeekGE(d.cmp, start)
		for m := iter.Current(); m != nil; m = iter.Next() {
			if end != nil && d.cmp(m.Smallest.UserKey, end) >= 0 /* exclusive end */ {
				// We've iterated too far.
				break
			}
			lStats.NumFiles++
			lStats.TotalBytes += m.Size
		}
		return
	}
	// L0 has its files sorted by seqnum. Use the L0 sublevels to compute the span
	// stats, as each sublevel has its files sorted by start key.
	stats.L0Sublevels = make([]SpanLevelStats, len(v.L0Sublevels.Levels))
	for i, subLevel := range v.L0Sublevels.Levels {
		s := levelStats(subLevel.Iter())
		if s.NumFiles == 0 {
			continue
		}
		// Update per-sublevel stats.
		stats.L0Sublevels[i].NumFiles += s.NumFiles
		stats.L0Sublevels[i].TotalBytes += s.TotalBytes
		// Update aggregate sublevel stats.
		stats.Levels[0].NumFiles += s.NumFiles
		stats.Levels[0].TotalBytes += s.TotalBytes
	}
	// Levels > 0 can use a regular level iterator.
	for i := 1; i < numLevels; i++ {
		stats.Levels[i] = levelStats(v.Levels[i].Iter())
	}
	return stats, nil
}
