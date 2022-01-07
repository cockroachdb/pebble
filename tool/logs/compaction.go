// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package logs

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/humanize"
	"github.com/spf13/cobra"
)

var (
	// Matches either a compaction or a memtable flush log line.
	//
	// A compaction start / end line resembles:
	//   "compact(ed|ing)($TYPE) L($LEVEL)"
	//
	// A memtable flush start / end line resembles:
	//   "flush(ed|ing) ($N) memtables to L($LEVEL)"
	sentinelPattern          = regexp.MustCompile(`(?P<prefix>compact|flush)(?P<suffix>ed|ing)(?:\(.*\)\sL[0-9]|\s\d+\smemtables\sto\sL[0-9])`)
	sentinelPatternPrefixIdx = sentinelPattern.SubexpIndex("prefix")
	sentinelPatternSuffixIdx = sentinelPattern.SubexpIndex("suffix")

	// Example compaction start and end log lines:
	//
	//   I211215 14:26:56.012382 51831533 3@vendor/github.com/cockroachdb/pebble/compaction.go:1845 ⋮ [n5,pebble,s5] 1216510  [JOB 284925] compacting(default) L2 [442555] (4.2 M) + L3 [445853] (8.4 M)
	//   I211215 14:26:56.318543 51831533 3@vendor/github.com/cockroachdb/pebble/compaction.go:1886 ⋮ [n5,pebble,s5] 1216554  [JOB 284925] compacted(default) L2 [442555] (4.2 M) + L3 [445853] (8.4 M) -> L3 [445883 445887] (13 M), in 0.3s, output rate 42 M/s
	//
	// NOTE: we use the log timestamp to compute the compaction duration rather
	// than the Pebble log output.
	compactionPattern = regexp.MustCompile(
		`^.` +
			/* Timestamp       */ `(?P<timestamp>\d{6} \d{2}:\d{2}:\d{2}.\d{6}).*` +
			/* Node / Store    */ `\[n(?P<node>\d+|\?),.*?,s(?P<store>\d+|\?).*?\].*` +
			/* Job ID          */ `\[JOB (?P<job>\d+)]\s` +
			/* Start / end     */ `compact(?P<suffix>ed|ing)` +
			/* Compaction type */ `\((?P<type>.*?)\) ` +
			/* Start level     */ `L(?P<from>\d)(?:.*(?:\+|->)\s` +
			/* End level       */ `L(?P<to>\d))?` +
			/* Bytes           */ `(?:.*?\((?P<digit>.*?)\s(?P<unit>.*?)\))?`,
	)
	compactionPatternTimestampIdx = compactionPattern.SubexpIndex("timestamp")
	compactionPatternNode         = compactionPattern.SubexpIndex("node")
	compactionPatternStore        = compactionPattern.SubexpIndex("store")
	compactionPatternJobIdx       = compactionPattern.SubexpIndex("job")
	compactionPatternSuffixIdx    = compactionPattern.SubexpIndex("suffix")
	compactionPatternTypeIdx      = compactionPattern.SubexpIndex("type")
	compactionPatternFromIdx      = compactionPattern.SubexpIndex("from")
	compactionPatternToIdx        = compactionPattern.SubexpIndex("to")
	compactionPatternDigitIdx     = compactionPattern.SubexpIndex("digit")
	compactionPatternUnitIdx      = compactionPattern.SubexpIndex("unit")

	// Example memtable flush log lines:
	//
	//   I211213 16:23:48.903751 21136 3@vendor/github.com/cockroachdb/pebble/event.go:599 ⋮ [n9,pebble,s9] 24 [JOB 10] flushing 2 memtables to L0
	//   I211213 16:23:49.134464 21136 3@vendor/github.com/cockroachdb/pebble/event.go:603 ⋮ [n9,pebble,s9] 26 [JOB 10] flushed 2 memtables to L0 [1535806] (1.3 M), in 0.2s, output rate 5.8 M/s
	//
	// NOTE: we use the log timestamp to compute the flush duration rather than
	// the Pebble log output.
	flushPattern = regexp.MustCompile(
		`^.` +
			/* Timestamp    */ `(?P<timestamp>\d{6} \d{2}:\d{2}:\d{2}.\d{6})\s.*` +
			/* Node / Store */ `\[n(?P<node>\d+|\?),.*?,s(?P<store>\d+|\?).*?\].*` +
			/* Job ID       */ `\[JOB (?P<job>\d+)]\s` +
			/* Start / End  */ `flush(?P<suffix>ed|ing)` +
			/* Bytes        */ `(?:.*?\((?P<digit>.*?)\s(?P<unit>.*?)\))?`,
	)
	flushPatternTimestampIdx = flushPattern.SubexpIndex("timestamp")
	flushPatternNode         = flushPattern.SubexpIndex("node")
	flushPatternStore        = flushPattern.SubexpIndex("store")
	flushPatternSuffixIdx    = flushPattern.SubexpIndex("suffix")
	flushPatternJobIdx       = flushPattern.SubexpIndex("job")
	flushPatternDigitIdx     = flushPattern.SubexpIndex("digit")
	flushPatternUnitIdx      = flushPattern.SubexpIndex("unit")

	// Example read-amp log line:
	//
	//   I211215 14:55:15.802648 155 kv/kvserver/store.go:2668 ⋮ [n5,s5] 109057 +  total     44905   672 G       -   1.2 T   714 G   118 K   215 G    34 K   4.0 T   379 K   2.8 T     514     3.4
	//
	readAmpPattern = regexp.MustCompile(
		`^.` +
			/* Timestamp    */ `(?P<timestamp>\d{6} \d{2}:\d{2}:\d{2}.\d{6}).*` +
			/* Node / Store */ `\[n(?P<node>\d+|\?),s(?P<store>\d+|\?)\].*` +
			/* Read-amp     */ `total.*?(?P<value>\d+).{8}$`,
	)
	readAmpPatternTimestampIdx = readAmpPattern.SubexpIndex("timestamp")
	readAmpPatternNode         = readAmpPattern.SubexpIndex("node")
	readAmpPatternStore        = readAmpPattern.SubexpIndex("store")
	readAmpPatternValueIdx     = readAmpPattern.SubexpIndex("value")
)

const (
	// timeFmt matches the Cockroach log timestamp format.
	// See: https://github.com/cockroachdb/cockroach/blob/master/pkg/util/log/format_crdb_v2.go
	timeFmt = "060102 15:04:05.000000"

	// timeFmtSlim is similar to timeFmt, except that it strips components with a
	// lower granularity than a minute.
	timeFmtSlim = "060102 15:04"

	// timeFmtHrMinSec prints only the hour, minute and second of the time.
	timeFmtHrMinSec = "15:04:05"

	// pebbleLogPrefix is the well-known prefix for a pebble log file.
	// See:https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20200728_log_modernization.md
	pebbleLogPrefix = "cockroach-pebble"
)

// compactionType is the type of compaction. It tracks the types in
// compaction.go. We copy the values here to avoid exporting the types in
// compaction.go.
type compactionType uint8

const (
	compactionTypeDefault compactionType = iota
	compactionTypeFlush
	compactionTypeMove
	compactionTypeDeleteOnly
	compactionTypeElisionOnly
	compactionTypeRead
)

// String implements fmt.Stringer.
func (c compactionType) String() string {
	switch c {
	case compactionTypeDefault:
		return "default"
	case compactionTypeMove:
		return "move"
	case compactionTypeDeleteOnly:
		return "delete-only"
	case compactionTypeElisionOnly:
		return "elision-only"
	case compactionTypeRead:
		return "read"
	default:
		panic(errors.Newf("unknown compaction type: %s", c))
	}
}

// parseCompactionType parses the given compaction type string and returns a
// compactionType.
func parseCompactionType(s string) (t compactionType, err error) {
	switch s {
	case "default":
		t = compactionTypeDefault
	case "move":
		t = compactionTypeMove
	case "delete-only":
		t = compactionTypeDeleteOnly
	case "elision-only":
		t = compactionTypeElisionOnly
	case "read":
		t = compactionTypeRead
	default:
		err = errors.Newf("unknown compaction type: %s", s)
	}
	return
}

// compactionStart is a compaction start event.
type compactionStart struct {
	nodeID    int
	storeID   int
	jobID     int
	cType     compactionType
	time      time.Time
	fromLevel int
	toLevel   int
}

// parseCompactionStart converts the given regular expression sub-matches for a
// compaction start log line into a compactionStart event.
func parseCompactionStart(matches []string) (compactionStart, error) {
	var start compactionStart

	// Parse start time.
	tStart, err := time.Parse(timeFmt, matches[compactionPatternTimestampIdx])
	if err != nil {
		return start, errors.Newf("could not parse start time: %s", err)
	}

	// Parse node and store.
	nodeID, err := strconv.Atoi(matches[compactionPatternNode])
	if err != nil {
		if matches[compactionPatternNode] != "?" {
			return start, errors.Newf("could not parse node ID: %s", err)
		}
		nodeID = -1
	}

	storeID, err := strconv.Atoi(matches[compactionPatternStore])
	if err != nil {
		if matches[compactionPatternStore] != "?" {
			return start, errors.Newf("could not parse store ID: %s", err)
		}
		storeID = -1
	}

	// Parse job ID.
	jobID, err := strconv.Atoi(matches[compactionPatternJobIdx])
	if err != nil {
		return start, errors.Newf("could not parse jobID: %s", err)
	}

	// Parse compaction type.
	cType, err := parseCompactionType(matches[compactionPatternTypeIdx])
	if err != nil {
		return start, err
	}

	// Parse from-level.
	from, err := strconv.Atoi(matches[compactionPatternFromIdx])
	if err != nil {
		return start, errors.Newf("could not parse from-level: %s", err)
	}

	// Parse to-level. For deletion and elision compactions, set the same level.
	to := from
	if cType != compactionTypeElisionOnly && cType != compactionTypeDeleteOnly {
		to, err = strconv.Atoi(matches[compactionPatternToIdx])
		if err != nil {
			return start, errors.Newf("could not parse to-level: %s", err)
		}
	}

	start = compactionStart{
		nodeID:    nodeID,
		storeID:   storeID,
		jobID:     jobID,
		cType:     cType,
		time:      tStart,
		fromLevel: from,
		toLevel:   to,
	}

	return start, nil
}

// compactionEnd is a compaction end event.
type compactionEnd struct {
	nodeID         int
	storeID        int
	jobID          int
	time           time.Time
	compactedBytes uint64
}

// parseCompactionEnd converts the given regular expression sub-matches for a
// compaction end log line into a compactionEnd event.
func parseCompactionEnd(matches []string) (compactionEnd, error) {
	var end compactionEnd

	// Parse end time.
	tEnd, err := time.Parse(timeFmt, matches[compactionPatternTimestampIdx])
	if err != nil {
		return end, errors.Newf("could not parse start time: %s", err)
	}

	// Parse node and store.
	nodeID, err := strconv.Atoi(matches[compactionPatternNode])
	if err != nil {
		if matches[compactionPatternNode] != "?" {
			return end, errors.Newf("could not parse node ID: %s", err)
		}
		nodeID = -1
	}

	storeID, err := strconv.Atoi(matches[compactionPatternStore])
	if err != nil {
		if matches[compactionPatternStore] != "?" {
			return end, errors.Newf("could not parse store ID: %s", err)
		}
		storeID = -1
	}

	// Parse job ID.
	jobID, err := strconv.Atoi(matches[compactionPatternJobIdx])
	if err != nil {
		return end, errors.Newf("could not parse jobID: %s", err)
	}

	end = compactionEnd{
		nodeID:  nodeID,
		storeID: storeID,
		jobID:   jobID,
		time:    tEnd,
	}

	// Optionally, if we have compacted bytes.
	if matches[compactionPatternDigitIdx] != "" {
		d, e := strconv.ParseFloat(matches[compactionPatternDigitIdx], 64)
		if e != nil {
			return end, errors.Newf("could not parse compacted bytes digit: %s", e)
		}
		end.compactedBytes = unHumanize(d, matches[compactionPatternUnitIdx])
	}

	return end, nil
}

// parseFlushStart converts the given regular expression sub-matches for a
// memtable flush start log line into a compactionStart event.
func parseFlushStart(matches []string) (compactionStart, error) {
	var start compactionStart

	// Parse start time.
	tStart, err := time.Parse(timeFmt, matches[flushPatternTimestampIdx])
	if err != nil {
		return start, errors.Newf("could not parse start time: %s", err)
	}

	// Parse node and store.
	nodeID, err := strconv.Atoi(matches[flushPatternNode])
	if err != nil {
		if matches[flushPatternNode] != "?" {
			return start, errors.Newf("could not parse node ID: %s", err)
		}
		nodeID = -1
	}

	storeID, err := strconv.Atoi(matches[flushPatternStore])
	if err != nil {
		if matches[flushPatternStore] != "?" {
			return start, errors.Newf("could not parse store ID: %s", err)
		}
		storeID = -1
	}

	// Parse job ID.
	jobID, err := strconv.Atoi(matches[flushPatternJobIdx])
	if err != nil {
		return start, errors.Newf("could not parse jobID: %s", err)
	}

	c := compactionStart{
		nodeID:    nodeID,
		storeID:   storeID,
		jobID:     jobID,
		cType:     compactionTypeFlush,
		time:      tStart,
		fromLevel: -1,
		toLevel:   0,
	}
	return c, nil
}

// parseFlushEnd converts the given regular expression sub-matches for a
// memtable flush end log line into a compactionEnd event.
func parseFlushEnd(matches []string) (compactionEnd, error) {
	var end compactionEnd

	// Parse end time.
	tEnd, err := time.Parse(timeFmt, matches[flushPatternTimestampIdx])
	if err != nil {
		return end, errors.Newf("could not parse start time: %s", err)
	}

	// Parse node and store.
	nodeID, err := strconv.Atoi(matches[flushPatternNode])
	if err != nil {
		if matches[flushPatternNode] != "?" {
			return end, errors.Newf("could not parse node ID: %s", err)
		}
		nodeID = -1
	}

	storeID, err := strconv.Atoi(matches[flushPatternStore])
	if err != nil {
		if matches[flushPatternStore] != "?" {
			return end, errors.Newf("could not parse store ID: %s", err)
		}
		storeID = -1
	}

	// Parse job ID.
	jobID, err := strconv.Atoi(matches[flushPatternJobIdx])
	if err != nil {
		return end, errors.Newf("could not parse jobID: %s", err)
	}

	end = compactionEnd{
		nodeID:  nodeID,
		storeID: storeID,
		jobID:   jobID,
		time:    tEnd,
	}

	// Optionally, if we have flushed bytes.
	if matches[flushPatternDigitIdx] != "" {
		d, e := strconv.ParseFloat(matches[flushPatternDigitIdx], 64)
		if e != nil {
			return end, errors.Newf("could not parse flushed bytes digit: %s", e)
		}
		end.compactedBytes = unHumanize(d, matches[flushPatternUnitIdx])
	}

	return end, nil
}

// compaction represents an aggregated compaction event (i.e. the combination of
// a start and end event).
type compaction struct {
	nodeID         int
	storeID        int
	jobID          int
	cType          compactionType
	timeStart      time.Time
	timeEnd        time.Time
	fromLevel      int
	toLevel        int
	compactedBytes uint64
}

// readAmp represents a read-amp event.
type readAmp struct {
	nodeID  int
	storeID int
	time    time.Time
	readAmp int
}

// logEventCollector keeps track of open compaction events and read-amp events
// over the course of parsing log line events. Completed compaction events are
// added to the collector once a matching start and end pair are encountered.
// Read-amp events are added as they are encountered (the have no start / end
// concept).
type logEventCollector struct {
	m           map[int]compactionStart
	compactions []compaction
	readAmps    []readAmp
}

// newEventCollector instantiates a new logEventCollector.
func newEventCollector() *logEventCollector {
	return &logEventCollector{
		m: make(map[int]compactionStart),
	}
}

// addCompactionStart adds a new compactionStart to the collector. The event is
// tracked by its job ID.
func (c *logEventCollector) addCompactionStart(start compactionStart) error {
	if _, ok := c.m[start.jobID]; ok {
		return errors.Newf("start event already seen for job %d", start.jobID)
	}
	c.m[start.jobID] = start
	return nil
}

// addCompactionEnd completes the compaction event for the for the given
// compactionEnd.
func (c *logEventCollector) addCompactionEnd(end compactionEnd) {
	start, ok := c.m[end.jobID]
	if !ok {
		_, _ = fmt.Fprintf(
			os.Stderr,
			"compaction end event missing start event for job ID %d; skipping", end.jobID)
		return
	}

	// Remove the job from the collector once it has been matched.
	delete(c.m, end.jobID)

	c.compactions = append(c.compactions, compaction{
		nodeID:         start.nodeID,
		storeID:        start.storeID,
		jobID:          start.jobID,
		cType:          start.cType,
		timeStart:      start.time,
		timeEnd:        end.time,
		fromLevel:      start.fromLevel,
		toLevel:        start.toLevel,
		compactedBytes: end.compactedBytes,
	})
}

// addReadAmp adds the readAmp event to the collector.
func (c *logEventCollector) addReadAmp(time time.Time, nodeID, storeID int, val int) {
	c.readAmps = append(c.readAmps, readAmp{
		nodeID:  nodeID,
		storeID: storeID,
		time:    time,
		readAmp: val,
	})
}

// level is a level in the LSM. The WAL is level -1.
type level int

// String implements fmt.Stringer.
func (l level) String() string {
	if l == -1 {
		return "WAL"
	}
	return "L" + strconv.Itoa(int(l))
}

// fromTo is a map key for (from, to) level tuples.
type fromTo struct {
	from, to level
}

// compactionTypeCount is a mapping from compaction type to count.
type compactionTypeCount map[compactionType]int

// windowSummary summarizes events in a window of time between a start and end
// time. The window tracks:
// - for each compaction type: counts, total bytes compacted, and total duration.
// - read amp magnitudes
type windowSummary struct {
	nodeID, storeID  int
	tStart, tEnd     time.Time
	eventCount       int
	compactionCounts map[fromTo]compactionTypeCount
	compactionBytes  map[fromTo]uint64
	compactionTime   map[fromTo]time.Duration
	readAmps         []readAmp
	longRunning      []compaction
}

// String implements fmt.Stringer, returning a formatted window summary.
func (s windowSummary) String() string {
	type fromToCount struct {
		ft       fromTo
		counts   compactionTypeCount
		bytes    uint64
		duration time.Duration
	}
	var pairs []fromToCount
	for k, v := range s.compactionCounts {
		pairs = append(pairs, fromToCount{
			ft:       k,
			counts:   v,
			bytes:    s.compactionBytes[k],
			duration: s.compactionTime[k],
		})
	}
	sort.Slice(pairs, func(i, j int) bool {
		l, r := pairs[i], pairs[j]
		if l.ft.from == r.ft.from {
			return l.ft.to < r.ft.to
		}
		return l.ft.from < r.ft.from
	})

	nodeID, storeID := "?", "?"
	if s.nodeID != -1 {
		nodeID = strconv.Itoa(s.nodeID)
	}
	if s.storeID != -1 {
		storeID = strconv.Itoa(s.storeID)
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("node: %s, store: %s\n", nodeID, storeID))
	sb.WriteString(fmt.Sprintf("from: %s\n", s.tStart.Format(timeFmtSlim)))
	sb.WriteString(fmt.Sprintf("  to: %s\n", s.tEnd.Format(timeFmtSlim)))
	sb.WriteString("______from________to___default______move_____elide____delete_____flush_____total_____bytes______time\n")

	var totalDef, totalMove, totalElision, totalDel, totalFlush int
	var totalBytes uint64
	var totalTime time.Duration
	for _, p := range pairs {
		def := p.counts[compactionTypeDefault]
		move := p.counts[compactionTypeMove]
		elision := p.counts[compactionTypeElisionOnly]
		del := p.counts[compactionTypeDeleteOnly]
		flush := p.counts[compactionTypeFlush]
		total := def + move + elision + del + flush

		str := fmt.Sprintf("%10s %9s %9d %9d %9d %9d %9d %9d %9s %9s\n",
			p.ft.from, p.ft.to, def, move, elision, del, flush, total,
			humanize.Uint64(p.bytes), p.duration.Truncate(time.Second))
		sb.WriteString(str)

		totalDef += def
		totalMove += move
		totalElision += elision
		totalDel += del
		totalFlush += flush
		totalBytes += p.bytes
		totalTime += p.duration
	}

	var count, sum int
	for _, ra := range s.readAmps {
		count++
		sum += ra.readAmp
	}
	sb.WriteString(fmt.Sprintf("     total %19d %9d %9d %9d %9d %9d %9s %9s\n",
		totalDef, totalMove, totalElision, totalDel, totalFlush, s.eventCount,
		humanize.Uint64(totalBytes), totalTime.Truncate(time.Minute)))
	sb.WriteString(fmt.Sprintf("     r-amp%10.1f\n", float64(sum)/float64(count)))

	// (Optional) Long running compactions.
	if len(s.longRunning) > 0 {
		sb.WriteString("long-running compactions (descending runtime):\n")
		sb.WriteString("______from________to_______job______type_____start_______end____dur(s)_____bytes:\n")
		for _, e := range s.longRunning {
			sb.WriteString(fmt.Sprintf("%10s %9s %9d %9s %9s %9s %9.0f %9s\n",
				level(e.fromLevel), level(e.toLevel), e.jobID, e.cType,
				e.timeStart.Format(timeFmtHrMinSec), e.timeEnd.Format(timeFmtHrMinSec),
				e.timeEnd.Sub(e.timeStart).Seconds(), humanize.Uint64(e.compactedBytes)))
		}
	}

	return sb.String()
}

// compactionSlice is a slice of compaction events that sorts in order of node,
// store, then compaction event start time.
type compactionSlice []compaction

func (c compactionSlice) Len() int {
	return len(c)
}

func (c compactionSlice) Less(i, j int) bool {
	if c[i].nodeID != c[j].nodeID {
		return c[i].nodeID < c[j].nodeID
	}
	if c[i].storeID != c[j].storeID {
		return c[i].storeID < c[j].storeID
	}
	return c[i].timeStart.Before(c[j].timeStart)
}

func (c compactionSlice) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

// readAmpSlice is a slice of readAmp events that sorts in order of node, store,
// then read amp event start time.
type readAmpSlice []readAmp

func (r readAmpSlice) Len() int {
	return len(r)
}

func (r readAmpSlice) Less(i, j int) bool {
	// Sort by node, store, then read-amp.
	if r[i].nodeID != r[j].nodeID {
		return r[i].nodeID < r[j].nodeID
	}
	if r[i].storeID != r[j].storeID {
		return r[i].storeID < r[j].storeID
	}
	return r[i].time.Before(r[j].time)
}

func (r readAmpSlice) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

// aggregator combines compaction and read-amp events within windows of fixed
// duration and returns one aggregated windowSummary struct per window.
type aggregator struct {
	window           time.Duration
	compactions      []compaction
	readAmps         []readAmp
	longRunningLimit time.Duration
}

// newAggregator returns a new aggregator.
func newAggregator(
	window, longRunningLimit time.Duration,
	compactions []compaction,
	readAmps []readAmp,
) *aggregator {
	return &aggregator{
		window:           window,
		compactions:      compactions,
		readAmps:         readAmps,
		longRunningLimit: longRunningLimit,
	}
}

// aggregate aggregates the events into windows, returning the windowSummary for
// each interval.
func (a *aggregator) aggregate() []windowSummary {
	if len(a.compactions) == 0 {
		return nil
	}

	// Sort the compaction and read-amp slices by start time.
	sort.Sort(compactionSlice(a.compactions))
	sort.Sort(readAmpSlice(a.readAmps))

	newWindow := func(start, end time.Time) windowSummary {
		return windowSummary{
			tStart:           start,
			tEnd:             end,
			compactionCounts: make(map[fromTo]compactionTypeCount),
			compactionBytes:  make(map[fromTo]uint64),
			compactionTime:   make(map[fromTo]time.Duration),
		}
	}
	var windows []windowSummary
	windowStart := a.compactions[0].timeStart.Truncate(a.window)
	windowEnd := windowStart.Add(a.window)
	curWindow := newWindow(windowStart, windowEnd)

	var j int // index for read-amps
	for i, e := range a.compactions {
		// Update compaction counts.
		ft := fromTo{level(e.fromLevel), level(e.toLevel)}
		m, ok := curWindow.compactionCounts[ft]
		if !ok {
			m = make(compactionTypeCount)
			curWindow.compactionCounts[ft] = m
		}
		m[e.cType]++
		curWindow.eventCount++

		// Update compacted bytes.
		_, ok = curWindow.compactionBytes[ft]
		if !ok {
			curWindow.compactionBytes[ft] = 0
		}
		curWindow.compactionBytes[ft] += e.compactedBytes

		// Update compaction time.
		_, ok = curWindow.compactionTime[ft]
		if !ok {
			curWindow.compactionTime[ft] = 0
		}
		curWindow.compactionTime[ft] += e.timeEnd.Sub(e.timeStart)

		// Add "long-running" compactions. Those that start in this window that have
		// duration longer than the window interval.
		if e.timeEnd.Sub(e.timeStart) > a.longRunningLimit {
			curWindow.longRunning = append(curWindow.longRunning, e)
		}

		// If we're at the end of an interval, finalize the current window and start
		// a new one.
		if i == len(a.compactions)-1 ||
			a.compactions[i+1].nodeID != a.compactions[i].nodeID ||
			a.compactions[i+1].storeID != a.compactions[i].storeID ||
			a.compactions[i+1].timeStart.After(windowEnd) {
			// Collect read-amp values for the current window.
			var readAmps []readAmp
			for j < len(a.readAmps) {
				ra := a.readAmps[j]

				// Skip values before the current window.
				if ra.nodeID < a.compactions[i].nodeID ||
					ra.storeID < a.compactions[i].storeID ||
					ra.time.Before(windowStart) {
					j++
					continue
				}

				// We've passed over the current window. Stop here.
				if ra.nodeID > a.compactions[i].nodeID ||
					ra.storeID > a.compactions[i].storeID ||
					ra.time.After(windowEnd) {
					break
				}

				// Collect this read-amp value.
				readAmps = append(readAmps, ra)
				j++
			}
			curWindow.readAmps = readAmps

			curWindow.nodeID = e.nodeID
			curWindow.storeID = e.storeID

			// Sort long running compactions in descending order of duration.
			sort.Slice(curWindow.longRunning, func(i, j int) bool {
				l := curWindow.longRunning[i]
				r := curWindow.longRunning[j]
				return l.timeEnd.Sub(l.timeStart) > r.timeEnd.Sub(r.timeStart)
			})

			// Add the completed window to the set of windows.
			windows = append(windows, curWindow)

			// Start a new window.
			windowStart = windowStart.Add(a.window)
			windowEnd = windowEnd.Add(a.window)
			curWindow = newWindow(windowStart, windowEnd)
		}
	}

	return windows
}

type parseFn func(string, *logEventCollector) error

// parseLog parses the log file with the given path, using the given parse
// function to collect events in the given logEventCollector.
func parseLog(path string, b *logEventCollector, parseFn parseFn) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	for s.Scan() {
		if err := parseFn(s.Text(), b); err != nil {
			return err
		}
	}

	return nil
}

// parseCompaction is a parseFn that parses parses and collects Pebble
// compaction events.
func parseCompaction(line string, b *logEventCollector) error {
	matches := compactionPattern.FindStringSubmatch(line)
	if matches == nil {
		return nil
	}

	if len(matches) != 11 {
		return errors.Newf(
			"could not parse compaction start / end line; found %d matches: %s",
			len(matches), line)
	}

	// "compacting": implies start line.
	if matches[compactionPatternSuffixIdx] == "ing" {
		start, err := parseCompactionStart(matches)
		if err != nil {
			return err
		}
		if err := b.addCompactionStart(start); err != nil {
			return err
		}
		return nil
	}

	// "compacted": implies end line.
	end, err := parseCompactionEnd(matches)
	if err != nil {
		return err
	}

	b.addCompactionEnd(end)
	return nil
}

// parseFlush is a parseFn that parses parses and collects Pebble memtable flush
// events.
func parseFlush(line string, b *logEventCollector) error {
	matches := flushPattern.FindStringSubmatch(line)
	if matches == nil {
		return nil
	}

	if len(matches) != 8 {
		return errors.Newf(
			"could not parse flush start / end line; found %d matches: %s",
			len(matches), line)
	}

	if matches[flushPatternSuffixIdx] == "ing" {
		start, err := parseFlushStart(matches)
		if err != nil {
			return err
		}
		return b.addCompactionStart(start)
	}

	end, err := parseFlushEnd(matches)
	if err != nil {
		return err
	}

	b.addCompactionEnd(end)
	return nil
}

// parsePebbleFn is a parseFn that parses parses and collects Pebble log lines
// into compaction events.
func parsePebbleFn(line string, b *logEventCollector) error {
	matches := sentinelPattern.FindStringSubmatch(line)
	if matches == nil {
		return nil
	}

	// Determine which regexp to apply by testing the first letter of the prefix.
	switch matches[sentinelPatternPrefixIdx][0] {
	case 'c':
		return parseCompaction(line, b)
	case 'f':
		return parseFlush(line, b)
	default:
		return errors.Newf("unexpected line: neither compaction nor flush: %s", line)
	}
}

// parsePebbleFn is a parseFn that parses parses and collects Cockroach log
// lines into read-amp events.
func parseCockroachFn(line string, b *logEventCollector) error {
	matches := readAmpPattern.FindStringSubmatch(line)
	if matches == nil {
		return nil
	}

	// Parse start time.
	t, err := time.Parse(timeFmt, matches[readAmpPatternTimestampIdx])
	if err != nil {
		return errors.Newf("could not parse start time: %s", err)
	}

	// Parse node and store.
	nodeID, err := strconv.Atoi(matches[readAmpPatternNode])
	if err != nil {
		if matches[readAmpPatternNode] != "?" {
			return errors.Newf("could not parse node ID: %s", err)
		}
		nodeID = -1
	}

	storeID, err := strconv.Atoi(matches[readAmpPatternStore])
	if err != nil {
		if matches[readAmpPatternStore] != "?" {
			return errors.Newf("could not parse store ID: %s", err)
		}
		storeID = -1
	}

	// Parse read-amp.
	ra, err := strconv.Atoi(matches[readAmpPatternValueIdx])
	if err != nil {
		return errors.Newf("could not parse read-amp: %s", err)
	}

	b.addReadAmp(t, nodeID, storeID, ra)
	return nil
}

// runCompactionLogs is runnable function of the top-level cobra.Command that
// parses and collects Pebble compaction events and LSM information.
func runCompactionLogs(cmd *cobra.Command, args []string) error {
	// The args contain a list of log files to read.
	files := args

	// Scan the log files collecting start and end compaction lines.
	b := newEventCollector()
	for _, file := range files {
		// If the filename contains 'cockroach-pebble', parse as a Pebble log file.
		if strings.HasPrefix(filepath.Base(file), pebbleLogPrefix) {
			err := parseLog(file, b, parsePebbleFn)
			if err != nil {
				return err
			}
			continue
		}

		// Otherwise parse as a DB log.
		if err := parseLog(file, b, parseCockroachFn); err != nil {
			return err
		}
	}

	window, err := cmd.Flags().GetDuration("window")
	if err != nil {
		return err
	}

	longRunningLimit, err := cmd.Flags().GetDuration("long-running-limit")
	if err != nil {
		return err
	}
	if longRunningLimit == 0 {
		// Off by default. Set to infinite duration.
		longRunningLimit = time.Duration(math.MaxInt64)
	}

	// Aggregate the lines.
	a := newAggregator(window, longRunningLimit, b.compactions, b.readAmps)
	summaries := a.aggregate()
	for _, s := range summaries {
		fmt.Printf("%s\n", s)
	}

	return nil
}

// unHumanize performs the opposite of humanize.Uint64, converting a
// human-readable digit and unit into a raw number of bytes.
func unHumanize(d float64, u string) uint64 {
	if u == "" {
		return uint64(d)
	}

	multiplier := uint64(1)
	switch u {
	case "B":
		// no-op: treat as regular bytes.
	case "K":
		multiplier = 1 << 10
	case "M":
		multiplier = 1 << 20
	case "G":
		multiplier = 1 << 30
	case "T":
		multiplier = 1 << 40
	case "P":
		multiplier = 1 << 50
	case "E":
		multiplier = 1 << 60
	default:
		panic(errors.Newf("unknown unit %q", u))
	}

	return uint64(d) * multiplier
}
