// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package logs

import (
	"bufio"
	"fmt"
	"math"
	"os"
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
	// Captures a common logging prefix that can be used as the context for the
	// surrounding information captured by other expressions. Example:
	//
	//   I211215 14:26:56.012382 51831533 3@vendor/github.com/cockroachdb/pebble/compaction.go:1845 ⋮ [n5,pebble,s5] ...
	//
	logContextPattern = regexp.MustCompile(
		`^.*` +
			/* Timestamp        */ `(?P<timestamp>\d{6} \d{2}:\d{2}:\d{2}.\d{6}).*` +
			/* Node / Store     */ `\[n(?P<node>\d+|\?),.*?,s(?P<store>\d+|\?).*?\].*`,
	)
	logContextPatternTimestampIdx = logContextPattern.SubexpIndex("timestamp")
	logContextPatternNodeIdx      = logContextPattern.SubexpIndex("node")
	logContextPatternStoreIdx     = logContextPattern.SubexpIndex("store")

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
		`^.*` +
			/* Job ID           */ `\[JOB (?P<job>\d+)]\s` +
			/* Start / end      */ `compact(?P<suffix>ed|ing)` +
			/* Compaction type  */ `\((?P<type>.*?)\)\s` +
			/* Start /end level */ `L(?P<from>\d)(?:.*(?:\+|->)\sL(?P<to>\d))?` +
			/* Bytes            */ `(?:.*?\((?P<digit>.*?)\s(?P<unit>.*?)\))?`,
	)
	compactionPatternJobIdx    = compactionPattern.SubexpIndex("job")
	compactionPatternSuffixIdx = compactionPattern.SubexpIndex("suffix")
	compactionPatternTypeIdx   = compactionPattern.SubexpIndex("type")
	compactionPatternFromIdx   = compactionPattern.SubexpIndex("from")
	compactionPatternToIdx     = compactionPattern.SubexpIndex("to")
	compactionPatternDigitIdx  = compactionPattern.SubexpIndex("digit")
	compactionPatternUnitIdx   = compactionPattern.SubexpIndex("unit")

	// Example memtable flush log lines:
	//
	//   I211213 16:23:48.903751 21136 3@vendor/github.com/cockroachdb/pebble/event.go:599 ⋮ [n9,pebble,s9] 24 [JOB 10] flushing 2 memtables to L0
	//   I211213 16:23:49.134464 21136 3@vendor/github.com/cockroachdb/pebble/event.go:603 ⋮ [n9,pebble,s9] 26 [JOB 10] flushed 2 memtables to L0 [1535806] (1.3 M), in 0.2s, output rate 5.8 M/s
	//
	// NOTE: we use the log timestamp to compute the flush duration rather than
	// the Pebble log output.
	flushPattern = regexp.MustCompile(
		`^..*` +
			/* Job ID          */ `\[JOB (?P<job>\d+)]\s` +
			/* Compaction type */ `flush(?P<suffix>ed|ing)` +
			/* Bytes           */ `(?:.*?\((?P<digit>.*?)\s(?P<unit>.*?)\))?`,
	)
	flushPatternSuffixIdx = flushPattern.SubexpIndex("suffix")
	flushPatternJobIdx    = flushPattern.SubexpIndex("job")
	flushPatternDigitIdx  = flushPattern.SubexpIndex("digit")
	flushPatternUnitIdx   = flushPattern.SubexpIndex("unit")

	// Example read-amp log line:
	//
	//   total     31766   188 G       -   257 G   187 G    48 K   3.6 G     744   536 G    49 K   278 G       5     2.1
	//
	readAmpPattern = regexp.MustCompile(
		/* Read-amp     */ `(?:^|\+)\s{2}total.*?(?P<value>\d+).{8}$`,
	)
	readAmpPatternValueIdx = readAmpPattern.SubexpIndex("value")
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
	ctx       logContext
	jobID     int
	cType     compactionType
	fromLevel int
	toLevel   int
}

// parseCompactionStart converts the given regular expression sub-matches for a
// compaction start log line into a compactionStart event.
func parseCompactionStart(matches []string) (compactionStart, error) {
	var start compactionStart

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
		jobID:     jobID,
		cType:     cType,
		fromLevel: from,
		toLevel:   to,
	}

	return start, nil
}

// compactionEnd is a compaction end event.
type compactionEnd struct {
	jobID          int
	compactedBytes uint64
}

// parseCompactionEnd converts the given regular expression sub-matches for a
// compaction end log line into a compactionEnd event.
func parseCompactionEnd(matches []string) (compactionEnd, error) {
	var end compactionEnd

	// Parse job ID.
	jobID, err := strconv.Atoi(matches[compactionPatternJobIdx])
	if err != nil {
		return end, errors.Newf("could not parse jobID: %s", err)
	}
	end = compactionEnd{jobID: jobID}

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
	// Parse job ID.
	jobID, err := strconv.Atoi(matches[flushPatternJobIdx])
	if err != nil {
		return start, errors.Newf("could not parse jobID: %s", err)
	}
	c := compactionStart{
		jobID:     jobID,
		cType:     compactionTypeFlush,
		fromLevel: -1,
		toLevel:   0,
	}
	return c, nil
}

// parseFlushEnd converts the given regular expression sub-matches for a
// memtable flush end log line into a compactionEnd event.
func parseFlushEnd(matches []string) (compactionEnd, error) {
	var end compactionEnd

	// Parse job ID.
	jobID, err := strconv.Atoi(matches[flushPatternJobIdx])
	if err != nil {
		return end, errors.Newf("could not parse jobID: %s", err)
	}
	end = compactionEnd{jobID: jobID}

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
	ctx     logContext
	readAmp int
}

type nodeStoreJob struct {
	node, store, job int
}

func (n nodeStoreJob) String() string {
	return fmt.Sprintf("(node=%d,store=%d,job=%d)", n.node, n.store, n.job)
}

// logEventCollector keeps track of open compaction events and read-amp events
// over the course of parsing log line events. Completed compaction events are
// added to the collector once a matching start and end pair are encountered.
// Read-amp events are added as they are encountered (the have no start / end
// concept).
type logEventCollector struct {
	ctx         logContext
	m           map[nodeStoreJob]compactionStart
	compactions []compaction
	readAmps    []readAmp
}

// newEventCollector instantiates a new logEventCollector.
func newEventCollector() *logEventCollector {
	return &logEventCollector{
		m: make(map[nodeStoreJob]compactionStart),
	}
}

// addCompactionStart adds a new compactionStart to the collector. The event is
// tracked by its job ID.
func (c *logEventCollector) addCompactionStart(start compactionStart) error {
	key := nodeStoreJob{c.ctx.node, c.ctx.store, start.jobID}
	if _, ok := c.m[key]; ok {
		return errors.Newf("start event already seen for %s", key)
	}
	start.ctx = c.ctx
	c.m[key] = start
	return nil
}

// addCompactionEnd completes the compaction event for the given compactionEnd.
func (c *logEventCollector) addCompactionEnd(end compactionEnd) {
	key := nodeStoreJob{c.ctx.node, c.ctx.store, end.jobID}
	start, ok := c.m[key]
	if !ok {
		_, _ = fmt.Fprintf(
			os.Stderr,
			"compaction end event missing start event for %s; skipping\n", key,
		)
		return
	}

	// Remove the job from the collector once it has been matched.
	delete(c.m, key)

	c.compactions = append(c.compactions, compaction{
		nodeID:         start.ctx.node,
		storeID:        start.ctx.store,
		jobID:          start.jobID,
		cType:          start.cType,
		timeStart:      start.ctx.timestamp,
		timeEnd:        c.ctx.timestamp,
		fromLevel:      start.fromLevel,
		toLevel:        start.toLevel,
		compactedBytes: end.compactedBytes,
	})
}

// addReadAmp adds the readAmp event to the collector.
func (c *logEventCollector) addReadAmp(ra readAmp) {
	ra.ctx = c.ctx
	c.readAmps = append(c.readAmps, ra)
}

// logContext captures the metadata of log lines.
type logContext struct {
	timestamp   time.Time
	node, store int
}

// saveContext saves the given logContext in the collector.
func (c *logEventCollector) saveContext(ctx logContext) {
	c.ctx = ctx
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
	if r[i].ctx.node != r[j].ctx.node {
		return r[i].ctx.node < r[j].ctx.node
	}
	if r[i].ctx.store != r[j].ctx.store {
		return r[i].ctx.store < r[j].ctx.store
	}
	return r[i].ctx.timestamp.Before(r[j].ctx.timestamp)
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

	initWindow := func(c compaction) *windowSummary {
		start := c.timeStart.Truncate(a.window)
		return &windowSummary{
			nodeID:           c.nodeID,
			storeID:          c.storeID,
			tStart:           start,
			tEnd:             start.Add(a.window),
			compactionCounts: make(map[fromTo]compactionTypeCount),
			compactionBytes:  make(map[fromTo]uint64),
			compactionTime:   make(map[fromTo]time.Duration),
		}
	}

	var windows []windowSummary
	var j int // index for read-amps
	finishWindow := func(cur *windowSummary) {
		// Collect read-amp values for the previous window.
		var readAmps []readAmp
		for j < len(a.readAmps) {
			ra := a.readAmps[j]

			// Skip values before the current window.
			if ra.ctx.node < cur.nodeID ||
				ra.ctx.store < cur.storeID ||
				ra.ctx.timestamp.Before(cur.tStart) {
				j++
				continue
			}

			// We've passed over the current window. Stop.
			if ra.ctx.node > cur.nodeID ||
				ra.ctx.store > cur.storeID ||
				ra.ctx.timestamp.After(cur.tEnd) {
				break
			}

			// Collect this read-amp value.
			readAmps = append(readAmps, ra)
			j++
		}
		cur.readAmps = readAmps

		// Sort long running compactions in descending order of duration.
		sort.Slice(cur.longRunning, func(i, j int) bool {
			l := cur.longRunning[i]
			r := cur.longRunning[j]
			return l.timeEnd.Sub(l.timeStart) > r.timeEnd.Sub(r.timeStart)
		})

		// Add the completed window to the set of windows.
		windows = append(windows, *cur)
	}

	// Move through the compactions, collecting relevant compactions into the same
	// window. Windows have the same node and store, and a compaction start time
	// within a given range.
	i := 0
	curWindow := initWindow(a.compactions[i])
	for ; ; i++ {
		// No more windows. Complete the current window.
		if i == len(a.compactions) {
			finishWindow(curWindow)
			break
		}
		c := a.compactions[i]

		// If we're at the start of a new interval, finalize the current window and
		// start a new one.
		if curWindow.nodeID != c.nodeID ||
			curWindow.storeID != c.storeID ||
			c.timeStart.After(curWindow.tEnd) {
			finishWindow(curWindow)
			curWindow = initWindow(c)
		}

		// Update compaction counts.
		ft := fromTo{level(c.fromLevel), level(c.toLevel)}
		m, ok := curWindow.compactionCounts[ft]
		if !ok {
			m = make(compactionTypeCount)
			curWindow.compactionCounts[ft] = m
		}
		m[c.cType]++
		curWindow.eventCount++

		// Update compacted bytes.
		_, ok = curWindow.compactionBytes[ft]
		if !ok {
			curWindow.compactionBytes[ft] = 0
		}
		curWindow.compactionBytes[ft] += c.compactedBytes

		// Update compaction time.
		_, ok = curWindow.compactionTime[ft]
		if !ok {
			curWindow.compactionTime[ft] = 0
		}
		curWindow.compactionTime[ft] += c.timeEnd.Sub(c.timeStart)

		// Add "long-running" compactions. Those that start in this window that have
		// duration longer than the window interval.
		if c.timeEnd.Sub(c.timeStart) > a.longRunningLimit {
			curWindow.longRunning = append(curWindow.longRunning, c)
		}
	}

	return windows
}

// parseLog parses the log file with the given path, using the given parse
// function to collect events in the given logEventCollector.
func parseLog(path string, b *logEventCollector) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	for s.Scan() {
		line := s.Text()

		// Store the log context for the current line, if we have one.
		if err := parseLogContext(line, b); err != nil {
			return err
		}

		// First check for a flush or compaction.
		matches := sentinelPattern.FindStringSubmatch(line)
		if matches != nil {
			// Determine which regexp to apply by testing the first letter of the prefix.
			switch matches[sentinelPatternPrefixIdx][0] {
			case 'c':
				if err = parseCompaction(line, b); err != nil {
					return err
				}
			case 'f':
				if err = parseFlush(line, b); err != nil {
					return err
				}
			default:
				return errors.Newf("unexpected line: neither compaction nor flush: %s", line)
			}
			continue
		}

		// Else check for an LSM debug line containing the read-amp value.
		if err = parseReadAmp(line, b); err != nil {
			return err
		}
	}

	return nil
}

// parseLogContext extracts contextual information from the log line (e.g. the
// timestamp, node and store).
func parseLogContext(line string, b *logEventCollector) error {
	matches := logContextPattern.FindStringSubmatch(line)
	if matches == nil {
		return nil
	}

	// Parse start time.
	t, err := time.Parse(timeFmt, matches[logContextPatternTimestampIdx])
	if err != nil {
		return errors.Newf("could not parse timestamp: %s", err)
	}

	// Parse node and store.
	nodeID, err := strconv.Atoi(matches[logContextPatternNodeIdx])
	if err != nil {
		if matches[logContextPatternNodeIdx] != "?" {
			return errors.Newf("could not parse node ID: %s", err)
		}
		nodeID = -1
	}

	storeID, err := strconv.Atoi(matches[logContextPatternStoreIdx])
	if err != nil {
		if matches[logContextPatternStoreIdx] != "?" {
			return errors.Newf("could not parse store ID: %s", err)
		}
		storeID = -1
	}

	b.saveContext(logContext{
		timestamp: t,
		node:      nodeID,
		store:     storeID,
	})
	return nil
}

// parseCompaction parses and collects Pebble compaction events.
func parseCompaction(line string, b *logEventCollector) error {
	matches := compactionPattern.FindStringSubmatch(line)
	if matches == nil {
		return nil
	}

	if len(matches) != 8 {
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

// parseFlush parses and collects Pebble memtable flush events.
func parseFlush(line string, b *logEventCollector) error {
	matches := flushPattern.FindStringSubmatch(line)
	if matches == nil {
		return nil
	}

	if len(matches) != 5 {
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

// parseReadAmp attempts to parse the current line as a read amp value
func parseReadAmp(line string, b *logEventCollector) error {
	matches := readAmpPattern.FindStringSubmatch(line)
	if matches == nil {
		return nil
	}
	val, err := strconv.Atoi(matches[readAmpPatternValueIdx])
	if err != nil {
		return errors.Newf("could not parse read amp: %s", err)
	}
	b.addReadAmp(readAmp{
		readAmp: val,
	})
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
		err := parseLog(file, b)
		if err != nil {
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
