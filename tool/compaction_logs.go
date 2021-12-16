package tool

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
	sentinelPattern = regexp.MustCompile(`(compact|flush)(ed|ing)(?:(?:.*?\sL[0-9])|(?:.*?to\sL[0-9]))`)
	//sentinelPattern = regexp.MustCompile(`(?:(c)ompact(ed|ing)(?:.*?\sL[0-9]))|(?:(f)lush(ed|ing)(?:.*?to\sL[0-9]))`)

	// Example compaction start and end log lines:
	//
	//   I211215 14:26:56.012382 51831533 3@vendor/github.com/cockroachdb/pebble/compaction.go:1845 ⋮ [n5,pebble,s5] 1216510  [JOB 284925] compacting(default) L2 [442555] (4.2 M) + L3 [445853] (8.4 M)
	//   I211215 14:26:56.318543 51831533 3@vendor/github.com/cockroachdb/pebble/compaction.go:1886 ⋮ [n5,pebble,s5] 1216554  [JOB 284925] compacted(default) L2 [442555] (4.2 M) + L3 [445853] (8.4 M) -> L3 [445883 445887] (13 M), in 0.3s, output rate 42 M/s
	//
	// Match groups:
	// - 1: datetime
	// - 2: job
	// - 3: compact suffix ("ed" vs "ing)
	// - 4: compaction type
	// - 5: from level
	// - 6: to level
	// - 7: compaction size (digit)
	// - 8: compaction size (unit)
	compactionPattern = regexp.MustCompile(`^.(\d{6} \d{2}:\d{2}:\d{2}.\d{6})\s.*\[JOB (\d+)] compact(ed|ing)\((.*?)\) L(\d)(?:.*(?:\+|->) L(\d))?(?:.*?\((.*?)\s(.*?)\))?`)

	// Example memtable flush log lines:
	//
	//   I211213 16:23:48.903751 21136 3@vendor/github.com/cockroachdb/pebble/event.go:599 ⋮ [n9,pebble,s9] 24 [JOB 10] flushing 2 memtables to L0
	//   I211213 16:23:49.134464 21136 3@vendor/github.com/cockroachdb/pebble/event.go:603 ⋮ [n9,pebble,s9] 26 [JOB 10] flushed 2 memtables to L0 [1535806] (1.3 M), in 0.2s, output rate 5.8 M/s
	//
	// Match groups:
	// - 1: datetime
	// - 2: job
	// - 3: flush suffix ("ed" vs "ing)
	// - 4: flush size (digit)
	// - 5: flush size (unit)
	flushPattern = regexp.MustCompile(`^.(\d{6} \d{2}:\d{2}:\d{2}.\d{6})\s.*\[JOB (\d+)] flush(ed|ing)(?:.*?\((.*?)\s(.*?)\))?`)

	// Example read-amp log line:
	//
	//   I211215 14:55:15.802648 155 kv/kvserver/store.go:2668 ⋮ [n5,s5] 109057 +  total     44905   672 G       -   1.2 T   714 G   118 K   215 G    34 K   4.0 T   379 K   2.8 T     514     3.4
	//
	// Match groups:
	// - 1: datetime
	// - 2: read-amp value
	readAmpPattern = regexp.MustCompile(`^.(\d{6} \d{2}:\d{2}:\d{2}.\d{6})\s.*total.*?(\d+).{8}$`)
)

const (
	// timeFmt matches the Cockroach log timestamp format.
	// See: https://github.com/cockroachdb/cockroach/blob/master/pkg/util/log/format_crdb_v2.go
	timeFmt = "060102 15:04:05.000000"

	// timeFmtSlim is similar to timeFmt, except that it strips components with a
	// lower granularity than a minute.
	timeFmtSlim = "060102 15:04"

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
	case compactionTypeFlush:
		return "flush"
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
	case "flush":
		t = compactionTypeFlush
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
	tStart, err := time.Parse(timeFmt, matches[1])
	if err != nil {
		return start, errors.Newf("could not parse start time: %s", err)
	}

	// Parse job ID.
	jobID, err := strconv.Atoi(matches[2])
	if err != nil {
		return start, errors.Newf("could not parse jobID: %s", err)
	}

	// Parse compaction type.
	cType, err := parseCompactionType(matches[4])
	if err != nil {
		return start, err
	}

	// Parse from-level.
	from, err := strconv.Atoi(matches[5])
	if err != nil {
		return start, errors.Newf("could not parse from-level: %s", err)
	}

	// Parse to-level. For deletion and elision compactions, set the same level.
	to := from
	if cType != compactionTypeElisionOnly && cType != compactionTypeDeleteOnly {
		to, err = strconv.Atoi(matches[6])
		if err != nil {
			return start, errors.Newf("could not parse to-level: %s", err)
		}
	}

	start = compactionStart{
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
	jobID          int
	time           time.Time
	compactedBytes uint64
}

// parseCompactionEnd converts the given regular expression sub-matches for a
// compaction end log line into a compactionEnd event.
func parseCompactionEnd(matches []string) (compactionEnd, error) {
	var end compactionEnd

	// Parse end time.
	tEnd, err := time.Parse(timeFmt, matches[1])
	if err != nil {
		return end, errors.Newf("could not parse start time: %s", err)
	}

	// Parse job ID.
	jobID, err := strconv.Atoi(matches[2])
	if err != nil {
		return end, errors.Newf("could not parse jobID: %s", err)
	}

	end = compactionEnd{
		jobID: jobID,
		time:  tEnd,
	}

	// Optionally, if we have compacted bytes.
	if matches[7] != "" {
		d, e := strconv.ParseFloat(matches[7], 64)
		if e != nil {
			return end, errors.Newf("could not parse compacted bytes digit: %s", e)
		}
		end.compactedBytes = unHumanize(d, matches[8])
	}

	return end, nil
}

// parseFlushStart converts the given regular expression sub-matches for a
// memtable flush start log line into a compactionStart event.
func parseFlushStart(matches []string) (compactionStart, error) {
	var start compactionStart

	// Parse start time.
	tStart, err := time.Parse(timeFmt, matches[1])
	if err != nil {
		return start, errors.Newf("could not parse start time: %s", err)
	}

	// Parse job ID.
	jobID, err := strconv.Atoi(matches[2])
	if err != nil {
		return start, errors.Newf("could not parse jobID: %s", err)
	}

	c := compactionStart{
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
	tEnd, err := time.Parse(timeFmt, matches[1])
	if err != nil {
		return end, errors.Newf("could not parse start time: %s", err)
	}

	// Parse job ID.
	jobID, err := strconv.Atoi(matches[2])
	if err != nil {
		return end, errors.Newf("could not parse jobID: %s", err)
	}

	end = compactionEnd{
		jobID: jobID,
		time:  tEnd,
	}

	// Optionally, if we have flushed bytes.
	if matches[4] != "" {
		d, e := strconv.ParseFloat(matches[4], 64)
		if e != nil {
			return end, errors.Newf("could not parse flushed bytes digit: %s", e)
		}
		end.compactedBytes = unHumanize(d, matches[5])
	}

	return end, nil
}

// compaction represents an aggregated compaction event (i.e. the combination of
// a start and end event).
type compaction struct {
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
func (c *logEventCollector) addCompactionEnd(end compactionEnd) error {
	start, ok := c.m[end.jobID]
	if !ok {
		return errors.Newf("job ID %d not present", end.jobID)
	}

	// Remove the job from the collector once it has been matched.
	delete(c.m, end.jobID)

	c.compactions = append(c.compactions, compaction{
		jobID:          start.jobID,
		cType:          start.cType,
		timeStart:      start.time,
		timeEnd:        end.time,
		fromLevel:      start.fromLevel,
		toLevel:        start.toLevel,
		compactedBytes: end.compactedBytes,
	})
	return nil
}

// addReadAmp adds the readAmp event to the collector.
func (c *logEventCollector) addReadAmp(time time.Time, val int) {
	c.readAmps = append(c.readAmps, readAmp{
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

	var sb strings.Builder
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
		sb.WriteString("\nlong-running compactions:\n")
		sb.WriteString("______from________to_______job______type_______duration:\n")
		for _, e := range s.longRunning {
			sb.WriteString(fmt.Sprintf("%10s %9s %9d %9s %14s\n",
				level(e.fromLevel), level(e.toLevel), e.jobID, e.cType, e.timeEnd.Sub(e.timeStart)))
		}
	}

	return sb.String()
}

// aggregator combines compaction and read-amp events within windows of fixed
// duration and returns one aggregated windowSummary struct per window.
type aggregator struct {
	window      time.Duration
	compactions []compaction
	readAmps    []readAmp

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
	sort.Slice(a.compactions, func(i, j int) bool {
		return a.compactions[i].timeStart.Before(a.compactions[j].timeEnd)
	})
	sort.Slice(a.readAmps, func(i, j int) bool {
		return a.readAmps[i].time.Before(a.readAmps[j].time)
	})

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
		if i == len(a.compactions)-1 || a.compactions[i+1].timeStart.After(windowEnd) {
			// Collect read-amp values for the current window.
			var readAmps []readAmp
			for j < len(a.readAmps) {
				ra := a.readAmps[j]

				// Skip values before the current window.
				if ra.time.Before(windowStart) {
					j++
					continue
				}

				// We've passed over the current window. Stop here.
				if ra.time.After(windowEnd) {
					break
				}

				// Collect this read-amp value.
				readAmps = append(readAmps, ra)
				j++
			}
			curWindow.readAmps = readAmps

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

	if len(matches) != 9 {
		return errors.Newf(
			"could not parse compaction start / end line; found %d matches: %s",
			len(matches), line)
	}

	// A line can only be a start or and end event. The neither case is handled by
	// the regexp itself.
	if (matches[1] == "ing") && (matches[2] == "ed") {
		return errors.Newf(
			"compaction event cannot be both a start and end line simultaneously: %s", line)
	}

	// Capture group 1 populated: this is a start line.
	if matches[3] == "ing" {
		start, err := parseCompactionStart(matches)
		if err != nil {
			return err
		}
		if err := b.addCompactionStart(start); err != nil {
			return err
		}
		return nil
	}

	// Capture group 2 populated: end line.
	end, err := parseCompactionEnd(matches)
	if err != nil {
		return err
	}
	return b.addCompactionEnd(end)
}

// parseFlush is a parseFn that parses parses and collects Pebble memtable flush
// events.
func parseFlush(line string, b *logEventCollector) error {
	matches := flushPattern.FindStringSubmatch(line)
	if matches == nil {
		return nil
	}

	if len(matches) != 6 {
		return errors.Newf(
			"could not parse flush start / end line; found %d matches: %s",
			len(matches), line)
	}

	if matches[3] == "ing" {
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
	return b.addCompactionEnd(end)
}

// parsePebbleFn is a parseFn that parses parses and collects Pebble log lines
// into compaction events.
func parsePebbleFn(line string, b *logEventCollector) error {
	matches := sentinelPattern.FindStringSubmatch(line)
	if matches == nil {
		return nil
	}

	// Determine which regexp to apply: compactions or flushes.
	switch matches[1][0] {
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
	t, err := time.Parse(timeFmt, matches[1])
	if err != nil {
		return errors.Newf("could not parse start time: %s", err)
	}

	// Parse read-amp.
	ra, err := strconv.Atoi(matches[2])
	if err != nil {
		return errors.Newf("could not parse read-amp: %s", err)
	}

	b.addReadAmp(t, ra)
	return nil
}

// runCompactionLogs is runnable function of the top-level cobra.Command that
// parses and collects Pebble compaction events and LSM information.
func (d *dbT) runCompactionLogs(cmd *cobra.Command, args []string) error {
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
