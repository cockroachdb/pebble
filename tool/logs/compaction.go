// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package logs

import (
	"bufio"
	"bytes"
	"cmp"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/ascii"
	"github.com/cockroachdb/pebble/internal/ascii/table"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/spf13/cobra"
)

const numLevels = manifest.NumLevels

var (
	compactionTable = table.Define[compactionTableRow](
		table.String("kind", 7, table.AlignLeft, func(r compactionTableRow) string { return r.Kind }),
		table.Div(),
		table.String("from", 4, table.AlignRight, func(r compactionTableRow) string { return r.From }),
		table.Div(),
		table.String("to", 3, table.AlignRight, func(r compactionTableRow) string { return r.To }),
		table.Div(),
		table.Int("def", 3, table.AlignRight, func(r compactionTableRow) int { return r.Default }),
		table.Div(),
		table.Int("move", 4, table.AlignRight, func(r compactionTableRow) int { return r.Move }),
		table.Div(),
		table.Int("elide", 5, table.AlignRight, func(r compactionTableRow) int { return r.Elide }),
		table.Div(),
		table.Int("del", 3, table.AlignRight, func(r compactionTableRow) int { return r.Delete }),
		table.Div(),
		table.Int("blob", 4, table.AlignRight, func(r compactionTableRow) int { return r.Blob }),
		table.Div(),
		table.Int("cnt", 3, table.AlignRight, func(r compactionTableRow) int { return r.Count }),
		table.Div(),
		table.Bytes("in(B)", 5, table.AlignRight, func(r compactionTableRow) uint64 { return r.BytesIn }),
		table.Div(),
		table.Bytes("out(B)", 6, table.AlignRight, func(r compactionTableRow) uint64 { return r.BytesOut }),
		table.Div(),
		table.Bytes("mov(B)", 6, table.AlignRight, func(r compactionTableRow) uint64 { return r.BytesMoved }),
		table.Div(),
		table.Bytes("del(B)", 6, table.AlignRight, func(r compactionTableRow) uint64 { return r.BytesDel }),
		table.Div(),
		table.String("time", 4, table.AlignRight, func(r compactionTableRow) string { return r.Duration }),
	)

	flushIngestTable = table.Define[flushIngestTableRow](
		table.String("kind", 7, table.AlignLeft, func(r flushIngestTableRow) string { return r.Kind }),
		table.Div(),
		table.String("from", 4, table.AlignRight, func(r flushIngestTableRow) string { return r.From }),
		table.Div(),
		table.String("to", 3, table.AlignRight, func(r flushIngestTableRow) string { return r.To }),
		table.Div(),
		table.Int("cnt", 3, table.AlignRight, func(r flushIngestTableRow) int { return r.Count }),
		table.Div(),
		table.Bytes("bytes", 5, table.AlignRight, func(r flushIngestTableRow) uint64 { return r.Bytes }),
		table.Div(),
		table.String("time", 4, table.AlignRight, func(r flushIngestTableRow) string { return r.Duration }),
	)

	longRunningTable = table.Define[longRunningTableRow](
		table.String("kind", 7, table.AlignLeft, func(r longRunningTableRow) string { return r.Kind }),
		table.Div(),
		table.String("from", 4, table.AlignRight, func(r longRunningTableRow) string { return r.From }),
		table.Div(),
		table.String("to", 3, table.AlignRight, func(r longRunningTableRow) string { return r.To }),
		table.Div(),
		table.Int("job", 4, table.AlignRight, func(r longRunningTableRow) int { return r.Job }),
		table.Div(),
		table.String("type", 6, table.AlignRight, func(r longRunningTableRow) string { return r.Type }),
		table.Div(),
		table.String("start", 7, table.AlignRight, func(r longRunningTableRow) string { return r.Start }),
		table.Div(),
		table.String("end", 7, table.AlignRight, func(r longRunningTableRow) string { return r.End }),
		table.Div(),
		table.Float("dur(s)", 7, table.AlignRight, func(r longRunningTableRow) float64 { return r.DurationSeconds }),
		table.Div(),
		table.Bytes("bytes", 5, table.AlignRight, func(r longRunningTableRow) uint64 { return r.Bytes }),
	)
)

type compactionTableRow struct {
	Kind       string
	From       string
	To         string
	Default    int
	Move       int
	Elide      int
	Delete     int
	Blob       int
	Count      int
	BytesIn    uint64
	BytesOut   uint64
	BytesMoved uint64
	BytesDel   uint64
	Duration   string
}

type flushIngestTableRow struct {
	Kind     string
	From     string
	To       string
	Count    int
	Bytes    uint64
	Duration string
}

type longRunningTableRow struct {
	Kind            string
	From            string
	To              string
	Job             int
	Type            string
	Start           string
	End             string
	DurationSeconds float64
	Bytes           uint64
}

var (
	// Captures a common logging prefix that can be used as the context for the
	// surrounding information captured by other expressions. Example:
	//
	//   I211215 14:26:56.012382 51831533 3@vendor/github.com/cockroachdb/pebble/compaction.go:1845 ⋮ [T1,n5,pebble,s5] ...
	//
	logContextPattern = regexp.MustCompile(
		`^.*` +
			/* Timestamp        */ `(?P<timestamp>\d{6} \d{2}:\d{2}:\d{2}.\d{6}).*` +
			/* Node / Store     */ `\[(T(\d+|\?),)?n(?P<node>\d+|\?).*,s(?P<store>\d+|\?).*?\].*`,
	)
	logContextPatternTimestampIdx = logContextPattern.SubexpIndex("timestamp")
	logContextPatternNodeIdx      = logContextPattern.SubexpIndex("node")
	logContextPatternStoreIdx     = logContextPattern.SubexpIndex("store")

	// Matches either a compaction or a memtable flush log line.
	//
	// A compaction start / end line resembles:
	//   "[JOB X] compact(ed|ing)"
	//
	// A memtable flush start / end line resembles:
	//   "[JOB X] flush(ed|ing)"
	//
	// An ingested sstable flush looks like:
	//   "[JOB 226] flushed 6 ingested flushables"
	sentinelPattern          = regexp.MustCompile(`\[JOB.*(?P<prefix>compact|flush|ingest)(?P<suffix>ed|ing)[^:]`)
	sentinelPatternPrefixIdx = sentinelPattern.SubexpIndex("prefix")
	sentinelPatternSuffixIdx = sentinelPattern.SubexpIndex("suffix")

	// Example compaction start and end log lines:
	// 23.1 and older:
	//   I211215 14:26:56.012382 51831533 3@vendor/github.com/cockroachdb/pebble/compaction.go:1845 ⋮ [n5,pebble,s5] 1216510  [JOB 284925] compacting(default) L2 [442555] (4.2 M) + L3 [445853] (8.4 M)
	//   I211215 14:26:56.318543 51831533 3@vendor/github.com/cockroachdb/pebble/compaction.go:1886 ⋮ [n5,pebble,s5] 1216554  [JOB 284925] compacted(default) L2 [442555] (4.2 M) + L3 [445853] (8.4 M) -> L3 [445883 445887] (13 M), in 0.3s, output rate 42 M/s
	// current:
	//   I211215 14:26:56.012382 51831533 3@vendor/github.com/cockroachdb/pebble/compaction.go:1845 ⋮ [n5,pebble,s5] 1216510  [JOB 284925] compacting(default) L2 [442555] (4.2MB) + L3 [445853] (8.4MB)
	//   I211215 14:26:56.318543 51831533 3@vendor/github.com/cockroachdb/pebble/compaction.go:1886 ⋮ [n5,pebble,s5] 1216554  [JOB 284925] compacted(default) L2 [442555] (4.2MB) + L3 [445853] (8.4MB) -> L3 [445883 445887] (13MB), in 0.3s, output rate 42MB/s
	//
	// NOTE: we use the log timestamp to compute the compaction duration rather
	// than the Pebble log output.
	compactionPattern = regexp.MustCompile(
		`^.*` +
			/* Job ID            */ `\[JOB (?P<job>\d+)]\s` +
			/* Start / end       */ `compact(?P<suffix>ed|ing)` +

			/* Compaction type   */
			`\((?P<type>.*?)\)\s` +
			/* Optional annotation*/ `?(\s*\[(?P<annotations>.*?)\]\s*)?` +

			/* Start / end level */
			`(?P<levels>L(?P<from>\d).*?(?:.*(?:\+|->)\sL(?P<to>\d))?` +
			/* Bytes             */
			`(?:.*?\((?P<bytes>[0-9.]+( [BKMGTPE]|[KMGTPE]?B))\))` +
			/* Score */
			`?(\s*(Score=\d+(\.\d+)))?)`,
	)
	compactionPatternJobIdx    = compactionPattern.SubexpIndex("job")
	compactionPatternSuffixIdx = compactionPattern.SubexpIndex("suffix")
	compactionPatternTypeIdx   = compactionPattern.SubexpIndex("type")
	compactionPatternLevels    = compactionPattern.SubexpIndex("levels")
	compactionPatternFromIdx   = compactionPattern.SubexpIndex("from")
	compactionPatternToIdx     = compactionPattern.SubexpIndex("to")
	compactionPatternBytesIdx  = compactionPattern.SubexpIndex("bytes")

	// Example memtable flush log lines:
	// 23.1 and older:
	//   I211213 16:23:48.903751 21136 3@vendor/github.com/cockroachdb/pebble/event.go:599 ⋮ [n9,pebble,s9] 24 [JOB 10] flushing 2 memtables to L0
	//   I211213 16:23:49.134464 21136 3@vendor/github.com/cockroachdb/pebble/event.go:603 ⋮ [n9,pebble,s9] 26 [JOB 10] flushed 2 memtables to L0 [1535806] (1.3 M), in 0.2s, output rate 5.8 M/s
	// current:
	//   I211213 16:23:48.903751 21136
	//   3@vendor/github.com/cockroachdb/pebble/event.go:599 ⋮ [n9,pebble,s9] 24 [JOB 10] flushing 2 memtables (1.4MB)  to L0
	//   I211213 16:23:49.134464 21136
	//   3@vendor/github.com/cockroachdb/pebble/event.go:603 ⋮ [n9,pebble,s9] 26 [JOB 10] flushed 2 memtables (1.4MB) to L0 [1535806] (1.3MB), in 0.2s, output rate 5.8MB/s
	//
	// NOTE: we use the log timestamp to compute the flush duration rather than
	// the Pebble log output.
	flushPattern = regexp.MustCompile(
		`^..*` +
			/* Job ID                       */ `\[JOB (?P<job>\d+)]\s` +
			/* Compaction type              */ `flush(?P<suffix>ed|ing)\s` +
			/* Memtable count; size (23.2+) */ `\d+ memtables? (\([^)]+\))?` +
			/* SSTable Bytes                */ `(?:.*?\((?P<bytes>[0-9.]+( [BKMGTPE]|[KMGTPE]?B))\))?`,
	)
	flushPatternSuffixIdx = flushPattern.SubexpIndex("suffix")
	flushPatternJobIdx    = flushPattern.SubexpIndex("job")
	flushPatternBytesIdx  = flushPattern.SubexpIndex("bytes")

	// Example ingested log lines:
	// 23.1 and older:
	//   I220228 16:01:22.487906 18476248525 3@vendor/github.com/cockroachdb/pebble/ingest.go:637 ⋮ [n24,pebble,s24] 33430782  [JOB 10211226] ingested L0:21818678 (1.8 K), L0:21818683 (1.2 K), L0:21818679 (1.6 K), L0:21818680 (1.1 K), L0:21818681 (1.1 K), L0:21818682 (160 M)
	// current:
	//   I220228 16:01:22.487906 18476248525 3@vendor/github.com/cockroachdb/pebble/ingest.go:637 ⋮ [n24,pebble,s24] 33430782  [JOB 10211226] ingested L0:21818678 (1.8KB), L0:21818683 (1.2KB), L0:21818679 (1.6KB), L0:21818680 (1.1KB), L0:21818681 (1.1KB), L0:21818682 (160MB)
	//
	ingestedPattern = regexp.MustCompile(
		`^.*` +
			/* Job ID           */ `\[JOB (?P<job>\d+)]\s` +
			/* ingested */ `ingested\s`)
	ingestedPatternJobIdx = ingestedPattern.SubexpIndex("job")
	ingestedFilePattern   = regexp.MustCompile(
		`L` +
			/* Level       */ `(?P<level>\d):` +
			/* File number */ `(?P<file>\d+)\s` +
			/* Bytes       */ `\((?P<bytes>[0-9.]+( [BKMGTPE]|[KMGTPE]?B))\)`)
	ingestedFilePatternLevelIdx = ingestedFilePattern.SubexpIndex("level")
	ingestedFilePatternFileIdx  = ingestedFilePattern.SubexpIndex("file")
	ingestedFilePatternBytesIdx = ingestedFilePattern.SubexpIndex("bytes")

	// flushable ingestions
	//
	// I230831 04:13:28.824280 3780 3@pebble/event.go:685 ⋮ [n10,s10,pebble] 365  [JOB 226] flushed 6 ingested flushables L0:024334 (1.5KB) + L0:024339 (1.0KB) + L0:024335 (1.9KB) + L0:024336 (1.1KB) + L0:024337 (1.1KB) + L0:024338 (12KB) in 0.0s (0.0s total), output rate 67MB/s
	flushableIngestedPattern = regexp.MustCompile(
		`^.*` +
			/* Job ID           */ `\[JOB (?P<job>\d+)]\s` +
			/* match ingested flushable */ `flushed \d ingested flushable`)
	flushableIngestedPatternJobIdx = flushableIngestedPattern.SubexpIndex("job")

	// Example read-amp log line:
	// 23.1 and older:
	//   total     31766   188 G       -   257 G   187 G    48 K   3.6 G     744   536 G    49 K   278 G       5     2.1
	// current:
	//   total |     1   639B     0B |     - |   84B |     0     0B |     0     0B |     3  1.9KB | 1.2KB |   1 23.7
	readAmpPattern = regexp.MustCompile(
		/* Read amp */ `(?:^|\+)(?:\s{2}total|total \|).*?\s(?P<value>\d+)\s.{4,7}$`,
	)
	readAmpPatternValueIdx = readAmpPattern.SubexpIndex("value")

	// Example blob file rewrite start and end log lines:
	//   I211215 14:26:56.318543 51831533 3@vendor/github.com/cockroachdb/pebble/compaction.go:1886 ⋮ [n5,pebble,s5] 1216554  [JOB 284925] rewrote blob file (B000006, 000006) (209B) -> (B000006, 000010) (203B), in 0.2s (0.35s total)
	blobRewritePattern = regexp.MustCompile(
		`^.*` +
			/* Job ID            */ `\[JOB (?P<job>\d+)]\s` +
			/* Match rewrite end logs only */ `rewrote blob file\s` +
			/* Physical file number before rewrite */
			`\([^,]+,\s*(?P<physicalFileBefore>\d+)\)\s+` +
			/* Size before rewrite */ `\((?P<sizeBefore>[0-9.]+(?: [BKMGTPE]|[KMGTPE]?B))\)\s*` +
			`->\s*` +
			/* Physical file number after rewrite */ `\([^,]+,\s*(?P<physicalFileAfter>\d+)\)\s+` +
			/* Size after rewrite */ `\((?P<sizeAfter>[0-9.]+(?: [BKMGTPE]|[KMGTPE]?B))\)` +
			/* Total time */ `(?:.*?in\s[0-9.]+s\s\((?P<totalTime>[0-9.]+)s\s+total\))?`,
	)
	blobRewritePatternJobIdx                = blobRewritePattern.SubexpIndex("job")
	blobRewritePatternPhysicalFileBeforeIdx = blobRewritePattern.SubexpIndex("physicalFileBefore")
	blobRewritePatternPhysicalFileAfterIdx  = blobRewritePattern.SubexpIndex("physicalFileAfter")
	blobRewritePatternSizeBeforeIdx         = blobRewritePattern.SubexpIndex("sizeBefore")
	blobRewritePatternSizeAfterIdx          = blobRewritePattern.SubexpIndex("sizeAfter")
	blobRewritePatternTotalTimeIdx          = blobRewritePattern.SubexpIndex("totalTime")
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
	compactionTypeBlobRewrite
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
	case compactionTypeBlobRewrite:
		return "blob-rewrite"
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
	case "blob-rewrite":
		t = compactionTypeBlobRewrite
	default:
		err = errors.Newf("unknown compaction type: %s", s)
	}
	return
}

// compactionStart is a compaction start event.
type compactionStart struct {
	ctx        logContext
	jobID      int
	cType      compactionType
	fromLevel  int
	toLevel    int
	inputBytes uint64

	// Blob-specific fields (only used when cType == compactionTypeBlobRewrite).
	physicalFileIDBefore int
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

	// Parse input bytes.
	inputBytes, err := sumInputBytes(matches[compactionPatternLevels])
	if err != nil {
		return start, errors.Newf("could not sum input bytes: %s", err)
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
		jobID:      jobID,
		cType:      cType,
		fromLevel:  from,
		toLevel:    to,
		inputBytes: inputBytes,
	}

	return start, nil
}

// compactionEnd is a compaction end event.
type compactionEnd struct {
	jobID        int
	writtenBytes uint64
	// TODO(jackson): Parse and include the aggregate size of input
	// sstables. It may be instructive, because compactions that drop
	// keys write less data than they remove from the input level.

	// Blob-specific fields (only used when cType == compactionTypeBlobRewrite).
	physicalFileIDAfter int
	duration            time.Duration
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
	if matches[compactionPatternBytesIdx] != "" {
		end.writtenBytes = unHumanize(matches[compactionPatternBytesIdx])
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
	if matches[flushPatternBytesIdx] != "" {
		end.writtenBytes = unHumanize(matches[flushPatternBytesIdx])
	}

	return end, nil
}

// event describes an aggregated event (eg, start and end events
// combined if necessary).
type event struct {
	nodeID     int
	storeID    int
	jobID      int
	timeStart  time.Time
	timeEnd    time.Time
	compaction *compaction
	ingest     *ingest
}

// compaction represents an aggregated compaction event (i.e. the combination of
// a start and end event).
type compaction struct {
	cType       compactionType
	fromLevel   int
	toLevel     int
	inputBytes  uint64
	outputBytes uint64

	// Blob-specific fields (only used when cType == compactionTypeBlobRewrite).
	physicalFileIDBefore int
	physicalFileIDAfter  int
	duration             time.Duration
}

// ingest describes the completion of an ingest.
type ingest struct {
	files []ingestedFile
}

type ingestedFile struct {
	level     int
	fileNum   int
	sizeBytes uint64
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

type errorEvent struct {
	path string
	line string
	err  error
}

// logEventCollector keeps track of open compaction events and read-amp events
// over the course of parsing log line events. Completed compaction events are
// added to the collector once a matching start and end pair are encountered.
// Read-amp events are added as they are encountered (the have no start / end
// concept).
type logEventCollector struct {
	ctx      logContext
	m        map[nodeStoreJob]compactionStart
	events   []event
	readAmps []readAmp
	errors   []errorEvent
}

// newEventCollector instantiates a new logEventCollector.
func newEventCollector() *logEventCollector {
	return &logEventCollector{
		m: make(map[nodeStoreJob]compactionStart),
	}
}

// addError records an error encountered during log parsing.
func (c *logEventCollector) addError(path, line string, err error) {
	c.errors = append(c.errors, errorEvent{path: path, line: line, err: err})
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

	comp := &compaction{
		cType:       start.cType,
		fromLevel:   start.fromLevel,
		toLevel:     start.toLevel,
		inputBytes:  start.inputBytes,
		outputBytes: end.writtenBytes,
	}

	// Add blob-specific fields if this is a blob rewrite.
	if start.cType == compactionTypeBlobRewrite {
		comp.physicalFileIDBefore = start.physicalFileIDBefore
		comp.physicalFileIDAfter = end.physicalFileIDAfter
		comp.duration = end.duration
	}

	c.events = append(c.events, event{
		nodeID:     start.ctx.node,
		storeID:    start.ctx.store,
		jobID:      start.jobID,
		timeStart:  start.ctx.timestamp,
		timeEnd:    c.ctx.timestamp,
		compaction: comp,
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
	if l == -2 {
		return "BLOB"
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
// - total ingested bytes for each level
// - read amp magnitudes
type windowSummary struct {
	nodeID, storeID      int
	tStart, tEnd         time.Time
	eventCount           int
	flushedCount         int
	flushedBytes         uint64
	flushedTime          time.Duration
	compactionCounts     map[fromTo]compactionTypeCount
	compactionBytesIn    map[fromTo]uint64
	compactionBytesOut   map[fromTo]uint64
	compactionBytesMoved map[fromTo]uint64
	compactionBytesDel   map[fromTo]uint64
	compactionTime       map[fromTo]time.Duration
	ingestedCount        [numLevels]int
	ingestedBytes        [numLevels]uint64
	readAmps             []readAmp
	longRunning          []event
}

// String implements fmt.Stringer, returning a formatted window summary.
func (s windowSummary) String() string {
	type fromToCount struct {
		ft         fromTo
		counts     compactionTypeCount
		bytesIn    uint64
		bytesOut   uint64
		bytesMoved uint64
		bytesDel   uint64
		duration   time.Duration
	}
	var pairs []fromToCount
	for k, v := range s.compactionCounts {
		pairs = append(pairs, fromToCount{
			ft:         k,
			counts:     v,
			bytesIn:    s.compactionBytesIn[k],
			bytesOut:   s.compactionBytesOut[k],
			bytesMoved: s.compactionBytesMoved[k],
			bytesDel:   s.compactionBytesDel[k],
			duration:   s.compactionTime[k],
		})
	}
	slices.SortFunc(pairs, func(l, r fromToCount) int {
		if v := cmp.Compare(l.ft.from, r.ft.from); v != 0 {
			return v
		}
		return cmp.Compare(l.ft.to, r.ft.to)
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
	sb.WriteString(fmt.Sprintf("   from: %s\n", s.tStart.Format(timeFmtSlim)))
	sb.WriteString(fmt.Sprintf("     to: %s\n", s.tEnd.Format(timeFmtSlim)))
	var count, sum int
	for _, ra := range s.readAmps {
		count++
		sum += ra.readAmp
	}
	sb.WriteString(fmt.Sprintf("  r-amp: %.1f\n", float64(sum)/float64(count)))
	sb.WriteString("\n")

	// Print flush+ingest statistics.
	{
		var flushIngestRows []flushIngestTableRow
		var totalCount, totalBytes int
		var totalTime time.Duration

		if s.flushedCount > 0 {
			flushIngestRows = append(flushIngestRows, flushIngestTableRow{
				Kind:     "flush",
				From:     "",
				To:       "L0",
				Count:    s.flushedCount,
				Bytes:    s.flushedBytes,
				Duration: s.flushedTime.Truncate(time.Second).String(),
			})
			totalCount += s.flushedCount
			totalBytes += int(s.flushedBytes)
			totalTime += s.flushedTime
		}

		for l := 0; l < len(s.ingestedBytes); l++ {
			if s.ingestedCount[l] == 0 {
				continue
			}
			flushIngestRows = append(flushIngestRows, flushIngestTableRow{
				Kind:     "ingest",
				From:     "",
				To:       fmt.Sprintf("L%d", l),
				Count:    s.ingestedCount[l],
				Bytes:    s.ingestedBytes[l],
				Duration: "",
			})
			totalCount += s.ingestedCount[l]
			totalBytes += int(s.ingestedBytes[l])
		}

		if len(flushIngestRows) > 0 {
			flushIngestRows = append(flushIngestRows, flushIngestTableRow{
				Kind:     "total",
				From:     "",
				To:       "",
				Count:    totalCount,
				Bytes:    uint64(totalBytes),
				Duration: totalTime.Truncate(time.Second).String(),
			})

			board := ascii.Make(20, 1)
			flushIngestTable.Render(board.At(0, 0), table.RenderOptions{}, flushIngestRows...)
			sb.WriteString(board.String())
			sb.WriteString("\n")
		}
	}

	// Print compactions statistics.
	if len(s.compactionCounts) > 0 {
		var compactionRows []compactionTableRow
		var totalDef, totalMove, totalElision, totalDel, totalBlob int
		var totalBytesIn, totalBytesOut, totalBytesMoved, totalBytesDel uint64
		var totalTime time.Duration

		for _, p := range pairs {
			def := p.counts[compactionTypeDefault]
			move := p.counts[compactionTypeMove]
			elision := p.counts[compactionTypeElisionOnly]
			del := p.counts[compactionTypeDeleteOnly]
			blob := p.counts[compactionTypeBlobRewrite]
			total := def + move + elision + del + blob

			compactionRows = append(compactionRows, compactionTableRow{
				Kind:       "compact",
				From:       p.ft.from.String(),
				To:         p.ft.to.String(),
				Default:    def,
				Move:       move,
				Elide:      elision,
				Delete:     del,
				Blob:       blob,
				Count:      total,
				BytesIn:    p.bytesIn,
				BytesOut:   p.bytesOut,
				BytesMoved: p.bytesMoved,
				BytesDel:   p.bytesDel,
				Duration:   p.duration.Truncate(time.Second).String(),
			})

			totalDef += def
			totalMove += move
			totalElision += elision
			totalDel += del
			totalBlob += blob
			totalBytesIn += p.bytesIn
			totalBytesOut += p.bytesOut
			totalBytesMoved += p.bytesMoved
			totalBytesDel += p.bytesDel
			totalTime += p.duration
		}

		compactionRows = append(compactionRows, compactionTableRow{
			Kind:       "total",
			From:       "",
			To:         "",
			Default:    totalDef,
			Move:       totalMove,
			Elide:      totalElision,
			Delete:     totalDel,
			Blob:       totalBlob,
			Count:      s.eventCount,
			BytesIn:    totalBytesIn,
			BytesOut:   totalBytesOut,
			BytesMoved: totalBytesMoved,
			BytesDel:   totalBytesDel,
			Duration:   totalTime.Truncate(time.Second).String(),
		})

		board := ascii.Make(20, 1)
		compactionTable.Render(board.At(0, 0), table.RenderOptions{}, compactionRows...)
		sb.WriteString(board.String())
		sb.WriteString("\n\n")
	}

	// (Optional) Long running events.
	if len(s.longRunning) > 0 {
		sb.WriteString("long-running events (descending runtime):\n")
		var longRunningRows []longRunningTableRow
		for _, e := range s.longRunning {
			if e.compaction != nil {
				c := e.compaction
				kind := "compact"
				if c.fromLevel == -1 {
					kind = "flush"
				} else if c.fromLevel == -2 {
					kind = "blob"
				}
				longRunningRows = append(longRunningRows, longRunningTableRow{
					Kind:            kind,
					From:            level(c.fromLevel).String(),
					To:              level(c.toLevel).String(),
					Job:             e.jobID,
					Type:            c.cType.String(),
					Start:           e.timeStart.Format(timeFmtHrMinSec),
					End:             e.timeEnd.Format(timeFmtHrMinSec),
					DurationSeconds: e.timeEnd.Sub(e.timeStart).Seconds(),
					Bytes:           c.outputBytes,
				})
			}
		}
		board := ascii.Make(20, 1)
		longRunningTable.Render(board.At(0, 0), table.RenderOptions{}, longRunningRows...)
		sb.WriteString(board.String())
		sb.WriteString("\n")
	}

	return sb.String()
}

// windowSummarySlice is a slice of windowSummary that sorts in order of start
// time, node, then store.
type windowsSummarySlice []windowSummary

func (s windowsSummarySlice) Len() int {
	return len(s)
}

func (s windowsSummarySlice) Less(i, j int) bool {
	if !s[i].tStart.Equal(s[j].tStart) {
		return s[i].tStart.Before(s[j].tStart)
	}
	if s[i].nodeID != s[j].nodeID {
		return s[i].nodeID < s[j].nodeID
	}
	return s[i].storeID < s[j].storeID
}

func (s windowsSummarySlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// eventSlice is a slice of events that sorts in order of node, store,
// then event start time.
type eventSlice []event

func (s eventSlice) Len() int {
	return len(s)
}

func (s eventSlice) Less(i, j int) bool {
	if s[i].nodeID != s[j].nodeID {
		return s[i].nodeID < s[j].nodeID
	}
	if s[i].storeID != s[j].storeID {
		return s[i].storeID < s[j].storeID
	}
	return s[i].timeStart.Before(s[j].timeStart)
}

func (s eventSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
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
	events           []event
	readAmps         []readAmp
	longRunningLimit time.Duration
}

// newAggregator returns a new aggregator.
func newAggregator(
	window, longRunningLimit time.Duration, events []event, readAmps []readAmp,
) *aggregator {
	return &aggregator{
		window:           window,
		events:           events,
		readAmps:         readAmps,
		longRunningLimit: longRunningLimit,
	}
}

// aggregate aggregates the events into windows, returning the windowSummary for
// each interval.
func (a *aggregator) aggregate() []windowSummary {
	if len(a.events) == 0 {
		return nil
	}

	// Sort the event and read-amp slices by start time.
	sort.Sort(eventSlice(a.events))
	sort.Sort(readAmpSlice(a.readAmps))

	initWindow := func(e event) *windowSummary {
		start := e.timeStart.Truncate(a.window)
		return &windowSummary{
			nodeID:               e.nodeID,
			storeID:              e.storeID,
			tStart:               start,
			tEnd:                 start.Add(a.window),
			compactionCounts:     make(map[fromTo]compactionTypeCount),
			compactionBytesIn:    make(map[fromTo]uint64),
			compactionBytesOut:   make(map[fromTo]uint64),
			compactionBytesMoved: make(map[fromTo]uint64),
			compactionBytesDel:   make(map[fromTo]uint64),
			compactionTime:       make(map[fromTo]time.Duration),
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
		slices.SortFunc(cur.longRunning, func(l, r event) int {
			return cmp.Compare(l.timeEnd.Sub(l.timeStart), r.timeEnd.Sub(r.timeStart))
		})

		// Add the completed window to the set of windows.
		windows = append(windows, *cur)
	}

	// Move through the compactions, collecting relevant compactions into the same
	// window. Windows have the same node and store, and a compaction start time
	// within a given range.
	i := 0
	curWindow := initWindow(a.events[i])
	for ; ; i++ {
		// No more windows. Complete the current window.
		if i == len(a.events) {
			finishWindow(curWindow)
			break
		}
		e := a.events[i]

		// If we're at the start of a new interval, finalize the current window and
		// start a new one.
		if curWindow.nodeID != e.nodeID ||
			curWindow.storeID != e.storeID ||
			e.timeStart.After(curWindow.tEnd) {
			finishWindow(curWindow)
			curWindow = initWindow(e)
		}

		switch {
		case e.ingest != nil:
			// Update ingest stats.
			for _, f := range e.ingest.files {
				curWindow.ingestedCount[f.level]++
				curWindow.ingestedBytes[f.level] += f.sizeBytes
			}
		case e.compaction != nil && e.compaction.cType == compactionTypeFlush:
			// Update flush stats.
			f := e.compaction
			curWindow.flushedCount++
			curWindow.flushedBytes += f.outputBytes
			curWindow.flushedTime += e.timeEnd.Sub(e.timeStart)
		case e.compaction != nil:
			// Update compaction stats.
			c := e.compaction
			// Update compaction counts.
			ft := fromTo{level(c.fromLevel), level(c.toLevel)}
			m, ok := curWindow.compactionCounts[ft]
			if !ok {
				m = make(compactionTypeCount)
				curWindow.compactionCounts[ft] = m
			}
			m[c.cType]++
			curWindow.eventCount++

			// Update compacted bytes in / out / moved / deleted.
			switch c.cType {
			case compactionTypeMove:
				curWindow.compactionBytesMoved[ft] += c.inputBytes
			case compactionTypeDeleteOnly:
				curWindow.compactionBytesDel[ft] += c.inputBytes
			default:
				curWindow.compactionBytesIn[ft] += c.inputBytes
				curWindow.compactionBytesOut[ft] += c.outputBytes
			}

			// Update compaction time.
			_, ok = curWindow.compactionTime[ft]
			if !ok {
				curWindow.compactionTime[ft] = 0
			}
			// For blob rewrites, use the stored duration from the log.
			// For other compaction types, use the timestamp difference.
			if c.cType == compactionTypeBlobRewrite && c.duration > 0 {
				curWindow.compactionTime[ft] += c.duration
			} else {
				curWindow.compactionTime[ft] += e.timeEnd.Sub(e.timeStart)
			}

		}
		// Add "long-running" events. Those that start in this window
		// that have duration longer than the window interval.
		if e.timeEnd.Sub(e.timeStart) > a.longRunningLimit {
			curWindow.longRunning = append(curWindow.longRunning, e)
		}
	}

	// Windows are added in order of (node, store, time). Re-sort the windows by
	// (time, node, store) for better presentation.
	sort.Sort(windowsSummarySlice(windows))

	return windows
}

// parseLog parses the log file with the given path, using the given parse
// function to collect events in the given logEventCollector. parseLog
// returns a non-nil error if an I/O error was encountered while reading
// the log file. Parsing errors are accumulated in the
// logEventCollector.
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
			var err error
			switch matches[sentinelPatternPrefixIdx][0] {
			case 'c':
				err = parseCompaction(line, b)
			case 'f':
				err = parseFlush(line, b)
			case 'i':
				err = parseIngest(line, b)
			default:
				err = errors.Newf("unexpected line: neither compaction nor flush: %s", line)
			}
			if err != nil {
				b.addError(path, line, err)
			}
			continue
		}

		// Check for blob rewrite events.
		if err = parseBlobRewrite(line, b); err != nil {
			b.addError(path, line, err)
			continue
		}

		// Else check for an LSM debug line.
		if err = parseReadAmp(line, b); err != nil {
			b.addError(path, line, err)
			continue
		}
	}
	return s.Err()
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

func parseIngestDuringFlush(line string, b *logEventCollector) error {
	matches := flushableIngestedPattern.FindStringSubmatch(line)
	if matches == nil {
		return nil
	}
	// Parse job ID.
	jobID, err := strconv.Atoi(matches[flushableIngestedPatternJobIdx])
	if err != nil {
		return errors.Newf("could not parse jobID: %s", err)
	}
	return parseRemainingIngestLogLine(jobID, line, b)
}

// parseIngest parses and collects Pebble ingest complete events.
func parseIngest(line string, b *logEventCollector) error {
	matches := ingestedPattern.FindStringSubmatch(line)
	if matches == nil {
		// Try and parse the other kind of ingest.
		return parseIngestDuringFlush(line, b)
	}
	// Parse job ID.
	jobID, err := strconv.Atoi(matches[ingestedPatternJobIdx])
	if err != nil {
		return errors.Newf("could not parse jobID: %s", err)
	}
	return parseRemainingIngestLogLine(jobID, line, b)
}

// parses the level, filenum, and bytes for the files which were ingested.
func parseRemainingIngestLogLine(jobID int, line string, b *logEventCollector) error {
	fileMatches := ingestedFilePattern.FindAllStringSubmatch(line, -1)
	files := make([]ingestedFile, len(fileMatches))
	for i := range fileMatches {
		level, err := strconv.Atoi(fileMatches[i][ingestedFilePatternLevelIdx])
		if err != nil {
			return errors.Newf("could not parse level: %s", err)
		}
		fileNum, err := strconv.Atoi(fileMatches[i][ingestedFilePatternFileIdx])
		if err != nil {
			return errors.Newf("could not parse file number: %s", err)
		}
		files[i] = ingestedFile{
			level:     level,
			fileNum:   fileNum,
			sizeBytes: unHumanize(fileMatches[i][ingestedFilePatternBytesIdx]),
		}
	}
	b.events = append(b.events, event{
		nodeID:    b.ctx.node,
		storeID:   b.ctx.store,
		jobID:     jobID,
		timeStart: b.ctx.timestamp,
		timeEnd:   b.ctx.timestamp,
		ingest: &ingest{
			files: files,
		},
	})
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

// parseBlobRewriteStartEnd converts the given regular expression sub-matches for a
// blob rewrite log line into a compactionEnd and compactionStart event.
func parseBlobRewriteStartEnd(matches []string) (compactionStart, compactionEnd, error) {
	var start compactionStart
	var end compactionEnd

	jobID, err := strconv.Atoi(matches[blobRewritePatternJobIdx])
	if err != nil {
		return start, end, errors.Newf("could not parse jobID: %s", err)
	}

	var sizeBefore uint64
	if matches[blobRewritePatternSizeBeforeIdx] != "" {
		sizeBefore = unHumanize(matches[blobRewritePatternSizeBeforeIdx])
	}

	var sizeAfter uint64
	if matches[blobRewritePatternSizeAfterIdx] != "" {
		sizeAfter = unHumanize(matches[blobRewritePatternSizeAfterIdx])
	}

	physicalFileIDBefore, err := strconv.Atoi(matches[blobRewritePatternPhysicalFileBeforeIdx])
	if err != nil {
		return start, end, errors.Newf("could not parse physical file ID before: %s", err)
	}

	physicalFileIDAfter, err := strconv.Atoi(matches[blobRewritePatternPhysicalFileAfterIdx])
	if err != nil {
		return start, end, errors.Newf("could not parse physical file ID after: %s", err)
	}

	var duration time.Duration
	if matches[blobRewritePatternTotalTimeIdx] != "" {
		totalTimeStr := matches[blobRewritePatternTotalTimeIdx]
		totalTimeFloat, err := strconv.ParseFloat(totalTimeStr, 64)
		if err != nil {
			return start, end, errors.Newf("could not parse total time: %s", err)
		}
		duration = time.Duration(totalTimeFloat * float64(time.Second))
	}

	start = compactionStart{
		jobID:                jobID,
		cType:                compactionTypeBlobRewrite,
		fromLevel:            -2,
		toLevel:              -2,
		inputBytes:           sizeBefore,
		physicalFileIDBefore: physicalFileIDBefore,
	}

	end = compactionEnd{
		jobID:               jobID,
		writtenBytes:        sizeAfter,
		physicalFileIDAfter: physicalFileIDAfter,
		duration:            duration,
	}

	return start, end, nil
}

// parseBlobRewrite parses and collects blob rewrite events.
func parseBlobRewrite(line string, b *logEventCollector) error {
	matches := blobRewritePattern.FindStringSubmatch(line)
	if matches == nil {
		return nil
	}

	start, end, err := parseBlobRewriteStartEnd(matches)
	if err != nil {
		return err
	}

	if err := b.addCompactionStart(start); err != nil {
		return err
	}
	b.addCompactionEnd(end)
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
		// parseLog returns an error only on I/O errors, which we
		// immediately exit with.
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
	a := newAggregator(window, longRunningLimit, b.events, b.readAmps)
	summaries := a.aggregate()
	for _, s := range summaries {
		fmt.Printf("%s\n", s)
	}

	// After the summaries, print accumulated parsing errors to stderr.
	for _, e := range b.errors {
		fmt.Fprintf(os.Stderr, "-\n%s: %s\nError: %s\n", filepath.Base(e.path), e.line, e.err)
	}
	return nil
}

// unHumanize performs the opposite of humanize.Bytes.Uint64 (e.g. "10B",
// "10MB") or the 23.1 humanize.IEC.Uint64 (e.g. "10 B", "10 M"), converting a
// human-readable value into a raw number of bytes.
func unHumanize(s string) uint64 {
	if len(s) < 2 || !(s[0] >= '0' && s[0] <= '9') {
		panic(errors.Newf("invalid bytes value %q", s))
	}
	if s[len(s)-1] == 'B' {
		s = s[:len(s)-1]
	}

	multiplier := uint64(1)
	switch s[len(s)-1] {
	case 'K':
		multiplier = 1 << 10
	case 'M':
		multiplier = 1 << 20
	case 'G':
		multiplier = 1 << 30
	case 'T':
		multiplier = 1 << 40
	case 'P':
		multiplier = 1 << 50
	case 'E':
		multiplier = 1 << 60
	}
	if multiplier != 1 {
		s = s[:len(s)-1]
	}
	if s[len(s)-1] == ' ' {
		s = s[:len(s)-1]
	}
	val, err := strconv.ParseFloat(s, 64)
	if err != nil {
		panic(fmt.Sprintf("parsing %s: %v", s, err))
	}

	return uint64(val * float64(multiplier))
}

// sumInputBytes takes a string as input and returns the sum of the
// human-readable sizes, as an integer number of bytes.
func sumInputBytes(s string) (total uint64, _ error) {
	var (
		open bool
		b    bytes.Buffer
	)
	for _, c := range s {
		switch c {
		case '(':
			open = true
		case ')':
			total += unHumanize(b.String())
			b.Reset()
			open = false
		default:
			if open {
				b.WriteRune(c)
			}
		}
	}
	return
}
