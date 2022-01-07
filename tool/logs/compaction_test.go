// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package logs

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/stretchr/testify/require"
)

const (
	compactionStartLine = `I211215 14:26:56.012382 51831533 3@vendor/github.com/cockroachdb/pebble/compaction.go:1845 ⋮ [n5,pebble,s6] 1216510  [JOB 284925] compacting(default) L2 [442555] (4.2 M) + L3 [445853] (8.4 M)`
	compactionEndLine   = `I211215 14:26:56.318543 51831533 3@vendor/github.com/cockroachdb/pebble/compaction.go:1886 ⋮ [n5,pebble,s6] 1216554  [JOB 284925] compacted(default) L2 [442555] (4.2 M) + L3 [445853] (8.4 M) -> L3 [445883 445887] (13 M), in 0.3s, output rate 42 M/s`
	flushStartLine      = `I211213 16:23:48.903751 21136 3@vendor/github.com/cockroachdb/pebble/event.go:599 ⋮ [n9,pebble,s8] 24 [JOB 10] flushing 2 memtables to L0`
	flushEndLine        = `I211213 16:23:49.134464 21136 3@vendor/github.com/cockroachdb/pebble/event.go:603 ⋮ [n9,pebble,s8] 26 [JOB 10] flushed 2 memtables to L0 [1535806] (1.3 M), in 0.2s, output rate 5.8 M/s`
	readAmpLine         = `I211215 14:55:15.802648 155 kv/kvserver/store.go:2668 ⋮ [n5,s6] 109057 +  total     44905   672 G       -   1.2 T   714 G   118 K   215 G    34 K   4.0 T   379 K   2.8 T     514     3.4`

	compactionStartNoNodeStoreLine = `I211215 14:26:56.012382 51831533 3@vendor/github.com/cockroachdb/pebble/compaction.go:1845 ⋮ [n?,pebble,s?] 1216510  [JOB 284925] compacting(default) L2 [442555] (4.2 M) + L3 [445853] (8.4 M)`
	flushStartNoNodeStoreLine      = `I211213 16:23:48.903751 21136 3@vendor/github.com/cockroachdb/pebble/event.go:599 ⋮ [n?,pebble,s?] 24 [JOB 10] flushing 2 memtables to L0`
	readAmpNoNodeStoreLine         = `I211215 14:55:15.802648 155 kv/kvserver/store.go:2668 ⋮ [n?,s?] 109057 +  total     44905   672 G       -   1.2 T   714 G   118 K   215 G    34 K   4.0 T   379 K   2.8 T     514     3.4`
)

func TestCompactionLogs_Regex(t *testing.T) {
	tcs := []struct {
		name    string
		re      *regexp.Regexp
		line    string
		matches map[int]string
	}{
		{
			name: "compaction start - sentinel",
			re:   sentinelPattern,
			line: compactionStartLine,
			matches: map[int]string{
				sentinelPatternPrefixIdx: "compact",
				sentinelPatternSuffixIdx: "ing",
			},
		},
		{
			name: "compaction end - sentinel",
			re:   sentinelPattern,
			line: compactionEndLine,
			matches: map[int]string{
				sentinelPatternPrefixIdx: "compact",
				sentinelPatternSuffixIdx: "ed",
			},
		},
		{
			name: "flush start - sentinel",
			re:   sentinelPattern,
			line: flushStartLine,
			matches: map[int]string{
				sentinelPatternPrefixIdx: "flush",
				sentinelPatternSuffixIdx: "ing",
			},
		},
		{
			name: "flush end - sentinel",
			re:   sentinelPattern,
			line: flushEndLine,
			matches: map[int]string{
				sentinelPatternPrefixIdx: "flush",
				sentinelPatternSuffixIdx: "ed",
			},
		},
		{
			name: "compaction start",
			re:   compactionPattern,
			line: compactionStartLine,
			matches: map[int]string{
				compactionPatternTimestampIdx: "211215 14:26:56.012382",
				compactionPatternNode:         "5",
				compactionPatternStore:        "6",
				compactionPatternJobIdx:       "284925",
				compactionPatternSuffixIdx:    "ing",
				compactionPatternTypeIdx:      "default",
				compactionPatternFromIdx:      "2",
				compactionPatternToIdx:        "3",
				compactionPatternDigitIdx:     "8.4",
				compactionPatternUnitIdx:      "M",
			},
		},
		{
			name: "compaction start - no node / store",
			re:   compactionPattern,
			line: compactionStartNoNodeStoreLine,
			matches: map[int]string{
				compactionPatternTimestampIdx: "211215 14:26:56.012382",
				compactionPatternNode:         "?",
				compactionPatternStore:        "?",
				compactionPatternJobIdx:       "284925",
				compactionPatternSuffixIdx:    "ing",
				compactionPatternTypeIdx:      "default",
				compactionPatternFromIdx:      "2",
				compactionPatternToIdx:        "3",
				compactionPatternDigitIdx:     "8.4",
				compactionPatternUnitIdx:      "M",
			},
		},
		{
			name: "compaction end",
			re:   compactionPattern,
			line: compactionEndLine,
			matches: map[int]string{
				compactionPatternTimestampIdx: "211215 14:26:56.318543",
				compactionPatternNode:         "5",
				compactionPatternStore:        "6",
				compactionPatternJobIdx:       "284925",
				compactionPatternSuffixIdx:    "ed",
				compactionPatternTypeIdx:      "default",
				compactionPatternFromIdx:      "2",
				compactionPatternToIdx:        "3",
				compactionPatternDigitIdx:     "13",
				compactionPatternUnitIdx:      "M",
			},
		},
		{
			name: "flush start",
			re:   flushPattern,
			line: flushStartLine,
			matches: map[int]string{
				flushPatternTimestampIdx: "211213 16:23:48.903751",
				flushPatternNode:         "9",
				flushPatternStore:        "8",
				flushPatternJobIdx:       "10",
				flushPatternSuffixIdx:    "ing",
				flushPatternDigitIdx:     "",
				flushPatternUnitIdx:      "",
			},
		},
		{
			name: "flush start - no node / store",
			re:   flushPattern,
			line: flushStartNoNodeStoreLine,
			matches: map[int]string{
				flushPatternTimestampIdx: "211213 16:23:48.903751",
				flushPatternNode:         "?",
				flushPatternStore:        "?",
				flushPatternJobIdx:       "10",
				flushPatternSuffixIdx:    "ing",
				flushPatternDigitIdx:     "",
				flushPatternUnitIdx:      "",
			},
		},
		{
			name: "flush end",
			re:   flushPattern,
			line: flushEndLine,
			matches: map[int]string{
				flushPatternTimestampIdx: "211213 16:23:49.134464",
				flushPatternNode:         "9",
				flushPatternStore:        "8",
				flushPatternJobIdx:       "10",
				flushPatternSuffixIdx:    "ed",
				flushPatternDigitIdx:     "1.3",
				flushPatternUnitIdx:      "M",
			},
		},
		{
			name: "read amp",
			re:   readAmpPattern,
			line: readAmpLine,
			matches: map[int]string{
				readAmpPatternTimestampIdx: "211215 14:55:15.802648",
				readAmpPatternNode:         "5",
				readAmpPatternStore:        "6",
				readAmpPatternValueIdx:     "514",
			},
		},
		{
			name: "read amp - no node / store",
			re:   readAmpPattern,
			line: readAmpNoNodeStoreLine,
			matches: map[int]string{
				readAmpPatternTimestampIdx: "211215 14:55:15.802648",
				readAmpPatternNode:         "?",
				readAmpPatternStore:        "?",
				readAmpPatternValueIdx:     "514",
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			matches := tc.re.FindStringSubmatch(tc.line)
			require.NotNil(t, matches)
			for idx, want := range tc.matches {
				require.Equal(t, want, matches[idx])
			}
		})
	}
}

func TestCompactionLogs(t *testing.T) {
	var c *logEventCollector
	var pebbleFileNum, cockroachFileNum int
	resetFn := func() {
		c = newEventCollector()
		pebbleFileNum, cockroachFileNum = 0, 0
	}
	resetFn()

	dir := t.TempDir()
	datadriven.RunTest(t, "testdata/compactions", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "pebble-log":
			basename := fmt.Sprintf("pebble.%d.log", pebbleFileNum)
			pebbleFileNum++
			f, err := os.Create(filepath.Join(dir, basename))
			if err != nil {
				panic(err)
			}
			if _, err = f.WriteString(td.Input); err != nil {
				panic(err)
			}
			_ = f.Close()

			if err = parseLog(f.Name(), c, parsePebbleFn); err != nil {
				return err.Error()
			}
			return basename

		case "cockroach-log":
			basename := fmt.Sprintf("cockroach.%d.log", cockroachFileNum)
			cockroachFileNum++
			f, err := os.Create(filepath.Join(dir, basename))
			if err != nil {
				panic(err)
			}
			if _, err := f.WriteString(td.Input); err != nil {
				panic(err)
			}
			_ = f.Close()

			if err = parseLog(f.Name(), c, parseCockroachFn); err != nil {
				return err.Error()
			}
			return basename

		case "summarize":
			window := 1 * time.Minute
			longRunning := time.Duration(math.MaxInt64)

			var err error
			for _, cmdArg := range td.CmdArgs {
				switch cmdArg.Key {
				case "window":
					window, err = time.ParseDuration(cmdArg.Vals[0])
					if err != nil {
						panic(errors.Newf("could not parse window: %s", err))
					}

				case "long-running":
					longRunning, err = time.ParseDuration(cmdArg.Vals[0])
					if err != nil {
						panic(errors.Newf("could not parse long-running: %s", err))
					}

				default:
					panic(errors.Newf("unknown arg %q", cmdArg.Key))
				}
			}

			a := newAggregator(window, longRunning, c.compactions, c.readAmps)
			windows := a.aggregate()

			var b bytes.Buffer
			for _, w := range windows {
				b.WriteString(w.String())
			}

			return b.String()

		case "reset":
			resetFn()
			return ""

		default:
			return fmt.Sprintf("unknown command %q", td.Cmd)
		}
	})
}
