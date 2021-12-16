// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package logs

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	compactionStartLine = `I211215 14:26:56.012382 51831533 3@vendor/github.com/cockroachdb/pebble/compaction.go:1845 ⋮ [n5,pebble,s5] 1216510  [JOB 284925] compacting(default) L2 [442555] (4.2 M) + L3 [445853] (8.4 M)`
	compactionEndLine   = `I211215 14:26:56.318543 51831533 3@vendor/github.com/cockroachdb/pebble/compaction.go:1886 ⋮ [n5,pebble,s5] 1216554  [JOB 284925] compacted(default) L2 [442555] (4.2 M) + L3 [445853] (8.4 M) -> L3 [445883 445887] (13 M), in 0.3s, output rate 42 M/s`
	flushStartLine      = `I211213 16:23:48.903751 21136 3@vendor/github.com/cockroachdb/pebble/event.go:599 ⋮ [n9,pebble,s9] 24 [JOB 10] flushing 2 memtables to L0`
	flushEndLine        = `I211213 16:23:49.134464 21136 3@vendor/github.com/cockroachdb/pebble/event.go:603 ⋮ [n9,pebble,s9] 26 [JOB 10] flushed 2 memtables to L0 [1535806] (1.3 M), in 0.2s, output rate 5.8 M/s`
	readAmpLine         = `I211215 14:55:15.802648 155 kv/kvserver/store.go:2668 ⋮ [n5,s5] 109057 +  total     44905   672 G       -   1.2 T   714 G   118 K   215 G    34 K   4.0 T   379 K   2.8 T     514     3.4`
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
