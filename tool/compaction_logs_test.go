package tool

import (
	"fmt"
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
		matches []string
	}{
		{
			name:    "compaction start - sentinel",
			re:      sentinelPattern,
			line:    compactionStartLine,
			matches: []string{"compact", "ing"},
		},
		{
			name:    "compaction end - sentinel",
			re:      sentinelPattern,
			line:    compactionEndLine,
			matches: []string{"compact", "ed"},
		},
		{
			name:    "flush start - sentinel",
			re:      sentinelPattern,
			line:    flushStartLine,
			matches: []string{"flush", "ing"},
		},
		{
			name:    "flush end - sentinel",
			re:      sentinelPattern,
			line:    flushEndLine,
			matches: []string{"flush", "ed"},
		},
		{
			name: "compaction start",
			re:   compactionPattern,
			line: compactionStartLine,
			matches: []string{
				"211215 14:26:56.012382",
				"284925",
				"ing",
				"default",
				"2",
				"3",
				"8.4",
				"M",
			},
		},
		{
			name: "compaction end",
			re:   compactionPattern,
			line: compactionEndLine,
			matches: []string{
				"211215 14:26:56.318543",
				"284925",
				"ed",
				"default",
				"2",
				"3",
				"13",
				"M",
			},
		},
		{
			name: "flush start",
			re:   flushPattern,
			line: flushStartLine,
			matches: []string{
				"211213 16:23:48.903751",
				"10",
				"ing",
				"",
				"",
			},
		},
		{
			name: "flush end",
			re:   flushPattern,
			line: flushEndLine,
			matches: []string{
				"211213 16:23:49.134464",
				"10",
				"ed",
				"1.3",
				"M",
			},
		},
		{
			name: "read amp",
			re:   readAmpPattern,
			line: readAmpLine,
			matches: []string{
				"211215 14:55:15.802648",
				"514",
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			matches := tc.re.FindStringSubmatch(tc.line)
			require.NotNil(t, matches)
			matches = matches[1:] // Trim the first sub-match, the whole string.
			fmt.Println(matches)
			require.Equal(t, tc.matches, matches)
		})
	}
}
