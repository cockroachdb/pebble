// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package problemspans

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/stretchr/testify/require"
)

func TestByLevel(t *testing.T) {
	const numLevels = 7
	now := crtime.Mono(0)
	nowFn := func() crtime.Mono { return now }
	var byLevel ByLevel
	byLevel.InitForTesting(numLevels, base.DefaultComparer.Compare, nowFn)

	datadriven.RunTest(t, "testdata/by_level", func(t *testing.T, td *datadriven.TestData) string {
		var nowStr string
		if td.MaybeScanArgs(t, "now", &nowStr) {
			var nowVal int
			if n, err := fmt.Sscanf(nowStr, "%ds", &nowVal); err != nil || n != 1 {
				td.Fatalf(t, "error parsing now %q: %v", nowStr, err)
			}
			v := crtime.Mono(time.Duration(nowVal) * time.Second)
			if v < now {
				td.Fatalf(t, "now cannot go backwards")
			}
			now = v
		}

		var out bytes.Buffer
		switch td.Cmd {

		case "add":
			for line := range crstrings.LinesSeq(td.Input) {
				parts := strings.SplitN(line, " ", 2)
				require.Equalf(t, 2, len(parts), "%s", line)
				var level int
				_, err := fmt.Sscanf(parts[0], "L%d", &level)
				require.NoError(t, err)
				bounds, expiration := parseSetLine(parts[1], true /* withTime */)
				// The test uses absolute expiration times.
				byLevel.Add(level, bounds, expiration.Sub(now))
			}

		case "excise":
			for line := range crstrings.LinesSeq(td.Input) {
				bounds, _ := parseSetLine(line, false /* withTime */)
				byLevel.Excise(bounds)
			}

		case "overlap":
			for line := range crstrings.LinesSeq(td.Input) {
				parts := strings.SplitN(line, " ", 2)
				require.Equal(t, 2, len(parts))
				var level int
				_, err := fmt.Sscanf(parts[0], "L%d", &level)
				require.NoError(t, err)
				bounds, _ := parseSetLine(parts[1], false /* withTime */)
				res := "overlap"
				if !byLevel.Overlaps(level, bounds) {
					res = "no overlap"
				}
				fmt.Fprintf(&out, "%s: %s\n", bounds, res)
			}

		case "is-empty":
			if byLevel.IsEmpty() {
				out.WriteString("empty\n")
			} else {
				out.WriteString("not empty\n")
			}
		default:
			td.Fatalf(t, "unknown command %q", td.Cmd)
		}
		out.WriteString("ByLevel:\n")
		for l := range crstrings.LinesSeq(byLevel.String()) {
			fmt.Fprintf(&out, "  %s\n", l)
		}
		return out.String()
	})
}
