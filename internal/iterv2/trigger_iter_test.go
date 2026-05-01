// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package iterv2

import (
	"fmt"
	"strings"
	"testing"

	"github.com/RaduBerinde/axisds/v3"
	"github.com/RaduBerinde/axisds/v3/regiontree"
	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/testkeys"
)

// testTrigger records trigger events for testing.
type testTrigger struct {
	events []string
}

func (tt *testTrigger) Trigger(key []byte, dir int8) {
	var dirStr string
	if dir > 0 {
		dirStr = "fwd"
	} else {
		dirStr = "bwd"
	}
	tt.events = append(tt.events, fmt.Sprintf("trigger: %s(%s)", key, dirStr))
}

func (tt *testTrigger) drain() string {
	if len(tt.events) == 0 {
		return ""
	}
	s := strings.Join(tt.events, "\n")
	tt.events = tt.events[:0]
	return s
}

func TestTriggerIter(t *testing.T) {
	cmp := testkeys.Comparer.Compare
	var tree regiontree.T[[]byte, int]
	var iter TriggerIter
	var tt testTrigger

	datadriven.RunTest(t, "testdata/trigger_iter", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			tree = regiontree.Make(
				axisds.CompareFn[[]byte](cmp),
				func(a, b int) bool { return a == b },
			)
			for line := range crstrings.LinesSeq(d.Input) {
				line = strings.TrimSpace(line)
				if line == "" {
					continue
				}
				// Parse "[start, end)=count".
				line = strings.TrimPrefix(line, "[")
				parts := strings.SplitN(line, ",", 2)
				start := []byte(strings.TrimSpace(parts[0]))
				rest := strings.TrimSpace(parts[1])
				// rest is "end)=count"; split on ")=" to separate end from count.
				parts2 := strings.SplitN(rest, ")=", 2)
				end := []byte(strings.TrimSpace(parts2[0]))
				var count int
				fmt.Sscanf(strings.TrimSpace(parts2[1]), "%d", &count)
				for range count {
					tree.Update(start, end, func(p int) int { return p + 1 })
				}
			}
			// Print the tree contents.
			var buf strings.Builder
			iFmt := axisds.MakeIntervalFormatter(func(b []byte) string {
				return string(b)
			})
			n := 0
			for interval, prop := range tree.All() {
				if n > 0 {
					buf.WriteString("\n")
				}
				fmt.Fprintf(&buf, "%s=%d", iFmt(interval), prop)
				n++
			}
			if n == 0 {
				return "<empty>"
			}
			return buf.String()

		case "iter":
			var lower, upper string
			d.MaybeScanArgs(t, "lower", &lower)
			d.MaybeScanArgs(t, "upper", &upper)
			key := func(s string) []byte {
				if s == "" {
					return nil
				}
				return []byte(s)
			}
			iter.Init(cmp, &tree, &tt, key(lower), key(upper))
			return RunIterOps(t, &iter, d.Input) + tt.drain()

		case "continue":
			return RunIterOps(t, &iter, d.Input) + tt.drain()

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}
