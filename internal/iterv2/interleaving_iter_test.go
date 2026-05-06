// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package iterv2

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/testkeys"
)

func TestInterleavingIter(t *testing.T) {
	var points []base.InternalKV
	var spans []keyspan.Span
	var iter InterleavingIter

	datadriven.RunTest(t, "testdata/interleaving_iter", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "define-points":
			points = nil
			for line := range crstrings.LinesSeq(d.Input) {
				line = strings.TrimSpace(line)
				if line == "" {
					continue
				}
				k := base.ParseInternalKey(line)
				points = append(points, base.InternalKV{K: k})
			}
			var buf strings.Builder
			for i, kv := range points {
				if i > 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(kv.K.String())
			}
			return buf.String()

		case "define-spans":
			spans = nil
			for line := range crstrings.LinesSeq(d.Input) {
				line = strings.TrimSpace(line)
				if line == "" {
					continue
				}
				spans = append(spans, keyspan.ParseSpan(line))
			}
			var buf strings.Builder
			for i, s := range spans {
				if i > 0 {
					buf.WriteString("\n")
				}
				buf.WriteString(s.String())
			}
			return buf.String()

		case "iter":
			var start, end, lower, upper string
			d.MaybeScanArgs(t, "start", &start)
			d.MaybeScanArgs(t, "end", &end)
			d.MaybeScanArgs(t, "lower", &lower)
			d.MaybeScanArgs(t, "upper", &upper)
			key := func(s string) []byte {
				if s == "" {
					return nil
				}
				return []byte(s)
			}
			pointIter := base.NewFakeIter(testkeys.Comparer, points)
			spanIter := keyspan.NewIter(testkeys.Comparer.Compare, spans)
			iter.Init(
				testkeys.Comparer,
				pointIter,
				spanIter,
				key(start), key(end),
				key(lower), key(upper),
			)
			return RunIterOps(t, &iter, d.Input)

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}
