// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/v2/internal/base"
)

// TODO(jackson): Add unit tests for all of the various Span methods.

func TestSpan_ParseRoundtrip(t *testing.T) {
	spans := []string{
		"a-c:{(#5,RANGEDEL)}",
		"a-c:{(#5,RANGEDEL) (#2,RANGEDEL)}",
		"h-z:{(#20,RANGEKEYSET,@5,foo) (#15,RANGEKEYUNSET,@9) (#2,RANGEKEYDEL)}",
	}
	for _, input := range spans {
		got := ParseSpan(input).String()
		if got != input {
			t.Errorf("ParseSpan(%q).String() = %q", input, got)
		}
	}
}

func TestSpan_Visible(t *testing.T) {
	var s Span
	datadriven.RunTest(t, "testdata/visible", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			s = ParseSpan(d.Input)
			return fmt.Sprint(s)
		case "visible":
			var buf bytes.Buffer
			for _, line := range strings.Split(d.Input, "\n") {
				snapshot := base.ParseSeqNum(line)
				fmt.Fprintf(&buf, "%-2d: %s\n", snapshot, s.Visible(snapshot))
			}
			return buf.String()
		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

func TestSpan_VisibleAt(t *testing.T) {
	var s Span
	datadriven.RunTest(t, "testdata/visible_at", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			s = ParseSpan(d.Input)
			return fmt.Sprint(s)
		case "visible-at":
			var buf bytes.Buffer
			for _, line := range strings.Split(d.Input, "\n") {
				snapshot := base.ParseSeqNum(line)
				fmt.Fprintf(&buf, "%-2d: %t\n", snapshot, s.VisibleAt(snapshot))
			}
			return buf.String()
		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

func TestSpan_CoversAt(t *testing.T) {
	var s Span
	datadriven.RunTest(t, "testdata/covers_at", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			s = ParseSpan(d.Input)
			return fmt.Sprint(s)
		case "covers-at":
			var buf bytes.Buffer
			for _, line := range strings.Split(d.Input, "\n") {
				fields := strings.Fields(line)
				snapshot := base.ParseSeqNum(fields[0])
				seqNum := base.ParseSeqNum(fields[1])
				fmt.Fprintf(&buf, "%d %d : %t\n", snapshot, seqNum, s.CoversAt(base.SeqNum(snapshot), base.SeqNum(seqNum)))
			}
			return buf.String()
		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}
