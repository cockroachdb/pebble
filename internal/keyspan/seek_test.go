// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
)

func TestSeek(t *testing.T) {
	cmp := base.DefaultComparer.Compare
	fmtKey := base.DefaultComparer.FormatKey
	var buf bytes.Buffer
	var spans []Span

	datadriven.RunTest(t, "testdata/seek", func(t *testing.T, d *datadriven.TestData) string {
		buf.Reset()
		switch d.Cmd {
		case "build":
			spans = buildSpans(t, cmp, fmtKey, d.Input, base.InternalKeyKindRangeDelete)
			for _, s := range spans {
				fmt.Fprintln(&buf, s)
			}
			return buf.String()
		case "seek-ge", "seek-le":
			var iter FragmentIterator = NewIter(cmp, spans)
			if cmdArg, ok := d.Arg("probes"); ok {
				iter = attachProbes(iter, probeContext{log: &buf},
					parseProbes(cmdArg.Vals...)...)
			}

			seek := SeekLE
			if d.Cmd == "seek-ge" {
				seek = func(_ base.Compare, iter FragmentIterator, key []byte) (*Span, error) {
					return iter.SeekGE(key)
				}
			}

			for line := range crstrings.LinesSeq(d.Input) {
				parts := strings.Fields(line)
				if len(parts) != 2 {
					return fmt.Sprintf("malformed input: %s", line)
				}
				seq := base.ParseSeqNum(parts[1])
				span, err := seek(cmp, iter, []byte(parts[0]))
				if err != nil {
					fmt.Fprintf(&buf, "<nil> <err=%q>\n", err)
				} else if span == nil {
					fmt.Fprintln(&buf, "<nil>")
				} else {
					visible := span.Visible(seq)
					span = &visible
					fmt.Fprintln(&buf, span)
				}
			}
			return buf.String()
		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}
