// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
)

func runFragmentIteratorCmd(iter FragmentIterator, input string, extraInfo func() string) string {
	var b bytes.Buffer
	for _, line := range strings.Split(input, "\n") {
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}
		var span Span
		switch parts[0] {
		case "seek-ge":
			if len(parts) != 2 {
				return "seek-ge <key>\n"
			}
			span = iter.SeekGE([]byte(strings.TrimSpace(parts[1])))
		case "seek-lt":
			if len(parts) != 2 {
				return "seek-lt <key>\n"
			}
			span = iter.SeekLT([]byte(strings.TrimSpace(parts[1])))
		case "first":
			span = iter.First()
		case "last":
			span = iter.Last()
		case "next":
			span = iter.Next()
		case "prev":
			span = iter.Prev()
		case "set-bounds":
			if len(parts) != 3 {
				return fmt.Sprintf("set-bounds expects 2 bounds, got %d", len(parts)-1)
			}
			l, u := []byte(parts[1]), []byte(parts[2])
			if parts[1] == "." {
				l = nil
			}
			if parts[2] == "." {
				u = nil
			}
			iter.SetBounds(l, u)
			fmt.Fprintf(&b, ".\n")
			continue
		default:
			return fmt.Sprintf("unknown op: %s", parts[0])
		}
		if span.Valid() {
			fmt.Fprintf(&b, "%s", span)
			if extraInfo != nil {
				fmt.Fprintf(&b, " (%s)", extraInfo())
			}
			b.WriteByte('\n')
		} else if err := iter.Error(); err != nil {
			fmt.Fprintf(&b, "err=%v\n", err)
		} else {
			fmt.Fprintf(&b, ".\n")
		}
	}
	return b.String()
}

func TestIter(t *testing.T) {
	var spans []Span
	datadriven.RunTest(t, "testdata/iter", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			spans = nil
			for _, line := range strings.Split(d.Input, "\n") {
				spans = append(spans, ParseSpan(line))
			}
			return ""

		case "iter":
			iter := NewIter(base.DefaultComparer.Compare, spans)
			defer iter.Close()

			return runFragmentIteratorCmd(iter, d.Input, nil)
		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}
