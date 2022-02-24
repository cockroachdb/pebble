// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
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

func TestFragmentBoundIterator(t *testing.T) {
	cmp := base.DefaultComparer.Compare
	var buf bytes.Buffer
	var iter fragmentBoundIterator

	formatKey := func(bk boundKey) {
		if !bk.valid() {
			fmt.Fprint(&buf, ".")
			return
		}
		fmt.Fprintf(&buf, "%s", bk)
	}

	datadriven.RunTest(t, "testdata/fragment_bound_iterator", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			var spans []Span
			lines := strings.Split(strings.TrimSpace(td.Input), "\n")
			for _, line := range lines {
				spans = append(spans, parseSpanWithKind(t, line))
			}
			iter.init(NewIter(cmp, spans))
			return "OK"
		case "iter":
			buf.Reset()
			lines := strings.Split(strings.TrimSpace(td.Input), "\n")
			for _, line := range lines {
				bufLen := buf.Len()
				line = strings.TrimSpace(line)
				i := strings.IndexByte(line, ' ')
				iterCmd := line
				if i > 0 {
					iterCmd = string(line[:i])
				}
				switch iterCmd {
				case "first":
					formatKey(iter.first(cmp))
				case "last":
					formatKey(iter.last(cmp))
				case "next":
					formatKey(iter.next(cmp))
				case "prev":
					formatKey(iter.prev(cmp))
				case "seek-ge":
					formatKey(iter.seekGE(cmp, []byte(strings.TrimSpace(line[i:]))))
				case "seek-lt":
					formatKey(iter.seekLT(cmp, []byte(strings.TrimSpace(line[i:]))))
				}
				if buf.Len() > bufLen {
					fmt.Fprintln(&buf)
				}
			}
			return strings.TrimSpace(buf.String())

		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
	})
}
