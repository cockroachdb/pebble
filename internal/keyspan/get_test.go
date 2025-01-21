// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/v2/internal/testkeys"
)

func TestGet(t *testing.T) {
	cmp := testkeys.Comparer.Compare
	var buf bytes.Buffer
	var spans []Span
	datadriven.RunTest(t, "testdata/get", func(t *testing.T, td *datadriven.TestData) string {
		buf.Reset()
		switch td.Cmd {
		case "define":
			spans = spans[:0]
			lines := strings.Split(strings.TrimSpace(td.Input), "\n")
			for _, line := range lines {
				spans = append(spans, ParseSpan(line))
			}
			return ""
		case "get":
			var iter FragmentIterator = NewIter(cmp, spans)
			if cmdArg, ok := td.Arg("probes"); ok {
				iter = attachProbes(iter, probeContext{log: &buf},
					parseProbes(cmdArg.Vals...)...)
			}

			for _, line := range strings.Split(strings.TrimSpace(td.Input), "\n") {
				s, err := Get(cmp, iter, []byte(line))
				if err != nil {
					fmt.Fprintf(&buf, "Get(%q) = nil <err=%q>\n", line, err)
					continue
				}
				fmt.Fprintf(&buf, "Get(%q) = %v\n", line, s)
			}
			return buf.String()
		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
	})
}
