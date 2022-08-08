// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/stretchr/testify/require"
)

func TestBoundedIter(t *testing.T) {
	getBounds := func(td *datadriven.TestData) (lower, upper []byte, ok bool) {
		for _, cmdArg := range td.CmdArgs {
			switch cmdArg.Key {
			case "lower":
				if len(cmdArg.Vals[0]) > 0 {
					lower = []byte(cmdArg.Vals[0])
				}
				ok = true
			case "upper":
				if len(cmdArg.Vals[0]) > 0 {
					upper = []byte(cmdArg.Vals[0])
				}
				ok = true
			}
		}
		return lower, upper, ok
	}

	cmp := testkeys.Comparer.Compare
	var buf bytes.Buffer
	var iter BoundedIter
	datadriven.RunTest(t, "testdata/bounded_iter", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			var spans []Span
			lines := strings.Split(strings.TrimSpace(td.Input), "\n")
			for _, line := range lines {
				spans = append(spans, ParseSpan(line))
			}
			inner := &invalidatingIter{iter: NewIter(cmp, spans)}
			lower, upper, _ := getBounds(td)
			iter.Init(cmp, inner, lower, upper)
			return ""
		case "iter":
			buf.Reset()
			if lower, upper, ok := getBounds(td); ok {
				iter.SetBounds(lower, upper)
			}

			lines := strings.Split(strings.TrimSpace(td.Input), "\n")
			for _, line := range lines {
				line = strings.TrimSpace(line)
				i := strings.IndexByte(line, ' ')
				iterCmd := line
				if i > 0 {
					iterCmd = string(line[:i])
				}
				switch iterCmd {
				case "first":
					fmt.Fprintln(&buf, iter.First())
				case "last":
					fmt.Fprintln(&buf, iter.Last())
				case "next":
					fmt.Fprintln(&buf, iter.Next())
				case "prev":
					fmt.Fprintln(&buf, iter.Prev())
				case "seek-ge":
					fmt.Fprintln(&buf, iter.SeekGE([]byte(strings.TrimSpace(line[i:]))))
				case "seek-lt":
					fmt.Fprintln(&buf, iter.SeekLT([]byte(strings.TrimSpace(line[i:]))))
				default:
					return fmt.Sprintf("unrecognized iter command %q", iterCmd)
				}
				require.NoError(t, iter.Error())
			}
			return strings.TrimSpace(buf.String())
		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
	})
}
