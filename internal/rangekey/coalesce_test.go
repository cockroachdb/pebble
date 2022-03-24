// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rangekey

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/stretchr/testify/require"
)

func TestCoalesce(t *testing.T) {
	var buf bytes.Buffer
	cmp := testkeys.Comparer.Compare

	datadriven.RunTest(t, "testdata/coalesce", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "coalesce":
			buf.Reset()
			span := keyspan.ParseSpan(td.Input)
			var coalesced keyspan.Span
			if err := Coalesce(cmp, span, &coalesced); err != nil {
				return err.Error()
			}
			fmt.Fprintln(&buf, coalesced)
			return buf.String()
		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
	})
}

func TestIter(t *testing.T) {
	cmp := testkeys.Comparer.Compare
	var iter keyspan.MergingIter
	var buf bytes.Buffer

	datadriven.RunTest(t, "testdata/iter", func(td *datadriven.TestData) string {
		buf.Reset()
		switch td.Cmd {
		case "define":
			visibleSeqNum := base.InternalKeySeqNumMax
			for _, arg := range td.CmdArgs {
				if arg.Key == "visible-seq-num" {
					var err error
					visibleSeqNum, err = strconv.ParseUint(arg.Vals[0], 10, 64)
					require.NoError(t, err)
				}
			}

			var spans []keyspan.Span
			lines := strings.Split(strings.TrimSpace(td.Input), "\n")
			for _, line := range lines {
				spans = append(spans, keyspan.ParseSpan(line))
			}
			transform := func(cmp base.Compare, s keyspan.Span, dst *keyspan.Span) error {
				s = s.Visible(visibleSeqNum)
				return Coalesce(cmp, s, dst)
			}
			iter.Init(cmp, transform, keyspan.NewIter(cmp, spans))
			return "OK"
		case "iter":
			buf.Reset()
			lines := strings.Split(strings.TrimSpace(td.Input), "\n")
			for _, line := range lines {
				line = strings.TrimSpace(line)
				i := strings.IndexByte(line, ' ')
				iterCmd := line
				if i > 0 {
					iterCmd = string(line[:i])
				}
				var s keyspan.Span
				switch iterCmd {
				case "first":
					s = iter.First()
				case "last":
					s = iter.Last()
				case "next":
					s = iter.Next()
				case "prev":
					s = iter.Prev()
				case "seek-ge":
					s = iter.SeekGE([]byte(strings.TrimSpace(line[i:])))
				case "seek-lt":
					s = iter.SeekLT([]byte(strings.TrimSpace(line[i:])))
				default:
					return fmt.Sprintf("unrecognized iter command %q", iterCmd)
				}
				require.NoError(t, iter.Error())
				fmt.Fprint(&buf, s)
				if buf.Len() > 0 {
					fmt.Fprintln(&buf)
				}
			}
			return buf.String()
		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
	})
}
