// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rangekey

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/stretchr/testify/require"
)

func TestCoalescer(t *testing.T) {
	var c Coalescer
	var buf bytes.Buffer
	c.Init(testkeys.Comparer.Compare, testkeys.Comparer.FormatKey, func(rks CoalescedSpan) {
		formatRangeKeySpan(&buf, &rks)
	})

	datadriven.RunTest(t, "testdata/coalescer", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "add":
			buf.Reset()

			lines := strings.Split(strings.TrimSpace(td.Input), "\n")
			for _, line := range lines {
				startKey, value := Parse(line)
				endKey, splitValue, ok := DecodeEndKey(startKey.Kind(), value)
				require.True(t, ok)
				require.NoError(t, c.Add(keyspan.Span{
					Start: startKey,
					End:   endKey,
					Value: splitValue,
				}))
			}
			return buf.String()

		case "add-reverse":
			buf.Reset()

			lines := strings.Split(strings.TrimSpace(td.Input), "\n")
			for _, line := range lines {
				startKey, value := Parse(line)
				endKey, splitValue, ok := DecodeEndKey(startKey.Kind(), value)
				require.True(t, ok)
				require.NoError(t, c.AddReverse(keyspan.Span{
					Start: startKey,
					End:   endKey,
					Value: splitValue,
				}))
			}
			return buf.String()

		case "finish":
			buf.Reset()
			c.Finish()
			return buf.String()
		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
	})
}

func formatRangeKeySpan(w io.Writer, rks *CoalescedSpan) {
	if rks == nil {
		fmt.Fprintf(w, ".")
		return
	}
	fmt.Fprintf(w, "●   [%s, %s)#%d", rks.Start, rks.End, rks.LargestSeqNum)
	if rks.Delete {
		fmt.Fprintf(w, " (DEL)")
	}
	for i := range rks.Items {
		fmt.Fprintln(w)
		if i != len(rks.Items)-1 {
			fmt.Fprint(w, "├──")
		} else {
			fmt.Fprint(w, "└──")
		}
		fmt.Fprintf(w, " %s", rks.Items[i].Suffix)
		if rks.Items[i].Unset {
			fmt.Fprint(w, " unset")
		} else {
			fmt.Fprintf(w, " : %s", rks.Items[i].Value)
		}
	}
}
