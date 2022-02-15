// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/stretchr/testify/require"
)

func TestRangeKeyIter(t *testing.T) {
	var r *Reader
	var buf bytes.Buffer
	datadriven.RunTest(t, "testdata/range_key_iter", func(td *datadriven.TestData) string {
		buf.Reset()

		switch td.Cmd {
		case "build":
			var err error
			r, err = runBuildRangeKeysCmd(td)
			require.NoError(t, err)
			return ""
		case "iter":
			iter, err := r.NewRangeKeyIter()
			require.NoError(t, err)

			lines := strings.Split(td.Input, "\n")
			for _, line := range lines {
				switch line {
				case "first":
					writeRangeKeyItem(&buf, iter.First())
				case "next":
					writeRangeKeyItem(&buf, iter.Next())
				default:
					return fmt.Sprintf("unrecognized iter command %q", line)
				}
			}
			require.NoError(t, iter.Error())
			require.NoError(t, iter.Close())
			return buf.String()
		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
	})
}

func writeRangeKeyItem(w io.Writer, rk *RangeKeyItem) {
	if rk == nil {
		fmt.Fprintln(w, ".")
		return
	}
	fmt.Fprintf(w, "[%s, %s) #%d %s", rk.Start, rk.End, rk.SeqNum, rk.Kind.String())
	switch rk.Kind {
	case InternalKeyKindRangeKeySet:
		fmt.Fprintf(w, " %s = %s", rk.Suffix, rk.Value)
	case InternalKeyKindRangeKeyUnset:
		fmt.Fprintf(w, " %s", rk.Suffix)
	}
	fmt.Fprintln(w)
}
