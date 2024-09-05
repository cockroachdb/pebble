// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/stretchr/testify/require"
)

func TestIndexBlock(t *testing.T) {
	var r IndexReader
	var buf bytes.Buffer
	datadriven.RunTest(t, "testdata/index_block", func(t *testing.T, d *datadriven.TestData) string {
		buf.Reset()
		switch d.Cmd {
		case "build":
			var w IndexBlockWriter
			w.Init()
			for _, line := range strings.Split(d.Input, "\n") {
				fields := strings.Fields(line)
				var err error
				var h block.Handle
				h.Offset, err = strconv.ParseUint(fields[1], 10, 64)
				require.NoError(t, err)
				h.Length, err = strconv.ParseUint(fields[2], 10, 64)
				require.NoError(t, err)
				var bp []byte
				if len(fields) > 3 {
					bp = []byte(fields[3])
				}
				w.AddBlockHandle([]byte(fields[0]), h, bp)
			}
			fmt.Fprintf(&buf, "UnsafeSeparator(Rows()-1) = %q\n", w.UnsafeSeparator(w.Rows()-1))
			data := w.Finish()
			r.Init(data)
			fmt.Fprint(&buf, r.DebugString())
			return buf.String()
		case "iter":
			var it IndexIter
			it.InitReader(&r)
			for _, line := range strings.Split(d.Input, "\n") {
				fields := strings.Fields(line)
				var valid bool
				switch fields[0] {
				case "seek-ge":
					valid = it.SeekGE([]byte(fields[1]))
				case "first":
					valid = it.First()
				case "last":
					valid = it.Last()
				case "next":
					valid = it.Next()
				case "prev":
					valid = it.Prev()
				default:
					panic(fmt.Sprintf("unknown command: %s", fields[0]))
				}
				if valid {
					var bp string
					bhp, err := it.BlockHandleWithProperties()
					if err != nil {
						fmt.Fprintf(&buf, "<err invalid bh: %s>", err)
						continue
					}
					if len(bhp.Props) > 0 {
						bp = fmt.Sprintf(" props=%q", bhp.Props)
					}
					fmt.Fprintf(&buf, "block %d: %d-%d%s\n", it.row, bhp.Offset, bhp.Offset+bhp.Length, bp)
				} else {
					fmt.Fprintln(&buf, ".")
				}
			}
			return buf.String()
		default:
			panic(fmt.Sprintf("unknown command: %s", d.Cmd))
		}
	})
}
