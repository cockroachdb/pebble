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
	"github.com/cockroachdb/pebble/internal/aligned"
	"github.com/cockroachdb/pebble/internal/binfmt"
)

func TestUintEncoding(t *testing.T) {
	for _, r := range interestingIntRanges {
		actual := DetermineUintEncoding(r.Min, r.Max)
		if actual != r.ExpectedEncoding {
			t.Errorf("%d/%d expected %s, but got %s", r.Min, r.Max, r.ExpectedEncoding, actual)
		}
	}
}

func TestUints(t *testing.T) {
	var b UintBuilder

	datadriven.RunTest(t, "testdata/uints", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "init":
			defaultZero := td.HasArg("default-zero")
			b.init(defaultZero)
			return ""
		case "write":
			for _, f := range strings.Fields(td.Input) {
				delim := strings.IndexByte(f, ':')
				i, err := strconv.Atoi(f[:delim])
				if err != nil {
					return err.Error()
				}
				v, err := strconv.ParseUint(f[delim+1:], 10, 64)
				if err != nil {
					return err.Error()
				}
				b.Set(i, v)
			}
			return ""
		case "size":
			var offset uint32
			var rowCounts []int
			td.ScanArgs(t, "rows", &rowCounts)
			td.MaybeScanArgs(t, "offset", &offset)
			var out bytes.Buffer
			for _, rows := range rowCounts {
				sz := b.Size(rows, offset)
				if offset > 0 {
					fmt.Fprintf(&out, "Size(%d, %d) = %d [%d w/o offset]\n", rows, offset, sz, sz-offset)
				} else {
					fmt.Fprintf(&out, "Size(%d, %d) = %d\n", rows, offset, sz)
				}
			}
			return out.String()
		case "finish":
			var rows int
			var offset uint32
			td.ScanArgs(t, "rows", &rows)
			td.MaybeScanArgs(t, "offset", &offset)

			sz := b.Size(rows, offset)
			buf := aligned.ByteSlice(int(sz))
			_ = b.Finish(0, rows, offset, buf)
			f := binfmt.New(buf).LineWidth(20)
			if offset > 0 {
				f.HexBytesln(int(offset), "artificial start offset")
			}
			uintsToBinFormatter(f, rows, b.DataType(0), nil)
			return f.String()
		default:
			panic(fmt.Sprintf("unknown command: %s", td.Cmd))
		}
	})
}
