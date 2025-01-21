// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/crlib/crbytes"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/v2/internal/binfmt"
	"github.com/cockroachdb/pebble/v2/internal/treeprinter"
)

func TestByteWidth(t *testing.T) {
	for _, tc := range []struct {
		val      uint64
		expected uint8
	}{
		{val: 0, expected: 0},
		{val: 1, expected: 1},
		{val: 100, expected: 1},
		{val: 255, expected: 1},
		{val: 256, expected: 2},
		{val: 500, expected: 2},
		{val: 511, expected: 2},
		{val: 512, expected: 2},
		{val: 60000, expected: 2},
		{val: 65535, expected: 2},
		{val: 65536, expected: 4},
		{val: 1 << 30, expected: 4},
		{val: 1<<32 - 1, expected: 4},
		{val: 1 << 32, expected: 8},
		{val: 1 << 50, expected: 8},
		{val: math.MaxUint64, expected: 8},
	} {
		if w := byteWidth(tc.val); w != tc.expected {
			t.Errorf("byteWidth(%d) = %d, want %d", tc.val, w, tc.expected)
		}
	}
}

func TestUintEncoding(t *testing.T) {
	for _, r := range interestingIntRanges {
		actual := DetermineUintEncoding(r.Min, r.Max, UintEncodingRowThreshold)
		if actual != r.ExpectedEncoding {
			t.Errorf("%d/%d expected %s, but got %s", r.Min, r.Max, r.ExpectedEncoding, actual)
		}
	}
	// Testcases around avoiding delta encodings for small number of rows.
	for _, tc := range []struct {
		min, max uint64
		numRows  int
		expected UintEncoding
	}{
		{min: 100, max: 300, numRows: 1, expected: makeUintEncoding(2, false)},
		{min: 100, max: 300, numRows: 5, expected: makeUintEncoding(2, false)},
		{min: 100, max: 300, numRows: 7, expected: makeUintEncoding(2, false)},
		{min: 100, max: 300, numRows: 8, expected: makeUintEncoding(1, true)},
		{min: 100, max: 300, numRows: 1000, expected: makeUintEncoding(1, true)},

		{min: 65000, max: 65100, numRows: 1, expected: makeUintEncoding(2, false)},
		{min: 65000, max: 65100, numRows: 3, expected: makeUintEncoding(2, false)},
		{min: 65000, max: 65100, numRows: 7, expected: makeUintEncoding(2, false)},
		{min: 65000, max: 65100, numRows: 8, expected: makeUintEncoding(1, true)},
		{min: 65000, max: 65100, numRows: 10, expected: makeUintEncoding(1, true)},

		{min: 80000, max: 100000, numRows: 1, expected: makeUintEncoding(4, false)},
		{min: 80000, max: 100000, numRows: 2, expected: makeUintEncoding(4, false)},
		{min: 80000, max: 100000, numRows: 3, expected: makeUintEncoding(4, false)},
		{min: 80000, max: 100000, numRows: 4, expected: makeUintEncoding(2, true)},
		{min: 80000, max: 100000, numRows: 10, expected: makeUintEncoding(2, true)},

		{min: 1 << 40, max: 1<<40 + 100, numRows: 1, expected: makeUintEncoding(8, false)},
		{min: 1 << 40, max: 1<<40 + 100, numRows: 2, expected: makeUintEncoding(1, true)},
		{min: 1 << 40, max: 1<<40 + 100, numRows: 3, expected: makeUintEncoding(1, true)},

		{min: 1 << 40, max: 1<<40 + 1000, numRows: 1, expected: makeUintEncoding(8, false)},
		{min: 1 << 40, max: 1<<40 + 1000, numRows: 2, expected: makeUintEncoding(2, true)},
		{min: 1 << 40, max: 1<<40 + 1000, numRows: 3, expected: makeUintEncoding(2, true)},
	} {
		actual := DetermineUintEncoding(tc.min, tc.max, tc.numRows)
		if actual != tc.expected {
			t.Errorf("%d/%d/%d expected %s, but got %s", tc.min, tc.max, tc.numRows, tc.expected, actual)
		}
	}

}

func TestUints(t *testing.T) {
	var b UintBuilder
	var out bytes.Buffer

	datadriven.RunTest(t, "testdata/uints", func(t *testing.T, td *datadriven.TestData) string {
		out.Reset()
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
		case "get":
			var indices []int
			td.ScanArgs(t, "indices", &indices)
			for _, i := range indices {
				fmt.Fprintf(&out, "b.Get(%d) = %d\n", i, b.Get(i))
			}
			return out.String()
		case "size":
			var offset uint32
			var rowCounts []int
			td.ScanArgs(t, "rows", &rowCounts)
			td.MaybeScanArgs(t, "offset", &offset)
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
			buf := crbytes.AllocAligned(int(sz))
			_ = b.Finish(0, rows, offset, buf)
			f := binfmt.New(buf).LineWidth(20)
			tp := treeprinter.New()
			n := tp.Child("uints")
			if offset > 0 {
				f.HexBytesln(int(offset), "artificial start offset")
				f.ToTreePrinter(n)
			}
			uintsToBinFormatter(f, n, rows, nil)
			return tp.String()
		default:
			panic(fmt.Sprintf("unknown command: %s", td.Cmd))
		}
	})
}
