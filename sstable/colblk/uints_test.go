// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"bytes"
	"fmt"
	"math"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/crlib/crbytes"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/v2/internal/binfmt"
	"github.com/cockroachdb/pebble/v2/internal/treeprinter"
	"github.com/stretchr/testify/require"
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

func BenchmarkByteWidth(b *testing.B) {
	for _, n := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			vals := make([]uint64, 128)
			for i := range vals {
				vals[i] = (1 << (8 * (n - 1))) + rand.Uint64N((1<<(8*n))-(1<<(8*(n-1))))
			}
			b.ResetTimer()
			var x int
			for i := 0; i < b.N; i++ {
				x += int(byteWidth(vals[i&127]))
			}
			if x != n*b.N {
				b.Fatal("unexpected result")
			}
		})
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
		if !actual.IsDelta() {
			require.Equal(t, actual, DetermineUintEncodingNoDelta(tc.max))
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
			if td.HasArg("zero-struct") {
				b = UintBuilder{}
			}
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

func TestUintsRandomized(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("Seed: %d", seed)

	type config struct {
		defaultZero bool
		rowsSet     int
		rowsFinish  int
		maxValue    uint64
		probNonZero float64
	}

	runTest := func(t *testing.T, cfg config) {
		var b UintBuilder
		b.init(cfg.defaultZero)
		rng := rand.New(rand.NewPCG(0, seed))
		vals := make([]uint64, max(cfg.rowsSet, cfg.rowsFinish))
		for i := 0; i < cfg.rowsSet; i++ {
			if rng.Float64() < cfg.probNonZero {
				vals[i] = rng.Uint64N(cfg.maxValue)
			}
			if vals[i] != 0 || !cfg.defaultZero {
				b.Set(i, vals[i])
			}
		}
		sz := b.Size(cfg.rowsFinish, 0)
		buf := crbytes.AllocAligned(int(sz) + 1 /* extra padding byte for pointer safety */)
		off := b.Finish(0, cfg.rowsFinish, 0, buf)
		require.Equal(t, sz, off)

		uu, endOff := DecodeUnsafeUints(buf, 0, cfg.rowsFinish)
		require.Equal(t, endOff, off)
		for i := 0; i < cfg.rowsFinish; i++ {
			if uu.At(i) != vals[i] {
				t.Fatalf("At(%d) = %d, want %d", i, uu.At(i), vals[i])
			}
		}
	}

	rng := rand.New(rand.NewPCG(0, seed))
	for i := 0; i < 20; i++ {
		rowsSet := rng.IntN(10000)
		cfg := config{
			defaultZero: rng.Float64() < 0.5,
			rowsSet:     rowsSet,
			rowsFinish:  rowsSet,
			maxValue:    math.MaxUint64 >> rng.Uint64N(63),
			probNonZero: rng.Float64(),
		}
		if p := rng.Float64(); p < 0.1 && cfg.defaultZero {
			cfg.rowsFinish = rng.IntN(10000)
		} else if p < 0.2 && cfg.rowsSet > 0 {
			cfg.rowsFinish = cfg.rowsSet - 1
		} else if p < 0.3 && cfg.defaultZero {
			cfg.rowsFinish = cfg.rowsSet + 1
		}
		t.Run(
			fmt.Sprintf("defaultZero=%t,rows=%d,finish=%d,max=%d,probNonZero=%.2f",
				cfg.defaultZero, cfg.rowsSet, cfg.rowsFinish, cfg.maxValue, cfg.probNonZero),
			func(t *testing.T) { runTest(t, cfg) },
		)
	}

}
