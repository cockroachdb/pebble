// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package monitoring

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/internal/datadriven"
)

func TestHistogram(t *testing.T) {
	var histogram Histogram

	datadriven.RunTest(t, "../testdata/histogram", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			lines := strings.Split(d.Input, "\n")
			histogram.Clear()
			for _, line := range lines {
				args := strings.Fields(line)
				cmd := args[0]
				args1 := args[1]
				val, err := strconv.ParseUint(args1, 10, 64)
				if err != nil {
					t.Fatal(err)
				}

				switch cmd {
				case "add":
					histogram.Add(val)
				}
			}

			return histogram.String()
		case "percentile":
			pstrs := strings.Split(d.Input, "\n")

			var result string
			for _, pstr := range pstrs {
				p, err := strconv.ParseFloat(pstr, 64)
				if err != nil {
					t.Fatal(err)
				}

				percentile := histogram.Percentile(p)
				result += fmt.Sprintf("%.2f\n", percentile)
			}
			return result
		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

func TestHistogramBucketMapperLen(t *testing.T) {
	if bucketLen != len(bucketMapper.bucketValues) {
		t.Fatalf("bucketLen expected:%v, found:%v", len(bucketMapper.bucketValues), bucketLen)
	}
}

func TestHistogramBucketMapper(t *testing.T) {
	tcs := []struct {
		Value uint64
		Index int
	}{
		{1, 0},
		{10, 5},
		{100, 11},
		{1000, 17},
		{10000, 23},
		{100000, 28},
		{1000000, 34},
		{10000000, 40},
		{100000000, 45},
		{1000000000, 51},
		{10000000000, 57},
		{100000000000, 62},
		{1000000000000, 68},
		{10000000000000, 74},
		{100000000000000, 79},
		{1000000000000000, 85},
		{10000000000000000, 91},
		{100000000000000000, 96},
		{1000000000000000000, 102},
		{10000000000000000000, 108},
	}

	for _, tc := range tcs {
		mIndex := bucketMapper.IndexForValue(tc.Value)
		if tc.Index != mIndex {
			t.Fatalf("expected:%v, found:%v", tc.Index, mIndex)
		}
	}
}
