// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package compressionanalyzer

import (
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/sstable/block/blockkind"
)

func TestBuckets(t *testing.T) {
	datadriven.RunTest(t, "testdata/buckets", func(t *testing.T, td *datadriven.TestData) string {
		var buf strings.Builder
		switch td.Cmd {
		case "block-size":
			for _, l := range crstrings.Lines(td.Input) {
				size, err := strconv.Atoi(l)
				if err != nil {
					td.Fatalf(t, "%v", err)
				}
				fmt.Fprintf(&buf, "%d: %s\n", size, MakeBlockSize(size))
			}

		case "compressibility":
			for _, l := range crstrings.Lines(td.Input) {
				var uncompressed, compressed int
				if _, err := fmt.Sscanf(l, "%d %d", &uncompressed, &compressed); err != nil {
					td.Fatalf(t, "%v", err)
				}
				c := MakeCompressibility(uncompressed, compressed)
				fmt.Fprintf(&buf, "%d %d: %s\n", uncompressed, compressed, c)
			}

		case "example-buckets-string":
			minSamples := 1
			td.MaybeScanArgs(t, "min-samples", &minSamples)
			buckets := exampleBuckets()
			buf.WriteString(buckets.String(minSamples))

		case "example-buckets-csv":
			minSamples := 1
			td.MaybeScanArgs(t, "min-samples", &minSamples)
			buckets := exampleBuckets()
			buf.WriteString(buckets.ToCSV(minSamples))

		default:
			td.Fatalf(t, "unknown command %s", td.Cmd)
		}
		return buf.String()
	})
}

func exampleBuckets() Buckets {
	var buckets Buckets
	r := rand.New(rand.NewPCG(0, 0))
	kinds := blockkind.All()
	for n := 0; n < 10; n++ {
		k := kinds[r.IntN(len(kinds))]
		sz := BlockSize(r.IntN(int(numBlockSizes)))
		c := Compressibility(r.IntN(int(numCompressibility)))
		b := &buckets[k][sz][c]
		for range int(5 * r.ExpFloat64()) {
			b.UncompressedSize.Add(100 + float64(r.IntN(64*1024)))
			for j := range b.Experiments {
				e := &b.Experiments[j]
				blockSize := uint64(50 + r.IntN(100))
				e.CompressionRatio.Add(float64(j+1)+0.1*float64(r.IntN(10)), blockSize)
				e.CompressionTime.Add(float64((j+1)*10)+0.1*float64(r.IntN(10)), blockSize)
				e.DecompressionTime.Add(float64((j+1)*100)+0.1*float64(r.IntN(10)), blockSize)
			}
		}
	}
	return buckets
}
