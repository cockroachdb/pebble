// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package bloom

import (
	"fmt"
	"math/rand/v2"

	"github.com/cockroachdb/pebble/sstable/tablefilters/internal/filtersim"
)

func SimulateFPR(bitsPerKey int, numProbes int) (float64, string) {
	const numRuns = 1000
	const avgSize = 10_000

	mean, stdDev := filtersim.SimulateFPR(numRuns, avgSize, func(size int) float64 {
		hc := &hashCollector{}
		hc.Init()
		for range size {
			hc.Add(rand.Uint32())
		}
		nLines := calculateNumLines(hc.NumHashes(), uint32(bitsPerKey))
		filter := buildFilter(nLines, uint32(numProbes), hc)
		hc.Reset()

		queries := cacheLineSize * size
		positives := 0
		bits := aliasFilterBits(filter, uint32(len(filter)-5)/cacheLineSize)
		for range queries {
			h := rand.Uint32()
			if bits.probe(uint8(numProbes), h) {
				positives++
			}
		}
		positiveRate := float64(positives) / float64(queries)
		truePositiveRate := float64(size) / float64(1<<32)
		return positiveRate - truePositiveRate
	})

	str := filtersim.FormatFPRWithStdDev(mean, stdDev)
	fmt.Printf("%d bits per key, %d probes: FPR %s\n", bitsPerKey, numProbes, str)
	return mean, str
}
