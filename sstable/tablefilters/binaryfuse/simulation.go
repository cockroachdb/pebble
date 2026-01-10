// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package binaryfuse

import (
	"fmt"
	"math/rand/v2"
	"sync/atomic"

	"github.com/cockroachdb/crlib/crhumanize"
	"github.com/cockroachdb/pebble/sstable/tablefilters/internal/testutils"
)

func SimulateFPR(avgSize int, fpBits int) (fprMean, fprStdDev, avgBitsPerKey float64) {
	const numRuns = 100

	var sizeSum, filterSizeSum atomic.Uint64

	fprMean, fprStdDev = testutils.SimulateFPR(numRuns, avgSize, func(size int) float64 {
		hc := &hashCollector{}
		hc.Init()
		for range size {
			hc.Add(rand.Uint64())
		}
		filter, ok := buildFilter(hc, fpBits)
		if !ok {
			panic(fmt.Sprintf("could not build filter (size=%d, bits=%d)", size, fpBits))
		}
		sizeSum.Add(uint64(size))
		filterSizeSum.Add(uint64(len(filter)))
		hc.Reset()

		queries := 10000 * (1 << fpBits)
		positives := 0
		for range queries {
			h := rand.Uint64()
			if mayContain(filter, h) {
				positives++
			}
		}
		// The contribution of hash collisions is insignificant.
		return float64(positives) / float64(queries)
	})
	avgBitsPerKey = float64(filterSizeSum.Load()) / float64(sizeSum.Load()) * 8
	fmt.Printf("%d bits per fingerprint, %s size: bpk %.1f  FPR %s\n",
		fpBits, crhumanize.Count(avgSize, crhumanize.Compact),
		avgBitsPerKey,
		testutils.FormatFPRWithStdDev(fprMean, fprStdDev),
	)
	return fprMean, fprStdDev, avgBitsPerKey
}
