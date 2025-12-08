// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package bloom

import (
	"fmt"
	"math"
	"math/rand/v2"

	"github.com/cockroachdb/crlib/crhumanize"
	"github.com/cockroachdb/pebble/internal/metricsutil"
)

func SimulateFPR(bitsPerKey int, numProbes int) (float64, string) {
	const size = 20_000
	const numRuns = 200

	rng := rand.New(rand.NewPCG(uint64(bitsPerKey), uint64(numProbes)))
	var fpr metricsutil.Welford
	for range numRuns {
		numHashes := size - size/10 + rng.IntN(size*2/10)
		w := newTableFilterWriter(uint32(bitsPerKey))
		w.numProbes = uint32(numProbes)
		for range numHashes {
			h := rng.Uint32()
			ofs := w.numHashes % hashBlockLen
			if ofs == 0 {
				// Time for a new block.
				w.blocks = append(w.blocks, new(hashBlock))
			}
			w.blocks[len(w.blocks)-1][ofs] = h
			w.numHashes++
		}
		filter := tableFilter(w.Finish(nil))

		queries := cacheLineSize * numHashes
		negatives := 0
		nLines := uint32(len(filter)-5) / cacheLineSize
		for range queries {
			h := rng.Uint32()
			delta := h>>17 | h<<15
			lineIdx := h % nLines
			// Set up a pointer to a [cacheLineSize]byte array. This avoids bound
			// checks inside the loop.
			line := (*[cacheLineSize]byte)(filter[lineIdx*cacheLineSize : (lineIdx+1)*cacheLineSize])
			for range numProbes {
				// The bit position within the line is (h % cacheLineBits).
				//  byte index: (h % cacheLineBits)/8 = (h/8) % cacheLineSize
				//  bit index: (h % cacheLineBits)%8 = h%8
				val := line[(h>>3)&(cacheLineSize-1)] & (1 << (h & 7)) //gcassert:bce
				if val == 0 {
					negatives++
					break
				}
				h += delta
			}
		}
		positiveRate := 1.0 - (float64(negatives) / float64(queries))
		truePositiveRate := float64(numHashes) / float64(1<<32)
		falsePositiveRate := positiveRate - truePositiveRate
		fpr.Add(falsePositiveRate)
	}
	mean := fpr.Mean()
	fmt.Printf(
		"%d bits per key, %d probes: FPR %s ± %s\n", bitsPerKey, numProbes,
		formatFPR(mean), crhumanize.Percent(fpr.StdDev(), mean),
	)
	return mean, fmt.Sprintf("%s ± %s", formatFPR(mean), crhumanize.Percent(fpr.StdDev(), mean))
}

// formatFPR formats a false positive rate as a percentage with "1 in N" ratio.
func formatFPR(fpr float64) string {
	ratio := int(math.Round(1.0 / fpr))
	switch {
	case fpr >= 0.1:
		return fmt.Sprintf("%.0f%% (1 in %d)", fpr*100, ratio)
	case fpr >= 0.01:
		return fmt.Sprintf("%.1f%% (1 in %d)", fpr*100, ratio)
	case fpr >= 0.001:
		return fmt.Sprintf("%.2f%% (1 in %d)", fpr*100, ratio)
	default:
		return fmt.Sprintf("%.3f%% (1 in %d)", fpr*100, ratio)
	}
}
