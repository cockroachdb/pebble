// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package filtersim

import (
	"fmt"
	"math"
	"math/rand/v2"
	"runtime"
	"sync"

	"github.com/cockroachdb/crlib/crhumanize"
	"github.com/cockroachdb/pebble/internal/metricsutil"
)

func SimulateFPR(
	numRuns, avgSize int, runExperiment func(size int) (fpr float64),
) (mean, stddev float64) {
	var wg sync.WaitGroup
	ch := make(chan struct{}, numRuns)
	for range numRuns {
		ch <- struct{}{}
	}
	close(ch)

	var fprMu sync.Mutex
	var fpr metricsutil.Welford

	for range runtime.GOMAXPROCS(0) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range ch {
				size := avgSize - avgSize/10 + rand.IntN(avgSize*2/10)
				falsePositiveRate := runExperiment(size)
				fprMu.Lock()
				fpr.Add(falsePositiveRate)
				fprMu.Unlock()
			}
		}()
	}
	wg.Wait()
	return fpr.Mean(), fpr.StdDev()
}

// FormatFPR formats a false positive rate as a percentage with "1 in N" ratio.
func FormatFPR(fpr float64) string {
	l10 := min(3, -int(math.Floor(math.Log10(fpr))))
	return fmt.Sprintf("%.*f%% (1 in %.*f)", l10, fpr*100, 3-l10, 1.0/fpr)
}

// FormatFPRWithStdDev formats a false positive rate mean and standard deviation.
func FormatFPRWithStdDev(mean, stdDev float64) string {
	return fmt.Sprintf("%s ± %s", FormatFPR(mean), crhumanize.Percent(stdDev, mean))
}
