// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"math"
	"time"

	"github.com/petermattis/pebble/internal/rate"
)

type findPeakBounds struct {
	lower, upper float64
}

// findPeakLoop finds the peak sustainable throughput by trialing rate limits. If a
// rate trial causes decreasing compaction debt the next trial will be for a higher
// rate. If a rate trial causes a write stall or increasing compaction debt, the
// next trial will be for a lower rate.
//
// TODO(ajkr): the implementation has several problems:
// (1) decreasing compaction debt over a few intervals may mislead us into
//     thinking we should raise the rate, while actually if we had kept the same
//     rate for longer we'd have hit memtable or L0 file count stalls. Instead
//     of a fixed number of intervals, we could require a trend lasts for one (or
//     more) full L0->Lbase compaction cycles.
// (2) at low compaction debt levels (below compactionSlowdownThreshold), the
//     rate limiter prevents compaction from happening at full speed. This makes
//     it more likely we observe increasing debt and thus lower the rate, perhaps
//     unnecessarily. We currently have a workaround to treat debt below 1GB as
//     non-increasing regardless of the actual trend; however, this does not solve
//     the problem in all cases.
// (3) also at low compaction debt levels, the write-amp is higher due to the
//     higher fanout. Again, this makes us more likely to see increasing debt
//     and lower the rate. Ideally we would run the benchmark as close to the
//     disk space limit as possible in order to minimize write-amp.
func findPeakLoop(db DB, limiter *rate.Limiter, update chan<- findPeakBounds) {
	const (
		// If compaction debt trends in the same direction for trendLen consecutive
		// intervals, the current rate trial ends. The direction of the trend
		// determines whether the rate increases or decreases.
		trendLen = 6
		// If trialLen intervals pass without identifying a trend of length trendLen,
		// the current rate trial ends. In this case the compaction debt at the
		// beginning and end of the rate trial are compared to decide whether the rate
		// should increase or decrease.
		trialLen = 18
		// Interval between measurements of compaction debt
		interval = 5 * time.Second
	)

	// The compaction debts seen in the current rate trial
	var compactionDebtHistory []uint64
	// Reserved ops at the start of the current rate trial
	trialStartOps := int64(0)
	// Time the current rate trial started
	trialStartTime := time.Now()
	bounds := findPeakBounds{
		lower: float64(0),
		upper: float64(limiter.Limit()),
	}

	// finishTrial updates the accounting info and reports updates to the test loop
	finishTrial := func() {
		metrics := db.Metrics()
		compactionDebtHistory = compactionDebtHistory[:0]
		compactionDebtHistory = append(compactionDebtHistory, metrics.EstimatedCompactionDebt)
		trialStartOps = limiter.TotalReserved()
		trialStartTime = time.Now()
		limiter.SetLimit(rate.Limit((bounds.lower + bounds.upper) / 2))
		update <- bounds
	}

	isIncreasing := func(i, j int) bool {
		// If compaction rate is close to zero (here defined as less than 1GB),
		// count it as non-increasing. That allows us to increase the rate if a
		// trivial compaction debt is seen for trendLen consecutive intervals, even
		// if debt is not strictly decreasing over those intervals. It also mitigates
		// the risk that we lower the rate due to seeing slowly increasing compaction
		// debt while compaction is still being throttled. Although ideally we'd use
		// compactionSlowdownThreshold instead of 1GB.
		return compactionDebtHistory[j] >= (1<<30) &&
			compactionDebtHistory[j] > compactionDebtHistory[i]
	}

	// hasTrend checks `compactionDebtHistory` to see if there's a trend in the direction
	// indicated by `increasing`. Or, if the current trial is over, just use the first and
	// last debt to determine trend.
	hasTrend := func(increasing bool) bool {
		n := len(compactionDebtHistory)
		if n < trendLen+1 {
			return false
		}
		var i int
		for i = n - 1; i >= n-trendLen; i-- {
			if isIncreasing(i-1, i) != increasing {
				break
			}
		}
		if i == n-trendLen-1 {
			return true
		}
		if n < trialLen {
			return false
		}
		return increasing == isIncreasing(0, n-1)
	}

	stallBegan, stallEnded := db.(pebbleDB).stallBegan, db.(pebbleDB).stallEnded
	stalled := false
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stallBegan:
			// The current rate is too fast as it caused a stall. Set it as the
			// upper-bound of the search.
			stalled = true
			bounds.upper = math.Min(
				bounds.upper,
				float64(limiter.TotalReserved()-trialStartOps)/
					time.Since(trialStartTime).Seconds())
			// Temporarily disable writes until stall ends
			limiter.SetLimit(0)

		case <-stallEnded:
			// Now that the stall ended, try a new (reduced) rate
			finishTrial()
			stalled = false

		case <-ticker.C:
			if stalled {
				break
			}

			metrics := db.Metrics()
			compactionDebtHistory = append(compactionDebtHistory, metrics.EstimatedCompactionDebt)
			intervalRate := float64(limiter.TotalReserved()-trialStartOps) /
				time.Since(trialStartTime).Seconds()
			if hasTrend(true /*increasing*/) {
				bounds.upper = math.Min(bounds.upper, intervalRate)
				finishTrial()
			} else if hasTrend(false /*increasing*/) {
				bounds.lower = math.Max(bounds.lower, intervalRate)
				finishTrial()
			}
		}
	}
}
