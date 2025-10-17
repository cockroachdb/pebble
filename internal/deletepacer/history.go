// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package deletepacer

import (
	"time"

	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/pebble/internal/invariants"
)

// history is a helper used to keep track of the recent history of a set of
// data points (in our case deleted bytes), at limited granularity.
// Specifically, we split the desired timeframe into 100 "epochs" and all times
// are effectively rounded down to the nearest epoch boundary.
type history struct {
	epochDuration time.Duration
	startTime     crtime.Mono
	// currEpoch is the epoch of the most recent operation.
	currEpoch int64
	// val contains the recent epoch values.
	// val[currEpoch % historyEpochs] is the current epoch.
	// val[(currEpoch + 1) % historyEpochs] is the oldest epoch.
	val [historyEpochs]uint64
	// sum is always equal to the sum of values in val.
	sum uint64
}

const historyEpochs = 100

// Init the history helper to keep track of data over the given number of
// seconds.
func (h *history) Init(now crtime.Mono, timeframe time.Duration) {
	*h = history{
		epochDuration: timeframe / time.Duration(historyEpochs),
		startTime:     now,
		currEpoch:     0,
		sum:           0,
	}
}

// Add adds a value for the current time.
func (h *history) Add(now crtime.Mono, val uint64) {
	h.advance(now)
	h.val[h.currEpoch%historyEpochs] += val
	h.sum += val
}

// Sum returns the sum of recent values. The result is approximate in that the
// cut-off time is within 1% of the exact one.
func (h *history) Sum(now crtime.Mono) uint64 {
	h.advance(now)
	return h.sum
}

func (h *history) epoch(t crtime.Mono) int64 {
	return int64(t.Sub(h.startTime) / h.epochDuration)
}

// advance advances the time to the given time.
func (h *history) advance(now crtime.Mono) {
	epoch := h.epoch(now)
	for h.currEpoch < epoch {
		h.currEpoch++
		// Forget the data for the oldest epoch.
		h.sum = invariants.SafeSub(h.sum, h.val[h.currEpoch%historyEpochs])
		h.val[h.currEpoch%historyEpochs] = 0
	}
}
