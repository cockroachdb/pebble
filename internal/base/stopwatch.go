// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import (
	"time"

	"github.com/cockroachdb/crlib/crtime"
)

// DeterministicReadDurationForTesting is for tests that want a deterministic
// value of the time to read (that is not in the cache). The return value is a
// function that must be called before the test exits.
func DeterministicReadDurationForTesting() func() {
	drbdForTesting := deterministicReadDurationForTesting
	deterministicReadDurationForTesting = true
	return func() {
		deterministicReadDurationForTesting = drbdForTesting
	}
}

var deterministicReadDurationForTesting = false

type deterministicStopwatchForTesting struct {
	startTime crtime.Mono
}

func MakeStopwatch() deterministicStopwatchForTesting {
	return deterministicStopwatchForTesting{startTime: crtime.NowMono()}
}

func (w deterministicStopwatchForTesting) Stop() time.Duration {
	dur := w.startTime.Elapsed()
	if deterministicReadDurationForTesting {
		dur = SlowReadTracingThreshold
	}
	return dur
}

// TODO(sumeer): should the threshold be configurable.
const SlowReadTracingThreshold = 5 * time.Millisecond
