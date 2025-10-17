// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package deletepacer

import "time"

// Options contains configuration options for the delete pacer.
type Options struct {
	// BaselineRate returns the baseline deletion rate in bytes/sec. This is the
	// minimum rate at which files are deleted, even when deletions are rare.
	//
	// The actual deletion rate may be higher based on recent deletion patterns,
	// backlog, or low disk space.
	//
	// A returned value of 0 disables deletion pacing (this is also the default).
	BaselineRate func() uint64

	// BacklogTimeframe is the target duration for clearing the backlog queue.
	// Deletions that have been queued for longer than RecentRateWindow (5
	// minutes) are considered backlog. When backlog is detected, the deletion
	// rate is increased to clear it within this timeframe.
	//
	// This prevents the delete queue from growing unbounded when deletions arrive
	// faster than they can be processed. Shorter timeframes result in more
	// aggressive deletion rates.
	//
	// The default value is 5 minutes.
	BacklogTimeframe time.Duration

	// FreeSpaceThresholdBytes is the minimum amount of free disk space to
	// maintain. If free space falls below this threshold, the deletion rate is
	// increased to reclaim enough space to return to the threshold within
	// FreeSpaceTimeframe.
	//
	// The default value is 16GB.
	FreeSpaceThresholdBytes uint64

	// FreeSpaceTimeframe is the target duration for reclaiming disk space when
	// free space falls below FreeSpaceThresholdBytes. The deletion rate is
	// increased to eliminate the deficit within this timeframe.
	//
	// Shorter timeframes result in more aggressive deletion rates when disk space
	// is low.
	//
	// The default value is 10 seconds.
	FreeSpaceTimeframe time.Duration
}

// EnsureDefaults ensures that the default values for all options are set if a
// valid value was not already specified.
func (o *Options) EnsureDefaults() {
	if o.BaselineRate == nil {
		o.BaselineRate = func() uint64 { return 0 }
	}

	if o.BacklogTimeframe == 0 {
		o.BacklogTimeframe = 5 * time.Minute
	}

	if o.FreeSpaceThresholdBytes == 0 {
		o.FreeSpaceThresholdBytes = 16 << 30 // 16 GB
	}

	if o.FreeSpaceTimeframe == 0 {
		o.FreeSpaceTimeframe = 10 * time.Second
	}
}
