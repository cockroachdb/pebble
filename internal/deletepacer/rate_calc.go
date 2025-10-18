// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package deletepacer

import (
	"fmt"
	"time"

	"github.com/cockroachdb/crlib/crtime"
)

// rateCalculator computes the target deletion throughput (bytes/sec) for a
// paced deleter. It maintains a baseline rate and adds a corrective "backlog
// component" using a constant-horizon drain model:
//
//   - Feed-forward: assume the next short interval will see new deletions at the
//     same average rate as the recent window (RecentRateWindow).

//   - Backlog drain: if a queue backlog is observed (or disk free space is
//     below a threshold), add just enough extra rate to eliminate that deficit
//     within a configured timeframe, and *hold that extra rate* until cleared.
//
// The calculator also tracks "debt": bytes from already-initiated deletions that
// we must wait out to respect the current pacing rate. Debt decays at the
// current rate over time, and callers can query how long to pause before
// starting the next deletion.
type rateCalculator struct {
	opts            *Options
	diskFreeSpaceFn DiskFreeSpaceFn

	lastUpdate crtime.Mono

	// currentRate is the target deletion throughput in bytes/sec currently in
	// effect. When pacing is disabled, this is 0.
	currentRate float64

	// debtBytes is the number of bytes of already-started deletions we must
	// "wait out" to respect currentRate.
	debtBytes float64

	// backlogRate is the extra throughput (bytes/sec) needed to eliminate
	// observed backlog within the configured horizon. Once raised, it does not
	// decrease until the backlog is gone (constant-horizon drain).
	backlogRate float64

	// freeSpaceRate is the extra throughput (bytes/sec) needed to eliminate
	// observed free-space deficit within the configured horizon. Once raised, it
	// does not decrease until the free space is above the threshold.
	// (constant-horizon drain).
	freeSpaceRate float64
}

const debug = false

func makeRateCalculator(
	opts *Options, diskFreeSpaceFn DiskFreeSpaceFn, now crtime.Mono,
) rateCalculator {
	return rateCalculator{
		opts:            opts,
		diskFreeSpaceFn: diskFreeSpaceFn,
		lastUpdate:      now,
	}
}

// Update recalculates the current deletion rate and updates the debt based on
// elapsed time. This should be called before each file deletion and when new
// files are enqueued.
//
// The deletion rate is determined by:
//  1. Starting with the baseline rate from the config
//  2. Taking the max of baseline and recent deletion rate (feed-forward estimate)
//  3. Adding extra throughput if there's a backlog or low disk space
//
// The backlog and free-space rates use a "constant-horizon drain" model: once
// raised, they don't decrease until the condition is cleared. This prevents
// oscillation and ensures consistent progress.
//
// The recentPacingBytes argument is the number of bytes that were enqueued for
// deletion in the last RecentRateWindow (regardless if those files were deleted
// already or not); queuedPacingBytes is the total bytes currently waiting in
// the queue.
func (rc *rateCalculator) Update(
	now crtime.Mono, recentPacingBytes uint64, queuedPacingBytes uint64, disablePacing bool,
) {
	deltaT := now.Sub(rc.lastUpdate)
	rc.lastUpdate = now
	baselineRate := rc.opts.BaselineRate()
	if baselineRate == 0 || disablePacing {
		// Pacing is disabled.
		rc.currentRate = 0
		rc.debtBytes = 0
		rc.backlogRate = 0
		rc.freeSpaceRate = 0
		return
	}
	rc.debtBytes = max(0, rc.debtBytes-rc.currentRate*deltaT.Seconds())

	// We assume that we will get incoming deletions at the same average rate as
	// over the last 5 minutes. This is effectively a feed-forward estimate.
	rc.currentRate = float64(baselineRate)
	rc.currentRate = max(rc.currentRate, float64(recentPacingBytes)/RecentRateWindow.Seconds())

	// Everything that stayed in the queue for more than 5 minutes is considered
	// backlog. Once we observe backlog, we increase the rate so that the backlog
	// is wiped out in the backlog time frame.
	if queuedPacingBytes > recentPacingBytes {
		if debug {
			fmt.Printf("backlog: queued %d  recent: %d\n", queuedPacingBytes, recentPacingBytes)
		}
		backlog := float64(queuedPacingBytes - recentPacingBytes)
		// Constant-horizon drain: don't let the rate go down until the backlog is
		// cleared. If backlog increases further, we increase the rate.
		rc.backlogRate = max(rc.backlogRate, backlog/rc.opts.BacklogTimeframe.Seconds())
	} else {
		rc.backlogRate = 0
	}

	if freeBytes := rc.diskFreeSpaceFn(); freeBytes < rc.opts.FreeSpaceThresholdBytes {
		// Constant-horizon drain: don't let the rate go down until the deficit is
		// cleared.
		deficit := float64(rc.opts.FreeSpaceThresholdBytes - freeBytes)
		rc.freeSpaceRate = max(rc.freeSpaceRate, deficit/rc.opts.FreeSpaceTimeframe.Seconds())
	} else {
		rc.freeSpaceRate = 0
	}

	// We take the maximum of the extra rates (deleting a file helps both).
	rc.currentRate += max(rc.backlogRate, rc.freeSpaceRate)
	if debug {
		fmt.Printf("%s current-rate=%v  backlog-rate=%v  free-space-rate=%v  debt-bytes=%v\n",
			time.Now().Format(time.Stamp), rc.currentRate, rc.backlogRate, rc.freeSpaceRate, rc.debtBytes)
	}
}

// AddDebt adds to the deletion debt when starting to delete a file. The debt
// represents bytes of files currently being deleted that we must "wait out" to
// maintain the target deletion rate.
//
// Debt decays over time at the current rate. Before starting the next deletion,
// the pacer waits until the debt is paid off (or low enough).
//
// The debt is capped at 1GB as a safety measure to prevent excessively long
// pauses when deleting very large files.
func (rc *rateCalculator) AddDebt(bytes uint64) {
	if rc.currentRate == 0 {
		// Pacing is disabled.
		return
	}
	// We cap the maximum debt as a fail-safe for extreme cases of very large
	// files. We don't want to pause deletions for minutes in such a case.
	const maxDebt = 1 << 30 // 1GB
	rc.debtBytes += float64(bytes)
	rc.debtBytes = min(rc.debtBytes, maxDebt)
}

// InDebt returns whether we are currently in debt (i.e. need to pause).
func (rc *rateCalculator) InDebt() bool {
	return rc.debtBytes > 0
}

// DebtWaitTime returns how long we need to wait before starting the next file
// deletion to maintain the target deletion rate. The wait time is calculated as
// the current debt divided by the current deletion rate.
//
// This method panics if called when there is no debt (InDebt() returns false).
func (rc *rateCalculator) DebtWaitTime() time.Duration {
	if rc.debtBytes == 0 {
		panic("no debt")
	}
	// We add 1ns as a way to round up. We must also make sure we never return 0,
	// as that will be problematic with synctest (which has exact sleeps).
	return time.Duration(1 + rc.debtBytes/rc.currentRate*float64(time.Second))
}
