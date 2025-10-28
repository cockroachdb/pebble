// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"sync"
	"time"

	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/pebble/internal/invariants"
)

// deletionPacerInfo contains any info from the db necessary to make deletion
// pacing decisions (to limit background IO usage so that it does not contend
// with foreground traffic).
type deletionPacerInfo struct {
	freeBytes uint64
	// obsoleteBytes is the total size of obsolete files in the latest version;
	// these are files that have not yet been enqueued for deletion.
	obsoleteBytes uint64
	liveBytes     uint64
}

// deletionPacer rate limits deletions of obsolete files. This is necessary to
// prevent overloading the disk with too many deletions too quickly after a
// large compaction, or an iterator close. On some SSDs, disk performance can be
// negatively impacted if too many blocks are deleted very quickly, so this
// mechanism helps mitigate that.
type deletionPacer struct {
	// If there are less than freeSpaceThreshold bytes of free space on
	// disk, increase the pace of deletions such that we delete enough bytes to
	// get back to the threshold within the freeSpaceTimeframe.
	freeSpaceThreshold uint64
	freeSpaceTimeframe time.Duration

	// If the ratio of obsolete bytes to live bytes is greater than
	// obsoleteBytesMaxRatio, increase the pace of deletions such that we delete
	// enough bytes to get back to the threshold within the obsoleteBytesTimeframe.
	obsoleteBytesMaxRatio  float64
	obsoleteBytesTimeframe time.Duration

	mu struct {
		sync.Mutex

		// history keeps rack of recent deletion history; it used to increase the
		// deletion rate to match the pace of deletions.
		history history

		// bytesToDelete is the sum of pacing bytes for all deletions that were
		// reported but not yet performed.
		bytesToDelete uint64
	}

	targetByteDeletionRate func() int

	getInfo func() deletionPacerInfo
}

const deletePacerHistory = 5 * time.Minute

// newDeletionPacer instantiates a new deletionPacer for use when deleting
// obsolete files.
//
// targetByteDeletionRate is the rate (in bytes/sec) at which we want to
// normally limit deletes (when we are not falling behind or running out of
// space). A value of 0.0 disables pacing.
func newDeletionPacer(
	now crtime.Mono,
	freeSpaceThreshold uint64,
	targetByteDeletionRate func() int,
	freeSpaceTimeframe time.Duration,
	obsoleteBytesMaxRatio float64,
	obsoleteBytesTimeframe time.Duration,
	getInfo func() deletionPacerInfo,
) *deletionPacer {
	d := &deletionPacer{
		freeSpaceThreshold: freeSpaceThreshold,
		freeSpaceTimeframe: freeSpaceTimeframe,

		obsoleteBytesMaxRatio:  obsoleteBytesMaxRatio,
		obsoleteBytesTimeframe: obsoleteBytesTimeframe,

		targetByteDeletionRate: targetByteDeletionRate,
		getInfo:                getInfo,
	}

	d.mu.history.Init(now, deletePacerHistory)
	return d
}

// DeletionEnqueued is used to report a new deletion request to the pacer. The
// pacer uses it to keep track of the recent rate of deletions and potentially
// increase the deletion rate accordingly.
//
// DeletionEnqueued is thread-safe.
func (p *deletionPacer) DeletionEnqueued(now crtime.Mono, bytesToDelete uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.bytesToDelete += bytesToDelete
	p.mu.history.Add(now, int64(bytesToDelete))
}

// DeletionPerformed is used to report that a deletion was performed. The pacer
// uses it to keep track of how many bytes are in the queue.
func (p *deletionPacer) DeletionPerformed(bytesToDelete uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.mu.bytesToDelete < bytesToDelete {
		if invariants.Enabled {
			panic("underflow")
		}
		p.mu.bytesToDelete = 0
	} else {
		p.mu.bytesToDelete -= bytesToDelete
	}
}

// PacingDelay returns the recommended pacing wait time (in seconds) for
// deleting the given number of bytes.
//
// PacingDelay is thread-safe.
func (p *deletionPacer) PacingDelay(now crtime.Mono, bytesToDelete uint64) (waitSeconds float64) {
	targetByteDeletionRate := p.targetByteDeletionRate()
	if targetByteDeletionRate == 0 {
		// Pacing disabled.
		return 0.0
	}

	baseRate := float64(targetByteDeletionRate)
	// If recent deletion rate is more than our target, use that so that we don't
	// fall behind.
	historicRate, totalBytesToDelete := func() (float64, uint64) {
		p.mu.Lock()
		defer p.mu.Unlock()
		return float64(p.mu.history.Sum(now)) / deletePacerHistory.Seconds(), p.mu.bytesToDelete
	}()
	if historicRate > baseRate {
		baseRate = historicRate
	}

	// Apply heuristics to increase the deletion rate.
	var extraRate float64
	info := p.getInfo()
	if info.freeBytes <= p.freeSpaceThreshold {
		// Increase the rate so that we can free up enough bytes within the timeframe.
		extraRate = float64(p.freeSpaceThreshold-info.freeBytes) / p.freeSpaceTimeframe.Seconds()
	}
	if info.liveBytes == 0 {
		// We don't know the obsolete bytes ratio. Disable pacing altogether.
		return 0.0
	}
	obsoleteBytesRatio := float64(info.obsoleteBytes+totalBytesToDelete) / float64(info.liveBytes)
	if obsoleteBytesRatio >= p.obsoleteBytesMaxRatio {
		// Increase the rate so that we can free up enough bytes within the timeframe.
		r := (obsoleteBytesRatio - p.obsoleteBytesMaxRatio) * float64(info.liveBytes) / p.obsoleteBytesTimeframe.Seconds()
		if extraRate < r {
			extraRate = r
		}
	}

	return float64(bytesToDelete) / (baseRate + extraRate)
}

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
	val [historyEpochs]int64
	// sum is always equal to the sum of values in val.
	sum int64
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
func (h *history) Add(now crtime.Mono, val int64) {
	h.advance(now)
	h.val[h.currEpoch%historyEpochs] += val
	h.sum += val
}

// Sum returns the sum of recent values. The result is approximate in that the
// cut-off time is within 1% of the exact one.
func (h *history) Sum(now crtime.Mono) int64 {
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
		h.sum -= h.val[h.currEpoch%historyEpochs]
		h.val[h.currEpoch%historyEpochs] = 0
	}
}
