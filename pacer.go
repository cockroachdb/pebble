// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/rate"
)

var nilPacer = &noopPacer{}

type limiter interface {
	DelayN(now time.Time, n int) time.Duration
	AllowN(now time.Time, n int) bool
	Burst() int
}

// pacer is the interface for flush and compaction rate limiters. The rate limiter
// is possible applied on each iteration step of a flush or compaction. This is to
// limit background IO usage so that it does not contend with foreground traffic.
type pacer interface {
	maybeThrottle(bytesIterated uint64) error

	reportDeletion(bytesToDelete uint64)
}

// deletionPacerInfo contains any info from the db necessary to make deletion
// pacing decisions.
type deletionPacerInfo struct {
	freeBytes     uint64
	obsoleteBytes uint64
	liveBytes     uint64
}

// deletionPacer rate limits deletions of obsolete files. This is necessary to
// prevent overloading the disk with too many deletions too quickly after a
// large compaction, or an iterator close. On some SSDs, disk performance can be
// negatively impacted if too many blocks are deleted very quickly, so this
// mechanism helps mitigate that.
type deletionPacer struct {
	limiter                limiter
	freeSpaceThreshold     uint64
	obsoleteBytesMaxRatio  float64
	targetByteDeletionRate int64

	getInfo func() deletionPacerInfo

	mu struct {
		sync.Mutex

		// history keeps rack of recent deletion history; it used to increase the
		// deletion rate to match the pace of deletions.
		history history
	}
}

const deletePacerHistory = 5 * time.Minute

// newDeletionPacer instantiates a new deletionPacer for use when deleting
// obsolete files. The limiter passed in must be a singleton shared across this
// pebble instance.
func newDeletionPacer(
	targetByteDeletionRate int64, getInfo func() deletionPacerInfo,
) *deletionPacer {
	d := &deletionPacer{
		limiter: rate.NewLimiter(rate.Limit(targetByteDeletionRate), int(targetByteDeletionRate)),

		// If there are less than freeSpaceThreshold bytes of free space on
		// disk, do not pace deletions at all.
		freeSpaceThreshold: 16 << 30, // 16 GB
		// If the ratio of obsolete bytes to live bytes is greater than
		// obsoleteBytesMaxRatio, do not pace deletions at all.
		obsoleteBytesMaxRatio: 0.20,

		targetByteDeletionRate: targetByteDeletionRate,

		getInfo: getInfo,
	}
	d.mu.history.Init(time.Now(), deletePacerHistory)
	return d
}

// limit applies rate limiting if the current free disk space is more than
// freeSpaceThreshold, and the ratio of obsolete to live bytes is less than
// obsoleteBytesMaxRatio.
func (p *deletionPacer) limit(amount uint64, info deletionPacerInfo) error {
	obsoleteBytesRatio := float64(1.0)
	if info.liveBytes > 0 {
		obsoleteBytesRatio = float64(info.obsoleteBytes) / float64(info.liveBytes)
	}
	paceDeletions := info.freeBytes > p.freeSpaceThreshold &&
		obsoleteBytesRatio < p.obsoleteBytesMaxRatio

	p.mu.Lock()
	if historyRate := p.mu.history.Sum(time.Now()) / int64(deletePacerHistory/time.Second); historyRate > p.targetByteDeletionRate {
		// Deletions have been happening at a rate higher than the target rate; we
		// want to speed up deletions so they match the historic rate. We do this by
		// decreasing the amount accordingly.
		amount = uint64(float64(p.targetByteDeletionRate) / float64(historyRate) * float64(amount))
	}
	p.mu.Unlock()

	if paceDeletions {
		burst := p.limiter.Burst()
		for amount > uint64(burst) {
			d := p.limiter.DelayN(time.Now(), burst)
			if d == rate.InfDuration {
				return errors.Errorf("pacing failed")
			}
			time.Sleep(d)
			amount -= uint64(burst)
		}
		d := p.limiter.DelayN(time.Now(), int(amount))
		if d == rate.InfDuration {
			return errors.Errorf("pacing failed")
		}
		time.Sleep(d)
	} else {
		burst := p.limiter.Burst()
		for amount > uint64(burst) {
			// AllowN will subtract burst if there are enough tokens available,
			// else leave the tokens untouched. That is, we are making a
			// best-effort to account for this activity in the limiter, but by
			// ignoring the return value, we do the activity instantaneously
			// anyway.
			p.limiter.AllowN(time.Now(), burst)
			amount -= uint64(burst)
		}
		p.limiter.AllowN(time.Now(), int(amount))
	}
	return nil
}

// maybeThrottle slows down a deletion of this file if it's faster than
// opts.Experimental.MinDeletionRate.
func (p *deletionPacer) maybeThrottle(bytesToDelete uint64) error {
	return p.limit(bytesToDelete, p.getInfo())
}

// reportDeletion is used to report a deletion to the pacer. The pacer uses it
// to keep track of the recent rate of deletions and potentially increase the
// deletion rate accordingly.
//
// reportDeletion is thread-safe.
func (p *deletionPacer) reportDeletion(bytesToDelete uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.history.Add(time.Now(), int64(bytesToDelete))
}

type noopPacer struct{}

func (p *noopPacer) maybeThrottle(_ uint64) error {
	return nil
}

func (p *noopPacer) reportDeletion(_ uint64) {}

// history is a helper used to keep track of the recent history of a set of
// data points (in our case deleted bytes), at limited granularity.
// Specifically, we split the desired timeframe into 100 "epochs" and all times
// are effectively rounded down to the nearest epoch boundary.
type history struct {
	epochDuration time.Duration
	startTime     time.Time
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
func (h *history) Init(now time.Time, timeframe time.Duration) {
	*h = history{
		epochDuration: timeframe / time.Duration(historyEpochs),
		startTime:     now,
		currEpoch:     0,
		sum:           0,
	}
}

// Add adds a value for the current time.
func (h *history) Add(now time.Time, val int64) {
	h.advance(now)
	h.val[h.currEpoch%historyEpochs] += val
	h.sum += val
}

// Sum returns the sum of recent values. The result is approximate in that the
// cut-off time is within 1% of the exact one.
func (h *history) Sum(now time.Time) int64 {
	h.advance(now)
	return h.sum
}

func (h *history) epoch(t time.Time) int64 {
	return int64(t.Sub(h.startTime) / h.epochDuration)
}

// advance advances the time to the given time.
func (h *history) advance(now time.Time) {
	epoch := h.epoch(now)
	for h.currEpoch < epoch {
		h.currEpoch++
		// Forget the data for the oldest epoch.
		h.sum -= h.val[h.currEpoch%historyEpochs]
		h.val[h.currEpoch%historyEpochs] = 0
	}
}
