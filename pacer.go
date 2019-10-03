// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"errors"
	"time"
)

var nilPacer = &noopPacer{}

type limiter interface {
	WaitN(ctx context.Context, n int) (err error)
	AllowN(now time.Time, n int) bool
	Burst() int
}

// pacer is the interface for flush and compaction rate limiters. The rate limiter
// is possible applied on each iteration step of a flush or compaction. This is to
// limit background IO usage so that it does not contend with foreground traffic.
type pacer interface {
	maybeThrottle(bytesIterated uint64) error
}

// internalPacer contains fields and methods common to both compactionPacer and
// flushPacer.
type internalPacer struct {
	limiter limiter

	iterCount             uint64
	prevBytesIterated     uint64
	refreshBytesThreshold uint64
	slowdownThreshold     uint64
}

// limit applies rate limiting if the current byte level is below the configured
// threshold.
func (p *internalPacer) limit(amount, currentLevel uint64) error {
	if currentLevel <= p.slowdownThreshold {
		burst := p.limiter.Burst()
		for amount > uint64(burst) {
			err := p.limiter.WaitN(context.Background(), burst)
			if err != nil {
				return err
			}
			amount -= uint64(burst)
		}
		err := p.limiter.WaitN(context.Background(), int(amount))
		if err != nil {
			return err
		}
	} else {
		burst := p.limiter.Burst()
		for amount > uint64(burst) {
			p.limiter.AllowN(time.Now(), burst)
			amount -= uint64(burst)
		}
		p.limiter.AllowN(time.Now(), int(amount))
	}
	return nil
}

// compactionPacerInfo contains information necessary for compaction pacing.
type compactionPacerInfo struct {
	// slowdownThreshold is the low watermark for compaction debt. If compaction debt is
	// below this threshold, we slow down compactions. If compaction debt is above this
	// threshold, we let compactions continue as fast as possible. We want to keep
	// compaction speed as slow as possible to match the speed of flushes. This threshold
	// is set so that a single flush cannot contribute enough compaction debt to overshoot
	// the threshold.
	slowdownThreshold   uint64
	totalCompactionDebt uint64
	// totalDirtyBytes is the number of dirty bytes in memtables. The compaction
	// pacer can monitor changes to this value to determine if user writes have
	// stopped.
	totalDirtyBytes uint64
}

// compactionPacerEnv defines the environment in which the compaction rate limiter
// is applied.
type compactionPacerEnv struct {
	limiter      limiter
	memTableSize uint64

	getInfo func() compactionPacerInfo
}

// compactionPacer rate limits compactions depending on compaction debt. The rate
// limiter is applied at a rate that keeps compaction debt at a steady level. If
// compaction debt increases at a rate that is faster than the system can handle,
// no rate limit is applied.
type compactionPacer struct {
	internalPacer
	env                 compactionPacerEnv
	totalCompactionDebt uint64
	totalDirtyBytes     uint64
}

func newCompactionPacer(env compactionPacerEnv) *compactionPacer {
	return &compactionPacer{
		env: env,
		internalPacer: internalPacer{
			limiter: env.limiter,
		},
	}
}

// maybeThrottle slows down compactions to match memtable flush rate. The DB
// provides a compaction debt estimate and a slowdown threshold. We subtract the
// compaction debt estimate by the bytes iterated in the current compaction. If
// the new compaction debt estimate is below the threshold, the rate limiter is
// applied. If the new compaction debt is above the threshold, the rate limiter
// is not applied.
func (p *compactionPacer) maybeThrottle(bytesIterated uint64) error {
	if bytesIterated == 0 {
		return errors.New("pebble: maybeThrottle supplied with invalid bytesIterated")
	}

	// Recalculate total compaction debt and the slowdown threshold only once
	// every 1000 iterations or when the refresh threshold is hit since it
	// requires grabbing DB.mu which is expensive.
	if p.iterCount == 0 || bytesIterated > p.refreshBytesThreshold {
		pacerInfo := p.env.getInfo()
		p.slowdownThreshold = pacerInfo.slowdownThreshold
		p.totalCompactionDebt = pacerInfo.totalCompactionDebt
		p.refreshBytesThreshold = bytesIterated + (p.env.memTableSize * 5 / 100)
		p.iterCount = 1000
		if p.totalDirtyBytes == pacerInfo.totalDirtyBytes {
			// The total dirty bytes in the memtables have not changed since the
			// previous call: user writes have completely stopped. Allow the
			// compaction to proceed as fast as possible until the next
			// recalculation. We adjust the recalculation threshold so that we can be
			// nimble in the face of new user writes.
			p.totalCompactionDebt += p.slowdownThreshold
			p.iterCount = 100
		}
		p.totalDirtyBytes = pacerInfo.totalDirtyBytes
	}
	p.iterCount--

	var curCompactionDebt uint64
	if p.totalCompactionDebt > bytesIterated {
		curCompactionDebt = p.totalCompactionDebt - bytesIterated
	}

	compactAmount := bytesIterated - p.prevBytesIterated
	p.prevBytesIterated = bytesIterated

	// We slow down compactions when the compaction debt falls below the slowdown
	// threshold, which is set dynamically based on the number of non-empty levels.
	// This will only occur if compactions can keep up with the pace of flushes. If
	// bytes are flushed faster than how fast compactions can occur, compactions
	// proceed at maximum (unthrottled) speed.
	return p.limit(compactAmount, curCompactionDebt)
}

// flushPacerInfo contains information necessary for compaction pacing.
type flushPacerInfo struct {
	inuseBytes uint64
}

// flushPacerEnv defines the environment in which the compaction rate limiter is
// applied.
type flushPacerEnv struct {
	limiter      limiter
	memTableSize uint64

	getInfo func() flushPacerInfo
}

// flushPacer rate limits memtable flushing to match the speed of incoming user
// writes. If user writes come in faster than the memtable can be flushed, no
// rate limit is applied.
type flushPacer struct {
	internalPacer
	env                flushPacerEnv
	inuseBytes         uint64
	adjustedInuseBytes uint64
}

func newFlushPacer(env flushPacerEnv) *flushPacer {
	return &flushPacer{
		env: env,
		internalPacer: internalPacer{
			limiter:           env.limiter,
			slowdownThreshold: env.memTableSize * 105 / 100,
		},
	}
}

// maybeThrottle slows down memtable flushing to match user write rate. The DB
// provides the total number of bytes in all the memtables. We subtract this total
// by the number of bytes flushed in the current flush to get a "dirty byte" count.
// If the dirty byte count is below the watermark (105% memtable size), the rate
// limiter is applied. If the dirty byte count is above the watermark, the rate
// limiter is not applied.
func (p *flushPacer) maybeThrottle(bytesIterated uint64) error {
	if bytesIterated == 0 {
		return errors.New("pebble: maybeThrottle supplied with invalid bytesIterated")
	}

	// Recalculate inuse memtable bytes only once every 1000 iterations or when
	// the refresh threshold is hit since getting the inuse memtable byte count
	// requires grabbing DB.mu which is expensive.
	if p.iterCount == 0 || bytesIterated > p.refreshBytesThreshold {
		pacerInfo := p.env.getInfo()
		p.iterCount = 1000
		p.refreshBytesThreshold = bytesIterated + (p.env.memTableSize * 5 / 100)
		p.adjustedInuseBytes = pacerInfo.inuseBytes
		if p.inuseBytes == pacerInfo.inuseBytes {
			// The inuse bytes in the memtables have not changed since the previous
			// call: user writes have completely stopped. Allow the flush to proceed
			// as fast as possible until the next recalculation. We adjust the
			// recalculation threshold so that we can be nimble in the face of new
			// user writes.
			p.adjustedInuseBytes += p.slowdownThreshold
			p.iterCount = 100
		}
		p.inuseBytes = pacerInfo.inuseBytes
	}
	p.iterCount--

	// dirtyBytes is the inuse number of bytes in the memtables minus the number of
	// bytes flushed. It represents unflushed bytes in all the memtables, even the
	// ones which aren't being flushed such as the mutable memtable.
	dirtyBytes := p.adjustedInuseBytes - bytesIterated
	flushAmount := bytesIterated - p.prevBytesIterated
	p.prevBytesIterated = bytesIterated

	// We slow down memtable flushing when the dirty bytes indicator falls
	// below the low watermark, which is 105% memtable size. This will only
	// occur if memtable flushing can keep up with the pace of incoming
	// writes. If writes come in faster than how fast the memtable can flush,
	// flushing proceeds at maximum (unthrottled) speed.
	return p.limit(flushAmount, dirtyBytes)
}

type noopPacer struct{}

func (p *noopPacer) maybeThrottle(_ uint64) error {
	return nil
}
