// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"sync/atomic"
	"time"
)

var nilPacer = &noopPacer{}

type limiter interface {
	WaitN(ctx context.Context, n int) (err error)
	AllowN(now time.Time, n int) bool
	Burst() int
}

type pacer interface {
	maybeThrottle(bytesIterated uint64) error
}

type internalPacer struct {
	limiter limiter

	iterCount             uint64
	prevBytesIterated     uint64
	refreshBytesThreshold uint64
	slowdownThreshold     uint64
}

// limit applies rate limiting if the current byte level is below the
// configured threshold.
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

type compactionPacerInfo struct {
	// slowdownThreshold is the low watermark for compaction debt. If compaction debt is
	// below this threshold, we slow down compactions. If compaction debt is above this
	// threshold, we let compactions continue as fast as possible. We want to keep
	// compaction speed as slow as possible to match the speed of flushes. This threshold
	// is set so that a single flush cannot contribute enough compaction debt to overshoot
	// the threshold.
	slowdownThreshold   uint64
	totalCompactionDebt uint64
}

type compactionPacerEnv struct {
	limiter      limiter
	memTableSize uint64

	getInfo func() compactionPacerInfo
}

type compactionPacer struct {
	internalPacer
	env                 compactionPacerEnv
	totalCompactionDebt uint64
}

func newCompactionPacer(env compactionPacerEnv) *compactionPacer {
	return &compactionPacer{
		env: env,
		internalPacer: internalPacer{
			limiter: env.limiter,
		},
	}
}

// maybeThrottle slows down compactions to match memtable flush rate.
func (p *compactionPacer) maybeThrottle(bytesIterated uint64) error {
	// Recalculate total compaction debt and estimated max w-amp only once
	// every 1000 iterations or when the refresh threshold is hit since it
	// requires grabbing DB.mu which is expensive.
	if p.iterCount == 0 || bytesIterated > p.refreshBytesThreshold {
		pacerInfo := p.env.getInfo()
		p.slowdownThreshold = pacerInfo.slowdownThreshold
		p.totalCompactionDebt = pacerInfo.totalCompactionDebt
		p.refreshBytesThreshold = bytesIterated + (p.env.memTableSize*5/100)
		p.iterCount = 1000
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

type flushPacerInfo struct {
	totalBytes uint64
}

type flushPacerEnv struct {
	bytesFlushed *uint64
	limiter      limiter
	memTableSize uint64

	getInfo func() flushPacerInfo
}

type flushPacer struct {
	internalPacer
	env        flushPacerEnv
	totalBytes uint64
}

func newFlushPacer(env flushPacerEnv) *flushPacer {
	return &flushPacer{
		env: env,
		internalPacer: internalPacer{
			limiter:           env.limiter,
			slowdownThreshold: env.memTableSize*105/100,
		},
	}
}

// maybeThrottle slows down memtable flushing to match user write rate.
func (p *flushPacer) maybeThrottle(bytesIterated uint64) error {
	// Recalculate total memtable bytes only once every 1000 iterations or
	// when the refresh threshold is hit since getting the total memtable
	// byte count requires grabbing DB.mu which is expensive.
	if p.iterCount == 0 || bytesIterated > p.refreshBytesThreshold {
		pacerInfo := p.env.getInfo()
		p.totalBytes = pacerInfo.totalBytes
		p.refreshBytesThreshold = bytesIterated + (p.env.memTableSize*5/100)
		p.iterCount = 1000
	}
	p.iterCount--

	// dirtyBytes is the total number of bytes in the memtables minus the number of
	// bytes flushed. It represents unflushed bytes in all the memtables, even the
	// ones which aren't being flushed such as the mutable memtable.
	dirtyBytes := p.totalBytes - bytesIterated
	flushAmount := bytesIterated - p.prevBytesIterated
	p.prevBytesIterated = bytesIterated

	atomic.StoreUint64(p.env.bytesFlushed, bytesIterated)

	// We slow down memtable flushing when the dirty bytes indicator falls
	// below the low watermark, which is 105% memtable size. This will only
	// occur if memtable flushing can keep up with the pace of incoming
	// writes. If writes come in faster than how fast the memtable can flush,
	// flushing proceeds at maximum (unthrottled) speed.
	return p.limit(flushAmount, dirtyBytes)
}

type noopPacer struct {}

func (p *noopPacer) maybeThrottle(_ uint64) error {
	return nil
}
