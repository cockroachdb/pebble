// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metricsutil

import (
	"sync"
	"time"

	"github.com/cockroachdb/crlib/crtime"
)

// NewWindow creates a new Window instance, which maintains a sliding window of
// metrics collected at regular intervals. It collects metrics every minute.
//
// Sample usage:
//
//	w := NewWindow[MyMetricsType](func() MyMetricsType {
//	  return retrieveCurrentMetrics()
//	})
//	w.Start()
//	defer w.Stop()
//	..
//	currentMetrics := retrieveCurrentMetrics()
//	prevMetrics, prevTime := w.TenMinutesAgo()
//	fmt.Printf("statistics over last %s: %s\n", prevTime.Elapsed(), currentMetrics.Subtract(prevMetrics))
func NewWindow[M any](collectFn CollectFn[M]) *Window[M] {
	return &Window[M]{collectFn: collectFn}
}

// CollectFn is a function used to collect the current metrics.
type CollectFn[M any] func() M

// Window maintains a sliding window of metrics collected at regular intervals,
// allowing the retrieval of metrics from approximately 10 minutes and 1 hour
// ago. These metrics can be used against the current metrics to get statistics
// for these recent timeframes.
type Window[M any] struct {
	collectFn CollectFn[M]
	mu        struct {
		sync.Mutex
		running   bool
		startTime crtime.Mono
		nextA     crtime.Mono
		ringA     ring[M]
		nextB     crtime.Mono
		ringB     ring[M]
		timer     *time.Timer
	}
}

// TenMinutesAgo returns the metrics from ~10 minutes ago (normally ±30s) and
// the time when they were collected.
//
// If no metrics are available (less than 10m passed), the zero values are
// returned.
func (w *Window[M]) TenMinutesAgo() (_ M, collectedAt crtime.Mono) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.mu.ringA.Oldest()
}

// OneHourAgo returns the metrics from ~1h ago (normally ±3m) and the time when
// they were collected.
//
// If no metrics are available (i.e. less than 1h passed), the zero values are
// returned.
func (w *Window[M]) OneHourAgo() (_ M, collectedAt crtime.Mono) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.mu.ringB.Oldest()
}

// Start background collection of metrics.
func (w *Window[M]) Start() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.mu.running {
		return
	}
	w.mu.ringA = ring[M]{}
	w.mu.ringB = ring[M]{}
	w.mu.running = true
	// Initialize both rings. We do this in a separate goroutine in case the
	// caller is holding a lock that the collect function uses as well.
	go func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		if !w.mu.running {
			// We were stopped before we got here.
			return
		}
		w.mu.startTime = crtime.NowMono()
		w.mu.nextA = w.mu.startTime + crtime.Mono(tickA)
		w.mu.nextB = w.mu.startTime + crtime.Mono(tickB)
		m := w.collectFn()
		w.mu.ringA.Add(m, w.mu.startTime)
		w.mu.ringB.Add(m, w.mu.startTime)
		// We prefer to use a timer instead of a ticker and goroutine to avoid yet
		// another goroutine showing up in goroutine dumps.
		w.mu.timer = time.AfterFunc(tickA, w.tick)
	}()
}

// Stop background collection of metrics and wait for any in-progress collection
// to finish.
func (w *Window[M]) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.mu.running = false
	if w.mu.timer != nil {
		// If Stop fails, the timer function didn't reach the critical section yet;
		// when it does it will notice running=false and exit.
		w.mu.timer.Stop()
		w.mu.timer = nil
	}
}

func (w *Window[M]) tick() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.mu.running {
		return
	}
	m := w.collectFn()
	now := crtime.NowMono()
	for w.mu.nextA <= now {
		w.mu.ringA.Add(m, now)
		w.mu.nextA += crtime.Mono(tickA)
	}
	for w.mu.nextB <= now {
		w.mu.ringB.Add(m, now)
		w.mu.nextB += crtime.Mono(tickB)
	}
	w.mu.timer.Reset(min(w.mu.nextA, w.mu.nextB).Sub(now))
}

// We scale down the timeframes by 5% so that the oldest sample is within ±5% of
// the desired target.
const timeframeA = 10 * time.Minute * 95 / 100
const timeframeB = time.Hour * 95 / 100

const resolution = 10
const tickA = timeframeA / time.Duration(resolution)
const tickB = timeframeB / time.Duration(resolution)

type ring[M any] struct {
	pos int
	buf [resolution]struct {
		m           M
		collectedAt crtime.Mono
	}
}

func (r *ring[M]) Add(value M, collectedAt crtime.Mono) {
	r.buf[r.pos].m = value
	r.buf[r.pos].collectedAt = collectedAt
	r.pos = (r.pos + 1) % resolution
}

func (r *ring[M]) Oldest() (_ M, collectedAt crtime.Mono) {
	return r.buf[r.pos].m, r.buf[r.pos].collectedAt
}
