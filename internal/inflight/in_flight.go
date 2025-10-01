// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package inflight provides a lightweight, sharded tracker for reporting
// long-running operations.
package inflight

import (
	"cmp"
	"fmt"
	"iter"
	"maps"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/crlib/crsync"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/puzpuzpuz/xsync/v3"
)

// Tracker is used to detect and report operations that run for too long.
//
// Usage:
//
//	t := NewPollingTracker(10*time.Minute, reportFn)
//	..
//	h := t.Start()
//	.. do work ..
//	t.Stop(h)
//
// If there are operations older than 10 minutes, reportFn() will be
// periodically called with a report (which includes stack traces for the
// Start() calls).

// Tracker is a lightweight, sharded tracker for detecting long-running
// operations. Call Start() at the beginning of an operation and Stop() when it
// completes; if an operation exceeds a configured age threshold, a formatted
// report that includes the caller stack of Start() can be produced via Report,
// or periodically via NewPollingTracker.
//
// The tracker is concurrency-safe and designed to have very low overhead in the
// common path (Start/Stop). Observability work (capturing up to a small fixed
// number of program counters and formatting stack frames) is deferred to when a
// report is requested.
//
// Example:
//
//	 pollInterval := time.Minute
//	 maxAge := 10 * time.Minute
//		t := NewPollingTracker(pollInterval, maxAge, func(r string) {
//		  log.Infof(ctx, "slow operations:\n%s", r)
//		})
//		defer t.Close()
//
//		h := t.Start()
//		// ... do work ...
//		t.Stop(h)
//
// If any operations have been running for more than 10 minutes, the report
// function is called every 1 minute with a human‑readable dump that deduplicates
// by Start() stack trace and shows the oldest occurrence per trace.
type Tracker struct {
	// The tracker has a fixed number of shards to reduce contention on
	// Start/Stop. The first byte of Handle identifies the shard that was used
	// during Start().

	// Both maps and handleCounters have one entry per shard. We separate them
	// because the map pointers don't change.
	maps           []*xsync.MapOf[Handle, entry]
	handleCounters []handleCounter

	mu struct {
		sync.Mutex
		// timer is non-nil if this tracker was created with NewPollingTracker() and
		// hasn't been closed yet.
		timer *time.Timer
	}
}

// Handle uniquely identifies a started operation. Treat it as an opaque token.
// A zero Handle is not valid.
type Handle uint64

// ReportFn is called (typically periodically) with a formatted report that
// describes entries older than some threshold. An empty string report is never
// delivered by the polling tracker.
//
// See Tracker.Report() for details on the report format.
type ReportFn func(report string)

// NewTracker creates a new tracker that can be used to generate reports on
// long-running operations. It does not start any background goroutines; callers
// can invoke Report() on demand.
func NewTracker() *Tracker {
	return newTracker(crsync.NumShards())
}

// NewPollingTracker creates a new tracker that will periodically generate
// reports on long-running operations.
//
// Specifically, every pollInterval, reportFn will be called if there are
// operations that have been running for more than maxAge.
//
// The tracker must be closed with Close() when no longer needed.
func NewPollingTracker(
	pollInterval time.Duration, maxAge time.Duration, reportFn ReportFn,
) *Tracker {
	t := newTracker(crsync.NumShards())

	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.timer = time.AfterFunc(pollInterval, func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		if t.mu.timer == nil {
			// Close was called.
			return
		}
		if report := t.Report(maxAge); report != "" {
			reportFn(report)
		}
		t.mu.timer.Reset(pollInterval)
	})
	return t
}

// Close stops background polling (if enabled).
//
// Note that Start, Stop, and Report can still be used during/after Close.
func (t *Tracker) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.mu.timer != nil {
		t.mu.timer.Stop()
		// If the timer function is waiting for the mutex, it will notice that timer
		// is nil and exit.
		t.mu.timer = nil
	}
}

// Start records the start of an operation and returns a handle that should be
// used with Stop.
func (t *Tracker) Start() Handle {
	// We record a monotonic start time and capture program counters from the
	// caller’s stack. We generate a handle that includes the shard index in its
	// high byte, making Stop an O(1) operation on the right shard without a
	// lookup.
	var e entry
	e.startTime = crtime.NowMono()
	runtime.Callers(2, e.stack[:])
	shardIdx := crsync.CPUBiasedInt() % len(t.maps)
	h := Handle(t.handleCounters[shardIdx].Add(1))
	t.maps[shardIdx].Store(h, e)
	return h
}

// Stop records the end of an operation. Does nothing if the operation was
// already stopped.
func (t *Tracker) Stop(h Handle) {
	// The high byte of h is the shard index; see newTracker.
	t.maps[h>>56].Delete(h)
}

// Report returns a multi-line, human-readable summary of all entries that were
// started before the given threshold (i.e., with age > threshold at the call
// time). The result is suitable for logging or debugging. If no entries exceed
// the threshold, Report returns "".
//
// The output is grouped by Start() stack trace: for each unique stack we show
// the number of occurrences and the oldest start time among them. Groups are
// ordered by age (oldest first), and for each group we print the resolved stack
// frames.
func (t *Tracker) Report(threshold time.Duration) string {
	type infoForStackTrace struct {
		oldestStartTime crtime.Mono
		occurrences     int
	}
	now := crtime.NowMono()
	cutoff := now - crtime.Mono(threshold)
	// We deduplicate stack traces in the report. For each stack, we mention the
	// number of long-running operations and the oldest operation time.
	var m map[stack]infoForStackTrace
	for e := range t.olderThan(cutoff) {
		if m == nil {
			m = make(map[stack]infoForStackTrace)
		}
		info, ok := m[e.stack]
		if ok {
			info.occurrences++
			info.oldestStartTime = min(info.oldestStartTime, e.startTime)
		} else {
			info.occurrences = 1
			info.oldestStartTime = e.startTime
		}
		m[e.stack] = info
	}
	if len(m) == 0 {
		return ""
	}
	// Sort by oldest start time.
	stacks := slices.Collect(maps.Keys(m))
	slices.SortFunc(stacks, func(a, b stack) int {
		return cmp.Compare(m[a].oldestStartTime, m[b].oldestStartTime)
	})
	var b strings.Builder
	for _, stack := range stacks {
		info := m[stack]
		if info.occurrences == 1 {
			fmt.Fprintf(&b, "started %s ago:\n", now.Sub(info.oldestStartTime))
		} else {
			fmt.Fprintf(&b, "%d occurrences, oldest started %s ago:\n", info.occurrences, now.Sub(info.oldestStartTime))
		}
		pcs := stack[:]
		for i := range pcs {
			if pcs[i] == 0 {
				pcs = pcs[:i]
				break
			}
		}
		frames := runtime.CallersFrames(pcs)
		for {
			frame, more := frames.Next()
			fmt.Fprintf(&b, "  %s\n   %s:%d\n", frame.Function, frame.File, frame.Line)
			if !more {
				break
			}
		}
		b.WriteString("\n")
	}
	return b.String()
}

func (t *Tracker) olderThan(cutoff crtime.Mono) iter.Seq[entry] {
	return func(yield func(entry) bool) {
		for i := range t.maps {
			for _, e := range t.maps[i].Range {
				if e.startTime < cutoff && !yield(e) {
					return
				}
			}
		}
	}
}

// We use the high byte of Handle as a shard index. This limits us to 256 shards
// (which is plenty).
const maxShards = 256
const mapPresize = 16

// handleCounter is an atomic counter with padding to avoid false sharing.
type handleCounter struct {
	atomic.Uint64
	_ [7]uint64
}

type entry struct {
	startTime crtime.Mono
	stack     stack
}

// stack contains program counters from Start()'s stack frame. Only the first 7
// frames are recorded to keep Start() overhead low. Unused slots are zero.
//
// We chose 7 so that entry{} fits in a typical cache line. It should be
// sufficient to identify the call path in most cases.
type stack [7]uintptr

func newTracker(numShards int) *Tracker {
	numShards = min(numShards, maxShards)

	maps := make([]*xsync.MapOf[Handle, entry], numShards)
	handleCounters := make([]handleCounter, numShards)
	for i := range numShards {
		// All handles have the shard index in the high byte; see Start().
		handleCounters[i].Store(uint64(i) << 56)
		maps[i] = xsync.NewMapOf[Handle, entry](xsync.WithPresize(mapPresize))
	}
	return &Tracker{
		maps:           maps,
		handleCounters: handleCounters,
	}
}
