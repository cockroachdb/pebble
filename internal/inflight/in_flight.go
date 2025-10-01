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
//	t := NewTrackerWithAutoReport(10*time.Minute, reportFn)
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
//	t := NewPollingTracker(1*time.Minute, 10*time.Minute, func(r string) {
//	  log.Infof(ctx, "slow operations:\n%s", r)
//	})
//	defer t.Close()
//
//	h := t.Start()
//	// ... do work ...
//	t.Stop(h)
//
// If any operations have been running for more than 10 minutes, the report
// function is called every 1 minute with a human‑readable dump that deduplicates
// by Start() stack trace and shows the oldest occurrence per trace.
type Tracker struct {
	shards []shard
	timer  *time.Timer
	// reportCh is a semaphore used to synchronize with the polling function.
	reportCh chan struct{}
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
	// reportCh acts as a semaphore allowing synchronizing Close() with the timer
	// function. When we close the tracker, we take the semaphore forever which
	// will block any further reports.
	t.reportCh = make(chan struct{}, 1)
	t.reportCh <- struct{}{}
	t.timer = time.AfterFunc(pollInterval, func() {
		select {
		case <-t.reportCh:
		default:
			// The tracker has been closed.
			return
		}
		if report := t.Report(maxAge); report != "" {
			reportFn(report)
		}
		t.timer.Reset(pollInterval)
		t.reportCh <- struct{}{}
	})
	return t
}

// Close stops background polling (if enabled).
//
// Note that Start, Stop, and Report can still be used during/after Close.
func (t *Tracker) Close() {
	if t.reportCh != nil {
		// Wait for any ongoing report to complete and block further reports.
		<-t.reportCh
		t.timer.Stop()
		t.reportCh = nil
		t.timer = nil
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
	s := &t.shards[crsync.CPUBiasedInt()%len(t.shards)]
	h := Handle(s.handleCounter.Add(1))
	s.m.Store(h, e)
	return h
}

// Stop records the end of an operation. Does nothing if the operation was
// already stopped.
func (t *Tracker) Stop(h Handle) {
	s := &t.shards[h>>56]
	s.m.Delete(h)
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
		for i := range t.shards {
			for _, e := range t.shards[i].m.Range {
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

type shard struct {
	// handleCounter is incremented every time we start an operation on this
	// shard.
	handleCounter atomic.Uint64
	m             *xsync.MapOf[Handle, entry]
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
	shards := make([]shard, numShards)
	for i := range shards {
		// All handles have the shard index in the high byte; see Start().
		shards[i].handleCounter.Store(uint64(i) << 56)
		shards[i].m = xsync.NewMapOf[Handle, entry](xsync.WithPresize(mapPresize))
	}
	return &Tracker{shards: shards}
}
