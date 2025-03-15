// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/datadriven"
)

type testTimeSource struct {
	tt *testTicker
}

func (tts *testTimeSource) newTicker(duration time.Duration) schedulerTicker {
	tts.tt = &testTicker{
		channel: make(chan time.Time),
	}
	return tts.tt
}

type testTicker struct {
	channel chan time.Time
}

func (t *testTicker) stop() {}

func (t *testTicker) ch() <-chan time.Time {
	return t.channel
}

type ongoingCompaction struct {
	index int
	h     CompactionGrantHandle
}
type testDBForCompaction struct {
	t            *testing.T
	b            *strings.Builder
	allowed      int
	nextIndex    int
	waitingCount int
	handles      []ongoingCompaction
}

var _ DBForCompaction = &testDBForCompaction{}

func (d *testDBForCompaction) GetAllowedWithoutPermission() int {
	fmt.Fprintf(d.b, "GetAllowedWithoutPermission(): %d\n", d.allowed)
	return d.allowed
}

func (d *testDBForCompaction) GetWaitingCompaction() (bool, WaitingCompaction) {
	waiting := d.waitingCount > 0
	fmt.Fprintf(d.b, "GetWaitingCompaction(): %t\n", waiting)
	return waiting, WaitingCompaction{}
}

func (d *testDBForCompaction) Schedule(h CompactionGrantHandle) bool {
	var success bool
	if d.waitingCount > 0 {
		d.addCompaction(h)
		d.waitingCount--
		success = true
	}
	fmt.Fprintf(d.b, "Schedule(): %t\n", success)
	return success
}

func (d *testDBForCompaction) addCompaction(h CompactionGrantHandle) {
	d.handles = append(d.handles, ongoingCompaction{index: d.nextIndex, h: h})
	d.nextIndex++
}

func (d *testDBForCompaction) compactionDone(index int) {
	for i, h := range d.handles {
		if h.index == index {
			d.handles = slices.Delete(d.handles, i, i+1)
			h.h.Done()
			return
		}
	}
	d.t.Fatalf("compactionDone: index %d not found", index)
}

func TestConcurrencyLimitScheduler(t *testing.T) {
	var tts testTimeSource
	var sched *ConcurrencyLimitScheduler
	var db *testDBForCompaction
	var b strings.Builder
	printAndReset := func() string {
		if len(db.handles) > 0 {
			fmt.Fprintf(&b, "Ongoing compactions nums:")
			for _, h := range db.handles {
				fmt.Fprintf(&b, " %d", h.index)
			}
			fmt.Fprintf(&b, "\n")
		}
		if db.waitingCount > 0 {
			fmt.Fprintf(&b, "Waiting compactions count: %d\n", db.waitingCount)
		}
		str := b.String()
		b.Reset()
		return str
	}
	datadriven.RunTest(t, "testdata/concurrency_limit_scheduler",
		func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "init":
				sched = newConcurrencyLimitScheduler(&tts)
				sched.periodicGranterRanChForTesting = make(chan struct{})
				db = &testDBForCompaction{t: t, b: &b, allowed: 1}
				sched.Register(2, db)
				return printAndReset()

			case "set-allowed":
				var allowed int
				td.ScanArgs(t, "allowed", &allowed)
				db.allowed = allowed
				if td.HasArg("inform-scheduler") {
					sched.UpdateGetAllowedWithoutPermission()
					if td.HasArg("wait-for-periodic-granter") {
						<-sched.periodicGranterRanChForTesting
					}
				}
				return printAndReset()

			case "set-waiting-count":
				var waitingCount int
				td.ScanArgs(t, "count", &waitingCount)
				db.waitingCount = waitingCount
				return printAndReset()

			case "try-schedule":
				success, h := sched.TrySchedule()
				if success {
					db.addCompaction(h)
					fmt.Fprintf(&b, "try-schedule succeeded\n")
				} else {
					fmt.Fprintf(&b, "try-schedule failed\n")
				}
				return printAndReset()

			case "tick":
				tts.tt.channel <- time.Time{}
				<-sched.periodicGranterRanChForTesting
				return printAndReset()

			case "compaction-done":
				var index int
				td.ScanArgs(t, "index", &index)
				db.compactionDone(index)
				return printAndReset()

			case "unregister":
				sched.Unregister()
				return printAndReset()

			default:
				return fmt.Sprintf("unknown command: %s", td.Cmd)
			}
		})
}
