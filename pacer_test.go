// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/datadriven"
)

type mockPrintLimiter struct {
	buf   bytes.Buffer
	burst int
}

func (m *mockPrintLimiter) DelayN(now time.Time, n int) time.Duration {
	fmt.Fprintf(&m.buf, "wait: %d\n", n)
	return 0
}

func (m *mockPrintLimiter) AllowN(now time.Time, n int) bool {
	fmt.Fprintf(&m.buf, "allow: %d\n", n)
	return true
}

func (m *mockPrintLimiter) Burst() int {
	return m.burst
}

func TestCompactionPacerMaybeThrottle(t *testing.T) {
	datadriven.RunTest(t, "testdata/compaction_pacer_maybe_throttle",
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				if len(d.CmdArgs) != 1 {
					return fmt.Sprintf("%s expects 1 argument", d.Cmd)
				}

				burst := uint64(1)
				var bytesIterated uint64
				var slowdownThreshold uint64
				var freeBytes, liveBytes, obsoleteBytes uint64
				if len(d.Input) > 0 {
					for _, data := range strings.Split(d.Input, "\n") {
						parts := strings.Split(data, ":")
						if len(parts) != 2 {
							return fmt.Sprintf("malformed test:\n%s", d.Input)
						}
						varKey := parts[0]
						varValue, err := strconv.ParseUint(strings.TrimSpace(parts[1]), 10, 64)
						if err != nil {
							return err.Error()
						}

						switch varKey {
						case "burst":
							burst = varValue
						case "bytesIterated":
							bytesIterated = varValue
						case "slowdownThreshold":
							slowdownThreshold = varValue
						case "freeBytes":
							freeBytes = varValue
						case "liveBytes":
							liveBytes = varValue
						case "obsoleteBytes":
							obsoleteBytes = varValue
						default:
							return fmt.Sprintf("unknown command: %s", varKey)
						}
					}
				}

				mockLimiter := mockPrintLimiter{burst: int(burst)}
				switch d.CmdArgs[0].Key {
				case "deletion":
					getInfo := func() deletionPacerInfo {
						return deletionPacerInfo{
							freeBytes:     freeBytes,
							liveBytes:     liveBytes,
							obsoleteBytes: obsoleteBytes,
						}
					}
					deletionPacer := newDeletionPacer(100, getInfo)
					deletionPacer.limiter = &mockLimiter
					deletionPacer.freeSpaceThreshold = slowdownThreshold
					err := deletionPacer.maybeThrottle(bytesIterated)
					if err != nil {
						return err.Error()
					}

					return mockLimiter.buf.String()
				default:
					return fmt.Sprintf("unknown command: %s", d.Cmd)
				}

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

// TestDeletionPacerHistory tests the history helper by crosschecking Sum()
// against a naive implementation.
func TestDeletionPacerHistory(t *testing.T) {
	type event struct {
		time time.Time
		// If report is 0, this event is a Sum(). Otherwise it is an Add().
		report int64
	}
	numEvents := 1 + rand.Intn(200)
	timeframe := time.Duration(1+rand.Intn(60*100)) * time.Second
	events := make([]event, numEvents)
	startTime := time.Now()
	for i := range events {
		events[i].time = startTime.Add(time.Duration(rand.Int63n(int64(timeframe))))
		if rand.Intn(3) == 0 {
			events[i].report = 0
		} else {
			events[i].report = int64(rand.Intn(100000))
		}
	}
	sort.Slice(events, func(i, j int) bool {
		return events[i].time.Before(events[j].time)
	})

	var h history
	h.Init(startTime, timeframe)

	// partialSums[i] := SUM_j<i events[j].report
	partialSums := make([]int64, len(events)+1)
	for i := range events {
		partialSums[i+1] = partialSums[i] + events[i].report
	}

	for i, e := range events {
		if e.report != 0 {
			h.Add(e.time, e.report)
			continue
		}

		result := h.Sum(e.time)

		// getIdx returns the largest event index <= i that is before the cutoff
		// time.
		getIdx := func(cutoff time.Time) int {
			for j := i; j >= 0; j-- {
				if events[j].time.Before(cutoff) {
					return j
				}
			}
			return -1
		}

		// Sum all report values in the last timeframe, and see if recent events
		// (allowing 1% error in the cutoff time) match the result.
		a := getIdx(e.time.Add(-timeframe * (historyEpochs + 1) / historyEpochs))
		b := getIdx(e.time.Add(-timeframe * (historyEpochs - 1) / historyEpochs))
		found := false
		for j := a; j <= b; j++ {
			if partialSums[i+1]-partialSums[j+1] == result {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("incorrect Sum() result %d; %v", result, events[a+1:i+1])
		}
	}
}
