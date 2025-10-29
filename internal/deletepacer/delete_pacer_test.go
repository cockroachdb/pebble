// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build go1.25

package deletepacer

import (
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cockroachdb/crlib/crhumanize"
	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/datadriven/diagram"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/testutils"
	"github.com/stretchr/testify/require"
)

func TestDataDriven(t *testing.T) {
	for _, path := range []string{"backlog", "basic", "free-space"} {
		t.Run(path, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				datadriven.RunTest(t, "testdata/"+path, func(t *testing.T, td *datadriven.TestData) string {
					switch td.Cmd {
					case "run":
						ts := &testState{
							startTime:  time.Now(),
							deleteSema: make(chan struct{}, 1),
						}
						ts.deleteSema <- struct{}{}
						ts.baselineRate.Store(128 * MB)
						ts.diskFreeSpace.Store(100 * GB)
						opts := Options{
							FreeSpaceTimeframe: 10 * time.Second,
							BacklogTimeframe:   50 * time.Minute,
							BaselineRate:       func() uint64 { return ts.baselineRate.Load() },
						}
						if arg, ok := td.Arg("free-space-threshold"); ok {
							threshold, err := crhumanize.ParseBytes[uint64](arg.Vals[0])
							require.NoErrorf(t, err, "%s: free-space-threshold %q", td.Pos, arg)
							opts.FreeSpaceThresholdBytes = threshold
						}

						diskFreeSpaceFn := func() uint64 { return ts.diskFreeSpace.Load() }

						deleteFn := func(of ObsoleteFile, jobID int) {
							<-ts.deleteSema
							ts.deleteSema <- struct{}{}
							ts.dels = append(ts.dels, logEntry{
								timestamp: time.Now(),
								fileSize:  of.FileSize,
							})
						}

						ts.dp = Open(opts, testutils.Logger{T: t}, diskFreeSpaceFn, deleteFn)
						defer ts.dp.Close()
						executeTest(t, td, ts)
						return fmt.Sprintf("Enqueue:\n%sDelete:\n%s",
							plot(ts.startTime, ts.adds, "•"),
							plot(ts.startTime, ts.dels, "•"),
						)

					default:
						td.Fatalf(t, "unknown command %q", td.Cmd)
						return ""
					}
				})
			})
		})
	}
}

type testState struct {
	dp            *DeletePacer
	baselineRate  atomic.Uint64
	diskFreeSpace atomic.Uint64
	adds, dels    []logEntry
	startTime     time.Time
	deleteSema    chan struct{}
}

func executeTest(t *testing.T, td *datadriven.TestData, ts *testState) {
	repeat := 1
	td.MaybeScanArgs(t, "repeat", &repeat)
	for _, l := range crstrings.Lines(strings.Repeat(td.Input+"\n", repeat)) {
		if l == "" {
			continue
		}
		words := strings.Split(l, " ")
		switch words[0] {
		case "del":
			fileSize, err := crhumanize.ParseBytes[uint64](words[1])
			require.NoErrorf(t, err, "%s: %q", td.Pos, l)
			ts.dp.Enqueue(0, ObsoleteFile{
				FileType:  base.FileTypeTable,
				FileSize:  fileSize,
				Placement: base.Local,
			})
			ts.adds = append(ts.adds, logEntry{
				timestamp: time.Now(),
				fileSize:  fileSize,
			})

		case "sleep":
			d, err := time.ParseDuration(words[1])
			require.NoErrorf(t, err, "%s: %q", td.Pos, l)
			time.Sleep(d)

		case "block-deletes":
			<-ts.deleteSema

		case "unblock-deletes":
			ts.deleteSema <- struct{}{}

		case "baseline-rate":
			rate, err := crhumanize.ParseBytesPerSec[uint64](words[1])
			require.NoErrorf(t, err, "%s: %q", td.Pos, l)
			ts.baselineRate.Store(rate)

		case "free-space":
			freeSpace, err := crhumanize.ParseBytes[uint64](words[1])
			require.NoErrorf(t, err, "%s: %q", td.Pos, l)
			ts.diskFreeSpace.Store(freeSpace)
		}
		synctest.Wait()
	}
}

type logEntry struct {
	timestamp time.Time
	fileSize  uint64
}

func plot(startTime time.Time, log []logEntry, point string) string {
	const width = 100
	const height = 8
	now := time.Now()
	deltaT := now.Sub(startTime)
	tStep := 1 + deltaT/width
	var vals [width]uint64
	for _, l := range log {
		vals[l.timestamp.Sub(startTime)/tStep] += l.fileSize
	}
	maxVal := slices.Max(vals[:])

	var wb diagram.Whiteboard
	for i := range width {
		wb.Write(height+1, i, "-")
	}
	for i := range height + 1 {
		wb.Write(i, -1, "|")
	}
	wb.Write(height+1, -1, "+")
	for i := range width {
		if vals[i] != 0 {
			// The maxVal/2 term is to round instead of flooring.
			y := int((vals[i]*uint64(height) + maxVal/2) / maxVal)
			wb.Write(height-y, i, point)
		}
	}
	maxValStr := string(crhumanize.Bytes(maxVal, crhumanize.Compact))
	wb.Write(0, -len(maxValStr)-2, maxValStr)
	wb.Write(height, -4, "0B")
	timeStr := deltaT.String()
	wb.Write(height+2, 0, "0s")
	wb.Write(height+2, width-len(timeStr), timeStr)
	return wb.String()
}

// TestCloseWithPacing verifies that DeletePacer.Close() disables pacing so it
// completes in a timely manner.
func TestCloseWithPacing(t *testing.T) {
	opts := Options{
		FreeSpaceThresholdBytes: 1,
		BaselineRate:            func() uint64 { return 1024 }, // 1 KB/s - slow pacing
	}
	opts.EnsureDefaults()

	diskFreeSpaceFn := func() uint64 { return 10 * GB }
	deleteFn := func(of ObsoleteFile, jobID int) {}

	dp := Open(opts, testutils.Logger{T: t}, diskFreeSpaceFn, deleteFn)

	// Create obsolete files that would normally take a long time to delete.
	// At 1 KB/s, 100 files of 10 KB each would take 1000 seconds.
	largeFiles := make([]ObsoleteFile, 100)
	for i := range largeFiles {
		largeFiles[i] = ObsoleteFile{
			FileType:  base.FileTypeTable,
			FileSize:  10 * 1024,
			Placement: base.Local,
		}
	}

	dp.Enqueue(1, largeFiles...)

	done := make(chan struct{})
	go func() {
		defer close(done)
		dp.Close()
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatalf("timed out waiting for cleanupManager.Close() to return")
	}
}

// TestFallingBehind verifies that we disable pacing when the queue gets too big.
func TestFallingBehind(t *testing.T) {
	var rate atomic.Uint64
	rate.Store(10 * MB) // 10MB/s

	opts := Options{
		BaselineRate: func() uint64 { return rate.Load() }, // 10 MB/s
	}
	opts.EnsureDefaults()

	diskFreeSpaceFn := func() uint64 { return 100 * GB }
	deleteFn := func(of ObsoleteFile, jobID int) {}

	dp := Open(opts, testutils.Logger{T: t}, diskFreeSpaceFn, deleteFn)
	defer dp.Close()

	x := 0
	addJob := func(fileSize int) {
		x++
		dp.Enqueue(1, ObsoleteFile{
			FileType:  base.FileTypeTable,
			FileSize:  uint64(fileSize),
			Placement: base.Local,
		})
	}

	const initial = maxQueueSize / 2
	for range initial {
		addJob(1 * MB)
	}
	queueSize := func() int {
		dp.mu.Lock()
		defer dp.mu.Unlock()
		return dp.mu.queue.Len()
	}
	// At 1MB, each job will take 100ms each. Note that the rate increase based on
	// history won't make much difference, since the enqueued size is averaged
	// over 5 minutes.
	time.Sleep(50 * time.Millisecond)
	require.Greater(t, queueSize(), initial/2)

	// Add enough jobs to exceed the threshold. We add small jobs so that the
	// historic rate doesn't grow significantly.
	for range maxQueueSize {
		addJob(1)
	}

	const sleepTime = 10 * time.Millisecond
	const timeout = 10 * time.Second
	for i := 0; ; i++ {
		time.Sleep(10 * time.Millisecond)
		if queueSize() <= maxQueueSize {
			break
		}
		if i == int(timeout/sleepTime) {
			t.Fatalf("jobs channel length never dropped below threshold (%d vs %d)", queueSize(), maxQueueSize)
		}
	}
}
