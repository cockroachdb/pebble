// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build go1.25

package inflight

import (
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTrackerBasic(t *testing.T) {
	tr := NewTracker()
	require.Empty(t, tr.Report(time.Minute))
	h := tr.Start()
	require.Empty(t, tr.Report(time.Minute))
	time.Sleep(10 * time.Millisecond)
	// Sample Report() output:
	//
	// started 11.123708ms ago:
	//   github.com/cockroachdb/pebble/internal/inflight.TestTrackerBasic
	//    /Users/radu/go/src/github.com/cockroachdb/pebble/internal/inflight/in_flight_test.go:17
	//   testing.tRunner
	//    /Users/radu/go/go1.24.2/src/testing/testing.go:1792
	//   runtime.goexit
	//    /Users/radu/go/go1.24.2/src/runtime/asm_arm64.s:1223
	require.NotEmpty(t, tr.Report(time.Millisecond))
	require.Empty(t, tr.Report(time.Minute))
	tr.Stop(h)
	require.Empty(t, tr.Report(time.Millisecond))
}

func TestPollingTracker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var lastReport atomic.Value
		lastReport.Store("")
		reportFn := func(report string) {
			lastReport.Store(report)
		}

		tr := NewPollingTracker(time.Millisecond, 10*time.Millisecond, reportFn)
		time.Sleep(11 * time.Millisecond)
		synctest.Wait()
		require.Empty(t, lastReport.Load())
		h := tr.Start()
		time.Sleep(11 * time.Millisecond)
		synctest.Wait()
		require.NotEmpty(t, lastReport.Load())

		tr.Stop(h)
		synctest.Wait()
		lastReport.Store("")
		time.Sleep(11 * time.Millisecond)
		require.Empty(t, lastReport.Load())

		tr.Start()
		time.Sleep(11 * time.Millisecond)
		tr.Close()
		// Make sure there are no further reports.
		lastReport.Store("")
		time.Sleep(11 * time.Millisecond)
		synctest.Wait()
		require.Empty(t, lastReport.Load())
	})
}
