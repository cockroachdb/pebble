// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package wal

import (
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/errorfs"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func createTestWALFileMetricsForWAL(prefix string) record.WALFileMetrics {
	return record.WALFileMetrics{
		CreateLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    prefix + "_create_latency",
			Buckets: []float64{1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9},
		}),
		WriteLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    prefix + "_write_latency",
			Buckets: []float64{1e3, 1e4, 1e5, 1e6, 1e7, 1e8},
		}),
		FsyncLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    prefix + "_sync_latency",
			Buckets: []float64{1e6, 1e7, 1e8, 1e9, 1e10},
		}),
		CloseLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    prefix + "_close_latency",
			Buckets: []float64{1e4, 1e5, 1e6, 1e7, 1e8},
		}),
		StatLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    prefix + "_stat_latency",
			Buckets: []float64{1e3, 1e4, 1e5, 1e6, 1e7},
		}),
		OpenDirLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    prefix + "_opendir_latency",
			Buckets: []float64{1e4, 1e5, 1e6, 1e7, 1e8, 1e9},
		}),
	}
}

func verifyHistogramCountWAL(t *testing.T, hist prometheus.Histogram, expectedCount uint64) {
	t.Helper()
	metric := &dto.Metric{}
	err := hist.Write(metric)
	require.NoError(t, err)
	actual := metric.GetHistogram().GetSampleCount()
	require.Equal(t, expectedCount, actual, "histogram sample count mismatch")
}

func verifyHistogramCountRangeWAL(t *testing.T, hist prometheus.Histogram, min, max uint64) {
	t.Helper()
	metric := &dto.Metric{}
	err := hist.Write(metric)
	require.NoError(t, err)
	actual := metric.GetHistogram().GetSampleCount()
	require.GreaterOrEqual(t, actual, min, "histogram sample count too low")
	require.LessOrEqual(t, actual, max, "histogram sample count too high")
}

func TestStandaloneManagerMetrics(t *testing.T) {
	memFS := vfs.NewMem()
	walFileMetrics := createTestWALFileMetricsForWAL("standalone")

	opts := Options{
		Primary: Dir{
			FS:      memFS,
			Dirname: "primary",
		},
		MinUnflushedWALNum:  1,
		WALFileMetrics:      walFileMetrics,
		PreallocateSize:     func() int { return 0 },
		WriteWALSyncOffsets: func() bool { return false },
		Logger:              base.DefaultLogger,
	}

	// Create primary directory
	require.NoError(t, memFS.MkdirAll("primary", 0755))

	// Initialize standalone manager
	initial := Logs{}
	manager, err := Init(opts, initial)
	require.NoError(t, err)
	defer manager.Close()

	// Verify it's a standalone manager
	_, isStandalone := manager.(*StandaloneManager)
	require.True(t, isStandalone, "expected StandaloneManager")

	// Create a WAL
	writer, err := manager.Create(NumWAL(1), 1)
	require.NoError(t, err)

	// Write some records
	testData := [][]byte{
		[]byte("test record 1"),
		[]byte("test record 2 with more data"),
		[]byte("short"),
	}

	for _, data := range testData {
		_, err := writer.WriteRecord(data, SyncOptions{}, nil)
		require.NoError(t, err)
	}

	// Close the writer
	_, err = writer.Close()
	require.NoError(t, err)

	// Get metrics from the writer
	metrics := writer.Metrics()

	// Verify basic operations were recorded in runtime metrics
	require.Greater(t, metrics.WriteThroughput.Bytes, int64(0))

	// The WAL metrics should have recorded operations in histograms
	// Write operations may vary due to buffering behavior
	verifyHistogramCountRangeWAL(t, walFileMetrics.WriteLatency, 1, 3) // actual write operations to disk
	verifyHistogramCountWAL(t, walFileMetrics.FsyncLatency, 1)         // close sync
	verifyHistogramCountWAL(t, walFileMetrics.CloseLatency, 1)         // close operation
	verifyHistogramCountWAL(t, walFileMetrics.OpenDirLatency, 1)       // directory open during init
	verifyHistogramCountWAL(t, walFileMetrics.CreateLatency, 1)        // file creation (no recycling)
	// StatLatency should be 0 since we're not recycling files
	verifyHistogramCountWAL(t, walFileMetrics.StatLatency, 0)
}

func TestFailoverManagerMetrics(t *testing.T) {
	memFS := vfs.NewMem()
	walFileMetrics := createTestWALFileMetricsForWAL("failover")

	// Inject error on all writes to primary to trigger failover to secondary
	inj, err := errorfs.ParseDSL("(ErrInjected (And Writes (PathMatch \"primary/000001.log\")))")
	require.NoError(t, err)
	fsWithErrors := errorfs.Wrap(memFS, inj)

	opts := Options{
		Primary: Dir{
			FS:      fsWithErrors,
			Dirname: "primary",
		},
		Secondary: Dir{
			FS:      fsWithErrors,
			Dirname: "secondary",
		},
		MinUnflushedWALNum:  1,
		WALFileMetrics:      walFileMetrics,
		PreallocateSize:     func() int { return 0 },
		WriteWALSyncOffsets: func() bool { return false },
		Logger:              base.DefaultLogger,
		FailoverOptions: FailoverOptions{
			PrimaryDirProbeInterval:      10 * time.Millisecond,
			HealthyProbeLatencyThreshold: 5 * time.Millisecond,
			HealthyInterval:              100 * time.Millisecond,
			UnhealthySamplingInterval:    10 * time.Millisecond,
			UnhealthyOperationLatencyThreshold: func() (time.Duration, bool) {
				return 5 * time.Millisecond, true
			},
			ElevatedWriteStallThresholdLag: 1 * time.Second,
		},
	}

	// Create directories
	require.NoError(t, memFS.MkdirAll("primary", 0755))
	require.NoError(t, memFS.MkdirAll("secondary", 0755))

	// Initialize failover manager
	initial := Logs{}
	manager, err := Init(opts, initial)
	require.NoError(t, err)
	defer manager.Close()

	// Verify it's a failover manager
	_, isFailover := manager.(*failoverManager)
	require.True(t, isFailover, "expected failoverManager")

	// Create a WAL - this will trigger async creation that will fail on primary
	// and eventually switch to secondary
	writer, err := manager.Create(NumWAL(1), 1)
	require.NoError(t, err)

	// Wait for the failover to happen in background
	// The monitor checks every UnhealthySamplingInterval (10ms) and needs to see
	// the error to trigger failover
	time.Sleep(200 * time.Millisecond)

	// Write operations - these should go to secondary after failover
	testData := [][]byte{
		[]byte("failover test record 1"),
		[]byte("failover test record 2"),
	}

	for _, data := range testData {
		_, err := writer.WriteRecord(data, SyncOptions{}, nil)
		require.NoError(t, err)
	}

	// Close the writer
	_, err = writer.Close()
	require.NoError(t, err)

	// Get metrics from the writer
	metrics := writer.Metrics()

	// Verify operations were recorded in runtime metrics
	require.Greater(t, metrics.WriteThroughput.Bytes, int64(0))

	// The WAL metrics should have recorded operations in histograms
	// Since failover occurred, we have operations on both primary and secondary:
	// - Primary: attempted create (which may have some writes/syncs before failing over)
	// - Secondary: successful writes and close
	// Write operations may vary due to buffering behavior
	verifyHistogramCountRangeWAL(t, walFileMetrics.WriteLatency, 1, 3)  // write operations on secondary
	verifyHistogramCountRangeWAL(t, walFileMetrics.FsyncLatency, 1, 2)  // syncs on both attempts
	verifyHistogramCountRangeWAL(t, walFileMetrics.CloseLatency, 1, 2)  // closes on both attempts
	verifyHistogramCountWAL(t, walFileMetrics.OpenDirLatency, 2)        // opened primary + secondary dirs
	verifyHistogramCountRangeWAL(t, walFileMetrics.CreateLatency, 1, 3) // creates on primary and/or secondary (may retry)
	verifyHistogramCountWAL(t, walFileMetrics.StatLatency, 0)           // no recycling in this test

	// Verify failover stats - the key assertion that proves failover occurred
	stats := manager.Stats()
	require.GreaterOrEqual(t, stats.LiveFileCount, 1)

	// Verify that failover actually occurred to secondary
	// The error on primary writes should have triggered a switch to secondary
	t.Logf("Failover stats: switches=%d, pri-dur=%s, sec-dur=%s",
		stats.Failover.DirSwitchCount, stats.Failover.PrimaryWriteDuration, stats.Failover.SecondaryWriteDuration)
	require.Greater(t, stats.Failover.DirSwitchCount, int64(0), "expected at least one directory switch (failover to secondary)")
	require.Greater(t, stats.Failover.SecondaryWriteDuration, time.Duration(0), "expected writes to secondary after failover")

	// Note: The system may have failed back to primary after the error cleared,
	// which is expected behavior. The key is that failover happened (DirSwitchCount > 0)
	// and writes went to secondary (SecondaryWriteDuration > 0).
}

func TestWALManagerDirectoryTypeTracking(t *testing.T) {
	memFS := vfs.NewMem()

	// Create separate metrics for primary and secondary
	primaryMetrics := createTestWALFileMetricsForWAL("primary")
	secondaryMetrics := createTestWALFileMetricsForWAL("secondary")

	testCases := []struct {
		name            string
		useSecondary    bool
		expectedMetrics record.WALFileMetrics
	}{
		{"Primary Directory", false, primaryMetrics},
		{"Secondary Directory", true, secondaryMetrics},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			opts := Options{
				Primary: Dir{
					FS:      memFS,
					Dirname: "primary",
				},
				MinUnflushedWALNum:  1,
				WALFileMetrics:      tc.expectedMetrics,
				PreallocateSize:     func() int { return 0 },
				WriteWALSyncOffsets: func() bool { return false },
				Logger:              base.DefaultLogger,
			}

			if tc.useSecondary {
				opts.Secondary = Dir{
					FS:      memFS,
					Dirname: "secondary",
				}
				require.NoError(t, memFS.MkdirAll("secondary", 0755))
			}

			require.NoError(t, memFS.MkdirAll("primary", 0755))

			initial := Logs{}
			manager, err := Init(opts, initial)
			require.NoError(t, err)
			defer manager.Close()

			// Create and use a WAL
			writer, err := manager.Create(NumWAL(1), 1)
			require.NoError(t, err)

			_, err = writer.WriteRecord([]byte("directory type test"), SyncOptions{}, nil)
			require.NoError(t, err)

			_, err = writer.Close()
			require.NoError(t, err)

			// Verify the correct metrics were updated
			// Write operations may vary due to buffering behavior
			verifyHistogramCountRangeWAL(t, tc.expectedMetrics.WriteLatency, 1, 2) // actual disk write operation(s)
			verifyHistogramCountWAL(t, tc.expectedMetrics.FsyncLatency, 1)         // close sync
			verifyHistogramCountWAL(t, tc.expectedMetrics.CloseLatency, 1)
		})
	}
}

func TestWALManagerMetricsRecycling(t *testing.T) {
	memFS := vfs.NewMem()
	walFileMetrics := createTestWALFileMetricsForWAL("recycling")

	opts := Options{
		Primary: Dir{
			FS:      memFS,
			Dirname: "primary",
		},
		MinUnflushedWALNum:   1,
		MaxNumRecyclableLogs: 2,
		WALFileMetrics:       walFileMetrics,
		PreallocateSize:      func() int { return 0 },
		WriteWALSyncOffsets:  func() bool { return false },
		Logger:               base.DefaultLogger,
	}

	require.NoError(t, memFS.MkdirAll("primary", 0755))

	initial := Logs{}
	manager, err := Init(opts, initial)
	require.NoError(t, err)
	defer manager.Close()

	// Create multiple WALs to test recycling
	for i := 1; i <= 3; i++ {
		writer, err := manager.Create(NumWAL(i), i)
		require.NoError(t, err)

		_, err = writer.WriteRecord([]byte("recycling test"), SyncOptions{}, nil)
		require.NoError(t, err)

		_, err = writer.Close()
		require.NoError(t, err)

		// Mark all WALs up to current as obsolete (by setting minUnflushed to i+1)
		// This allows the current WAL to be recycled for the next iteration
		_, err = manager.Obsolete(NumWAL(i+1), false)
		require.NoError(t, err)
	}

	// Verify metrics accumulated across all WAL operations
	// Each WAL: 1 actual disk write operation
	// Each WAL: 1 close sync = 1 sync
	// Each WAL: 1 close = 1 close
	// WAL #1: create (no recycle)
	// WAL #2: recycle (after WAL #1 is obsolete) → stat
	// WAL #3: recycle (after WAL #2 is obsolete) → stat
	expectedWrites := uint64(3 * 1) // 3 WALs × 1 actual write each
	expectedSyncs := uint64(3 * 1)  // 3 WALs × 1 sync each
	expectedCloses := uint64(3 * 1) // 3 WALs × 1 close each

	verifyHistogramCountWAL(t, walFileMetrics.WriteLatency, expectedWrites)
	verifyHistogramCountWAL(t, walFileMetrics.FsyncLatency, expectedSyncs)
	verifyHistogramCountWAL(t, walFileMetrics.CloseLatency, expectedCloses)
	verifyHistogramCountWAL(t, walFileMetrics.OpenDirLatency, 1) // directory open during init
	verifyHistogramCountWAL(t, walFileMetrics.CreateLatency, 1)  // only first WAL is created
	verifyHistogramCountWAL(t, walFileMetrics.StatLatency, 2)    // WAL #2 and #3 recycle → stat
}

func TestWALManagerMetricsConsistency(t *testing.T) {
	memFS := vfs.NewMem()
	walFileMetrics := createTestWALFileMetricsForWAL("consistency")

	opts := Options{
		Primary: Dir{
			FS:      memFS,
			Dirname: "primary",
		},
		MinUnflushedWALNum:  1,
		WALFileMetrics:      walFileMetrics,
		PreallocateSize:     func() int { return 0 },
		WriteWALSyncOffsets: func() bool { return false },
		Logger:              base.DefaultLogger,
	}

	require.NoError(t, memFS.MkdirAll("primary", 0755))

	initial := Logs{}
	manager, err := Init(opts, initial)
	require.NoError(t, err)
	defer manager.Close()

	writer, err := manager.Create(NumWAL(1), 1)
	require.NoError(t, err)

	// Perform a mix of operations
	operations := []struct {
		name string
		op   func() error
	}{
		{"write1", func() error {
			_, err := writer.WriteRecord([]byte("consistency test 1"), SyncOptions{}, nil)
			return err
		}},
		{"write2", func() error {
			_, err := writer.WriteRecord([]byte("consistency test 2"), SyncOptions{}, nil)
			return err
		}},
		{"close", func() error { _, err := writer.Close(); return err }},
	}

	for _, op := range operations {
		err := op.op()
		require.NoError(t, err, "operation %s failed", op.name)
	}

	// Verify WAL-level histogram metrics
	walWriteMetric := &dto.Metric{}
	err = walFileMetrics.WriteLatency.Write(walWriteMetric)
	require.NoError(t, err)

	walSyncMetric := &dto.Metric{}
	err = walFileMetrics.FsyncLatency.Write(walSyncMetric)
	require.NoError(t, err)

	walCloseMetric := &dto.Metric{}
	err = walFileMetrics.CloseLatency.Write(walCloseMetric)
	require.NoError(t, err)

	// Verify histogram counts are as expected
	// Write operations may vary due to buffering behavior
	verifyHistogramCountRangeWAL(t, walFileMetrics.WriteLatency, 1, 2)          // actual write operations to disk
	require.Equal(t, uint64(1), walSyncMetric.GetHistogram().GetSampleCount())  // 1 close sync
	require.Equal(t, uint64(1), walCloseMetric.GetHistogram().GetSampleCount()) // 1 close operation
}
