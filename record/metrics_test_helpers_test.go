// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package record

import (
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// createTestWALFileMetrics creates a WALFileMetrics struct with test histograms
// for all operation types using appropriate buckets.
func createTestWALFileMetrics(prefix string) WALFileMetrics {
	return WALFileMetrics{
		CreateLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    prefix + "_create_latency",
			Buckets: []float64{1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9}, // ns buckets
		}),
		WriteLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    prefix + "_write_latency",
			Buckets: []float64{1e3, 1e4, 1e5, 1e6, 1e7, 1e8}, // ns buckets
		}),
		FsyncLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    prefix + "_sync_latency",
			Buckets: []float64{1e6, 1e7, 1e8, 1e9, 1e10}, // Higher buckets for sync
		}),
		CloseLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    prefix + "_close_latency",
			Buckets: []float64{1e4, 1e5, 1e6, 1e7, 1e8},
		}),
		StatLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    prefix + "_stat_latency",
			Buckets: []float64{1e3, 1e4, 1e5, 1e6, 1e7}, // Fast operations
		}),
		OpenDirLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    prefix + "_opendir_latency",
			Buckets: []float64{1e4, 1e5, 1e6, 1e7, 1e8, 1e9},
		}),
	}
}

// verifyHistogramCount checks that a histogram has the expected number of observations.
func verifyHistogramCount(t *testing.T, hist prometheus.Histogram, expectedCount uint64) {
	t.Helper()

	metric := &dto.Metric{}
	err := hist.Write(metric)
	require.NoError(t, err)

	actual := metric.GetHistogram().GetSampleCount()
	require.Equal(t, expectedCount, actual, "histogram sample count mismatch")
}

// verifyHistogramSum checks that a histogram's sum is within expected range.
func verifyHistogramSum(t *testing.T, hist prometheus.Histogram, minSum, maxSum float64) {
	t.Helper()

	metric := &dto.Metric{}
	err := hist.Write(metric)
	require.NoError(t, err)

	actual := metric.GetHistogram().GetSampleSum()
	require.GreaterOrEqual(t, actual, minSum, "histogram sum too low")
	require.LessOrEqual(t, actual, maxSum, "histogram sum too high")
}

// verifyHistogramCountRange checks that a histogram sample count is within a reasonable range
func verifyHistogramCountRange(t *testing.T, hist prometheus.Histogram, min, max uint64) {
	t.Helper()

	metric := &dto.Metric{}
	err := hist.Write(metric)
	require.NoError(t, err)

	actual := metric.GetHistogram().GetSampleCount()
	require.GreaterOrEqual(t, actual, min, "histogram sample count too low")
	require.LessOrEqual(t, actual, max, "histogram sample count too high")
}

// latencyFile is a test file wrapper that injects controlled latency into operations.
type latencyFile struct {
	vfs.File
	writeLatency time.Duration
	syncLatency  time.Duration
	closeLatency time.Duration
	statLatency  time.Duration
}

// newLatencyFile creates a new latency file wrapper with specified latencies.
func newLatencyFile(
	file vfs.File, writeLatency, syncLatency, closeLatency, statLatency time.Duration,
) *latencyFile {
	return &latencyFile{
		File:         file,
		writeLatency: writeLatency,
		syncLatency:  syncLatency,
		closeLatency: closeLatency,
		statLatency:  statLatency,
	}
}

func (f *latencyFile) Write(p []byte) (int, error) {
	if f.writeLatency > 0 {
		time.Sleep(f.writeLatency)
	}
	return f.File.Write(p)
}

func (f *latencyFile) Sync() error {
	if f.syncLatency > 0 {
		time.Sleep(f.syncLatency)
	}
	return f.File.Sync()
}

func (f *latencyFile) Close() error {
	if f.closeLatency > 0 {
		time.Sleep(f.closeLatency)
	}
	return f.File.Close()
}

func (f *latencyFile) Stat() (vfs.FileInfo, error) {
	if f.statLatency > 0 {
		time.Sleep(f.statLatency)
	}
	return f.File.Stat()
}

// testLogWriterConfig creates a LogWriterConfig for testing with metrics.
func testLogWriterConfig(
	walFileMetrics WALFileMetrics, directoryType DirectoryType,
) LogWriterConfig {
	return LogWriterConfig{
		DirectoryType:       directoryType,
		WALFileMetrics:      walFileMetrics,
		WriteWALSyncOffsets: func() bool { return false },
	}
}

// verifyNoHistogramActivity checks that a histogram has no recorded observations.
func verifyNoHistogramActivity(t *testing.T, hist prometheus.Histogram) {
	t.Helper()
	verifyHistogramCount(t, hist, 0)
}
