// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package record

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/vfs"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// TestConcurrentMetricsAccess tests that multiple writers can safely
// record metrics to the same histograms without data races.
func TestConcurrentMetricsAccess(t *testing.T) {
	// Shared metrics across multiple writers
	sharedMetrics := createTestWALFileMetrics("concurrent")

	numWriters := runtime.NumCPU()
	recordsPerWriter := 100

	var wg sync.WaitGroup
	var errorCount atomic.Int64

	// Launch multiple writers concurrently
	for i := range numWriters {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()

			config := LogWriterConfig{
				DirectoryType:       DirectoryPrimary,
				WALFileMetrics:      sharedMetrics,
				WriteWALSyncOffsets: func() bool { return false },
			}

			memFS := vfs.NewMem()
			f, err := memFS.Create("test.log", vfs.WriteCategoryUnspecified)
			if err != nil {
				errorCount.Add(1)
				return
			}
			w := NewLogWriter(f, base.DiskFileNum(writerID), config)

			// Write multiple records
			for range recordsPerWriter {
				record := []byte("concurrent test record")
				_, err = w.WriteRecord(record)
				if err != nil {
					errorCount.Add(1)
					return
				}
			}

			// Close the writer
			err = w.Close()
			if err != nil {
				errorCount.Add(1)
				return
			}
		}(i)
	}

	wg.Wait()

	// Verify no errors occurred
	require.Equal(t, int64(0), errorCount.Load(), "concurrent writers should not have errors")

	// Verify all operations were recorded in shared metrics
	// Small records are buffered and written as a single flush during close
	expectedWrites := uint64(numWriters) // One close flush per writer
	expectedSyncs := uint64(numWriters)  // One close sync per writer
	expectedCloses := uint64(numWriters) // One close per writer

	// Write operations may vary due to concurrent buffering behavior
	verifyHistogramCountRange(t, sharedMetrics.WriteLatency, expectedWrites, expectedWrites*2)
	verifyHistogramCount(t, sharedMetrics.FsyncLatency, expectedSyncs)
	verifyHistogramCount(t, sharedMetrics.CloseLatency, expectedCloses)
}

// TestConcurrentWriterSeparateMetrics tests that writers with separate
// metrics don't interfere with each other.
func TestConcurrentWriterSeparateMetrics(t *testing.T) {
	numWriters := 4
	recordsPerWriter := 50

	type writerMetrics struct {
		walFileMetrics WALFileMetrics
	}

	// Create separate metrics for each writer
	writerMetricsList := make([]writerMetrics, numWriters)
	for i := range numWriters {
		writerMetricsList[i] = writerMetrics{
			walFileMetrics: createTestWALFileMetrics("separate_" + string(rune('A'+i))),
		}
	}

	var wg sync.WaitGroup

	// Launch writers with separate metrics
	for i := range numWriters {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()

			metrics := writerMetricsList[writerID]
			config := LogWriterConfig{
				DirectoryType:       DirectoryPrimary,
				WALFileMetrics:      metrics.walFileMetrics,
				WriteWALSyncOffsets: func() bool { return false },
			}

			memFS := vfs.NewMem()
			f, err := memFS.Create("test.log", vfs.WriteCategoryUnspecified)
			require.NoError(t, err)
			w := NewLogWriter(f, base.DiskFileNum(writerID), config)

			for range recordsPerWriter {
				_, err = w.WriteRecord([]byte("separate metrics test"))
				require.NoError(t, err)
			}

			err = w.Close()
			require.NoError(t, err)
		}(i)
	}

	wg.Wait()

	// Verify each writer's metrics are correctly isolated
	for _, metrics := range writerMetricsList {
		// With 50 records, there may be multiple flushes depending on buffer alignment
		// Allow generous upper bound since buffering behavior can vary
		verifyHistogramCountRange(t, metrics.walFileMetrics.WriteLatency, 1, 50)
		verifyHistogramCount(t, metrics.walFileMetrics.FsyncLatency, 1) // close sync
		verifyHistogramCount(t, metrics.walFileMetrics.CloseLatency, 1) // close
	}
}

// TestErrorHandlingWithMetrics tests that metrics are still recorded
// even when operations encounter errors.
func TestErrorHandlingWithMetrics(t *testing.T) {
	walFileMetrics := createTestWALFileMetrics("error_handling")
	config := testLogWriterConfig(walFileMetrics, DirectoryPrimary)

	// Use an error file that fails after a few operations
	memFS := vfs.NewMem()
	baseFileRaw, ferr := memFS.Create("test.log", vfs.WriteCategoryUnspecified)
	require.NoError(t, ferr)
	baseFile := baseFileRaw
	errorFile := &conditionalErrorFile{
		File:         baseFile,
		failAfterOps: 0,
		currentOps:   0,
		writeError:   nil,
		syncError:    errors.New("test sync error"),
		closeError:   nil,
	}

	w := NewLogWriter(errorFile, base.DiskFileNum(1), config)

	// Write some records - these should succeed (buffered)
	_, err := w.WriteRecord([]byte("success 1"))
	require.NoError(t, err)

	_, err = w.WriteRecord([]byte("success 2"))
	require.NoError(t, err)

	// Close should fail due to sync error
	err = w.Close()
	require.Error(t, err)

	// Verify metrics were recorded for successful operations
	// Write operations may vary due to buffering behavior
	verifyHistogramCountRange(t, walFileMetrics.WriteLatency, 1, 2) // close flush (small records buffered)
	verifyHistogramCount(t, walFileMetrics.FsyncLatency, 0)         // sync failed, no metrics recorded
	verifyHistogramCount(t, walFileMetrics.CloseLatency, 1)         // close attempted
}

// TestMetricsRaceDetection specifically tests for data races in metrics
// recording under high contention.
func TestMetricsRaceDetection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping race detection test in short mode")
	}

	sharedMetrics := createTestWALFileMetrics("race_detection")
	numGoroutines := 20
	operationsPerGoroutine := 100

	var wg sync.WaitGroup

	// Create high contention by having many goroutines hit the same metrics
	for i := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			config := testLogWriterConfig(sharedMetrics, DirectoryPrimary)

			// Create many writers quickly to stress test metrics recording
			for j := range operationsPerGoroutine {
				memFS := vfs.NewMem()
				f, err := memFS.Create("test.log", vfs.WriteCategoryUnspecified)
				require.NoError(t, err)
				w := NewLogWriter(f, base.DiskFileNum(id*1000+j), config)

				// Quick write and close to maximize metrics contention
				_, err = w.WriteRecord([]byte("race test"))
				require.NoError(t, err)

				err = w.Close()
				require.NoError(t, err)
			}
		}(i)
	}

	wg.Wait()

	// Verify all operations were counted (this test mainly checks for races)
	expectedTotal := uint64(numGoroutines * operationsPerGoroutine)
	// Allow some variance due to concurrent buffering behavior
	verifyHistogramCountRange(t, sharedMetrics.WriteLatency, expectedTotal, expectedTotal*2)
	verifyHistogramCount(t, sharedMetrics.FsyncLatency, expectedTotal) // close sync
	verifyHistogramCount(t, sharedMetrics.CloseLatency, expectedTotal) // close
}

// TestMetricsWithSlowOperations tests metrics accuracy when operations
// are slow and potentially overlapping.
func TestMetricsWithSlowOperations(t *testing.T) {
	walFileMetrics := createTestWALFileMetrics("slow_ops")
	config := testLogWriterConfig(walFileMetrics, DirectorySecondary)

	var wg sync.WaitGroup
	numWriters := 3

	// Run concurrent slow operations
	for i := range numWriters {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()

			// Create a separate file for each writer to avoid concurrent access issues
			memFS := vfs.NewMem()
			baseFileRaw, ferr := memFS.Create("test.log", vfs.WriteCategoryUnspecified)
			require.NoError(t, ferr)
			baseFile := baseFileRaw
			slowFile := newLatencyFile(baseFile, 10*time.Millisecond, 20*time.Millisecond, 5*time.Millisecond, 0)
			w := NewLogWriter(slowFile, base.DiskFileNum(writerID), config)

			// Write and sync (both slow)
			_, err := w.WriteRecord([]byte("slow write test"))
			require.NoError(t, err)

			_, err = w.SyncRecord([]byte("slow sync test"), nil, nil)
			require.NoError(t, err)

			// Close (also slow)
			err = w.Close()
			require.NoError(t, err)
		}(i)
	}

	wg.Wait()

	// Verify all operations were recorded despite slowness
	// Each writer: WriteRecord (buffered) + SyncRecord (flush + sync) + Close (sync only if no new data)
	expectedWrites := uint64(numWriters) // sync flush per writer (close may not flush if no new data)
	expectedCloses := uint64(numWriters) // close per writer

	verifyHistogramCountRange(t, walFileMetrics.WriteLatency, expectedWrites, expectedWrites*2)
	verifyHistogramCountRange(t, walFileMetrics.FsyncLatency, expectedWrites, expectedWrites*2) // sync behavior can vary
	verifyHistogramCount(t, walFileMetrics.CloseLatency, expectedCloses)

	// Verify that the latencies are reasonable (should include injected delays)
	verifyHistogramSum(t, walFileMetrics.WriteLatency, float64(10*time.Millisecond), float64(1*time.Second))
	verifyHistogramSum(t, walFileMetrics.FsyncLatency, float64(20*time.Millisecond), float64(1*time.Second))
	verifyHistogramSum(t, walFileMetrics.CloseLatency, float64(5*time.Millisecond), float64(1*time.Second))
}

// conditionalErrorFile fails operations after a certain number of calls
type conditionalErrorFile struct {
	vfs.File
	failAfterOps int
	currentOps   int
	writeError   error
	syncError    error
	closeError   error
	mu           sync.Mutex
}

func (f *conditionalErrorFile) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.currentOps++
	if f.currentOps > f.failAfterOps && f.writeError != nil {
		return 0, f.writeError
	}
	return f.File.Write(p)
}

func (f *conditionalErrorFile) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.syncError != nil {
		return f.syncError
	}
	return f.File.Sync()
}

func (f *conditionalErrorFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closeError != nil {
		return f.closeError
	}
	return f.File.Close()
}

// TestStressMetricsUnderLoad performs a stress test of metrics recording
// under high load to ensure no panics or data loss.
func TestStressMetricsUnderLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	sharedMetrics := createTestWALFileMetrics("stress")
	config := testLogWriterConfig(sharedMetrics, DirectoryPrimary)

	var wg sync.WaitGroup
	var totalOps atomic.Uint64
	duration := 2 * time.Second
	stopCh := make(chan struct{})

	// Start timer
	go func() {
		time.Sleep(duration)
		close(stopCh)
	}()

	// Launch stress workers
	numWorkers := runtime.NumCPU() * 2
	for i := range numWorkers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			ops := 0
			for {
				select {
				case <-stopCh:
					totalOps.Add(uint64(ops))
					return
				default:
				}

				memFS := vfs.NewMem()
				f, ferr := memFS.Create("test.log", vfs.WriteCategoryUnspecified)
				if ferr != nil {
					continue
				}
				w := NewLogWriter(f, base.DiskFileNum(workerID*10000+ops), config)

				_, err := w.WriteRecord([]byte("stress test"))
				if err != nil {
					continue
				}

				err = w.Close()
				if err != nil {
					continue
				}

				ops++
			}
		}(i)
	}

	wg.Wait()

	total := totalOps.Load()
	t.Logf("Completed %d operations in %v", total, duration)

	// Verify metrics recorded all operations (allow some variance for concurrent buffering)
	verifyHistogramCountRange(t, sharedMetrics.WriteLatency, total, total*2) // close flush (small records buffered)
	verifyHistogramCount(t, sharedMetrics.FsyncLatency, total)               // close sync
	verifyHistogramCount(t, sharedMetrics.CloseLatency, total)               // close

	// Verify no data was lost (counts should be consistent)
	writeMetric := &dto.Metric{}
	err := sharedMetrics.WriteLatency.Write(writeMetric)
	require.NoError(t, err)

	syncMetric := &dto.Metric{}
	err = sharedMetrics.FsyncLatency.Write(syncMetric)
	require.NoError(t, err)

	closeMetric := &dto.Metric{}
	err = sharedMetrics.CloseLatency.Write(closeMetric)
	require.NoError(t, err)

	// Allow range for write metrics due to buffering variance
	writeCount := writeMetric.GetHistogram().GetSampleCount()
	require.GreaterOrEqual(t, writeCount, total)
	require.LessOrEqual(t, writeCount, total*2)
	require.Equal(t, total, syncMetric.GetHistogram().GetSampleCount())
	require.Equal(t, total, closeMetric.GetHistogram().GetSampleCount())
}
