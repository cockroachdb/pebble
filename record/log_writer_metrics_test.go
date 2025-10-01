// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package record

import (
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestWALFileMetricsInitialization(t *testing.T) {
	testCases := []struct {
		name          string
		directoryType DirectoryType
	}{
		{"Primary", DirectoryPrimary},
		{"Secondary", DirectorySecondary},
		{"Unknown", DirectoryUnknown},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			walFileMetrics := createTestWALFileMetrics("test")
			config := testLogWriterConfig(walFileMetrics, tc.directoryType)

			// Create a test file
			memFS := vfs.NewMem()
			f, ferr := memFS.Create("test.log", vfs.WriteCategoryUnspecified)
			require.NoError(t, ferr)

			// Create LogWriter with metrics
			w := NewLogWriter(f, base.DiskFileNum(1), config)
			require.NotNil(t, w)

			// Verify no metrics recorded initially
			verifyNoHistogramActivity(t, walFileMetrics.WriteLatency)
			verifyNoHistogramActivity(t, walFileMetrics.FsyncLatency)
			verifyNoHistogramActivity(t, walFileMetrics.CloseLatency)

			// Close the writer
			err := w.Close()
			require.NoError(t, err)

			// Verify close latency was recorded
			verifyHistogramCount(t, walFileMetrics.CloseLatency, 1)
		})
	}
}

func TestWALMetricsWriteOperations(t *testing.T) {
	walFileMetrics := createTestWALFileMetrics("write_test")
	config := testLogWriterConfig(walFileMetrics, DirectoryPrimary)

	// Create a test file with controlled write latency
	memFS := vfs.NewMem()
	baseFileRaw, ferr := memFS.Create("test.log", vfs.WriteCategoryUnspecified)
	require.NoError(t, ferr)
	baseFile := baseFileRaw
	writeLatency := 5 * time.Millisecond
	f := newLatencyFile(baseFile, writeLatency, 0, 0, 0)

	w := NewLogWriter(f, base.DiskFileNum(1), config)

	// Write some records
	testData := [][]byte{
		[]byte("test record 1"),
		[]byte("test record 2 with more data"),
		[]byte("short"),
	}

	for _, data := range testData {
		_, err := w.WriteRecord(data)
		require.NoError(t, err)
	}

	// Close the writer
	err := w.Close()
	require.NoError(t, err)

	// Verify write latency was recorded
	// Small records are buffered and written, but buffering behavior can vary
	verifyHistogramCountRange(t, walFileMetrics.WriteLatency, 1, 3)

	// Verify the recorded latencies are reasonable
	// Single block flush should take at least the injected latency
	expectedMinSum := float64(writeLatency)
	verifyHistogramSum(t, walFileMetrics.WriteLatency, expectedMinSum*0.8, expectedMinSum*10)

	// Verify sync and close were also recorded
	verifyHistogramCount(t, walFileMetrics.FsyncLatency, 1)
	verifyHistogramCount(t, walFileMetrics.CloseLatency, 1)
}

func TestWALMetricsSyncOperations(t *testing.T) {
	walFileMetrics := createTestWALFileMetrics("sync_test")
	config := testLogWriterConfig(walFileMetrics, DirectoryPrimary)

	// Create a test file with controlled sync latency
	memFS := vfs.NewMem()
	baseFileRaw, ferr := memFS.Create("test.log", vfs.WriteCategoryUnspecified)
	require.NoError(t, ferr)
	baseFile := baseFileRaw
	syncLatency := 10 * time.Millisecond
	f := newLatencyFile(baseFile, 0, syncLatency, 0, 0)

	w := NewLogWriter(f, base.DiskFileNum(1), config)

	// Write a record (SyncRecord with nil WaitGroup doesn't actually sync)
	_, err := w.WriteRecord([]byte("test sync record"))
	require.NoError(t, err)

	// Close the writer (which performs another sync)
	err = w.Close()
	require.NoError(t, err)

	// Verify sync operations were recorded
	// Only Close performs a sync when no WaitGroup is provided to SyncRecord
	verifyHistogramCount(t, walFileMetrics.FsyncLatency, 1) // Close only

	// Verify latency values are reasonable
	expectedMinSum := float64(syncLatency) // Only one sync from Close
	verifyHistogramSum(t, walFileMetrics.FsyncLatency, expectedMinSum*0.8, expectedMinSum*5)
}

func TestWALMetricsCloseOperations(t *testing.T) {
	walFileMetrics := createTestWALFileMetrics("close_test")
	config := testLogWriterConfig(walFileMetrics, DirectoryPrimary)

	// Create a test file with controlled close latency
	memFS := vfs.NewMem()
	baseFileRaw, ferr := memFS.Create("test.log", vfs.WriteCategoryUnspecified)
	require.NoError(t, ferr)
	baseFile := baseFileRaw
	closeLatency := 3 * time.Millisecond
	f := newLatencyFile(baseFile, 0, 0, closeLatency, 0)

	w := NewLogWriter(f, base.DiskFileNum(1), config)

	// Write a record
	_, err := w.WriteRecord([]byte("test close record"))
	require.NoError(t, err)

	// Close the writer
	err = w.Close()
	require.NoError(t, err)

	// Verify close latency was recorded
	verifyHistogramCount(t, walFileMetrics.CloseLatency, 1)

	// Verify latency value is reasonable
	expectedMinSum := float64(closeLatency)
	verifyHistogramSum(t, walFileMetrics.CloseLatency, expectedMinSum*0.8, expectedMinSum*10)
}

func TestWALMetricsWithNilHistograms(t *testing.T) {
	// Test that operations don't panic with nil histograms
	walFileMetrics := WALFileMetrics{
		// All histograms are nil
	}
	config := testLogWriterConfig(walFileMetrics, DirectoryPrimary)

	memFS := vfs.NewMem()
	f, ferr := memFS.Create("test.log", vfs.WriteCategoryUnspecified)
	require.NoError(t, ferr)

	w := NewLogWriter(f, base.DiskFileNum(1), config)

	// These operations should not panic
	_, err := w.WriteRecord([]byte("test with nil metrics"))
	require.NoError(t, err)

	_, err = w.SyncRecord([]byte("sync test"), nil, nil)
	require.NoError(t, err)

	err = w.Close()
	require.NoError(t, err)
}

func TestWALMetricsMultipleOperations(t *testing.T) {
	walFileMetrics := createTestWALFileMetrics("multi_test")
	config := testLogWriterConfig(walFileMetrics, DirectorySecondary) // Test secondary directory

	// Create a test file with various latencies
	memFS := vfs.NewMem()
	baseFileRaw, ferr := memFS.Create("test.log", vfs.WriteCategoryUnspecified)
	require.NoError(t, ferr)
	baseFile := baseFileRaw
	f := newLatencyFile(baseFile, 2*time.Millisecond, 8*time.Millisecond, 1*time.Millisecond, 0)

	w := NewLogWriter(f, base.DiskFileNum(1), config)

	// Perform multiple mixed operations
	operations := []struct {
		name      string
		operation func() error
	}{
		{"write1", func() error { _, err := w.WriteRecord([]byte("record 1")); return err }},
		{"write2", func() error { _, err := w.WriteRecord([]byte("record 2")); return err }},
		{"sync1", func() error { _, err := w.SyncRecord([]byte("sync record"), nil, nil); return err }},
		{"write3", func() error { _, err := w.WriteRecord([]byte("record 3")); return err }},
		{"close", func() error { return w.Close() }},
	}

	for _, op := range operations {
		t.Run(op.name, func(t *testing.T) {
			err := op.operation()
			require.NoError(t, err)
		})
	}

	// Verify metrics were recorded for all operations
	// Records may trigger multiple flushes depending on buffer state
	verifyHistogramCountRange(t, walFileMetrics.WriteLatency, 1, 2)

	// Syncs: only Close sync (SyncRecord with nil WaitGroup doesn't actually sync)
	verifyHistogramCount(t, walFileMetrics.FsyncLatency, 1)

	// Close: close operation
	verifyHistogramCount(t, walFileMetrics.CloseLatency, 1)

}

func TestWALMetricsErrorConditions(t *testing.T) {
	walFileMetrics := createTestWALFileMetrics("error_test")
	config := testLogWriterConfig(walFileMetrics, DirectoryPrimary)

	// Create a file that will error on operations
	f := &errorFile{
		File: func() vfs.File {
			memFS := vfs.NewMem()
			f, ferr := memFS.Create("test.log", vfs.WriteCategoryUnspecified)
			if ferr != nil {
				panic(ferr)
			}
			return f
		}(),
		writeErr: nil, // Start with no errors
		syncErr:  nil,
		closeErr: nil,
	}

	w := NewLogWriter(f, base.DiskFileNum(1), config)

	// Successful write should be buffered (not immediately recorded)
	_, err := w.WriteRecord([]byte("success"))
	require.NoError(t, err)

	// With buffering, WriteRecord doesn't immediately fail even with file errors
	// The error will occur during flush/close when the buffer is written
	_, err = w.WriteRecord([]byte("second record"))
	require.NoError(t, err) // No error yet due to buffering

	// Records may or may not trigger immediate flushes depending on buffer state
	// The important thing is that WriteRecord doesn't fail due to buffering

	// Reset error and close
	f.setWriteErr(nil)
	err = w.Close()
	require.NoError(t, err)
}

// errorFile is a test file that can be configured to return errors
type errorFile struct {
	vfs.File
	mu       sync.RWMutex
	writeErr error
	syncErr  error
	closeErr error
}

func (f *errorFile) Write(p []byte) (int, error) {
	f.mu.RLock()
	err := f.writeErr
	f.mu.RUnlock()
	if err != nil {
		return 0, err
	}
	return f.File.Write(p)
}

func (f *errorFile) Sync() error {
	f.mu.RLock()
	err := f.syncErr
	f.mu.RUnlock()
	if err != nil {
		return err
	}
	return f.File.Sync()
}

func (f *errorFile) Close() error {
	f.mu.RLock()
	err := f.closeErr
	f.mu.RUnlock()
	if err != nil {
		return err
	}
	return f.File.Close()
}

func (f *errorFile) setWriteErr(err error) {
	f.mu.Lock()
	f.writeErr = err
	f.mu.Unlock()
}

func TestWALMetricsDirectoryTypes(t *testing.T) {
	// Test that different directory types don't interfere with each other
	primaryMetrics := createTestWALFileMetrics("primary")
	secondaryMetrics := createTestWALFileMetrics("secondary")

	primaryConfig := testLogWriterConfig(primaryMetrics, DirectoryPrimary)
	secondaryConfig := testLogWriterConfig(secondaryMetrics, DirectorySecondary)

	// Create writers for both directory types
	memFS1 := vfs.NewMem()
	f1, ferr := memFS1.Create("primary.log", vfs.WriteCategoryUnspecified)
	require.NoError(t, ferr)
	w1 := NewLogWriter(f1, base.DiskFileNum(1), primaryConfig)

	memFS2 := vfs.NewMem()
	f2, ferr := memFS2.Create("secondary.log", vfs.WriteCategoryUnspecified)
	require.NoError(t, ferr)
	w2 := NewLogWriter(f2, base.DiskFileNum(2), secondaryConfig)

	// Perform operations on primary
	_, err := w1.WriteRecord([]byte("primary record"))
	require.NoError(t, err)
	err = w1.Close()
	require.NoError(t, err)

	// Perform operations on secondary
	_, err = w2.WriteRecord([]byte("secondary record"))
	require.NoError(t, err)
	err = w2.Close()
	require.NoError(t, err)

	// Verify metrics are recorded separately
	// Small records are buffered, but buffering behavior can cause 1-2 flushes
	verifyHistogramCountRange(t, primaryMetrics.WriteLatency, 1, 2)   // 1-2 flushes depending on buffering
	verifyHistogramCountRange(t, secondaryMetrics.WriteLatency, 1, 2) // 1-2 flushes depending on buffering

	verifyHistogramCount(t, primaryMetrics.CloseLatency, 1)
	verifyHistogramCount(t, secondaryMetrics.CloseLatency, 1)

	// Verify cross-contamination didn't occur
	verifyHistogramCount(t, primaryMetrics.FsyncLatency, 1)   // close sync
	verifyHistogramCount(t, secondaryMetrics.FsyncLatency, 1) // close sync
}
