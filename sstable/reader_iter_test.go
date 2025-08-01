// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build invariants

package sstable

import (
	"context"
	"fmt"
	"math/rand/v2"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/testutils"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/errorfs"
	"github.com/stretchr/testify/require"
)

// TestIteratorErrorOnInit tests the path where iterator operations fail
// due to errors when reading the index block. With lazy loading, the error
// may occur during construction or on first use.
func TestIteratorErrorOnInit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()

	f0, err := mem.Create("test.sst", vfs.WriteCategoryUnspecified)
	require.NoError(t, err)
	writerOpts := WriterOptions{
		Comparer:   base.DefaultComparer,
		MergerName: base.DefaultMerger.Name,
	}
	w := NewWriter(objstorageprovider.NewFileWritable(f0), writerOpts)
	require.NoError(t, w.Set([]byte("test"), nil))
	require.NoError(t, w.Close())

	toggle := errorfs.Toggle{Injector: errorfs.ErrInjected}
	f1 := errorfs.WrapFile(testutils.CheckErr(mem.Open("test.sst")), &toggle)

	r, err := newReader(f1, ReaderOptions{
		Comparer: base.DefaultComparer,
		Merger:   base.DefaultMerger,
	})
	require.NoError(t, err)
	defer r.Close()

	var pool block.BufferPool
	pool.Init(5)
	defer pool.Release()

	toggle.On()
	defer toggle.Off()

	var stats base.InternalIteratorStats
	for range 20 {
		if rand.IntN(2) == 0 {
			iter, err := newRowBlockSingleLevelIterator(context.Background(), r, IterOptions{
				Transforms:           NoTransforms,
				FilterBlockSizeLimit: NeverUseFilterBlock,
				Env:                  ReadEnv{Block: block.ReadEnv{Stats: &stats, BufferPool: &pool}},
				ReaderProvider:       MakeTrivialReaderProvider(r),
			})
			if err != nil {
				// If construction fails, that's also valid (for non-lazy implementations)
				continue
			}
			// With lazy loading, the error should happen on first use
			kv := iter.First()
			require.Nil(t, kv, "First() should return nil due to error injection")
			require.Error(t, iter.Error(), "Error() should return the injected error")
			iter.Close()
		} else {
			iter, err := newRowBlockTwoLevelIterator(context.Background(), r, IterOptions{
				Transforms:           NoTransforms,
				FilterBlockSizeLimit: NeverUseFilterBlock,
				Env:                  ReadEnv{Block: block.ReadEnv{Stats: &stats, BufferPool: &pool}},
				ReaderProvider:       MakeTrivialReaderProvider(r),
			})
			if err != nil {
				// If construction fails, that's also valid (for non-lazy implementations)
				continue
			}
			// With lazy loading, the error should happen on first use
			kv := iter.First()
			require.Nil(t, kv, "First() should return nil due to error injection")
			require.Error(t, iter.Error(), "Error() should return the injected error")
			iter.Close()
		}
	}
}

// TestLazyLoadingBasicFunctionality tests that lazy loading works correctly
// for basic iterator operations.
func TestLazyLoadingBasicFunctionality(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name        string
		tableFormat TableFormat
		useBloom    bool
	}{
		{"RowBlocks_NoBloom", TableFormatPebblev3, false},
		{"RowBlocks_WithBloom", TableFormatPebblev3, true},
		{"ColumnBlocks_NoBloom", TableFormatPebblev5, false},
		{"ColumnBlocks_WithBloom", TableFormatPebblev5, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Generate test data
			keys := generateSimpleTestKeys(100)
			reader := createSimpleTestSSTable(t, keys, tc.tableFormat, tc.useBloom)
			defer reader.Close()

			// Test basic iterator operations
			testBasicIteratorOperations(t, reader, keys)
		})
	}
}

// TestLazyLoadingSeekOperations tests seek operations with lazy loading.
func TestLazyLoadingSeekOperations(t *testing.T) {
	defer leaktest.AfterTest(t)()

	keys := generateSimpleTestKeys(200)
	reader := createSimpleTestSSTable(t, keys, TableFormatPebblev5, true)
	defer reader.Close()

	testSeekOperations(t, reader, keys)
}

// TestLazyLoadingResourceManagement tests resource management.
func TestLazyLoadingResourceManagement(t *testing.T) {
	defer leaktest.AfterTest(t)()

	keys := generateSimpleTestKeys(50)
	reader := createSimpleTestSSTable(t, keys, TableFormatPebblev5, true)
	defer reader.Close()

	testResourceManagement(t, reader)
}

// Helper functions

func generateSimpleTestKeys(count int) [][]byte {
	keys := make([][]byte, count)

	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key_%08d", i)
		keys[i] = []byte(key)
	}

	return keys
}

func createSimpleTestSSTable(
	t *testing.T, keys [][]byte, format TableFormat, useBloom bool,
) *Reader {
	mem := vfs.NewMem()
	f, err := mem.Create("simple_test.sst", vfs.WriteCategoryUnspecified)
	require.NoError(t, err)

	writerOpts := WriterOptions{
		Comparer:       base.DefaultComparer,
		MergerName:     base.DefaultMerger.Name,
		TableFormat:    format,
		BlockSize:      4096,
		IndexBlockSize: 2048,
	}

	if useBloom {
		writerOpts.FilterPolicy = bloom.FilterPolicy(10)
	}

	w := NewWriter(objstorageprovider.NewFileWritable(f), writerOpts)

	for i, key := range keys {
		value := []byte(fmt.Sprintf("value_%d", i))
		require.NoError(t, w.Set(key, value))
	}
	require.NoError(t, w.Close())

	// Open reader
	f2, err := mem.Open("simple_test.sst")
	require.NoError(t, err)

	reader, err := newReader(f2, ReaderOptions{
		Comparer: base.DefaultComparer,
		Merger:   base.DefaultMerger,
	})
	require.NoError(t, err)

	return reader
}

func testBasicIteratorOperations(t *testing.T, reader *Reader, keys [][]byte) {
	iter, err := reader.NewPointIter(context.Background(), IterOptions{
		Transforms:     NoTransforms,
		Env:            NoReadEnv,
		ReaderProvider: MakeTrivialReaderProvider(reader),
	})
	require.NoError(t, err)
	defer iter.Close()

	if len(keys) == 0 {
		kv := iter.First()
		require.Nil(t, kv)
		require.NoError(t, iter.Error())
		return
	}

	// Test forward iteration
	count := 0
	for kv := iter.First(); kv != nil; kv = iter.Next() {
		require.NoError(t, iter.Error())
		count++
	}
	require.Equal(t, len(keys), count, "Should visit all keys in forward iteration")

	// Test backward iteration
	count = 0
	for kv := iter.Last(); kv != nil; kv = iter.Prev() {
		require.NoError(t, iter.Error())
		count++
	}
	require.Equal(t, len(keys), count, "Should visit all keys in backward iteration")
}

func testSeekOperations(t *testing.T, reader *Reader, keys [][]byte) {
	iter, err := reader.NewPointIter(context.Background(), IterOptions{
		Transforms:     NoTransforms,
		Env:            NoReadEnv,
		ReaderProvider: MakeTrivialReaderProvider(reader),
	})
	require.NoError(t, err)
	defer iter.Close()

	if len(keys) == 0 {
		return
	}

	// Test SeekGE with existing keys
	for i := 0; i < len(keys) && i < 10; i++ {
		kv := iter.SeekGE(keys[i], base.SeekGEFlagsNone)
		require.NotNil(t, kv, "SeekGE should find existing key")
		require.NoError(t, iter.Error())
		require.Equal(t, 0, base.DefaultComparer.Compare(keys[i], kv.K.UserKey))
	}

	// Test SeekLT with existing keys (skip first key)
	for i := 1; i < len(keys) && i < 10; i++ {
		kv := iter.SeekLT(keys[i], base.SeekLTFlagsNone)
		require.NotNil(t, kv, "SeekLT should find key before existing key")
		require.NoError(t, iter.Error())
		require.True(t, base.DefaultComparer.Compare(kv.K.UserKey, keys[i]) < 0)
	}

	// Test SeekGE with non-existent keys
	nonExistentKeys := [][]byte{
		[]byte("nonexistent_key_1"),
		[]byte("nonexistent_key_2"),
	}

	for _, key := range nonExistentKeys {
		kv := iter.SeekGE(key, base.SeekGEFlagsNone)
		require.NoError(t, iter.Error())

		if kv != nil {
			// If we found something, it should be >= our search key
			require.True(t, base.DefaultComparer.Compare(kv.K.UserKey, key) >= 0)
		}
	}
}

func testResourceManagement(t *testing.T, reader *Reader) {
	// Test multiple iterator creation and cleanup
	const numIterators = 10
	iterators := make([]Iterator, numIterators)

	// Create all iterators
	for i := 0; i < numIterators; i++ {
		iter, err := reader.NewPointIter(context.Background(), IterOptions{
			Transforms:     NoTransforms,
			Env:            NoReadEnv,
			ReaderProvider: MakeTrivialReaderProvider(reader),
		})
		require.NoError(t, err)
		iterators[i] = iter
	}

	// Close all iterators
	for i := 0; i < numIterators; i++ {
		require.NoError(t, iterators[i].Close())
	}
}

// TestLazyLoadingConcurrentAccess tests concurrent access to lazy iterators.
func TestLazyLoadingConcurrentAccess(t *testing.T) {
	defer leaktest.AfterTest(t)()

	keys := generateConcurrentTestKeys(1000)
	reader := createConcurrentTestSSTable(t, keys, TableFormatPebblev5)
	defer reader.Close()

	const numGoroutines = 10
	const operationsPerGoroutine = 50

	var wg sync.WaitGroup
	errorCh := make(chan error, numGoroutines)

	// Launch concurrent goroutines performing iterator operations
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			if err := performConcurrentOps(reader, keys, operationsPerGoroutine, goroutineID); err != nil {
				errorCh <- err
			}
		}(i)
	}

	wg.Wait()
	close(errorCh)

	// Check for any errors
	for err := range errorCh {
		require.NoError(t, err)
	}
}

// TestLazyLoadingResourceCleanup tests resource cleanup scenarios.
func TestLazyLoadingResourceCleanup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	keys := generateConcurrentTestKeys(200)
	reader := createConcurrentTestSSTable(t, keys, TableFormatPebblev5)
	defer reader.Close()

	// Test early termination cleanup
	testEarlyTermination(t, reader, keys)

	// Test multiple iterator cleanup
	testMultipleIteratorCleanup(t, reader)

	// Test memory leak prevention
	testMemoryLeakPrevention(t, reader)
}

// TestLazyLoadingErrorHandling tests error handling in various scenarios.
func TestLazyLoadingErrorHandling(t *testing.T) {
	defer leaktest.AfterTest(t)()

	keys := generateConcurrentTestKeys(100)
	reader := createConcurrentTestSSTable(t, keys, TableFormatPebblev5)
	defer reader.Close()

	iter, err := reader.NewPointIter(context.Background(), IterOptions{
		Transforms:     NoTransforms,
		Env:            NoReadEnv,
		ReaderProvider: MakeTrivialReaderProvider(reader),
	})
	require.NoError(t, err)

	// Test error consistency
	err1 := iter.Error()
	err2 := iter.Error()
	require.Equal(t, err1, err2, "Error() should be consistent across calls")

	// Test operations produce consistent errors
	if len(keys) > 0 {
		iter.First()
		require.NoError(t, iter.Error())

		iter.SeekGE(keys[0], base.SeekGEFlagsNone)
		require.NoError(t, iter.Error())
	}

	// Test closed iterator error handling
	require.NoError(t, iter.Close())

	// Note: The current implementation may not return an error for closed iterators
	// This is implementation-dependent behavior
	err = iter.Error()
	// Just check that Error() doesn't panic and returns consistently
	err2 = iter.Error()
	require.Equal(t, err, err2, "Error() should be consistent after close")
}

// TestLazyLoadingBoundaryConditions tests boundary conditions.
func TestLazyLoadingBoundaryConditions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	boundaryCases := []struct {
		name string
		keys [][]byte
	}{
		{
			name: "EmptyTable",
			keys: [][]byte{},
		},
		{
			name: "SingleKey",
			keys: [][]byte{[]byte("single_key")},
		},
		{
			name: "TwoKeys",
			keys: [][]byte{[]byte("key1"), []byte("key2")},
		},
	}

	for _, bc := range boundaryCases {
		t.Run(bc.name, func(t *testing.T) {
			if len(bc.keys) == 0 {
				testEmptyTable(t)
			} else {
				reader := createConcurrentTestSSTable(t, bc.keys, TableFormatPebblev5)
				defer reader.Close()
				testBoundaryConditions(t, reader, bc.keys)
			}
		})
	}
}

// TestLazyLoadingStressOperations performs stress testing.
func TestLazyLoadingStressOperations(t *testing.T) {
	defer leaktest.AfterTest(t)()

	keys := generateConcurrentTestKeys(500)
	reader := createConcurrentTestSSTable(t, keys, TableFormatPebblev5)
	defer reader.Close()

	iter, err := reader.NewPointIter(context.Background(), IterOptions{
		Transforms:     NoTransforms,
		Env:            NoReadEnv,
		ReaderProvider: MakeTrivialReaderProvider(reader),
	})
	require.NoError(t, err)
	defer iter.Close()

	// Perform many repeated operations
	const numOperations = 5000
	for i := 0; i < numOperations; i++ {
		switch i % 5 {
		case 0:
			iter.First()
		case 1:
			iter.Last()
		case 2:
			if len(keys) > 0 {
				iter.SeekGE(keys[i%len(keys)], base.SeekGEFlagsNone)
			}
		case 3:
			iter.Next()
		case 4:
			iter.Prev()
		}

		require.NoError(t, iter.Error())
	}
}

// Additional helper functions for edge case tests

func generateConcurrentTestKeys(count int) [][]byte {
	keys := make([][]byte, count)
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("concurrent_key_%08d", i)
		keys[i] = []byte(key)
	}
	return keys
}

func createConcurrentTestSSTable(t *testing.T, keys [][]byte, format TableFormat) *Reader {
	mem := vfs.NewMem()
	f, err := mem.Create("concurrent_test.sst", vfs.WriteCategoryUnspecified)
	require.NoError(t, err)

	writerOpts := WriterOptions{
		Comparer:       base.DefaultComparer,
		MergerName:     base.DefaultMerger.Name,
		TableFormat:    format,
		BlockSize:      4096,
		IndexBlockSize: 2048,
		FilterPolicy:   bloom.FilterPolicy(10),
	}

	w := NewWriter(objstorageprovider.NewFileWritable(f), writerOpts)

	for i, key := range keys {
		value := []byte(fmt.Sprintf("value_%d", i))
		require.NoError(t, w.Set(key, value))
	}
	require.NoError(t, w.Close())

	// Open reader
	f2, err := mem.Open("concurrent_test.sst")
	require.NoError(t, err)

	reader, err := newReader(f2, ReaderOptions{
		Comparer: base.DefaultComparer,
		Merger:   base.DefaultMerger,
	})
	require.NoError(t, err)

	return reader
}

func performConcurrentOps(reader *Reader, keys [][]byte, numOps int, goroutineID int) error {
	iter, err := reader.NewPointIter(context.Background(), IterOptions{
		Transforms:     NoTransforms,
		Env:            NoReadEnv,
		ReaderProvider: MakeTrivialReaderProvider(reader),
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	for i := 0; i < numOps; i++ {
		// Vary operations based on goroutine ID and iteration
		switch (goroutineID + i) % 4 {
		case 0:
			iter.First()
		case 1:
			iter.Last()
		case 2:
			if len(keys) > 0 {
				keyIdx := (goroutineID*numOps + i) % len(keys)
				iter.SeekGE(keys[keyIdx], base.SeekGEFlagsNone)
			}
		case 3:
			iter.Next()
		}

		if err := iter.Error(); err != nil {
			return errors.Errorf("operation %d failed: %w", i, err)
		}

		// Add small delay to increase chance of concurrent access
		if i%10 == 0 {
			time.Sleep(time.Microsecond)
		}
	}

	return nil
}

func testEarlyTermination(t *testing.T, reader *Reader, keys [][]byte) {
	// Test closing iterator immediately after creation
	iter, err := reader.NewPointIter(context.Background(), IterOptions{
		Transforms:     NoTransforms,
		Env:            NoReadEnv,
		ReaderProvider: MakeTrivialReaderProvider(reader),
	})
	require.NoError(t, err)
	require.NoError(t, iter.Close())

	// Test closing after one operation
	iter, err = reader.NewPointIter(context.Background(), IterOptions{
		Transforms:     NoTransforms,
		Env:            NoReadEnv,
		ReaderProvider: MakeTrivialReaderProvider(reader),
	})
	require.NoError(t, err)

	if len(keys) > 0 {
		iter.SeekGE(keys[0], base.SeekGEFlagsNone)
	}
	require.NoError(t, iter.Close())
}

func testMultipleIteratorCleanup(t *testing.T, reader *Reader) {
	const numIterators = 10
	iterators := make([]Iterator, numIterators)

	// Create all iterators
	for i := 0; i < numIterators; i++ {
		iter, err := reader.NewPointIter(context.Background(), IterOptions{
			Transforms:     NoTransforms,
			Env:            NoReadEnv,
			ReaderProvider: MakeTrivialReaderProvider(reader),
		})
		require.NoError(t, err)
		iterators[i] = iter
	}

	// Close all iterators
	for i := 0; i < numIterators; i++ {
		require.NoError(t, iterators[i].Close())
	}
}

func testMemoryLeakPrevention(t *testing.T, reader *Reader) {
	// Force garbage collection and check for memory leaks
	runtime.GC()
	runtime.GC()

	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)

	// Create and destroy many iterators
	const numIterations = 100
	for i := 0; i < numIterations; i++ {
		iter, err := reader.NewPointIter(context.Background(), IterOptions{
			Transforms:     NoTransforms,
			Env:            NoReadEnv,
			ReaderProvider: MakeTrivialReaderProvider(reader),
		})
		require.NoError(t, err)

		iter.First()
		require.NoError(t, iter.Close())

		// Periodically force GC
		if i%20 == 19 {
			runtime.GC()
		}
	}

	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&m2)

	// Check that memory usage didn't grow excessively
	memGrowth := int64(m2.Alloc) - int64(m1.Alloc)
	t.Logf("Memory growth: %d bytes", memGrowth)

	// This is a heuristic check
	maxExpectedGrowth := int64(512 * 1024) // 512KB
	require.Less(t, memGrowth, maxExpectedGrowth, "Excessive memory growth suggests a leak")
}

func testEmptyTable(t *testing.T) {
	// Create empty SSTable
	mem := vfs.NewMem()
	f, err := mem.Create("empty.sst", vfs.WriteCategoryUnspecified)
	require.NoError(t, err)

	writerOpts := WriterOptions{
		Comparer:    base.DefaultComparer,
		MergerName:  base.DefaultMerger.Name,
		TableFormat: TableFormatPebblev5,
		BlockSize:   4096,
	}

	w := NewWriter(objstorageprovider.NewFileWritable(f), writerOpts)
	require.NoError(t, w.Close())

	// Open reader
	f2, err := mem.Open("empty.sst")
	require.NoError(t, err)

	reader, err := newReader(f2, ReaderOptions{
		Comparer: base.DefaultComparer,
		Merger:   base.DefaultMerger,
	})
	require.NoError(t, err)
	defer reader.Close()

	iter, err := reader.NewPointIter(context.Background(), IterOptions{
		Transforms:     NoTransforms,
		Env:            NoReadEnv,
		ReaderProvider: MakeTrivialReaderProvider(reader),
	})
	require.NoError(t, err)
	defer iter.Close()

	// Test operations on empty table
	require.Nil(t, iter.First())
	require.Nil(t, iter.Last())
	require.Nil(t, iter.SeekGE([]byte("any_key"), base.SeekGEFlagsNone))
	require.NoError(t, iter.Error())
}

func testBoundaryConditions(t *testing.T, reader *Reader, keys [][]byte) {
	iter, err := reader.NewPointIter(context.Background(), IterOptions{
		Transforms:     NoTransforms,
		Env:            NoReadEnv,
		ReaderProvider: MakeTrivialReaderProvider(reader),
	})
	require.NoError(t, err)
	defer iter.Close()

	// Test that all operations work with small datasets
	if len(keys) > 0 {
		// Test First
		kv := iter.First()
		require.NotNil(t, kv)
		require.NoError(t, iter.Error())

		// Test Last
		kv = iter.Last()
		require.NotNil(t, kv)
		require.NoError(t, iter.Error())

		// Test SeekGE
		kv = iter.SeekGE(keys[0], base.SeekGEFlagsNone)
		require.NotNil(t, kv)
		require.NoError(t, iter.Error())
	}
}
