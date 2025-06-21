// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// setupTestSSTable creates a test SSTable for benchmarking
func setupTestSSTable(t testing.TB) (*Reader, func()) {
	mem := vfs.NewMem()
	f, err := mem.Create("test.sst", vfs.WriteCategoryUnspecified)
	require.NoError(t, err)

	w := NewWriter(objstorageprovider.NewFileWritable(f), WriterOptions{
		BlockSize:      4096,
		IndexBlockSize: 4096,
		FilterPolicy:   bloom.FilterPolicy(10),
		TableFormat:    TableFormatPebblev3,
		Comparer:       base.DefaultComparer,
		MergerName:     base.DefaultMerger.Name,
	})

	const numKeys = 10000
	for i := range numKeys {
		key := fmt.Appendf(nil, "key%08d", i)
		value := fmt.Sprintf("value%d", i)
		require.NoError(t, w.Set(key, []byte(value)))
	}
	require.NoError(t, w.Close())

	// Open the file for reading
	f2, err := mem.Open("test.sst")
	require.NoError(t, err)

	// Create reader
	r, err := newReader(f2, ReaderOptions{
		Comparer: base.DefaultComparer,
		Merger:   base.DefaultMerger,
	})
	require.NoError(t, err)

	cleanup := func() {
		if r != nil {
			r.Close()
			// Reader.Close() should close the underlying file, so don't close f2 again
		} else if f2 != nil {
			f2.Close()
		}
	}

	return r, cleanup
}

// BenchmarkIteratorConstruction measures the performance of iterator construction
// with the current lazy loading implementation (default behavior)
// How to: go test -bench=BenchmarkIteratorConstruction -count=5 -run=^$ ./sstable
func BenchmarkIteratorConstruction(b *testing.B) {
	r, cleanup := setupTestSSTable(b)
	defer cleanup()

	var stats base.InternalIteratorStats
	var bufferPool block.BufferPool
	bufferPool.Init(5)
	defer bufferPool.Release()

	iterOpts := IterOptions{
		Transforms:           NoTransforms,
		FilterBlockSizeLimit: NeverUseFilterBlock,
		Env:                  ReadEnv{Block: block.ReadEnv{Stats: &stats, BufferPool: &bufferPool}},
		ReaderProvider:       MakeTrivialReaderProvider(r),
	}

	b.Run("RowBlock", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			iter, err := newRowBlockSingleLevelIterator(context.Background(), r, iterOpts)
			if err != nil {
				b.Fatal(err)
			}
			iter.Close()
		}
	})
}

// BenchmarkIteratorConstructionWithFirstAccess measures construction + first access
// to understand the total cost when index loading is actually needed
// How to: go test -bench=BenchmarkIteratorConstructionWithFirstAccess -run=^$ ./sstable
func BenchmarkIteratorConstructionWithFirstAccess(b *testing.B) {
	r, cleanup := setupTestSSTable(b)
	defer cleanup()

	var stats base.InternalIteratorStats
	var bufferPool block.BufferPool
	bufferPool.Init(5)
	defer bufferPool.Release()

	iterOpts := IterOptions{
		Transforms:           NoTransforms,
		FilterBlockSizeLimit: NeverUseFilterBlock,
		Env:                  ReadEnv{Block: block.ReadEnv{Stats: &stats, BufferPool: &bufferPool}},
		ReaderProvider:       MakeTrivialReaderProvider(r),
	}

	b.Run("RowBlock_First", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			iter, err := newRowBlockSingleLevelIterator(context.Background(), r, iterOpts)
			if err != nil {
				b.Fatal(err)
			}
			_ = iter.First() // Trigger index loading
			iter.Close()
		}
	})

	b.Run("RowBlock_SeekGE", func(b *testing.B) {
		key := []byte("key00005000") // Middle key
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			iter, err := newRowBlockSingleLevelIterator(context.Background(), r, iterOpts)
			if err != nil {
				b.Fatal(err)
			}
			_ = iter.SeekGE(key, base.SeekGEFlagsNone) // Trigger index loading
			iter.Close()
		}
	})
}

// BenchmarkFirstAccessLatency compares the latency of first access operations
// between lazy loading (index loaded on first access) vs when index is already loaded
// How to: go test -bench=BenchmarkFirstAccessLatency -run=^$ ./sstable
func BenchmarkFirstAccessLatency(b *testing.B) {
	r, cleanup := setupTestSSTable(b)
	defer cleanup()

	var stats base.InternalIteratorStats
	var bufferPool block.BufferPool
	bufferPool.Init(5)
	defer bufferPool.Release()

	iterOpts := IterOptions{
		Transforms:           NoTransforms,
		FilterBlockSizeLimit: NeverUseFilterBlock,
		Env:                  ReadEnv{Block: block.ReadEnv{Stats: &stats, BufferPool: &bufferPool}},
		ReaderProvider:       MakeTrivialReaderProvider(r),
	}

	// Benchmark: First() call including index loading time (lazy loading)
	b.Run("LazyLoading_First", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			iter, err := newRowBlockSingleLevelIterator(context.Background(), r, iterOpts)
			if err != nil {
				b.Fatal(err)
			}
			_ = iter.First() // Index loaded here
			iter.Close()
		}
	})

	// Benchmark: First() call when index is already loaded (simulated eager loading)
	b.Run("EagerLoading_First", func(b *testing.B) {
		// Create a single iterator and pre-load its index once
		iter, err := newRowBlockSingleLevelIterator(context.Background(), r, iterOpts)
		if err != nil {
			b.Fatal(err)
		}
		defer iter.Close()

		_ = iter.First() // Pre-load index

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = iter.First() // Index already loaded - this is what we're measuring
		}
	})

	// Benchmark: SeekGE() call including index loading time (lazy loading)
	b.Run("LazyLoading_SeekGE", func(b *testing.B) {
		key := []byte("key00005000") // Middle key
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			iter, err := newRowBlockSingleLevelIterator(context.Background(), r, iterOpts)
			if err != nil {
				b.Fatal(err)
			}
			_ = iter.SeekGE(key, base.SeekGEFlagsNone) // Index loaded here
			iter.Close()
		}
	})

	// Benchmark: SeekGE() call when index is already loaded (simulated eager loading)
	b.Run("EagerLoading_SeekGE", func(b *testing.B) {
		key := []byte("key00005000") // Middle key

		// Create a single iterator and pre-load its index once
		iter, err := newRowBlockSingleLevelIterator(context.Background(), r, iterOpts)
		if err != nil {
			b.Fatal(err)
		}
		defer iter.Close()

		_ = iter.First() // Pre-load index

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = iter.SeekGE(key, base.SeekGEFlagsNone) // Index already loaded - this is what we're measuring
		}
	})
}

// BenchmarkBloomFilterEarlyExit measures the performance benefit of lazy loading
// when bloom filter rejects the prefix (index never loaded)
// How to: go test -bench=BenchmarkBloomFilterEarlyExit -run=^$ ./sstable
func BenchmarkBloomFilterEarlyExit(b *testing.B) {
	r, cleanup := setupTestSSTable(b)
	defer cleanup()

	var stats base.InternalIteratorStats
	var bufferPool block.BufferPool
	bufferPool.Init(5)
	defer bufferPool.Release()

	iterOpts := IterOptions{
		Transforms:           NoTransforms,
		FilterBlockSizeLimit: AlwaysUseFilterBlock, // Enable bloom filter
		Env:                  ReadEnv{Block: block.ReadEnv{Stats: &stats, BufferPool: &bufferPool}},
		ReaderProvider:       MakeTrivialReaderProvider(r),
	}

	// Use prefixes that likely don't exist to trigger bloom filter rejection
	rejectedPrefixes := [][]byte{
		[]byte("notfound1"),
		[]byte("notfound2"),
		[]byte("notfound3"),
		[]byte("zzzzzzzzz"),
	}

	b.Run("SeekPrefixGE_BloomReject", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			iter, err := newRowBlockSingleLevelIterator(context.Background(), r, iterOpts)
			if err != nil {
				b.Fatal(err)
			}

			prefix := rejectedPrefixes[i%len(rejectedPrefixes)]
			_ = iter.SeekPrefixGE(prefix, prefix, base.SeekGEFlagsNone)
			iter.Close()
		}
	})

	// Compare with existing prefixes that will be found
	existingPrefixes := [][]byte{
		[]byte("key00001000"),
		[]byte("key00005000"),
		[]byte("key00009000"),
	}

	b.Run("SeekPrefixGE_BloomAccept", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			iter, err := newRowBlockSingleLevelIterator(context.Background(), r, iterOpts)
			if err != nil {
				b.Fatal(err)
			}

			prefix := existingPrefixes[i%len(existingPrefixes)]
			_ = iter.SeekPrefixGE(prefix, prefix, base.SeekGEFlagsNone)
			iter.Close()
		}
	})
}

// BenchmarkBatchIteratorCreation simulates creating many iterators
// where only some are actually used (common in merge iterator scenarios)
// How to: go test -bench=BenchmarkBatchIteratorCreation -run=^$ ./sstable
func BenchmarkBatchIteratorCreation(b *testing.B) {
	r, cleanup := setupTestSSTable(b)
	defer cleanup()

	var stats base.InternalIteratorStats
	var bufferPool block.BufferPool
	bufferPool.Init(5)
	defer bufferPool.Release()

	iterOpts := IterOptions{
		Transforms:           NoTransforms,
		FilterBlockSizeLimit: NeverUseFilterBlock,
		Env:                  ReadEnv{Block: block.ReadEnv{Stats: &stats, BufferPool: &bufferPool}},
		ReaderProvider:       MakeTrivialReaderProvider(r),
	}

	b.Run("CreateMany_UseNone", func(b *testing.B) {
		const batchSize = 10
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			iters := make([]*singleLevelIteratorRowBlocks, batchSize)

			// Create many iterators
			for j := range batchSize {
				iter, err := newRowBlockSingleLevelIterator(context.Background(), r, iterOpts)
				if err != nil {
					b.Fatal(err)
				}
				iters[j] = iter
			}

			// Close all without using them (index never loaded)
			for j := range batchSize {
				iters[j].Close()
			}
		}
	})

	b.Run("CreateMany_UseSome", func(b *testing.B) {
		const batchSize = 10
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			iters := make([]*singleLevelIteratorRowBlocks, batchSize)

			// Create many iterators
			for j := range batchSize {
				iter, err := newRowBlockSingleLevelIterator(context.Background(), r, iterOpts)
				if err != nil {
					b.Fatal(err)
				}
				iters[j] = iter
			}

			// Use only some of them (triggers index loading for those)
			for j := range batchSize {
				if j%3 == 0 { // Use every 3rd iterator
					_ = iters[j].First()
				}
			}

			// Close all
			for j := range batchSize {
				iters[j].Close()
			}
		}
	})
}

// TestLazyLoadingBehavior verifies that lazy loading works as expected
// How to: go test -run TestLazyLoadingBehavior -v ./sstable
func TestLazyLoadingBehavior(t *testing.T) {
	r, cleanup := setupTestSSTable(t)
	defer cleanup()

	var stats base.InternalIteratorStats
	var bufferPool block.BufferPool
	bufferPool.Init(5)
	defer bufferPool.Release()

	iterOpts := IterOptions{
		Transforms:           NoTransforms,
		FilterBlockSizeLimit: NeverUseFilterBlock,
		Env:                  ReadEnv{Block: block.ReadEnv{Stats: &stats, BufferPool: &bufferPool}},
		ReaderProvider:       MakeTrivialReaderProvider(r),
	}

	t.Run("IndexNotLoadedOnConstruction", func(t *testing.T) {
		iter, err := newRowBlockSingleLevelIterator(context.Background(), r, iterOpts)
		require.NoError(t, err)
		defer iter.Close()

		// Index should not be loaded yet
		require.False(t, iter.indexLoaded, "Index should not be loaded on construction")
	})

	t.Run("IndexLoadedOnFirstAccess", func(t *testing.T) {
		iter, err := newRowBlockSingleLevelIterator(context.Background(), r, iterOpts)
		require.NoError(t, err)
		defer iter.Close()

		// Index should not be loaded yet
		require.False(t, iter.indexLoaded, "Index should not be loaded on construction")

		// Trigger index loading
		_ = iter.First()

		// Index should now be loaded
		require.True(t, iter.indexLoaded, "Index should be loaded after first access")
	})

	t.Run("IndexStaysLoadedAfterPoolReuse", func(t *testing.T) {
		iter, err := newRowBlockSingleLevelIterator(context.Background(), r, iterOpts)
		require.NoError(t, err)

		// Load the index
		_ = iter.First()
		require.True(t, iter.indexLoaded, "Index should be loaded after first access")

		// Close and return to pool
		iter.Close()

		// Create new iterator (may reuse from pool)
		iter2, err := newRowBlockSingleLevelIterator(context.Background(), r, iterOpts)
		require.NoError(t, err)
		defer iter2.Close()

		// Index should not be loaded for new iterator (flag reset on pool reuse)
		require.False(t, iter2.indexLoaded, "Index should not be loaded on new iterator from pool")
	})
}
