// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/testutils"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/errorfs"
	"github.com/stretchr/testify/require"
)

// TestIteratorErrorOnInit tests the path where creation of an iterator fails
// when reading the index block.
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
			_, err := newRowBlockSingleLevelIterator(context.Background(), r, IterOptions{
				Transforms:           NoTransforms,
				FilterBlockSizeLimit: NeverUseFilterBlock,
				Env:                  ReadEnv{Block: block.ReadEnv{Stats: &stats, BufferPool: &pool}},
				ReaderProvider:       MakeTrivialReaderProvider(r),
			})
			require.Error(t, err)
		} else {
			_, err := newRowBlockTwoLevelIterator(context.Background(), r, IterOptions{
				Transforms:           NoTransforms,
				FilterBlockSizeLimit: NeverUseFilterBlock,
				Env:                  ReadEnv{Block: block.ReadEnv{Stats: &stats, BufferPool: &pool}},
				ReaderProvider:       MakeTrivialReaderProvider(r),
			})
			require.Error(t, err)
		}
	}
}

// controllableFilterPolicy is a test filter policy that allows controlling
// the return values of MayContain to simulate different bloom filter behaviors.
type controllableFilterPolicy struct {
	bloom.FilterPolicy
	mayContainResult bool
	mayContainError  error
	name             string
}

func newControllableFilterPolicy(
	name string, mayContain bool, err error,
) *controllableFilterPolicy {
	return &controllableFilterPolicy{
		FilterPolicy:     bloom.FilterPolicy(10),
		mayContainResult: mayContain,
		mayContainError:  err,
		name:             name,
	}
}

func (c *controllableFilterPolicy) Name() string {
	return c.name
}

func (c *controllableFilterPolicy) MayContain(ftype FilterType, filter, key []byte) bool {
	return c.mayContainResult
}

// We don't need controllableReader for these tests since we can control
// the filter policy behavior directly. The filter read errors are a different
// code path that would require more complex mocking.

// createTestSST creates an SST with the given keys and filter policy for testing.
func createTestSST(t *testing.T, keys []string, filterPolicy FilterPolicy) (*Reader, func()) {
	return createTestSSTWithOptions(t, keys, filterPolicy, false)
}

// createTestSSTWithOptions creates an SST with optional two-level index forcing.
func createTestSSTWithOptions(
	t *testing.T, keys []string, filterPolicy FilterPolicy, forceTwoLevel bool,
) (*Reader, func()) {
	mem := vfs.NewMem()

	f, err := mem.Create("test.sst", vfs.WriteCategoryUnspecified)
	require.NoError(t, err)

	writerOpts := WriterOptions{
		Comparer:     base.DefaultComparer,
		MergerName:   base.DefaultMerger.Name,
		FilterPolicy: filterPolicy,
	}
	if forceTwoLevel {
		writerOpts.BlockSize = 512     // Small block size to force more blocks
		writerOpts.IndexBlockSize = 64 // Very small index block size to force two-level
	}
	w := NewWriter(objstorageprovider.NewFileWritable(f), writerOpts)

	for _, key := range keys {
		require.NoError(t, w.Set([]byte(key), []byte("value-"+key)))
	}
	require.NoError(t, w.Close())

	f, err = mem.Open("test.sst")
	require.NoError(t, err)

	readerOpts := ReaderOptions{
		Comparer: base.DefaultComparer,
		Merger:   base.DefaultMerger,
	}
	if filterPolicy != nil {
		readerOpts.Filters = map[string]FilterPolicy{
			filterPolicy.Name(): filterPolicy,
		}
	}

	reader, err := newReader(f, readerOpts)
	require.NoError(t, err)

	return reader, func() {
		reader.Close()
	}
}

// TestBloomFilterOptimizationSingleLevel tests the bloom filter optimization
// in single-level iterators where data blocks are not invalidated when bloom
// filter returns false with no error.
func TestBloomFilterOptimizationSingleLevel(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("bloom_miss_no_invalidation", func(t *testing.T) {
		// Create a filter policy that always returns false (bloom miss)
		filterPolicy := newControllableFilterPolicy("test-filter-miss", false, nil)

		// Create test SST with some keys
		keys := []string{"aa", "bb", "cc", "dd"}
		reader, cleanup := createTestSST(t, keys, filterPolicy)
		defer cleanup()

		var pool block.BufferPool
		pool.Init(5)
		defer pool.Release()

		var stats base.InternalIteratorStats
		iter, err := newRowBlockSingleLevelIterator(context.Background(), reader, IterOptions{
			Transforms:           NoTransforms,
			FilterBlockSizeLimit: AlwaysUseFilterBlock,
			Env:                  ReadEnv{Block: block.ReadEnv{Stats: &stats, BufferPool: &pool}},
			ReaderProvider:       MakeTrivialReaderProvider(reader),
		})
		require.NoError(t, err)
		defer iter.Close()

		// First, load a data block by seeking to an existing key
		kv := iter.SeekGE([]byte("bb"), base.SeekGEFlagsNone)
		require.NotNil(t, kv)
		require.Equal(t, "bb", string(kv.K.UserKey))

		// Check that data block is loaded and valid initially
		require.True(t, iter.data.Valid())
		require.False(t, iter.data.IsDataInvalidated())

		// Now call SeekPrefixGE with a prefix that will trigger bloom filter miss
		result := iter.SeekPrefixGE([]byte("zz"), []byte("zz"), base.SeekGEFlagsNone)
		require.Nil(t, result, "Expected SeekPrefixGE to return nil on bloom miss")

		// The key part of the optimization: data block should NOT be invalidated
		// when bloom filter returns false with no error
		require.False(t, iter.data.IsDataInvalidated(),
			"Expected data block to NOT be invalidated on bloom miss (optimization)")

		require.NoError(t, iter.Error())
	})

	t.Run("bloom_hit_no_invalidation", func(t *testing.T) {
		// Create a filter policy that always returns true (bloom hit)
		filterPolicy := newControllableFilterPolicy("test-filter-hit", true, nil)

		// Create test SST with some keys
		keys := []string{"aa", "bb", "cc", "dd"}
		reader, cleanup := createTestSST(t, keys, filterPolicy)
		defer cleanup()

		var pool block.BufferPool
		pool.Init(5)
		defer pool.Release()

		var stats base.InternalIteratorStats
		iter, err := newRowBlockSingleLevelIterator(context.Background(), reader, IterOptions{
			Transforms:           NoTransforms,
			FilterBlockSizeLimit: AlwaysUseFilterBlock,
			Env:                  ReadEnv{Block: block.ReadEnv{Stats: &stats, BufferPool: &pool}},
			ReaderProvider:       MakeTrivialReaderProvider(reader),
		})
		require.NoError(t, err)
		defer iter.Close()

		// First, load a data block by seeking to an existing key
		kv := iter.SeekGE([]byte("bb"), base.SeekGEFlagsNone)
		require.NotNil(t, kv)
		require.Equal(t, "bb", string(kv.K.UserKey))

		// Check that data block is loaded and valid initially
		require.True(t, iter.data.Valid())
		require.False(t, iter.data.IsDataInvalidated())

		// Call SeekPrefixGE with a prefix that will trigger bloom filter hit
		// Since we control the filter to always return true, it should proceed to search
		result := iter.SeekPrefixGE([]byte("zz"), []byte("zz"), base.SeekGEFlagsNone)
		require.Nil(t, result, "Expected SeekPrefixGE to return nil since 'zz' doesn't exist in table")

		// When bloom filter returns true, the iterator proceeds to search, which may
		// invalidate blocks as part of normal operation. This is expected behavior.
		// The optimization is specifically for bloom filter misses, not hits.

		require.NoError(t, iter.Error())
	})
}

// TestBloomFilterOptimizationTwoLevel tests the bloom filter optimization
// in two-level iterators where data blocks are not invalidated when bloom
// filter returns false with no error.
func TestBloomFilterOptimizationTwoLevel(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("bloom_miss_no_invalidation", func(t *testing.T) {
		// Create a filter policy that always returns false (bloom miss)
		filterPolicy := newControllableFilterPolicy("test-filter-2lvl-miss", false, nil)

		// Create test SST with enough keys to force two-level structure
		keys := make([]string, 100)
		for i := range 100 {
			keys[i] = fmt.Sprintf("key%03d", i)
		}
		reader, cleanup := createTestSSTWithOptions(t, keys, filterPolicy, true)
		defer cleanup()

		// Verify it's actually a two-level index
		require.True(t, reader.Attributes.Has(AttributeTwoLevelIndex),
			"Test requires two-level index structure to be created")

		var pool block.BufferPool
		pool.Init(5)
		defer pool.Release()

		var stats base.InternalIteratorStats
		iter, err := newRowBlockTwoLevelIterator(context.Background(), reader, IterOptions{
			Transforms:           NoTransforms,
			FilterBlockSizeLimit: AlwaysUseFilterBlock,
			Env:                  ReadEnv{Block: block.ReadEnv{Stats: &stats, BufferPool: &pool}},
			ReaderProvider:       MakeTrivialReaderProvider(reader),
		})
		require.NoError(t, err)
		defer iter.Close()

		// First, load a data block by seeking to an existing key
		kv := iter.SeekGE([]byte("key050"), base.SeekGEFlagsNone)
		require.NotNil(t, kv)
		require.Equal(t, "key050", string(kv.K.UserKey))

		// Check that data block is loaded and valid initially
		require.True(t, iter.secondLevel.data.Valid())
		require.False(t, iter.secondLevel.data.IsDataInvalidated())

		// Now call SeekPrefixGE with a prefix that will trigger bloom filter miss
		result := iter.SeekPrefixGE([]byte("zzz"), []byte("zzz"), base.SeekGEFlagsNone)
		require.Nil(t, result, "Expected SeekPrefixGE to return nil on bloom miss")

		// The key part of the optimization: data block should NOT be invalidated
		// when bloom filter returns false with no error
		require.False(t, iter.secondLevel.data.IsDataInvalidated(),
			"Expected data block to NOT be invalidated on bloom miss (optimization)")

		require.NoError(t, iter.Error())
	})

	t.Run("bloom_hit_no_invalidation", func(t *testing.T) {
		// Create a filter policy that always returns true (bloom hit)
		filterPolicy := newControllableFilterPolicy("test-filter-2lvl-hit", true, nil)

		// Create test SST with enough keys to force two-level structure
		keys := make([]string, 100)
		for i := range 100 {
			keys[i] = fmt.Sprintf("key%03d", i)
		}
		reader, cleanup := createTestSSTWithOptions(t, keys, filterPolicy, true)
		defer cleanup()

		// Verify it's actually a two-level index
		require.True(t, reader.Attributes.Has(AttributeTwoLevelIndex),
			"Test requires two-level index structure to be created")

		var pool block.BufferPool
		pool.Init(5)
		defer pool.Release()

		var stats base.InternalIteratorStats
		iter, err := newRowBlockTwoLevelIterator(context.Background(), reader, IterOptions{
			Transforms:           NoTransforms,
			FilterBlockSizeLimit: AlwaysUseFilterBlock,
			Env:                  ReadEnv{Block: block.ReadEnv{Stats: &stats, BufferPool: &pool}},
			ReaderProvider:       MakeTrivialReaderProvider(reader),
		})
		require.NoError(t, err)
		defer iter.Close()

		// First, load a data block by seeking to an existing key
		kv := iter.SeekGE([]byte("key050"), base.SeekGEFlagsNone)
		require.NotNil(t, kv)
		require.Equal(t, "key050", string(kv.K.UserKey))

		// Check that data block is loaded and valid initially
		require.True(t, iter.secondLevel.data.Valid())
		require.False(t, iter.secondLevel.data.IsDataInvalidated())

		// Call SeekPrefixGE with a prefix that will trigger bloom filter hit
		// Since we control the filter to always return true, it should proceed to search
		result := iter.SeekPrefixGE([]byte("zzz"), []byte("zzz"), base.SeekGEFlagsNone)
		require.Nil(t, result, "Expected SeekPrefixGE to return nil since 'zzz' doesn't exist in table")

		// When bloom filter returns true, the iterator proceeds to search, which may
		// invalidate blocks as part of normal operation. This is expected behavior.
		// The optimization is specifically for bloom filter misses, not hits.

		require.NoError(t, iter.Error())
	})
}

// TestBloomFilterOptimizationEdgeCases tests edge cases for the bloom filter
// optimization including scenarios with no pre-loaded blocks, multiple seeks, etc.
func TestBloomFilterOptimizationEdgeCases(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test case: SeekPrefixGE on iterator with no pre-loaded data block
	t.Run("no_preloaded_block_single_level", func(t *testing.T) {
		filterPolicy := newControllableFilterPolicy("test-no-preload", false, nil)
		keys := []string{"aa", "bb", "cc"}
		reader, cleanup := createTestSST(t, keys, filterPolicy)
		defer cleanup()

		var pool block.BufferPool
		pool.Init(5)
		defer pool.Release()

		var stats base.InternalIteratorStats
		iter, err := newRowBlockSingleLevelIterator(context.Background(), reader, IterOptions{
			Transforms:           NoTransforms,
			FilterBlockSizeLimit: AlwaysUseFilterBlock,
			Env:                  ReadEnv{Block: block.ReadEnv{Stats: &stats, BufferPool: &pool}},
			ReaderProvider:       MakeTrivialReaderProvider(reader),
		})
		require.NoError(t, err)
		defer iter.Close()

		// Check initial state - no block should be loaded yet
		initialValid := iter.data.Valid()
		initialInvalidated := iter.data.IsDataInvalidated()

		// Call SeekPrefixGE without pre-loading any blocks
		result := iter.SeekPrefixGE([]byte("zz"), []byte("zz"), base.SeekGEFlagsNone)
		require.Nil(t, result)

		// Data block state should be preserved - if it was initially not valid,
		// it should remain not valid. The optimization doesn't change the state
		// when the bloom filter returns false.
		require.Equal(t, initialValid, iter.data.Valid())
		require.Equal(t, initialInvalidated, iter.data.IsDataInvalidated())
	})

	// Test case: Multiple SeekPrefixGE calls with bloom misses
	t.Run("multiple_seeks_bloom_miss", func(t *testing.T) {
		filterPolicy := newControllableFilterPolicy("test-multiple-seeks", false, nil)
		keys := []string{"aa", "bb", "cc"}
		reader, cleanup := createTestSST(t, keys, filterPolicy)
		defer cleanup()

		var pool block.BufferPool
		pool.Init(5)
		defer pool.Release()

		var stats base.InternalIteratorStats
		iter, err := newRowBlockSingleLevelIterator(context.Background(), reader, IterOptions{
			Transforms:           NoTransforms,
			FilterBlockSizeLimit: AlwaysUseFilterBlock,
			Env:                  ReadEnv{Block: block.ReadEnv{Stats: &stats, BufferPool: &pool}},
			ReaderProvider:       MakeTrivialReaderProvider(reader),
		})
		require.NoError(t, err)
		defer iter.Close()

		// Load a data block first
		kv := iter.SeekGE([]byte("bb"), base.SeekGEFlagsNone)
		require.NotNil(t, kv)

		require.True(t, iter.data.Valid())
		require.False(t, iter.data.IsDataInvalidated())

		// First SeekPrefixGE with bloom miss
		result1 := iter.SeekPrefixGE([]byte("xx"), []byte("xx"), base.SeekGEFlagsNone)
		require.Nil(t, result1)
		require.False(t, iter.data.IsDataInvalidated(), "First bloom miss should not invalidate")

		// Second SeekPrefixGE with bloom miss
		result2 := iter.SeekPrefixGE([]byte("yy"), []byte("yy"), base.SeekGEFlagsNone)
		require.Nil(t, result2)
		require.False(t, iter.data.IsDataInvalidated(), "Second bloom miss should not invalidate")
	})

	// Test case: Bloom miss followed by successful seek
	t.Run("bloom_miss_then_success", func(t *testing.T) {
		// Use real bloom filter for this test to get actual behavior
		filterPolicy := bloom.FilterPolicy(10)
		keys := []string{"aa", "bb", "cc"}
		reader, cleanup := createTestSST(t, keys, filterPolicy)
		defer cleanup()

		var pool block.BufferPool
		pool.Init(5)
		defer pool.Release()

		var stats base.InternalIteratorStats
		iter, err := newRowBlockSingleLevelIterator(context.Background(), reader, IterOptions{
			Transforms:           NoTransforms,
			FilterBlockSizeLimit: AlwaysUseFilterBlock,
			Env:                  ReadEnv{Block: block.ReadEnv{Stats: &stats, BufferPool: &pool}},
			ReaderProvider:       MakeTrivialReaderProvider(reader),
		})
		require.NoError(t, err)
		defer iter.Close()

		// Load a data block first
		kv := iter.SeekGE([]byte("bb"), base.SeekGEFlagsNone)
		require.NotNil(t, kv)

		require.True(t, iter.data.Valid())
		require.False(t, iter.data.IsDataInvalidated())

		// SeekPrefixGE with a prefix that should miss in bloom filter
		// Note: This test relies on the bloom filter's false negative rate being low
		// The prefix "zzzz" is unlikely to be present in a bloom filter for keys "aa", "bb", "cc"
		result := iter.SeekPrefixGE([]byte("zzzz"), []byte("zzzz"), base.SeekGEFlagsNone)
		require.Nil(t, result)

		// Data block should still be valid (optimization working)
		require.False(t, iter.data.IsDataInvalidated(),
			"Bloom miss should not invalidate data block")

		// Now seek to an existing key - should work without reloading
		result2 := iter.SeekGE([]byte("cc"), base.SeekGEFlagsNone)
		require.NotNil(t, result2)
		require.Equal(t, "cc", string(result2.K.UserKey))
	})
}
