// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable/tablefilters/bloom"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// setupTwoLevelBenchmarkData creates test data designed for two-level indexes
func setupTwoLevelBenchmarkData(b *testing.B, blockSize int) (*Reader, [][]byte) {
	// Generate a large dataset to ensure two-level index creation
	// Using smaller block size to force more blocks and two-level index
	numKeys := 50000
	keys := make([][]byte, numKeys)

	// Generate keys that will create a two-level index
	ks := testkeys.Alpha(5) // Use larger alphabet to support more keys
	for i := 0; i < numKeys; i++ {
		key := testkeys.Key(ks, uint64(i))
		keys[i] = []byte(key)
	}

	// Create SSTable with small block size to force two-level index
	mem := vfs.NewMem()
	f, err := mem.Create("bench", vfs.WriteCategoryUnspecified)
	require.NoError(b, err)

	w := NewWriter(objstorageprovider.NewFileWritable(f), WriterOptions{
		BlockSize:      blockSize, // Small block size
		IndexBlockSize: 1024,      // Small index block size to force two-level
		Comparer:       testkeys.Comparer,
		MergerName:     "nullptr",
		TableFormat:    TableFormatPebblev3,
	})

	// Write keys to create two-level index
	for i, key := range keys {
		value := fmt.Sprintf("value-%d", i)
		require.NoError(b, w.Set(key, []byte(value)))
	}

	require.NoError(b, w.Close())

	// Re-open the file for reading
	f, err = mem.Open("bench")
	require.NoError(b, err)

	// Create reader
	r, err := newReader(f, ReaderOptions{
		Comparer: testkeys.Comparer,
	})
	require.NoError(b, err)

	// Verify we actually created a two-level index
	if !r.Attributes.Has(AttributeTwoLevelIndex) {
		b.Fatalf("Test data did not create two-level index (keys: %d, blockSize: %d)", numKeys, blockSize)
	}

	return r, keys
}

// BenchmarkTwoLevelIteratorConstruction measures just the construction time
// How to: go test -bench=BenchmarkTwoLevelIteratorConstruction -run=^$ -count=10 ./sstable
func BenchmarkTwoLevelIteratorConstruction(b *testing.B) {
	b.Run("rowblk", func(b *testing.B) {
		reader, _ := setupTwoLevelBenchmarkData(b, 1024)
		defer reader.Close()

		opts := IterOptions{}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			var iter Iterator
			var err error

			iter, err = newRowBlockTwoLevelIterator(context.Background(), reader, opts)

			require.NoError(b, err)

			// Close immediately to measure just construction
			iter.Close()
		}
	})
}

// BenchmarkTwoLevelIteratorFirst measures construction + first access
// How to: go test -bench=BenchmarkTwoLevelIteratorFirst -run=^$ -count=10 ./sstable
func BenchmarkTwoLevelIteratorFirst(b *testing.B) {
	b.Run("rowblk", func(b *testing.B) {
		reader, _ := setupTwoLevelBenchmarkData(b, 1024)
		defer reader.Close()

		opts := IterOptions{}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			var iter Iterator
			var err error

			iter, err = newRowBlockTwoLevelIterator(context.Background(), reader, opts)

			require.NoError(b, err)

			// First access
			kv := iter.First()
			if kv == nil {
				b.Fatal("Expected non-nil key-value from First()")
			}

			iter.Close()
		}
	})
}

// BenchmarkTwoLevelIteratorSeekGE measures construction + SeekGE performance
// How to: go test -bench=BenchmarkTwoLevelIteratorSeekGE -run=^$ -count=10 ./sstable
func BenchmarkTwoLevelIteratorSeekGE(b *testing.B) {
	b.Run("rowblk", func(b *testing.B) {
		reader, keys := setupTwoLevelBenchmarkData(b, 1024)
		defer reader.Close()

		opts := IterOptions{}
		seekKey := keys[len(keys)/2]

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			var iter Iterator
			var err error
			iter, err = newRowBlockTwoLevelIterator(context.Background(), reader, opts)
			require.NoError(b, err)

			// First SeekGE call
			kv := iter.SeekGE(seekKey, base.SeekGEFlagsNone)
			if kv == nil {
				b.Fatal("Expected non-nil key-value from SeekGE")
			}

			iter.Close()
		}
	})
}

// setupBloomFilterData creates test data with bloom filter
func setupBloomFilterData(b *testing.B) (*Reader, []string) {
	mem := vfs.NewMem()
	f, err := mem.Create("bench", vfs.WriteCategoryUnspecified)
	require.NoError(b, err)

	w := NewWriter(objstorageprovider.NewFileWritable(f), WriterOptions{
		BlockSize:      512, // Smaller block size to force two-level index
		IndexBlockSize: 512, // Smaller index block size to force two-level index
		Comparer:       testkeys.Comparer,
		MergerName:     "nullptr",
		FilterPolicy:   bloom.FilterPolicy(10), // Enable bloom filter
		TableFormat:    TableFormatPebblev3,
	})

	// Generate enough keys to force two-level index creation
	ks := testkeys.Alpha(5) // Use 5-character alphabet for more keys
	var keys []string

	// Generate many more keys to ensure two-level index
	for i := range uint64(10000) {
		// Use decreasing timestamps since testkeys.Comparer sorts them in reverse
		timestamp := int64(10000 - i)
		key := testkeys.KeyAt(ks, i%ks.Count(), timestamp)
		keys = append(keys, string(key))
		require.NoError(b, w.Set(key, []byte("value")))
	}

	require.NoError(b, w.Close())

	// Read the file back
	f, err = mem.Open("bench")
	require.NoError(b, err)

	readable, err := objstorage.NewSimpleReadable(f)
	require.NoError(b, err)

	reader, err := NewReader(context.Background(), readable, ReaderOptions{
		Comparer: testkeys.Comparer,
	})
	require.NoError(b, err)

	// Verify we actually created a two-level index
	if !reader.Attributes.Has(AttributeTwoLevelIndex) {
		b.Fatalf("Bloom filter test data did not create two-level index")
	}

	return reader, keys
}

// BenchmarkTwoLevelIteratorSeekPrefixGE_NoHit measures construction + SeekPrefixGE with bloom filter miss
// How to: go test -bench=BenchmarkTwoLevelIteratorSeekPrefixGE_NoHit -run=^$ -count=10 ./sstable
func BenchmarkTwoLevelIteratorSeekPrefixGE_NoHit(b *testing.B) {
	b.Run("rowblk", func(b *testing.B) {
		reader, _ := setupBloomFilterData(b)
		defer reader.Close()

		opts := IterOptions{}
		// Use a prefix that doesn't exist to trigger bloom filter miss
		prefix := []byte("nonexistent")
		seekKey := []byte("nonexistent@1")

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			var iter Iterator
			var err error
			iter, err = newRowBlockTwoLevelIterator(context.Background(), reader, opts)
			require.NoError(b, err)

			// First SeekPrefixGE call - should be rejected by bloom filter
			kv := iter.SeekPrefixGE(prefix, seekKey, base.SeekGEFlagsNone)
			// Expect nil due to bloom filter rejection
			if kv != nil {
				b.Fatal("Expected nil key-value from SeekPrefixGE with non-existent prefix")
			}

			iter.Close()
		}
	})
}

// BenchmarkTwoLevelIteratorSeekPrefixGE_Hit measures construction + SeekPrefixGE with bloom filter hit
// How to: go test -bench=BenchmarkTwoLevelIteratorSeekPrefixGE_Hit -run=^$ -count=10 ./sstable
func BenchmarkTwoLevelIteratorSeekPrefixGE_Hit(b *testing.B) {
	b.Run("rowblk", func(b *testing.B) {
		reader, keys := setupBloomFilterData(b)
		defer reader.Close()

		opts := IterOptions{}
		// Use the first key from our generated keys to ensure it exists
		fullKey := []byte(keys[0])
		// Extract prefix using the comparer's Split method
		splitIndex := testkeys.Comparer.Split(fullKey)
		prefix := fullKey[:splitIndex]

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			var iter Iterator
			var err error
			iter, err = newRowBlockTwoLevelIterator(context.Background(), reader, opts)
			require.NoError(b, err)

			// First SeekPrefixGE call - should pass bloom filter and find data
			kv := iter.SeekPrefixGE(prefix, fullKey, base.SeekGEFlagsNone)
			if kv == nil {
				b.Fatalf("Expected non-nil key-value from SeekPrefixGE, prefix=%s, key=%s", string(prefix), string(fullKey))
			}

			iter.Close()
		}
	})
}

// BenchmarkTwoLevelLazyLoadingConstruction measures pure iterator construction overhead
// This validates the lazy loading benefit - construction should be faster since
// top-level index loading is deferred
func BenchmarkTwoLevelLazyLoadingConstruction(b *testing.B) {
	testCases := []struct {
		name      string
		blockSize int
		useBloom  bool
	}{
		{"rowblk-1024", 1024, false},
		{"rowblk-4096", 4096, false},
		{"rowblk-bloom", 1024, true},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			var reader *Reader
			if tc.useBloom {
				reader, _ = setupBloomFilterData(b)
			} else {
				reader, _ = setupTwoLevelBenchmarkData(b, tc.blockSize)
			}
			defer reader.Close()

			opts := IterOptions{}
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				iter, err := newRowBlockTwoLevelIterator(context.Background(), reader, opts)
				if err != nil {
					b.Fatal(err)
				}
				// Just construction, no access
				iter.Close()
			}
		})
	}
}

// BenchmarkTwoLevelLazyLoadingFirstAccess measures construction + first access
// This shows the total cost when lazy loading actually needs to load the index
func BenchmarkTwoLevelLazyLoadingFirstAccess(b *testing.B) {
	testCases := []struct {
		name      string
		blockSize int
		useBloom  bool
	}{
		{"rowblk-1024", 1024, false},
		{"rowblk-4096", 4096, false},
		{"rowblk-bloom", 1024, true},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			var reader *Reader
			if tc.useBloom {
				reader, _ = setupBloomFilterData(b)
			} else {
				reader, _ = setupTwoLevelBenchmarkData(b, tc.blockSize)
			}
			defer reader.Close()

			opts := IterOptions{}
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				iter, err := newRowBlockTwoLevelIterator(context.Background(), reader, opts)
				if err != nil {
					b.Fatal(err)
				}

				// First access triggers lazy loading
				kv := iter.First()
				if kv == nil {
					b.Fatal("Expected non-nil key-value from First()")
				}

				iter.Close()
			}
		})
	}
}

// BenchmarkTwoLevelLazyLoadingBloomFilterComparison measures the key benefit of lazy loading:
// bloom filter misses should be much faster than hits because no index loading is needed
func BenchmarkTwoLevelLazyLoadingBloomFilterComparison(b *testing.B) {
	reader, keys := setupBloomFilterData(b)
	defer reader.Close()

	// Get a valid prefix for hits
	fullKey := []byte(keys[0])
	splitIndex := testkeys.Comparer.Split(fullKey)
	validPrefix := fullKey[:splitIndex]

	testCases := []struct {
		name      string
		prefix    []byte
		key       []byte
		expectHit bool
	}{
		{"miss", []byte("nonexistent"), []byte("nonexistent@1"), false},
		{"hit", validPrefix, fullKey, true},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			opts := IterOptions{}
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				iter, err := newRowBlockTwoLevelIterator(context.Background(), reader, opts)
				if err != nil {
					b.Fatal(err)
				}

				kv := iter.SeekPrefixGE(tc.prefix, tc.key, base.SeekGEFlagsNone)
				if tc.expectHit && kv == nil {
					b.Fatal("Expected hit but got miss")
				}
				if !tc.expectHit && kv != nil {
					b.Fatal("Expected miss but got hit")
				}

				iter.Close()
			}
		})
	}
}

// BenchmarkTwoLevelLazyLoadingIteratorReuse measures the performance of reusing iterators
// This validates that lazy state is properly reset between uses
func BenchmarkTwoLevelLazyLoadingIteratorReuse(b *testing.B) {
	reader, keys := setupBloomFilterData(b)
	defer reader.Close()

	// Create a pool for testing reuse
	var pool sync.Pool
	pool.New = func() interface{} {
		return &twoLevelIteratorRowBlocks{pool: &pool}
	}

	opts := IterOptions{}
	seekKey := []byte(keys[len(keys)/2])

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Get iterator from pool (simulating reuse)
		iter := pool.Get().(*twoLevelIteratorRowBlocks)
		iter.secondLevel.init(context.Background(), reader, opts)
		iter.useFilterBlock = iter.secondLevel.useFilterBlock
		iter.secondLevel.useFilterBlock = false

		// Use the iterator
		kv := iter.SeekGE(seekKey, base.SeekGEFlagsNone)
		if kv == nil {
			b.Fatal("Expected non-nil key-value from SeekGE")
		}

		// Clean up and return to pool
		iter.Close()
		iter.secondLevel.resetForReuse()
		pool.Put(iter)
	}
}

// BenchmarkTwoLevelLazyLoadingConcurrentAccess measures concurrent iterator creation
// This validates thread safety and performance under concurrent load
func BenchmarkTwoLevelLazyLoadingConcurrentAccess(b *testing.B) {
	reader, keys := setupTwoLevelBenchmarkData(b, 1024)
	defer reader.Close()

	opts := IterOptions{}
	seekKey := []byte(keys[len(keys)/4])

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			iter, err := newRowBlockTwoLevelIterator(context.Background(), reader, opts)
			if err != nil {
				b.Fatal(err)
			}

			kv := iter.SeekGE(seekKey, base.SeekGEFlagsNone)
			if kv == nil {
				b.Fatal("Expected non-nil key-value from SeekGE")
			}

			iter.Close()
		}
	})
}

// BenchmarkTwoLevelLazyLoadingOperations measures various iterator operations
// to ensure lazy loading doesn't add overhead to normal operations
func BenchmarkTwoLevelLazyLoadingOperations(b *testing.B) {
	reader, keys := setupTwoLevelBenchmarkData(b, 1024)
	defer reader.Close()

	operations := []struct {
		name string
		fn   func(iter Iterator, keys [][]byte) *base.InternalKV
	}{
		{"First", func(iter Iterator, keys [][]byte) *base.InternalKV {
			return iter.First()
		}},
		{"Last", func(iter Iterator, keys [][]byte) *base.InternalKV {
			return iter.Last()
		}},
		{"SeekGE", func(iter Iterator, keys [][]byte) *base.InternalKV {
			return iter.SeekGE(keys[len(keys)/2], base.SeekGEFlagsNone)
		}},
		{"SeekLT", func(iter Iterator, keys [][]byte) *base.InternalKV {
			return iter.SeekLT(keys[len(keys)/2], base.SeekLTFlagsNone)
		}},
	}

	for _, op := range operations {
		b.Run(op.name, func(b *testing.B) {
			opts := IterOptions{}
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				iter, err := newRowBlockTwoLevelIterator(context.Background(), reader, opts)
				if err != nil {
					b.Fatal(err)
				}

				kv := op.fn(iter, keys)
				if kv == nil {
					b.Fatalf("Expected non-nil key-value from %s", op.name)
				}

				iter.Close()
			}
		})
	}
}

// BenchmarkTwoLevelLazyLoadingMemoryUsage measures memory allocation patterns
// This ensures lazy loading doesn't cause excessive allocations
func BenchmarkTwoLevelLazyLoadingMemoryUsage(b *testing.B) {
	reader, keys := setupTwoLevelBenchmarkData(b, 1024)
	defer reader.Close()

	testCases := []struct {
		name string
		fn   func(iter Iterator, keys [][]byte)
	}{
		{"construction-only", func(iter Iterator, keys [][]byte) {
			// Just construct, don't access
		}},
		{"single-access", func(iter Iterator, keys [][]byte) {
			iter.First()
		}},
		{"multiple-seeks", func(iter Iterator, keys [][]byte) {
			iter.SeekGE(keys[len(keys)/4], base.SeekGEFlagsNone)
			iter.SeekGE(keys[len(keys)/2], base.SeekGEFlagsNone)
			iter.SeekGE(keys[3*len(keys)/4], base.SeekGEFlagsNone)
		}},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			opts := IterOptions{}
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				iter, err := newRowBlockTwoLevelIterator(context.Background(), reader, opts)
				if err != nil {
					b.Fatal(err)
				}

				tc.fn(iter, keys)

				iter.Close()
			}
		})
	}
}

// BenchmarkTwoLevelLazyLoadingTableFormats compares performance across different table formats
func BenchmarkTwoLevelLazyLoadingTableFormats(b *testing.B) {
	formats := []struct {
		name   string
		format TableFormat
	}{
		{"v3", TableFormatPebblev3},
		{"v4", TableFormatPebblev4},
		{"v5", TableFormatPebblev5},
	}

	for _, format := range formats {
		b.Run(format.name, func(b *testing.B) {
			// Create test data with specific table format
			mem := vfs.NewMem()
			f, err := mem.Create("bench", vfs.WriteCategoryUnspecified)
			require.NoError(b, err)

			w := NewWriter(objstorageprovider.NewFileWritable(f), WriterOptions{
				BlockSize:      1024,
				IndexBlockSize: 1024,
				Comparer:       testkeys.Comparer,
				MergerName:     "nullptr",
				TableFormat:    format.format,
			})

			// Write enough keys to create two-level index
			ks := testkeys.Alpha(5)
			for i := range uint64(10000) {
				key := testkeys.Key(ks, i)
				require.NoError(b, w.Set(key, []byte(fmt.Sprintf("value-%d", i))))
			}
			require.NoError(b, w.Close())

			// Open reader
			f, err = mem.Open("bench")
			require.NoError(b, err)

			reader, err := newReader(f, ReaderOptions{
				Comparer: testkeys.Comparer,
			})
			require.NoError(b, err)
			defer reader.Close()

			if !reader.Attributes.Has(AttributeTwoLevelIndex) {
				b.Skip("Test data did not create two-level index")
			}

			opts := IterOptions{}
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				var iter Iterator
				var err error

				if format.format.BlockColumnar() {
					iter, err = newColumnBlockTwoLevelIterator(context.Background(), reader, opts)
				} else {
					iter, err = newRowBlockTwoLevelIterator(context.Background(), reader, opts)
				}
				if err != nil {
					b.Fatal(err)
				}

				kv := iter.First()
				if kv == nil {
					b.Fatal("Expected non-nil key-value from First()")
				}

				iter.Close()
			}
		})
	}
}
