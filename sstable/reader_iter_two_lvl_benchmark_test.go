// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
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
		key := testkeys.Key(ks, int64(i))
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
	for i := range int64(10000) {
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

	readable, err := NewSimpleReadable(f)
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
