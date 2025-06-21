// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"context"
	"testing"
)

// simulateEagerLoading creates an iterator and immediately loads the index
// to simulate what eager loading would have done
func simulateEagerLoading(r *Reader, opts IterOptions) (*singleLevelIteratorRowBlocks, error) {
	iter, err := newRowBlockSingleLevelIterator(context.Background(), r, opts)
	if err != nil {
		return nil, err
	}

	// Force index loading immediately (simulating eager loading)
	if err := iter.ensureIndexLoaded(); err != nil {
		iter.Close()
		return nil, err
	}

	return iter, nil
}

// BenchmarkEagerVsLazyComparison compares eager vs lazy loading performance
// How to: go test -bench=BenchmarkEagerVsLazyComparison -run=^$ ./sstable
func BenchmarkEagerVsLazyComparison(b *testing.B) {
	b.Run("LazyLoading_ConstructOnly", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			iter, err := newRowBlockSingleLevelIterator(context.Background(), globalReader, globalIterOpts)
			if err != nil {
				b.Fatal(err)
			}
			// Don't access index - pure construction cost
			iter.Close()
		}
	})

	b.Run("SimulatedEagerLoading_ConstructOnly", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			iter, err := simulateEagerLoading(globalReader, globalIterOpts)
			if err != nil {
				b.Fatal(err)
			}
			// Index was loaded during construction
			iter.Close()
		}
	})

	b.Run("LazyLoading_ConstructAndUse", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			iter, err := newRowBlockSingleLevelIterator(context.Background(), globalReader, globalIterOpts)
			if err != nil {
				b.Fatal(err)
			}
			// Use iterator (triggers lazy loading)
			_ = iter.First()
			iter.Close()
		}
	})

	b.Run("SimulatedEagerLoading_ConstructAndUse", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			iter, err := simulateEagerLoading(globalReader, globalIterOpts)
			if err != nil {
				b.Fatal(err)
			}
			// Use iterator (index already loaded)
			_ = iter.First()
			iter.Close()
		}
	})
}

// BenchmarkShortLivedIterators simulates scenarios where iterators are created but not used
// How to: go test -bench=BenchmarkShortLivedIterators -run=^$ ./sstable
func BenchmarkShortLivedIterators(b *testing.B) {
	b.Run("LazyLoading_ManyUnused", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Create 10 iterators but don't use them
			for range 10 {
				iter, err := newRowBlockSingleLevelIterator(context.Background(), globalReader, globalIterOpts)
				if err != nil {
					b.Fatal(err)
				}
				// Don't use - index never loaded
				iter.Close()
			}
		}
	})

	b.Run("SimulatedEagerLoading_ManyUnused", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Create 10 iterators with eager loading simulation
			for range 10 {
				iter, err := simulateEagerLoading(globalReader, globalIterOpts)
				if err != nil {
					b.Fatal(err)
				}
				// Don't use but index was already loaded
				iter.Close()
			}
		}
	})
}
