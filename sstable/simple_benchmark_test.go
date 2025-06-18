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

var globalReader *Reader
var globalIterOpts IterOptions

func init() {
	// Setup global test data once
	mem := vfs.NewMem()
	f, err := mem.Create("bench.sst", vfs.WriteCategoryUnspecified)
	if err != nil {
		panic(err)
	}

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
		if err := w.Set(key, []byte(value)); err != nil {
			panic(err)
		}
	}
	if err := w.Close(); err != nil {
		panic(err)
	}

	f2, err := mem.Open("bench.sst")
	if err != nil {
		panic(err)
	}

	globalReader, err = newReader(f2, ReaderOptions{
		Comparer: base.DefaultComparer,
		Merger:   base.DefaultMerger,
	})
	if err != nil {
		panic(err)
	}

	var stats base.InternalIteratorStats
	var bufferPool block.BufferPool
	bufferPool.Init(5)

	globalIterOpts = IterOptions{
		Transforms:           NoTransforms,
		FilterBlockSizeLimit: NeverUseFilterBlock,
		Env:                  ReadEnv{Block: block.ReadEnv{Stats: &stats, BufferPool: &bufferPool}},
		ReaderProvider:       MakeTrivialReaderProvider(globalReader),
	}
}

// BenchmarkSimpleIteratorConstruction measures pure construction performance
// How to: go test -bench=BenchmarkSimpleIteratorConstruction -run=^$ ./sstable
func BenchmarkSimpleIteratorConstruction(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter, err := newRowBlockSingleLevelIterator(context.Background(), globalReader, globalIterOpts)
		if err != nil {
			b.Fatal(err)
		}
		// Don't access index - just construct and close
		iter.Close()
	}
}

// BenchmarkConstructionPlusFirstAccess measures construction + first index access
// How to: go test -bench=BenchmarkConstructionPlusFirstAccess -run=^$ ./sstable
func BenchmarkConstructionPlusFirstAccess(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter, err := newRowBlockSingleLevelIterator(context.Background(), globalReader, globalIterOpts)
		if err != nil {
			b.Fatal(err)
		}
		// Trigger index loading
		_ = iter.First()
		iter.Close()
	}
}

// BenchmarkConstructionPlusSeek measures construction + seek (which loads index)
// How to: go test -bench=BenchmarkConstructionPlusSeek -run=^$ ./sstable
func BenchmarkConstructionPlusSeek(b *testing.B) {
	key := []byte("key00005000")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter, err := newRowBlockSingleLevelIterator(context.Background(), globalReader, globalIterOpts)
		if err != nil {
			b.Fatal(err)
		}
		// Trigger index loading
		_ = iter.SeekGE(key, base.SeekGEFlagsNone)
		iter.Close()
	}
}

// TestSimpleLazyLoading verifies lazy loading behavior
func TestSimpleLazyLoading(t *testing.T) {
	iter, err := newRowBlockSingleLevelIterator(context.Background(), globalReader, globalIterOpts)
	require.NoError(t, err)
	defer iter.Close()

	// Index should not be loaded on construction
	require.False(t, iter.indexLoaded, "Index should not be loaded on construction")

	// Trigger index loading
	_ = iter.First()

	// Index should now be loaded
	require.True(t, iter.indexLoaded, "Index should be loaded after first access")
}
