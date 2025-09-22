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
)

var (
	benchReader   *Reader
	benchIterOpts IterOptions
)

func init() {
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

	benchReader, err = newReader(f2, ReaderOptions{
		Comparer: base.DefaultComparer,
		Merger:   base.DefaultMerger,
	})
	if err != nil {
		panic(err)
	}

	var stats base.InternalIteratorStats
	var bufferPool block.BufferPool
	bufferPool.Init(5, block.ForCompaction)

	benchIterOpts = IterOptions{
		Transforms:           NoTransforms,
		FilterBlockSizeLimit: NeverUseFilterBlock,
		Env:                  ReadEnv{Block: block.ReadEnv{Stats: &stats, BufferPool: &bufferPool}},
		ReaderProvider:       MakeTrivialReaderProvider(benchReader),
	}
}

// BenchmarkIteratorConstruction measures pure iterator construction performance
// How to: go test -bench=BenchmarkIteratorConstruction -run=^$ ./sstable
func BenchmarkIteratorConstruction(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter, err := newRowBlockSingleLevelIterator(context.Background(), benchReader, benchIterOpts)
		if err != nil {
			b.Fatal(err)
		}
		iter.Close()
	}
}

// BenchmarkIteratorFirst measures first-access First() call performance
// How to: go test -bench=BenchmarkIteratorFirst -run=^$ ./sstable
func BenchmarkIteratorFirst(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		iter, err := newRowBlockSingleLevelIterator(context.Background(), benchReader, benchIterOpts)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		_ = iter.First() // Only this is timed
		b.StopTimer()
		iter.Close()
	}
}

// BenchmarkIteratorSeekGE measures first-access SeekGE() call performance
// How to: go test -bench=BenchmarkIteratorSeekGE -run=^$ ./sstable
func BenchmarkIteratorSeekGE(b *testing.B) {
	key := []byte("key00005000") // Middle key
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		iter, err := newRowBlockSingleLevelIterator(context.Background(), benchReader, benchIterOpts)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		_ = iter.SeekGE(key, base.SeekGEFlagsNone) // Only this is timed
		b.StopTimer()
		iter.Close()
	}
}

// BenchmarkIteratorSeekPrefixGE_Hit measures first-access SeekPrefixGE() call performance with existing prefix
// How to: go test -bench=BenchmarkIteratorSeekPrefixGE_Hit -run=^$ ./sstable
func BenchmarkIteratorSeekPrefixGE_Hit(b *testing.B) {
	// Setup iterator options with bloom filter enabled
	var stats base.InternalIteratorStats
	var bufferPool block.BufferPool
	bufferPool.Init(5, block.ForCompaction)
	defer bufferPool.Release()

	iterOpts := IterOptions{
		Transforms:           NoTransforms,
		FilterBlockSizeLimit: AlwaysUseFilterBlock, // Enable bloom filter
		Env:                  ReadEnv{Block: block.ReadEnv{Stats: &stats, BufferPool: &bufferPool}},
		ReaderProvider:       MakeTrivialReaderProvider(benchReader),
	}

	// Use existing prefix that will be found
	prefix := []byte("key00005000")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		iter, err := newRowBlockSingleLevelIterator(context.Background(), benchReader, iterOpts)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		_ = iter.SeekPrefixGE(prefix, prefix, base.SeekGEFlagsNone) // Only this is timed
		b.StopTimer()
		iter.Close()
	}
}

// BenchmarkIteratorSeekPrefixGE_NoHit measures first-access SeekPrefixGE() call performance with non-existing prefix
// How to: go test -bench=BenchmarkIteratorSeekPrefixGE_NoHit -run=^$ ./sstable
func BenchmarkIteratorSeekPrefixGE_NoHit(b *testing.B) {
	var stats base.InternalIteratorStats
	var bufferPool block.BufferPool
	bufferPool.Init(5, block.ForCompaction)
	defer bufferPool.Release()

	iterOpts := IterOptions{
		Transforms:           NoTransforms,
		FilterBlockSizeLimit: AlwaysUseFilterBlock, // Enable bloom filter
		Env:                  ReadEnv{Block: block.ReadEnv{Stats: &stats, BufferPool: &bufferPool}},
		ReaderProvider:       MakeTrivialReaderProvider(benchReader),
	}

	// Use non-existing prefix that will be rejected by bloom filter
	prefix := []byte("notfound")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		iter, err := newRowBlockSingleLevelIterator(context.Background(), benchReader, iterOpts)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		_ = iter.SeekPrefixGE(prefix, prefix, base.SeekGEFlagsNone) // Only this is timed
		b.StopTimer()
		iter.Close()
	}
}
