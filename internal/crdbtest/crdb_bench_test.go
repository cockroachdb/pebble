// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package crdbtest

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/sstableinternal"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/stretchr/testify/require"
)

func BenchmarkRandSeekInSST(b *testing.B) {
	configs := []struct {
		name     string
		numKeys  int
		valueLen int
		version  sstable.TableFormat
	}{
		{
			name:     "v4/single-level",
			numKeys:  200 * 100, // ~100 data blocks.
			valueLen: 128,       // ~200 KVs per data block
			version:  sstable.TableFormatPebblev4,
		},
		{
			name:     "v4/two-level",
			numKeys:  200 * 5000, // ~5000 data blocks
			valueLen: 128,        // ~200 KVs per data block
			version:  sstable.TableFormatPebblev4,
		},
		{
			name:     "v5/single-level",
			numKeys:  200 * 100, // ~100 data blocks.
			valueLen: 128,       // ~200 KVs per data block
			version:  sstable.TableFormatPebblev5,
		},
		{
			name:     "v5/two-level",
			numKeys:  200 * 5000, // ~5000 data blocks
			valueLen: 128,        // ~200 KVs per data block
			version:  sstable.TableFormatPebblev5,
		},
	}
	keyCfg := KeyConfig{
		PrefixAlphabetLen: 26,
		PrefixLen:         12,
		PrefixLenShared:   4,
		AvgKeysPerPrefix:  1,
		BaseWallTime:      uint64(time.Now().UnixNano()),
	}
	rng := rand.New(rand.NewPCG(0, rand.Uint64()))
	for _, cfg := range configs {
		o := sstable.WriterOptions{
			BlockSize:            32 * 1024,
			IndexBlockSize:       128 * 1024,
			AllocatorSizeClasses: sstable.JemallocSizeClasses,
			TableFormat:          cfg.version,
			Comparer:             &Comparer,
			KeySchema:            KeySchema,
		}
		b.Run(cfg.name, func(b *testing.B) {
			benchmarkRandSeekInSST(b, rng, cfg.numKeys, keyCfg, cfg.valueLen, o)
		})

	}
}

func benchmarkRandSeekInSST(
	b *testing.B,
	rng *rand.Rand,
	numKeys int,
	keyCfg KeyConfig,
	valueLen int,
	writerOpts sstable.WriterOptions,
) {
	keys, values := RandomKVs(rng, numKeys, keyCfg, valueLen)
	obj := &objstorage.MemObj{}
	w := sstable.NewWriter(obj, writerOpts)
	for i := range keys {
		require.NoError(b, w.Set(keys[i], values[i]))
	}
	require.NoError(b, w.Close())
	// Make the cache twice the size of the object, to allow for decompression and
	// overhead.
	c := cache.New(obj.Size() * 2)
	defer c.Unref()
	ctx := context.Background()
	readerOpts := sstable.ReaderOptions{
		Comparer:   writerOpts.Comparer,
		KeySchemas: sstable.MakeKeySchemas(KeySchema),
	}
	readerOpts.SetInternalCacheOpts(sstableinternal.CacheOptions{
		Cache:   c,
		CacheID: c.NewID(),
		FileNum: 1,
	})
	reader, err := sstable.NewReader(ctx, obj, readerOpts)
	require.NoError(b, err)
	defer reader.Close()

	// Iterate through the entire table to warm up the cache.
	var stats base.InternalIteratorStats
	rp := sstable.MakeTrivialReaderProvider(reader)
	iter, err := reader.NewPointIter(
		ctx, sstable.NoTransforms, nil, nil, nil, sstable.NeverUseFilterBlock,
		&stats, sstable.CategoryAndQoS{}, nil, rp)
	require.NoError(b, err)
	n := 0
	for kv := iter.First(); kv != nil; kv = iter.Next() {
		n++
	}
	require.Equal(b, len(keys), n)
	require.NoError(b, iter.Close())

	const numQueryKeys = 65536
	queryKeys := RandomQueryKeys(rng, numQueryKeys, keys, keyCfg.BaseWallTime)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := queryKeys[i%numQueryKeys]
		iter, err := reader.NewPointIter(
			ctx, sstable.NoTransforms, nil, nil, nil, sstable.NeverUseFilterBlock,
			&stats, sstable.CategoryAndQoS{}, nil, rp)
		if err != nil {
			b.Fatal(err)
		}
		iter.SeekGE(key, base.SeekGEFlagsNone)
		if err := iter.Close(); err != nil {
			b.Fatal(err)
		}
	}
	// Stop the timer before any deferred cleanup.
	b.StopTimer()
}
