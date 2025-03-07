// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package cockroachkvs

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/crlib/testutils/require"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/sstableinternal"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/colblk"
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
		{
			name:     "v6/single-level",
			numKeys:  200 * 100, // ~100 data blocks.
			valueLen: 128,       // ~200 KVs per data block
			version:  sstable.TableFormatPebblev6,
		},
		{
			name:     "v6/two-level",
			numKeys:  200 * 5000, // ~5000 data blocks
			valueLen: 128,        // ~200 KVs per data block
			version:  sstable.TableFormatPebblev6,
		},
	}
	keyCfg := keyGenConfig{
		PrefixAlphabetLen: 26,
		RoachKeyLen:       12,
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
			KeySchema:            &KeySchema,
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
	keyCfg keyGenConfig,
	valueLen int,
	writerOpts sstable.WriterOptions,
) {
	keys, values := randomKVs(rng, numKeys, keyCfg, valueLen)
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
	ch := c.NewHandle()
	defer ch.Close()
	ctx := context.Background()
	readerOpts := sstable.ReaderOptions{
		ReaderOptions: block.ReaderOptions{
			CacheOpts: sstableinternal.CacheOptions{
				CacheHandle: ch,
				FileNum:     1,
			},
		},
		Comparer:   writerOpts.Comparer,
		KeySchemas: sstable.MakeKeySchemas(&KeySchema),
	}
	reader, err := sstable.NewReader(ctx, obj, readerOpts)
	require.NoError(b, err)
	defer reader.Close()

	// Iterate through the entire table to warm up the cache.
	var stats base.InternalIteratorStats
	rp := sstable.MakeTrivialReaderProvider(reader)
	iter, err := reader.NewPointIter(
		ctx, sstable.NoTransforms, nil, nil, nil, sstable.NeverUseFilterBlock,
		sstable.ReadEnv{Block: block.ReadEnv{Stats: &stats, IterStats: nil}}, rp)
	require.NoError(b, err)
	n := 0
	for kv := iter.First(); kv != nil; kv = iter.Next() {
		n++
	}
	require.Equal(b, len(keys), n)
	require.NoError(b, iter.Close())

	const numQueryKeys = 65536
	queryKeys := randomQueryKeys(rng, numQueryKeys, keys, keyCfg.BaseWallTime)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := queryKeys[i%numQueryKeys]
		iter, err := reader.NewPointIter(
			ctx, sstable.NoTransforms, nil, nil, nil, sstable.NeverUseFilterBlock,
			sstable.ReadEnv{Block: block.ReadEnv{Stats: &stats, IterStats: nil}}, rp)
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

func BenchmarkCockroachDataColBlockWriter(b *testing.B) {
	for _, cfg := range benchConfigs {
		b.Run(cfg.String(), func(b *testing.B) {
			benchmarkCockroachDataColBlockWriter(b, cfg.keyGenConfig, cfg.ValueLen)
		})
	}
}

func benchmarkCockroachDataColBlockWriter(b *testing.B, keyConfig keyGenConfig, valueLen int) {
	const targetBlockSize = 32 << 10
	seed := uint64(time.Now().UnixNano())
	rng := rand.New(rand.NewPCG(0, seed))
	_, keys, values := generateDataBlock(rng, targetBlockSize, keyConfig, valueLen)

	var w colblk.DataBlockEncoder
	w.Init(&KeySchema)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.Reset()
		var count int
		for w.Size() < targetBlockSize {
			ik := base.MakeInternalKey(keys[count], base.SeqNum(rng.Uint64N(uint64(base.SeqNumMax))), base.InternalKeyKindSet)
			kcmp := w.KeyWriter.ComparePrev(ik.UserKey)
			w.Add(ik, values[count], block.InPlaceValuePrefix(kcmp.PrefixEqual()), kcmp, false /* isObsolete */)
			count++
		}
		_, _ = w.Finish(w.Rows(), w.Size())
	}
}

var benchConfigs = []benchConfig{
	{
		keyGenConfig: keyGenConfig{
			PrefixAlphabetLen: 8,
			RoachKeyLen:       8,
			PrefixLenShared:   4,
			AvgKeysPerPrefix:  4,
			PercentLogical:    10,
		},
		ValueLen: 8,
	},
	{
		keyGenConfig: keyGenConfig{
			PrefixAlphabetLen: 8,
			RoachKeyLen:       128,
			PrefixLenShared:   64,
			AvgKeysPerPrefix:  4,
			PercentLogical:    50,
		},
		ValueLen: 128,
	},
	{
		keyGenConfig: keyGenConfig{
			PrefixAlphabetLen: 26,
			RoachKeyLen:       1024,
			PrefixLenShared:   512,
			AvgKeysPerPrefix:  1,
			PercentLogical:    0,
		},
		ValueLen: 1024,
	},
}

func BenchmarkCockroachDataColBlockIter(b *testing.B) {
	for _, cfg := range benchConfigs {
		b.Run(cfg.String(), func(b *testing.B) {
			benchmarkCockroachDataColBlockIter(b, cfg, block.IterTransforms{})
		})
	}
}

func BenchmarkCockroachDataColBlockIterTransforms(b *testing.B) {
	transforms := []struct {
		description string
		transforms  block.IterTransforms
	}{
		{},
		{
			description: "SynthSeqNum",
			transforms: block.IterTransforms{
				SyntheticSeqNum: 1234,
			},
		},
		{
			description: "HideObsolete",
			transforms: block.IterTransforms{
				HideObsoletePoints: true,
			},
		},
		{
			description: "SyntheticPrefix",
			transforms: block.IterTransforms{
				SyntheticPrefixAndSuffix: block.MakeSyntheticPrefixAndSuffix([]byte("prefix_"), nil),
			},
		},
		{
			description: "SyntheticSuffix",
			transforms: block.IterTransforms{
				SyntheticPrefixAndSuffix: block.MakeSyntheticPrefixAndSuffix(
					nil,
					EncodeTimestamp(make([]byte, 0, 20), 1_000_000_000_000, 0)[1:],
				),
			},
		},
	}
	for _, cfg := range benchConfigs {
		for _, t := range transforms {
			name := cfg.String() + crstrings.If(t.description != "", ","+t.description)
			b.Run(name, func(b *testing.B) {
				benchmarkCockroachDataColBlockIter(b, cfg, t.transforms)
			})
		}
	}
}

type benchConfig struct {
	keyGenConfig
	ValueLen int
}

func (cfg benchConfig) String() string {
	return fmt.Sprintf("%s,ValueLen=%d", cfg.keyGenConfig, cfg.ValueLen)
}

func benchmarkCockroachDataColBlockIter(
	b *testing.B, cfg benchConfig, transforms block.IterTransforms,
) {
	const targetBlockSize = 32 << 10
	seed := uint64(time.Now().UnixNano())
	rng := rand.New(rand.NewPCG(0, seed))
	cfg.BaseWallTime = seed

	serializedBlock, keys, _ := generateDataBlock(rng, targetBlockSize, cfg.keyGenConfig, cfg.ValueLen)

	var decoder colblk.DataBlockDecoder
	var it colblk.DataBlockIter
	it.InitOnce(&KeySchema, &Comparer, getInternalValuer(func([]byte) base.InternalValue {
		return base.MakeInPlaceValue([]byte("mock external value"))
	}))
	decoder.Init(&KeySchema, serializedBlock)
	if err := it.Init(&decoder, transforms); err != nil {
		b.Fatal(err)
	}
	avgRowSize := float64(len(serializedBlock)) / float64(len(keys))

	if transforms.HasSyntheticPrefix() {
		for i := range keys {
			keys[i] = slices.Concat(transforms.SyntheticPrefix(), keys[i])
		}
	}

	b.Run("Next", func(b *testing.B) {
		kv := it.First()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if kv == nil {
				kv = it.First()
			} else {
				kv = it.Next()
			}
		}
		b.StopTimer()
		b.ReportMetric(avgRowSize, "bytes/row")
	})
	for _, queryLatest := range []bool{false, true} {
		b.Run("SeekGE"+crstrings.If(queryLatest, "Latest"), func(b *testing.B) {
			rng := rand.New(rand.NewPCG(1, seed))
			const numQueryKeys = 65536
			baseWallTime := cfg.BaseWallTime
			if queryLatest {
				baseWallTime += 24 * uint64(time.Hour)
			}
			queryKeys := randomQueryKeys(rng, numQueryKeys, keys, baseWallTime)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				k := queryKeys[i%numQueryKeys]
				if kv := it.SeekGE(k, base.SeekGEFlagsNone); kv == nil {
					// SeekGE should always end up finding a key if we are querying for the
					// latest version of each prefix and we are not hiding any points.
					if queryLatest && !transforms.HideObsoletePoints {
						b.Fatalf("%q not found", k)
					}
				}
			}
			b.StopTimer()
			b.ReportMetric(avgRowSize, "bytes/row")
		})
	}
}
