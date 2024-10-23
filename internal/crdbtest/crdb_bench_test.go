// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package crdbtest

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/sstableinternal"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/colblk"
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
	keyCfg := keyGenConfig{
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
	ctx := context.Background()
	readerOpts := sstable.ReaderOptions{
		Comparer:   writerOpts.Comparer,
		KeySchemas: sstable.MakeKeySchemas(&KeySchema),
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
		&stats, nil, rp)
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
			&stats, nil, rp)
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

func BenchmarkCockroachDataBlockWriter(b *testing.B) {
	for _, alphaLen := range []int{4, 8, 26} {
		for _, lenSharedPct := range []float64{0.25, 0.5} {
			for _, prefixLen := range []int{8, 32, 128} {
				lenShared := int(float64(prefixLen) * lenSharedPct)
				for _, valueLen := range []int{8, 128, 1024} {
					keyConfig := keyGenConfig{
						PrefixAlphabetLen: alphaLen,
						PrefixLen:         prefixLen,
						PrefixLenShared:   lenShared,
						PercentLogical:    0,
						AvgKeysPerPrefix:  2,
						BaseWallTime:      uint64(time.Now().UnixNano()),
					}
					b.Run(fmt.Sprintf("%s,valueLen=%d", keyConfig, valueLen), func(b *testing.B) {
						benchmarkCockroachDataBlockWriter(b, keyConfig, valueLen)
					})
				}
			}
		}
	}
}

func benchmarkCockroachDataBlockWriter(b *testing.B, keyConfig keyGenConfig, valueLen int) {
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

func BenchmarkCockroachDataBlockIterFull(b *testing.B) {
	for _, alphaLen := range []int{4, 8, 26} {
		for _, lenSharedPct := range []float64{0.25, 0.5} {
			for _, prefixLen := range []int{8, 32, 128} {
				lenShared := int(float64(prefixLen) * lenSharedPct)
				for _, avgKeysPerPrefix := range []int{1, 10, 100} {
					for _, percentLogical := range []int{0, 50} {
						for _, valueLen := range []int{8, 128, 1024} {
							cfg := benchConfig{
								keyGenConfig: keyGenConfig{
									PrefixAlphabetLen: alphaLen,
									PrefixLen:         prefixLen,
									PrefixLenShared:   lenShared,
									AvgKeysPerPrefix:  avgKeysPerPrefix,
									PercentLogical:    percentLogical,
								},
								ValueLen: valueLen,
							}
							b.Run(cfg.String(), func(b *testing.B) {
								benchmarkCockroachDataBlockIter(b, cfg, block.IterTransforms{})
							})
						}
					}
				}
			}
		}
	}
}

var shortBenchConfigs = []benchConfig{
	{
		keyGenConfig: keyGenConfig{
			PrefixAlphabetLen: 8,
			PrefixLen:         8,
			PrefixLenShared:   4,
			AvgKeysPerPrefix:  4,
			PercentLogical:    10,
		},
		ValueLen: 8,
	},
	{
		keyGenConfig: keyGenConfig{
			PrefixAlphabetLen: 8,
			PrefixLen:         128,
			PrefixLenShared:   64,
			AvgKeysPerPrefix:  4,
			PercentLogical:    10,
		},
		ValueLen: 128,
	},
}

func BenchmarkCockroachDataBlockIterShort(b *testing.B) {
	for _, cfg := range shortBenchConfigs {
		b.Run(cfg.String(), func(b *testing.B) {
			benchmarkCockroachDataBlockIter(b, cfg, block.IterTransforms{})
		})
	}
}

func BenchmarkCockroachDataBlockIterTransforms(b *testing.B) {
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
	for _, cfg := range shortBenchConfigs {
		for _, t := range transforms {
			name := cfg.String() + crstrings.If(t.description != "", ","+t.description)
			b.Run(name, func(b *testing.B) {
				benchmarkCockroachDataBlockIter(b, cfg, t.transforms)
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

func benchmarkCockroachDataBlockIter(
	b *testing.B, cfg benchConfig, transforms block.IterTransforms,
) {
	const targetBlockSize = 32 << 10
	seed := uint64(time.Now().UnixNano())
	rng := rand.New(rand.NewPCG(0, seed))
	cfg.BaseWallTime = seed

	serializedBlock, keys, _ := generateDataBlock(rng, targetBlockSize, cfg.keyGenConfig, cfg.ValueLen)

	var decoder colblk.DataBlockDecoder
	var it colblk.DataBlockIter
	it.InitOnce(&KeySchema, Compare, Split, getLazyValuer(func([]byte) base.LazyValue {
		return base.LazyValue{ValueOrHandle: []byte("mock external value")}
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
