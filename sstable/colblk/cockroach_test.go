// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk_test

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/crdbtest"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/colblk"
	"github.com/stretchr/testify/require"
)

func TestCockroachDataBlock(t *testing.T) {
	const targetBlockSize = 32 << 10
	const valueLen = 100
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))
	serializedBlock, keys, values := generateDataBlock(rng, targetBlockSize, crdbtest.KeyConfig{
		PrefixAlphabetLen: 26,
		PrefixLen:         12,
		AvgKeysPerPrefix:  2,
		BaseWallTime:      seed,
	}, valueLen)

	var decoder colblk.DataBlockDecoder
	var it colblk.DataBlockIter
	it.InitOnce(crdbtest.KeySchema, crdbtest.Compare, crdbtest.Split, getLazyValuer(func([]byte) base.LazyValue {
		return base.LazyValue{ValueOrHandle: []byte("mock external value")}
	}))
	decoder.Init(crdbtest.KeySchema, serializedBlock)
	if err := it.Init(&decoder, block.IterTransforms{}); err != nil {
		t.Fatal(err)
	}

	t.Run("Next", func(t *testing.T) {
		// Scan the block using Next and ensure that all the keys values match.
		i := 0
		for kv := it.First(); kv != nil; i, kv = i+1, it.Next() {
			if !bytes.Equal(kv.K.UserKey, keys[i]) {
				t.Fatalf("expected %q, but found %q", keys[i], kv.K.UserKey)
			}
			if !bytes.Equal(kv.V.InPlaceValue(), values[i]) {
				t.Fatalf("expected %x, but found %x", values[i], kv.V.InPlaceValue())
			}
		}
		require.Equal(t, len(keys), i)
	})
	t.Run("SeekGE", func(t *testing.T) {
		rng := rand.New(rand.NewPCG(0, seed))
		for _, i := range rng.Perm(len(keys)) {
			kv := it.SeekGE(keys[i], base.SeekGEFlagsNone)
			if kv == nil {
				t.Fatalf("%q not found", keys[i])
			}
			if !bytes.Equal(kv.V.InPlaceValue(), values[i]) {
				t.Fatalf(
					"expected:\n    %x\nfound:\n    %x\nquery key:\n    %x\nreturned key:\n    %x",
					values[i], kv.V.InPlaceValue(), keys[i], kv.K.UserKey)
			}
		}
	})
}

// generateDataBlock writes out a random cockroach data block using the given
// parameters. Returns the serialized block data and the keys and values
// written.
func generateDataBlock(
	rng *rand.Rand, targetBlockSize int, cfg crdbtest.KeyConfig, valueLen int,
) (data []byte, keys [][]byte, values [][]byte) {
	keys, values = crdbtest.RandomKVs(rng, targetBlockSize/valueLen, cfg, valueLen)

	var w colblk.DataBlockEncoder
	w.Init(crdbtest.KeySchema)
	count := 0
	for w.Size() < targetBlockSize {
		ik := base.MakeInternalKey(keys[count], base.SeqNum(rng.Uint64N(uint64(base.SeqNumMax))), base.InternalKeyKindSet)
		kcmp := w.KeyWriter.ComparePrev(ik.UserKey)
		w.Add(ik, values[count], block.InPlaceValuePrefix(kcmp.PrefixEqual()), kcmp, false /* isObsolete */)
		count++
	}
	data, _ = w.Finish(w.Rows(), w.Size())
	return data, keys[:count], values[:count]
}

func BenchmarkCockroachDataBlockWriter(b *testing.B) {
	for _, alphaLen := range []int{4, 8, 26} {
		for _, lenSharedPct := range []float64{0.25, 0.5} {
			for _, prefixLen := range []int{8, 32, 128} {
				lenShared := int(float64(prefixLen) * lenSharedPct)
				for _, valueLen := range []int{8, 128, 1024} {
					keyConfig := crdbtest.KeyConfig{
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

func benchmarkCockroachDataBlockWriter(b *testing.B, keyConfig crdbtest.KeyConfig, valueLen int) {
	const targetBlockSize = 32 << 10
	seed := uint64(time.Now().UnixNano())
	rng := rand.New(rand.NewPCG(0, seed))
	_, keys, values := generateDataBlock(rng, targetBlockSize, keyConfig, valueLen)

	var w colblk.DataBlockEncoder
	w.Init(crdbtest.KeySchema)

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
								KeyConfig: crdbtest.KeyConfig{
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
		KeyConfig: crdbtest.KeyConfig{
			PrefixAlphabetLen: 8,
			PrefixLen:         8,
			PrefixLenShared:   4,
			AvgKeysPerPrefix:  4,
			PercentLogical:    10,
		},
		ValueLen: 8,
	},
	{
		KeyConfig: crdbtest.KeyConfig{
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
				SyntheticPrefix: []byte("prefix_"),
			},
		},
		{
			description: "SyntheticSuffix",
			transforms: block.IterTransforms{
				SyntheticSuffix: crdbtest.EncodeTimestamp(make([]byte, 0, 20), 1_000_000_000_000, 0)[1:],
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
	crdbtest.KeyConfig
	ValueLen int
}

func (cfg benchConfig) String() string {
	return fmt.Sprintf("%s,ValueLen=%d", cfg.KeyConfig, cfg.ValueLen)
}

func benchmarkCockroachDataBlockIter(
	b *testing.B, cfg benchConfig, transforms block.IterTransforms,
) {
	const targetBlockSize = 32 << 10
	seed := uint64(time.Now().UnixNano())
	rng := rand.New(rand.NewPCG(0, seed))
	cfg.BaseWallTime = seed

	serializedBlock, keys, _ := generateDataBlock(rng, targetBlockSize, cfg.KeyConfig, cfg.ValueLen)

	var decoder colblk.DataBlockDecoder
	var it colblk.DataBlockIter
	it.InitOnce(crdbtest.KeySchema, crdbtest.Compare, crdbtest.Split, getLazyValuer(func([]byte) base.LazyValue {
		return base.LazyValue{ValueOrHandle: []byte("mock external value")}
	}))
	decoder.Init(crdbtest.KeySchema, serializedBlock)
	if err := it.Init(&decoder, transforms); err != nil {
		b.Fatal(err)
	}
	avgRowSize := float64(len(serializedBlock)) / float64(len(keys))

	if transforms.SyntheticPrefix.IsSet() {
		for i := range keys {
			keys[i] = slices.Concat(transforms.SyntheticPrefix, keys[i])
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
			queryKeys := crdbtest.RandomQueryKeys(rng, numQueryKeys, keys, baseWallTime)
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

type getLazyValuer func([]byte) base.LazyValue

func (g getLazyValuer) GetLazyValueForPrefixAndValueHandle(handle []byte) base.LazyValue {
	return g(handle)
}
