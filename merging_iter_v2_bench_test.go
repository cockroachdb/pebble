// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"math/rand/v2"
	"testing"

	"github.com/cockroachdb/pebble/cockroachkvs"
	"github.com/cockroachdb/pebble/internal/base"
)

func BenchmarkMergingIterV2Heap(b *testing.B) {
	// 8 is a typical number of levels: 6 levels plus one L0 sublevel plus a
	// memtable.
	const numLevels = 8

	type keyConfig struct {
		name string
		cfg  cockroachkvs.KeyGenConfig
	}
	configs := []keyConfig{
		{name: "short", cfg: cockroachkvs.KeyGenConfig{
			PrefixAlphabetLen: 8, RoachKeyLen: 10, PrefixLenShared: 2,
			AvgKeysPerPrefix: 2, BaseWallTime: 1e18,
		}},
		{name: "long", cfg: cockroachkvs.KeyGenConfig{
			PrefixAlphabetLen: 20, RoachKeyLen: 512, PrefixLenShared: 64,
			AvgKeysPerPrefix: 2, BaseWallTime: 1e18,
		}},
	}

	for _, tc := range configs {
		b.Run(tc.name, func(b *testing.B) {
			rng := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
			levels := make([]mergingIterV2Level, numLevels)
			for l := range levels {
				levels[l].index = l
			}

			// Init measures the time to initialize the heap.
			b.Run("Init", func(b *testing.B) {
				var heap mergingIterV2Heap
				heap.cmp = cockroachkvs.Comparer.Compare
				heap.items = make([]mergingIterV2HeapItem, 0, numLevels)

				const numKVs = 1024
				keys, _ := cockroachkvs.RandomKVs(rng, numKVs, tc.cfg, 0)
				rand.Shuffle(len(keys), func(i, j int) {
					keys[i], keys[j] = keys[j], keys[i]
				})
				levelKVs := make([]base.InternalKV, numLevels)
				p := 0
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					heap.Reset()
					for l := range levels {
						levelKVs[l].K = base.MakeInternalKey(keys[(p+l)%numKVs], 1, base.InternalKeyKindSet)
						p++
						levels[l].iterKV = &levelKVs[l]
						heap.Append(&levels[l])
					}
					p += numLevels
					heap.Init(+1)
				}
			})

			b.Run("FixTop", func(b *testing.B) {
				// Generate a shared pool of keys and distribute them to levels
				// with exponential bias (lower levels get more keys), mimicking
				// a real LSM.
				const totalKeys = 100_000
				allKeys, _ := cockroachkvs.RandomKVs(rng, totalKeys, tc.cfg, 0)

				var heap mergingIterV2Heap
				heap.cmp = cockroachkvs.Comparer.Compare
				heap.items = make([]mergingIterV2HeapItem, 0, numLevels)

				b.ResetTimer()
				for done := 0; done < b.N; {
					b.StopTimer()
					levelKVs := make([][]base.InternalKV, numLevels)
					for _, k := range allKeys {
						// Each level has ~4x more keys than the next level.
						l := 0
						for ; l < numLevels-1 && rng.IntN(4) == 1; l++ {
						}
						levelKVs[l] = append(levelKVs[l], base.InternalKV{
							K: base.MakeInternalKey(k, 1, base.InternalKeyKindSet),
						})
					}
					pos := make([]int, numLevels)
					heap.Reset()
					for l := range levels {
						if len(levelKVs[l]) == 0 {
							continue
						}
						levels[l].iterKV = &levelKVs[l][0]
						heap.Append(&levels[l])
					}
					heap.Init(+1)
					b.StartTimer()

					for heap.Len() > 0 && done < b.N {
						top := heap.Top()
						l := top.index
						pos[l]++
						if pos[l] >= len(levelKVs[l]) {
							heap.PopTop()
							top.iterKV = nil
						} else {
							top.iterKV = &levelKVs[l][pos[l]]
							heap.FixTop()
						}
						done++
					}
				}
			})
		})
	}
}
