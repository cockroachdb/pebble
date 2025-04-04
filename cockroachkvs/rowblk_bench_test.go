// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package cockroachkvs

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/rowblk"
)

func BenchmarkCockroachDataRowBlockWriter(b *testing.B) {
	for _, alphaLen := range []int{4, 8, 26} {
		for _, lenSharedPct := range []float64{0.25, 0.5} {
			for _, roachKeyLen := range []int{8, 32, 128} {
				lenShared := int(float64(roachKeyLen) * lenSharedPct)
				for _, valueLen := range []int{8, 128, 1024} {
					keyConfig := KeyGenConfig{
						PrefixAlphabetLen: alphaLen,
						RoachKeyLen:       roachKeyLen,
						PrefixLenShared:   lenShared,
						AvgKeysPerPrefix:  2,
						PercentLogical:    0,
						BaseWallTime:      uint64(time.Now().UnixNano()),
					}
					b.Run(fmt.Sprintf("%s,valueLen=%d", keyConfig, valueLen), func(b *testing.B) {
						benchmarkCockroachDataRowBlockWriter(b, keyConfig, valueLen)
					})
				}
			}
		}
	}
}

func benchmarkCockroachDataRowBlockWriter(b *testing.B, keyConfig KeyGenConfig, valueLen int) {
	const targetBlockSize = 32 << 10
	seed := uint64(time.Now().UnixNano())
	rng := rand.New(rand.NewPCG(0, seed))
	keys, values := RandomKVs(rng, targetBlockSize/valueLen, keyConfig, valueLen)

	var w rowblk.Writer
	w.RestartInterval = 16
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.Reset()
		var j int
		var prevKeyLen int
		for w.EstimatedSize() < targetBlockSize {
			ik := base.MakeInternalKey(keys[j], base.SeqNum(rng.Uint64N(uint64(base.SeqNumMax))), base.InternalKeyKindSet)
			var samePrefix bool
			if j > 0 {
				samePrefix = bytes.Equal(keys[j], keys[j-1])
			}
			err := w.AddWithOptionalValuePrefix(
				ik, false, values[j], prevKeyLen, true, block.InPlaceValuePrefix(samePrefix), samePrefix)
			if err != nil {
				b.Fatalf("error adding key: %v", err)
			}
			j++
			prevKeyLen = len(ik.UserKey)
		}
		w.Finish()
	}
}

func BenchmarkCockroachDataBlockIter(b *testing.B) {
	for _, alphaLen := range []int{4, 8, 26} {
		for _, lenSharedPct := range []float64{0.25, 0.5} {
			for _, roachKeyLen := range []int{8, 32, 128} {
				lenShared := int(float64(roachKeyLen) * lenSharedPct)
				for _, logical := range []int{0, 100} {
					for _, valueLen := range []int{8, 128, 1024} {
						keyConfig := KeyGenConfig{
							PrefixAlphabetLen: alphaLen,
							RoachKeyLen:       roachKeyLen,
							PrefixLenShared:   lenShared,
							PercentLogical:    logical,
							BaseWallTime:      uint64(time.Now().UnixNano()),
						}
						b.Run(fmt.Sprintf("%s,value=%d", keyConfig, valueLen),
							func(b *testing.B) {
								benchmarkCockroachDataRowBlockIter(b, keyConfig, valueLen)
							})
					}
				}
			}
		}
	}
}

func benchmarkCockroachDataRowBlockIter(b *testing.B, keyConfig KeyGenConfig, valueLen int) {
	const targetBlockSize = 32 << 10
	seed := uint64(time.Now().UnixNano())
	rng := rand.New(rand.NewPCG(0, seed))
	keys, values := RandomKVs(rng, targetBlockSize/valueLen, keyConfig, valueLen)

	var w rowblk.Writer
	w.RestartInterval = 16
	var count int
	var prevKeyLen int
	for w.EstimatedSize() < targetBlockSize {
		ik := base.MakeInternalKey(keys[count], base.SeqNum(rng.Uint64N(uint64(base.SeqNumMax))), base.InternalKeyKindSet)
		var samePrefix bool
		if count > 0 {
			samePrefix = bytes.Equal(keys[count], keys[count-1])
		}
		err := w.AddWithOptionalValuePrefix(
			ik, false, values[count], prevKeyLen, true, block.InPlaceValuePrefix(samePrefix), samePrefix)
		if err != nil {
			b.Fatalf("error adding key: %v", err)
		}
		count++
		prevKeyLen = len(ik.UserKey)
	}
	serializedBlock := w.Finish()
	var it rowblk.Iter
	it.Init(Compare, ComparePointSuffixes, Split, serializedBlock, block.NoTransforms)
	avgRowSize := float64(len(serializedBlock)) / float64(count)

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
	b.Run("SeekGE", func(b *testing.B) {
		rng := rand.New(rand.NewPCG(0, seed))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			k := keys[rng.IntN(count)]
			if kv := it.SeekGE(k, base.SeekGEFlagsNone); kv == nil {
				b.Fatalf("%q not found", k)
			}
		}
		b.StopTimer()
		b.ReportMetric(avgRowSize, "bytes/row")
	})
}
