// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package cockroachkvs

import (
	"bytes"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/rowblk"
)

func BenchmarkCockroachDataRowBlockWriter(b *testing.B) {
	for _, cfg := range benchConfigs {
		b.Run(cfg.String(), func(b *testing.B) {
			benchmarkCockroachDataRowBlockWriter(b, cfg.KeyGenConfig, cfg.ValueLen)
		})
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
			w.AddWithOptionalValuePrefix(
				ik, false, values[j], prevKeyLen, true, block.InPlaceValuePrefix(samePrefix), samePrefix)
			j++
			prevKeyLen = len(ik.UserKey)
		}
		w.Finish()
	}
}

func BenchmarkCockroachDataRowBlockIter(b *testing.B) {
	for _, cfg := range benchConfigs {
		b.Run(cfg.String(), func(b *testing.B) {
			benchmarkCockroachDataRowBlockIter(b, cfg.KeyGenConfig, cfg.ValueLen)
		})
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
		w.AddWithOptionalValuePrefix(
			ik, false, values[count], prevKeyLen, true, block.InPlaceValuePrefix(samePrefix), samePrefix)
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
