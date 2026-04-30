// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/blockiter"
)

func BenchmarkDataBlockIter(b *testing.B) {
	const targetBlockSize = 32 << 10
	const versionsPerPrefix = 10

	var w DataBlockEncoder
	w.Init(&testKeysSchema, NoTieringColumns())

	var prefixes [][]byte
	keyBuf := make([]byte, 0, 64)
	val := []byte{0}
	for i := 0; w.Size() < targetBlockSize; i++ {
		prefix := []byte(fmt.Sprintf("%08d", i))
		prefixes = append(prefixes, prefix)
		// Suffixes sort in reverse, so write largest timestamp first to keep
		// keys in ascending order.
		for ts := int64(versionsPerPrefix); ts >= 1; ts-- {
			sl := testkeys.SuffixLen(ts)
			keyBuf = keyBuf[:len(prefix)+sl]
			copy(keyBuf, prefix)
			testkeys.WriteSuffix(keyBuf[len(prefix):], ts)
			ik := base.MakeInternalKey(keyBuf, 1, base.InternalKeyKindSet)
			kcmp := w.KeyWriter.ComparePrev(ik.UserKey)
			vp := block.InPlaceValuePrefix(kcmp.PrefixEqual())
			w.Add(ik, val, vp, kcmp, false /* isObsolete */, base.KVMeta{})
		}
	}
	finished, _ := w.Finish(w.Rows(), w.Size())

	var dec DataBlockDecoder
	bd := dec.Init(&testKeysSchema, finished)

	var it DataBlockIter
	it.InitOnce(&testKeysSchema, testkeys.Comparer,
		getInternalValuer(func([]byte) base.InternalValue {
			return base.MakeInPlaceValue(nil)
		}), NoTieringColumns())
	if err := it.Init(&dec, bd, blockiter.Transforms{}, NoTieringColumns()); err != nil {
		b.Fatal(err)
	}

	// Shuffled prefix order for the random-access SeekGE benchmark. We use a
	// fixed-seed rng so the benchmark is reproducible; the shuffle happens
	// outside the timed loop.
	shuffled := append([][]byte(nil), prefixes...)
	rng := rand.New(rand.NewPCG(0, 1))
	rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })

	b.Run("SeekGE", func(b *testing.B) {
		idx := 0
		for b.Loop() {
			it.SeekGE(shuffled[idx], base.SeekGEFlagsNone)
			idx++
			if idx == len(shuffled) {
				idx = 0
			}
		}
	})

	b.Run("SeekPrefixGE", func(b *testing.B) {
		idx := 0
		for b.Loop() {
			it.SeekPrefixGE(shuffled[idx], base.SeekGEFlagsNone)
			idx++
			if idx == len(shuffled) {
				idx = 0
			}
		}
	})

	b.Run("SeekGE-TrySeekUsingNext", func(b *testing.B) {
		// Walk forward through prefixes monotonically, resetting with a plain
		// SeekGE when wrapping back to the beginning.
		idx := 0
		flags := base.SeekGEFlagsNone.EnableTrySeekUsingNext()
		b.ResetTimer()
		for b.Loop() {
			if idx == 0 {
				it.SeekGE(prefixes[0], base.SeekGEFlagsNone)
			} else {
				it.SeekGE(prefixes[idx], flags)
			}
			idx++
			if idx == len(prefixes) {
				idx = 0
			}
		}
	})

	b.Run("Next", func(b *testing.B) {
		kv := it.First()
		for b.Loop() {
			if kv == nil {
				kv = it.First()
			} else {
				kv = it.Next()
			}
		}
	})

	b.Run("Prev", func(b *testing.B) {
		kv := it.First()
		for b.Loop() {
			if kv == nil {
				kv = it.Last()
			} else {
				kv = it.Prev()
			}
		}
	})
}
