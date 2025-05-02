// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest_test

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/cockroachkvs"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
)

func BenchmarkLevelIteratorSeekGE(b *testing.B) {
	const countTables = 10_000
	fileAlloc := make([]manifest.TableMetadata, countTables)
	files := make([]*manifest.TableMetadata, countTables)
	rng := rand.New(rand.NewPCG(0, uint64(time.Now().UnixNano())))
	keys, _ := cockroachkvs.RandomKVs(rng, 2*countTables, cockroachkvs.KeyGenConfig{
		PrefixAlphabetLen:  26,
		PrefixLenShared:    2,
		RoachKeyLen:        16,
		AvgKeysPerPrefix:   1,
		BaseWallTime:       uint64(time.Now().UnixNano()),
		PercentLogical:     0,
		PercentEmptySuffix: 0,
		PercentLockSuffix:  0,
	}, 0)
	for i := 0; i < countTables; i++ {
		fileAlloc[i] = manifest.TableMetadata{
			FileNum: base.FileNum(i),
		}
		fileAlloc[i].ExtendPointKeyBounds(cockroachkvs.Compare,
			base.MakeInternalKey(keys[i*2], base.SeqNum(i), base.InternalKeyKindSet),
			base.MakeInternalKey(keys[i*2+1], base.SeqNum(i), base.InternalKeyKindSet))
		fileAlloc[i].InitPhysicalBacking()
		files[i] = &fileAlloc[i]
	}

	lm := manifest.MakeLevelMetadata(cockroachkvs.Compare, 0, files)
	iter := lm.Iter()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = iter.SeekGE(cockroachkvs.Compare, keys[i%len(keys)])
	}
}
