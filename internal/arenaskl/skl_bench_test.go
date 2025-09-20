// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package arenaskl_test

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/cockroachkvs"
	"github.com/cockroachdb/pebble/internal/arenaskl"
	"github.com/cockroachdb/pebble/internal/base"
)

// BenchmarkCockroachKeysSeekPrefixGE looks at the performance of repeated calls
// to SeekPrefixGE, with different skip distances and different settings of
// trySeekUsingNext.
func BenchmarkCockroachKeysSeekPrefixGE(b *testing.B) {
	l := arenaskl.NewSkiplist(arenaskl.NewArena(make([]byte, 64<<20)), cockroachkvs.Compare)
	rng := rand.New(rand.NewPCG(0, uint64(10000)))
	keys, vals := cockroachkvs.RandomKVs(rng, 1000000, cockroachkvs.KeyGenConfig{
		PrefixAlphabetLen: 26,
		PrefixLenShared:   4,
		RoachKeyLen:       16,
		AvgKeysPerPrefix:  2,
		BaseWallTime:      uint64(time.Now().UnixNano()),
	}, 8 /* value len */)
	prefixes := make([][]byte, len(keys))
	for i := range prefixes {
		prefixes[i] = keys[i][:cockroachkvs.Split(keys[i])]
	}

	var count int
	for count = 0; ; count++ {
		if err := l.Add(base.InternalKey{UserKey: keys[count]}, vals[count]); err == arenaskl.ErrArenaFull {
			break
		}
	}

	for _, skip := range []int{1, 2, 4, 8, 16} {
		for _, useNext := range []bool{false, true} {
			b.Run(fmt.Sprintf("skip=%d/use-next=%t", skip, useNext), func(b *testing.B) {
				it := l.NewIter(nil, nil)
				j := 0
				b.ResetTimer()
				it.SeekPrefixGE(prefixes[j], keys[j], base.SeekGEFlagsNone)
				for i := 1; i < b.N; i++ {
					j += skip
					var flags base.SeekGEFlags
					if useNext {
						flags = flags.EnableTrySeekUsingNext()
					}
					if j >= count {
						j = 0
						flags = flags.DisableTrySeekUsingNext()
					}
					it.SeekPrefixGE(prefixes[j], keys[j], flags)
				}
			})
		}
	}
}
