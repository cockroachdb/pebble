// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package filtertestutils

import (
	crand "crypto/rand"
	"fmt"
	"testing"

	"github.com/cockroachdb/crlib/crhumanize"
	"github.com/cockroachdb/pebble/internal/base"
)

func BenchmarkWriter(b *testing.B, policy base.TableFilterPolicy) {
	for _, keyLen := range []int{6, 16, 128} {
		for _, numKeys := range []int{10_000, 100_000, 1_000_000} {
			b.Run(fmt.Sprintf("len=%d/n=%s", keyLen, crhumanize.Count(numKeys, crhumanize.Compact)), func(b *testing.B) {
				keys := make([][]byte, numKeys)
				for i := range keys {
					keys[i] = randKey(keyLen)
				}
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					w := policy.NewWriter()
					for _, key := range keys {
						w.AddKey(key)
					}
					w.Finish()
				}
				b.ReportMetric(float64(b.N*numKeys)/b.Elapsed().Seconds()/1e6, "MKeys/s")
			})
		}
	}
}

func BenchmarkMayContain(
	b *testing.B, policy base.TableFilterPolicy, decoder base.TableFilterDecoder,
) {
	for _, keyLen := range []int{6, 16, 128} {
		for _, numKeys := range []int{10_000, 100_000, 1_000_000} {
			b.Run(fmt.Sprintf("len=%d/n=%s", keyLen, crhumanize.Count(numKeys, crhumanize.Compact)), func(b *testing.B) {
				keys := randKeys(numKeys, keyLen)
				w := policy.NewWriter()
				for _, key := range keys {
					w.AddKey(key)
				}
				filter, _, ok := w.Finish()
				if !ok {
					b.Fatalf("failed to create filter")
				}
				const numQueryKeys = 4096
				b.Run("positive", func(b *testing.B) {
					for i := 0; b.Loop(); i++ {
						k := keys[i%numQueryKeys]
						if !decoder.MayContain(filter, k) {
							b.Fatalf("expected to contain key")
						}
					}
				})
				otherKeys := randKeys(numQueryKeys, keyLen)
				b.Run("negative", func(b *testing.B) {
					for i := 0; b.Loop(); i++ {
						k := otherKeys[i%numQueryKeys]
						decoder.MayContain(filter, k)
					}
				})
			})
		}
	}
}

func randKeys(numKeys int, keyLen int) [][]byte {
	buf := make([]byte, numKeys*keyLen)
	_, _ = crand.Read(buf)
	keys := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = buf[i*keyLen : (i+1)*keyLen]
	}
	return keys
}
