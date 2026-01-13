// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package filtertestutils

import (
	crand "crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand/v2"
	"runtime"
	"sync"
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

// BenchmarkMayContainLarge should be run with -benchtime=100000000.
func BenchmarkMayContainLarge(
	b *testing.B, policy base.TableFilterPolicy, decoder base.TableFilterDecoder,
) {
	const keyLen = 8
	const numKeys = 1_000_000
	const numFilters = 1024
	filters := make([][]byte, numFilters)
	b.Log("Generating filters...")
	for i := range filters {
		keys := randKeys(numKeys, keyLen)
		w := policy.NewWriter()
		for _, key := range keys {
			w.AddKey(key)
		}
		filter, _, ok := w.Finish()
		if !ok {
			b.Fatalf("failed to create filter")
		}
		filters[i] = filter
		if i*10/len(filters) != (i+1)*10/len(filters) {
			b.Logf("..%d%%", (i+1)*100/len(filters))
		}
	}
	for p := 1; p <= runtime.GOMAXPROCS(0); p *= 2 {
		b.Run(fmt.Sprintf("procs=%d", p), func(b *testing.B) {
			ch := make(chan int, 10*p)
			var wg sync.WaitGroup
			wg.Add(p)
			for range p {
				go func() {
					defer wg.Done()
					var key [keyLen]byte
					var hits int
					for n := range ch {
						for range n {
							binary.NativeEndian.PutUint64(key[:], rand.Uint64())
							res := decoder.MayContain(filters[rand.IntN(numFilters)], key[:])
							if res {
								hits++
							}
						}
					}
					fmt.Fprint(io.Discard, hits)
				}()
			}
			for n := b.N * p; n > 0; {
				batch := min(n, 1000)
				ch <- batch
				n -= batch
			}
			close(ch)
			wg.Wait()
		})
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
