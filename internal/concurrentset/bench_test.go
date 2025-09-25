// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package concurrentset

import (
	"fmt"
	"math/rand/v2"
	"runtime"
	"sync"
	"testing"
)

// Sample benchmark results.
//
// On Apple M1 (10 core):
//   Set/1-10            59.3ns ± 4%
//   Set/4-10            67.1ns ± 2%
//   Set/10-10           66.9ns ± 1%
//   Set/40-10           67.3ns ± 1%
//   MapWithMutex/1-10   91.8ns ± 2%
//   MapWithMutex/4-10    194ns ± 1%
//   MapWithMutex/10-10   294ns ± 1%
//   MapWithMutex/40-10   278ns ± 2%
//
// On Intel(R) Xeon(R) CPU @ 2.80GHz (GCP n2-custom-24-32768):
//   Set/1-24            132ns ± 3%
//   Set/4-24            154ns ± 3%
//   Set/24-24           155ns ± 1%
//   Set/96-24           156ns ± 2%
//   MapWithMutex/1-24   168ns ± 6%
//   MapWithMutex/4-24   244ns ± 3%
//   MapWithMutex/24-24  331ns ± 1%
//   MapWithMutex/96-24  348ns ± 2%

func BenchmarkSet(b *testing.B) {
	for _, p := range []int{1, 4, runtime.GOMAXPROCS(0), 4 * runtime.GOMAXPROCS(0)} {
		b.Run(fmt.Sprint(p), func(b *testing.B) {
			runBenchmark(b, p, New[uint64](), nil)
		})
	}
}

func BenchmarkMapWithMutex(b *testing.B) {
	for _, p := range []int{1, 4, runtime.GOMAXPROCS(0), 4 * runtime.GOMAXPROCS(0)} {
		b.Run(fmt.Sprint(p), func(b *testing.B) {
			runBenchmark(b, p, nil, newRefSet[uint64]())
		})
	}
}

func runBenchmark(b *testing.B, parallelism int, set *Set[uint64], refSet *refSet[uint64]) {
	b.SetParallelism(parallelism)

	var wg sync.WaitGroup

	for range parallelism {
		wg.Add(1)
		go func() {
			defer wg.Done()

			rng := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
			// Maintain a random set of recent handles.
			handles := make([]Handle, 0, 128)
			randHandle := func() Handle {
				i := rng.IntN(len(handles))
				h := handles[i]
				handles[i] = handles[len(handles)-1]
				handles = handles[:len(handles)-1]
				return h
			}
			for range b.N / parallelism {
				if len(handles) == 0 || rng.IntN(100) < 60 { // 60% adds, 40% removes
					if len(handles) == cap(handles) {
						randHandle()
					}
					value := rng.Uint64()
					if set != nil {
						handles = append(handles, set.Add(value))
					} else {
						h := Handle(value)
						refSet.Add(h, value)
						handles = append(handles, Handle(value))
					}
				} else {
					h := randHandle()
					if set != nil {
						set.Remove(h)
					} else {
						refSet.Remove(h)
					}
				}
			}
		}()
	}
	wg.Wait()
}
