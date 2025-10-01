// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package inflight

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
)

// BenchmarkTracker benchmarks the overhead of Start/Stop under varying parallelism.
//
// Sample results on an Apple M1 Pro (10 core), without and with the cockroach
// Go runtime:
//
//	name             vanilla-go time/op  crdb-go time/op  delta
//	Tracker/p=1-10           230ns ± 2%       217ns ± 2%   -5.75%  (p=0.008 n=5+5)
//	Tracker/p=5-10           337ns ± 2%       301ns ± 1%  -10.74%  (p=0.008 n=5+5)
//	Tracker/p=10-10          544ns ± 3%       539ns ± 1%     ~     (p=0.690 n=5+5)
func BenchmarkTracker(b *testing.B) {
	procs := runtime.GOMAXPROCS(0)
	for _, parallelism := range []int{1, procs / 2, procs} {
		b.Run(fmt.Sprintf("p=%d", parallelism), func(b *testing.B) {
			const batchSize = 100
			// Each element of ch corresponds to a batch of operations to be performed.
			ch := make(chan int, 1+b.N/batchSize)

			var wg sync.WaitGroup
			for range parallelism {
				wg.Add(1)
				tr := NewTracker()
				go func() {
					defer wg.Done()

					for numOps := range ch {
						for range numOps {
							h := tr.Start()
							tr.Stop(h)
						}
					}
				}()
			}

			numOps := int64(b.N) * int64(parallelism)
			for i := int64(0); i < numOps; i += batchSize {
				ch <- int(min(batchSize, numOps-i))
			}
			close(ch)
			wg.Wait()
		})
	}
}
