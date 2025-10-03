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
//	Tracker/p=1-10           231ns ± 1%       215ns ± 0%  -7.00%  (p=0.008 n=5+5)
//	Tracker/p=5-10           325ns ± 1%       332ns ± 1%  +2.18%  (p=0.008 n=5+5)
//	Tracker/p=10-10          540ns ±15%       527ns ± 4%    ~     (p=0.690 n=5+5)
//	Tracker/p=20-10         1.05µs ± 8%      1.07µs ± 1%    ~     (p=0.135 n=5+5)
func BenchmarkTracker(b *testing.B) {
	procs := runtime.GOMAXPROCS(0)
	for _, parallelism := range []int{1, procs / 2, procs, 2 * procs} {
		b.Run(fmt.Sprintf("p=%d", parallelism), func(b *testing.B) {
			const batchSize = 100
			// Each element of ch corresponds to a batch of operations to be performed.
			// The batch size is intended to amortize the overhead of channel operations.
			ch := make(chan int, 1+b.N/batchSize)

			tr := NewTracker()
			var wg sync.WaitGroup
			for range parallelism {
				wg.Add(1)
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
