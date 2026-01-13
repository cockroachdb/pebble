// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package binaryfuse

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/sstable/tablefilters/internal/filtertestutils"
)

func TestEndToEnd(t *testing.T) {
	filtertestutils.RunEndToEndTest(t, FilterPolicy(4), Decoder, 0.5)
	filtertestutils.RunEndToEndTest(t, FilterPolicy(8), Decoder, 0.2)
	filtertestutils.RunEndToEndTest(t, FilterPolicy(12), Decoder, 0.1)
	filtertestutils.RunEndToEndTest(t, FilterPolicy(16), Decoder, 0.1)
}

// Results on n4d-standard-8 (AMD EPYC Turin, go 1.25):
//
// name                                         MKeys/s
// Writer/bits=4/len=6/n=10K-8                     47.2 ± 1%
// Writer/bits=4/len=6/n=100K-8                    30.0 ± 0%
// Writer/bits=4/len=6/n=1M-8                      26.4 ± 1%
// Writer/bits=4/len=16/n=10K-8                    46.4 ± 2%
// Writer/bits=4/len=16/n=100K-8                   29.8 ± 0%
// Writer/bits=4/len=16/n=1M-8                     26.1 ± 2%
// Writer/bits=4/len=128/n=10K-8                   41.1 ± 2%
// Writer/bits=4/len=128/n=100K-8                  27.4 ± 0%
// Writer/bits=4/len=128/n=1M-8                    24.2 ± 1%
// Writer/bits=8/len=6/n=10K-8                     46.4 ± 1%
// Writer/bits=8/len=6/n=100K-8                    30.0 ± 0%
// Writer/bits=8/len=6/n=1M-8                      26.6 ± 1%
// Writer/bits=8/len=16/n=10K-8                    45.4 ± 3%
// Writer/bits=8/len=16/n=100K-8                   29.8 ± 1%
// Writer/bits=8/len=16/n=1M-8                     26.2 ± 2%
// Writer/bits=8/len=128/n=10K-8                   40.5 ± 2%
// Writer/bits=8/len=128/n=100K-8                  27.5 ± 1%
// Writer/bits=8/len=128/n=1M-8                    24.3 ± 1%
// Writer/bits=12/len=6/n=10K-8                    47.3 ± 1%
// Writer/bits=12/len=6/n=100K-8                   31.5 ± 1%
// Writer/bits=12/len=6/n=1M-8                     26.2 ± 2%
// Writer/bits=12/len=16/n=10K-8                   46.7 ± 2%
// Writer/bits=12/len=16/n=100K-8                  31.2 ± 1%
// Writer/bits=12/len=16/n=1M-8                    26.2 ± 2%
// Writer/bits=12/len=128/n=10K-8                  41.5 ± 0%
// Writer/bits=12/len=128/n=100K-8                 28.7 ± 0%
// Writer/bits=12/len=128/n=1M-8                   23.5 ± 2%
// Writer/bits=16/len=6/n=10K-8                    47.5 ± 1%
// Writer/bits=16/len=6/n=100K-8                   31.6 ± 1%
// Writer/bits=16/len=6/n=1M-8                     26.7 ± 2%
// Writer/bits=16/len=16/n=10K-8                   46.7 ± 1%
// Writer/bits=16/len=16/n=100K-8                  31.4 ± 1%
// Writer/bits=16/len=16/n=1M-8                    26.6 ± 3%
// Writer/bits=16/len=128/n=10K-8                  41.3 ± 1%
// Writer/bits=16/len=128/n=100K-8                 28.9 ± 1%
// Writer/bits=16/len=128/n=1M-8                   24.1 ± 2%
func BenchmarkWriter(b *testing.B) {
	for _, fpBits := range []int{4, 8, 12, 16} {
		b.Run(fmt.Sprintf("bits=%d", fpBits), func(b *testing.B) {
			filtertestutils.BenchmarkWriter(b, FilterPolicy(fpBits))
		})
	}
}

// Results on n4d-standard-8 (AMD EPYC Turin, go 1.25):
//
// MayContain/bits=4/len=6/n=10K/positive-8      11.9ns ± 0%
// MayContain/bits=4/len=6/n=10K/negative-8      11.8ns ± 0%
// MayContain/bits=4/len=6/n=100K/positive-8     12.0ns ± 0%
// MayContain/bits=4/len=6/n=100K/negative-8     11.9ns ± 0%
// MayContain/bits=4/len=6/n=1M/positive-8       12.5ns ± 0%
// MayContain/bits=4/len=6/n=1M/negative-8       12.3ns ± 0%
// MayContain/bits=4/len=16/n=10K/positive-8     11.6ns ± 0%
// MayContain/bits=4/len=16/n=10K/negative-8     11.4ns ± 0%
// MayContain/bits=4/len=16/n=100K/positive-8    11.8ns ± 0%
// MayContain/bits=4/len=16/n=100K/negative-8    11.6ns ± 0%
// MayContain/bits=4/len=16/n=1M/positive-8      12.1ns ± 0%
// MayContain/bits=4/len=16/n=1M/negative-8      11.9ns ± 0%
// MayContain/bits=4/len=128/n=10K/positive-8    14.8ns ± 0%
// MayContain/bits=4/len=128/n=10K/negative-8    14.8ns ± 0%
// MayContain/bits=4/len=128/n=100K/positive-8   15.1ns ± 0%
// MayContain/bits=4/len=128/n=100K/negative-8   15.1ns ± 0%
// MayContain/bits=4/len=128/n=1M/positive-8     16.2ns ± 1%
// MayContain/bits=4/len=128/n=1M/negative-8     17.4ns ± 0%
// MayContain/bits=8/len=6/n=10K/positive-8      10.6ns ± 0%
// MayContain/bits=8/len=6/n=10K/negative-8      10.4ns ± 0%
// MayContain/bits=8/len=6/n=100K/positive-8     10.7ns ± 0%
// MayContain/bits=8/len=6/n=100K/negative-8     10.6ns ± 0%
// MayContain/bits=8/len=6/n=1M/positive-8       10.8ns ± 0%
// MayContain/bits=8/len=6/n=1M/negative-8       10.8ns ± 0%
// MayContain/bits=8/len=16/n=10K/positive-8     10.3ns ± 0%
// MayContain/bits=8/len=16/n=10K/negative-8     10.1ns ± 0%
// MayContain/bits=8/len=16/n=100K/positive-8    10.3ns ± 0%
// MayContain/bits=8/len=16/n=100K/negative-8    10.3ns ± 0%
// MayContain/bits=8/len=16/n=1M/positive-8      10.6ns ± 0%
// MayContain/bits=8/len=16/n=1M/negative-8      10.5ns ± 1%
// MayContain/bits=8/len=128/n=10K/positive-8    13.7ns ± 0%
// MayContain/bits=8/len=128/n=10K/negative-8    13.7ns ± 0%
// MayContain/bits=8/len=128/n=100K/positive-8   14.1ns ± 0%
// MayContain/bits=8/len=128/n=100K/negative-8   14.0ns ± 0%
// MayContain/bits=8/len=128/n=1M/positive-8     15.4ns ± 0%
// MayContain/bits=8/len=128/n=1M/negative-8     15.9ns ± 1%
// MayContain/bits=12/len=6/n=10K/positive-8     13.1ns ± 0%
// MayContain/bits=12/len=6/n=10K/negative-8     12.8ns ± 0%
// MayContain/bits=12/len=6/n=100K/positive-8    13.2ns ± 0%
// MayContain/bits=12/len=6/n=100K/negative-8    13.1ns ± 0%
// MayContain/bits=12/len=6/n=1M/positive-8      14.5ns ± 1%
// MayContain/bits=12/len=6/n=1M/negative-8      14.0ns ± 0%
// MayContain/bits=12/len=16/n=10K/positive-8    12.6ns ± 0%
// MayContain/bits=12/len=16/n=10K/negative-8    12.5ns ± 0%
// MayContain/bits=12/len=16/n=100K/positive-8   12.9ns ± 0%
// MayContain/bits=12/len=16/n=100K/negative-8   12.7ns ± 0%
// MayContain/bits=12/len=16/n=1M/positive-8     13.9ns ± 1%
// MayContain/bits=12/len=16/n=1M/negative-8     14.3ns ± 0%
// MayContain/bits=12/len=128/n=10K/positive-8   16.0ns ± 0%
// MayContain/bits=12/len=128/n=10K/negative-8   15.9ns ± 0%
// MayContain/bits=12/len=128/n=100K/positive-8  16.3ns ± 0%
// MayContain/bits=12/len=128/n=100K/negative-8  16.2ns ± 0%
// MayContain/bits=12/len=128/n=1M/positive-8    19.0ns ± 0%
// MayContain/bits=12/len=128/n=1M/negative-8    19.0ns ± 1%
// MayContain/bits=16/len=6/n=10K/positive-8     10.8ns ± 0%
// MayContain/bits=16/len=6/n=10K/negative-8     10.6ns ± 0%
// MayContain/bits=16/len=6/n=100K/positive-8    10.9ns ± 0%
// MayContain/bits=16/len=6/n=100K/negative-8    10.8ns ± 0%
// MayContain/bits=16/len=6/n=1M/positive-8      12.7ns ± 0%
// MayContain/bits=16/len=6/n=1M/negative-8      12.6ns ± 1%
// MayContain/bits=16/len=16/n=10K/positive-8    10.5ns ± 0%
// MayContain/bits=16/len=16/n=10K/negative-8    10.3ns ± 0%
// MayContain/bits=16/len=16/n=100K/positive-8   10.6ns ± 0%
// MayContain/bits=16/len=16/n=100K/negative-8   10.5ns ± 0%
// MayContain/bits=16/len=16/n=1M/positive-8     11.6ns ± 0%
// MayContain/bits=16/len=16/n=1M/negative-8     11.3ns ± 1%
// MayContain/bits=16/len=128/n=10K/positive-8   14.1ns ± 0%
// MayContain/bits=16/len=128/n=10K/negative-8   13.9ns ± 0%
// MayContain/bits=16/len=128/n=100K/positive-8  14.2ns ± 0%
// MayContain/bits=16/len=128/n=100K/negative-8  14.2ns ± 0%
// MayContain/bits=16/len=128/n=1M/positive-8    17.3ns ± 1%
// MayContain/bits=16/len=128/n=1M/negative-8    17.4ns ± 1%
func BenchmarkMayContain(b *testing.B) {
	for _, fpBits := range []int{4, 8, 12, 16} {
		b.Run(fmt.Sprintf("bits=%d", fpBits), func(b *testing.B) {
			filtertestutils.BenchmarkMayContain(b, FilterPolicy(fpBits), Decoder)
		})
	}
}

// Results on n4d-standard-8 (AMD EPYC Turin, go 1.25):
// BenchmarkMayContainLarge/procs=1-8          100000000        121.3 ns/op
// BenchmarkMayContainLarge/procs=2-8          100000000        127.2 ns/op
// BenchmarkMayContainLarge/procs=4-8          100000000        150.4 ns/op
// BenchmarkMayContainLarge/procs=8-8          100000000        214.5 ns/op
func BenchmarkMayContainLarge(b *testing.B) {
	filtertestutils.BenchmarkMayContainLarge(b, FilterPolicy(8), Decoder)
}
