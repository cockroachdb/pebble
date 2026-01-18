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
	filtertestutils.RunEndToEndTest(t, FilterPolicy(10), Decoder, 0.2)
	filtertestutils.RunEndToEndTest(t, FilterPolicy(12), Decoder, 0.1)
	filtertestutils.RunEndToEndTest(t, FilterPolicy(16), Decoder, 0.1)
}

// Results on n4d-standard-8 (AMD EPYC Turin, go 1.25):
//
// name \ MKeys/s            bits=4       bits=8       bits=10      bits=12      bits=16
// len=6/n=10K-8             46.5 ± 1%    47.5 ± 1%    47.5 ± 1%    47.4 ± 1%    47.9 ± 1%
// len=6/n=100K-8            29.5 ± 1%    29.6 ± 1%    31.3 ± 1%    31.4 ± 1%    31.5 ± 1%
// len=6/n=1M-8              25.7 ± 1%    25.7 ± 1%    25.8 ± 1%    25.5 ± 2%    25.9 ± 2%
// len=16/n=10K-8            47.4 ± 3%    48.0 ± 1%    48.0 ± 1%    47.7 ± 2%    48.5 ± 1%
// len=16/n=100K-8           29.7 ± 1%    29.7 ± 0%    31.4 ± 0%    31.3 ± 1%    31.7 ± 1%
// len=16/n=1M-8             25.9 ± 1%    26.0 ± 1%    25.7 ± 1%    25.4 ± 2%    26.2 ± 1%
// len=128/n=10K-8           41.7 ± 2%    41.9 ± 2%    41.9 ± 0%    41.8 ± 0%    42.2 ± 1%
// len=128/n=100K-8          27.3 ± 0%    27.4 ± 0%    28.8 ± 1%    28.7 ± 1%    28.8 ± 1%
// len=128/n=1M-8            23.4 ± 1%    23.5 ± 1%    23.3 ± 1%    23.1 ± 1%    23.5 ± 1%
func BenchmarkWriter(b *testing.B) {
	for _, fpBits := range SupportedBitsPerFingerprint {
		b.Run(fmt.Sprintf("bits=%d", fpBits), func(b *testing.B) {
			filtertestutils.BenchmarkWriter(b, FilterPolicy(fpBits))
		})
	}
}

// Results on n4d-standard-8 (AMD EPYC Turin, go 1.25):
//
// name                       bits=4        bits=8        bits=10       bits=12       bits=16
// len=6/n=10K/positive-8     11.9ns ± 0%   10.5ns ± 0%   12.6ns ± 0%   13.0ns ± 0%   10.7ns ± 0%
// len=6/n=10K/negative-8     11.8ns ± 0%   10.5ns ± 0%   12.5ns ± 0%   12.9ns ± 0%   10.6ns ± 0%
// len=6/n=100K/positive-8    12.1ns ± 0%   10.7ns ± 0%   12.9ns ± 0%   13.3ns ± 0%   10.9ns ± 0%
// len=6/n=100K/negative-8    11.9ns ± 0%   10.6ns ± 0%   12.7ns ± 0%   13.1ns ± 0%   10.9ns ± 0%
// len=6/n=1M/positive-8      12.5ns ± 0%   10.9ns ± 1%   13.6ns ± 1%   14.2ns ± 1%   11.7ns ± 0%
// len=6/n=1M/negative-8      12.4ns ± 0%   10.8ns ± 0%   13.4ns ± 0%   14.3ns ± 0%   12.1ns ± 1%
// len=16/n=10K/positive-8    11.5ns ± 0%   10.2ns ± 0%   12.3ns ± 0%   12.6ns ± 0%   10.5ns ± 0%
// len=16/n=10K/negative-8    11.5ns ± 0%   10.1ns ± 0%   12.2ns ± 0%   12.6ns ± 0%   10.3ns ± 0%
// len=16/n=100K/positive-8   11.7ns ± 0%   10.3ns ± 0%   12.5ns ± 0%   12.9ns ± 0%   10.5ns ± 0%
// len=16/n=100K/negative-8   11.6ns ± 0%   10.2ns ± 0%   12.5ns ± 0%   12.8ns ± 0%   10.5ns ± 0%
// len=16/n=1M/positive-8     12.1ns ± 0%   11.5ns ± 6%   13.1ns ± 1%   14.0ns ± 3%   11.5ns ± 1%
// len=16/n=1M/negative-8     12.0ns ± 0%   10.9ns ± 2%   12.8ns ± 0%   13.4ns ± 0%   11.9ns ± 0%
// len=128/n=10K/positive-8   14.9ns ± 0%   13.8ns ± 0%   15.6ns ± 0%   16.2ns ± 0%   14.1ns ± 0%
// len=128/n=10K/negative-8   14.8ns ± 0%   13.7ns ± 0%   15.6ns ± 0%   16.0ns ± 0%   13.9ns ± 0%
// len=128/n=100K/positive-8  15.2ns ± 0%   14.2ns ± 0%   16.1ns ± 0%   16.5ns ± 0%   14.4ns ± 1%
// len=128/n=100K/negative-8  15.1ns ± 0%   14.0ns ± 0%   16.0ns ± 1%   16.3ns ± 0%   14.3ns ± 0%
// len=128/n=1M/positive-8    16.1ns ± 0%   15.8ns ± 0%   18.2ns ± 0%   19.2ns ± 0%   17.5ns ± 1%
// len=128/n=1M/negative-8    16.0ns ± 0%   15.7ns ± 1%   18.5ns ± 0%   19.3ns ± 0%   17.8ns ± 0%
func BenchmarkMayContain(b *testing.B) {
	for _, fpBits := range SupportedBitsPerFingerprint {
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
