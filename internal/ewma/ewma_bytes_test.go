// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package ewma

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBytes(t *testing.T) {
	const eps = 0.1
	var b Bytes

	b.Init(1000)

	require.True(t, math.IsNaN(b.Estimate()))

	b.SampledBlock(100, 1.0)
	require.InEpsilon(t, 1.0, b.Estimate(), eps)

	b.NoSample(10000)
	require.InEpsilon(t, 1.0, b.Estimate(), eps)
	b.SampledBlock(100, 10.0)
	require.InEpsilon(t, 10.0, b.Estimate(), eps)

	b.NoSample(10000)
	b.SampledBlock(10, 0.0)
	b.SampledBlock(10, 10.0)
	b.SampledBlock(10, 0.0)
	b.SampledBlock(10, 10.0)
	require.InEpsilon(t, 5.0, b.Estimate(), eps)
	b.NoSample(1000)
	b.SampledBlock(10, 0.0)
	require.InEpsilon(t, 3.3, b.Estimate(), eps)

	b.Init(1000)
	b.SampledBlock(1, 0.0)
	b.NoSample(1000)
	b.SampledBlock(1, 1.0)
	// The byte 1 half-life ago matters 1/2 as much, so the estimate is 2/3.
	require.InEpsilon(t, 0.66, b.Estimate(), eps)
}

// TestBytesHalfLife verifies that the alpha and decay calculations are accurate.
func TestBytesHalfLife(t *testing.T) {
	for _, n := range []int64{1, 2, 3, 10, 100, 1000, 10_000, 1 << 20, 128 << 20, 1 << 30} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			var b Bytes
			b.Init(n)
			const eps = 1e-8
			require.InEpsilon(t, 1.0/2, b.decay(n), eps)
			require.InDelta(t, 1.0/4, b.decay(2*n), eps)
			require.InDelta(t, 1.0/8, b.decay(3*n), eps)
			require.InDelta(t, 1.0/16, b.decay(4*n), eps)
			require.InDelta(t, 1.0/32, b.decay(5*n), eps)
			require.InDelta(t, 1.0/64, b.decay(6*n), eps)
		})
	}
}
