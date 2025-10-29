// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metrics

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func expect(t *testing.T, cs CountAndSize, expCount uint64, expBytes uint64) {
	t.Helper()
	require.Equal(t, expCount, cs.Count)
	require.Equal(t, expBytes, cs.Bytes)
}

func TestCountAndSize_Inc(t *testing.T) {
	var cs CountAndSize

	cs.Inc(1000)
	expect(t, cs, 1, 1000)

	cs.Inc(2000)
	expect(t, cs, 2, 3000)
}

func TestCountAndSize_Dec(t *testing.T) {
	cs := CountAndSize{Count: 5, Bytes: 5000}

	cs.Dec(1000)
	expect(t, cs, 4, 4000)

	cs.Dec(2000)
	expect(t, cs, 3, 2000)
}

func TestCountAndSize_Accumulate(t *testing.T) {
	cs1 := CountAndSize{Count: 10, Bytes: 1000}
	cs2 := CountAndSize{Count: 5, Bytes: 500}

	cs1.Accumulate(cs2)
	expect(t, cs1, 15, 1500)
}

func TestCountAndSize_Deduct(t *testing.T) {
	cs1 := CountAndSize{Count: 10, Bytes: 1000}
	cs2 := CountAndSize{Count: 3, Bytes: 300}

	cs1.Deduct(cs2)
	expect(t, cs1, 7, 700)
}

func TestCountAndSize_Sum(t *testing.T) {
	cs1 := CountAndSize{Count: 10, Bytes: 1000}
	cs2 := CountAndSize{Count: 5, Bytes: 500}

	result := cs1.Sum(cs2)
	expect(t, result, 15, 1500)

	// Original should be unchanged
	expect(t, cs1, 10, 1000)
	expect(t, cs2, 5, 500)
}

func TestCountAndSize_IsZero(t *testing.T) {
	var cs CountAndSize
	require.True(t, cs.IsZero())

	cs.Inc(100)
	require.False(t, cs.IsZero())

	cs.Dec(100)
	require.True(t, cs.IsZero())
}
