// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package compression

import (
	"math/rand/v2"
	"testing"

	"github.com/minio/minlz"
	"github.com/stretchr/testify/require"
)

func TestMinLZLargeBlock(t *testing.T) {
	for _, delta := range []int{-1, 0, 1, 1 << rand.IntN(24)} {
		b := make([]byte, minlz.MaxBlockSize+delta)
		for i := range b {
			b[i] = byte(i)
		}
		c := GetCompressor(MinLZFastest)
		defer c.Close()
		compressed, st := c.Compress(nil, b)

		d := GetDecompressor(st.Algorithm)
		decompressed := make([]byte, len(b))
		require.NoError(t, d.DecompressInto(decompressed, compressed))
		require.Equal(t, b, decompressed)
		d.Close()

		// Verify that a MinLZ decompressor always works (even if Compress returned
		// Snappy).
		d = GetDecompressor(MinLZ)
		clear(decompressed)
		require.NoError(t, d.DecompressInto(decompressed, compressed))
		require.Equal(t, b, decompressed)
		d.Close()
	}
}

func TestMinLZFastestGoEncoderFallsBackToSnappy(t *testing.T) {
	b := make([]byte, 64<<10)
	for i := range b {
		b[i] = byte(i % 251)
	}
	expectedFastest := MinLZFastest
	if minlzUsesGoEncoder {
		expectedFastest = SnappySetting
	}
	for _, tc := range []struct {
		setting  Setting
		expected Setting
	}{
		{setting: MinLZFastest, expected: expectedFastest},
		{setting: MinLZBalanced, expected: MinLZBalanced},
	} {
		c := GetCompressor(tc.setting)
		compressed, st := c.Compress(nil, b)
		c.Close()
		require.Equal(t, tc.expected, st)
		decompressed := make([]byte, len(b))
		d := GetDecompressor(st.Algorithm)
		require.NoError(t, d.DecompressInto(decompressed, compressed))
		d.Close()
		require.Equal(t, b, decompressed)
	}
}
