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
		compressed := c.Compress(nil, b)
		d := GetDecompressor(MinLZ)
		decompressed := make([]byte, len(b))
		defer d.Close()

		require.NoError(t, d.DecompressInto(decompressed, compressed))
		require.Equal(t, b, decompressed)
	}
}
