// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package compression

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAdaptiveCompressorRand(t *testing.T) {
	// Test the adaptive compressor with random data and make sure it uses the
	// fast algorithm.
	ac := NewAdaptiveCompressor(AdaptiveCompressorParams{
		Fast:            MinLZFastest,
		Slow:            ZstdLevel1,
		ReductionCutoff: 0.2,
		SampleEvery:     10,
		SampleHalfLife:  128 * 1024,
		SamplingSeed:    1,
	})
	defer ac.Close()
	for i := 0; i < 100; i++ {
		data := make([]byte, 10+rand.IntN(64*1024))
		for j := range data {
			data[j] = byte(rand.Uint32())
		}
		_, setting := ac.Compress(nil, data)
		require.Equal(t, MinLZFastest, setting)
	}
}

func TestAdaptiveCompressorCompressible(t *testing.T) {
	// Test the adaptive compressor with compressible data and make sure it uses
	// the slow algorithm.
	ac := NewAdaptiveCompressor(AdaptiveCompressorParams{
		Fast:            NoCompression,
		Slow:            ZstdLevel1,
		ReductionCutoff: 0.6,
		SampleEvery:     10,
		SampleHalfLife:  128 * 1024,
		SamplingSeed:    1,
	})
	rng := rand.New(rand.NewPCG(0, 0))
	defer ac.Close()
	for i := 0; i < 100; i++ {
		data := make([]byte, 512+rng.IntN(64*1024))
		for j := range data {
			data[j] = byte(j / 100)
		}
		_, setting := ac.Compress(nil, data)
		require.Equal(t, ZstdLevel1, setting)
	}
}
