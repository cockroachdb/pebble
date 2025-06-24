// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package compression

import (
	"encoding/binary"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/stretchr/testify/require"
)

func TestCompressionRoundtrip(t *testing.T) {
	defer leaktest.AfterTest(t)()

	seed := uint64(time.Now().UnixNano())
	t.Logf("seed %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	for _, s := range presets {
		t.Run(s.String(), func(t *testing.T) {
			payload := make([]byte, 1+rng.IntN(10<<10 /* 10 KiB */))
			for i := range payload {
				payload[i] = byte(rng.Uint32())
			}
			// Create a randomly-sized buffer to house the compressed output. If it's
			// not sufficient, Compress should allocate one that is.
			compressedBuf := make([]byte, 1+rng.IntN(1<<10 /* 1 KiB */))
			compressor := GetCompressor(s)
			defer compressor.Close()
			compressed, st := compressor.Compress(compressedBuf, payload)
			got, err := decompress(st.Algorithm, compressed)
			require.NoError(t, err)
			require.Equal(t, payload, got)
		})
	}
}

// TestDecompressionError tests that a decompressing a value that does not
// decompress returns an error.
func TestDecompressionError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rng := rand.New(rand.NewPCG(0, 1 /* fixed seed */))

	// Create a buffer to represent a faux zstd compressed block. It's prefixed
	// with a uvarint of the appropriate length, followed by garabge.
	fauxCompressed := make([]byte, rng.IntN(10<<10 /* 10 KiB */))
	compressedPayloadLen := len(fauxCompressed) - binary.MaxVarintLen64
	n := binary.PutUvarint(fauxCompressed, uint64(compressedPayloadLen))
	fauxCompressed = fauxCompressed[:n+compressedPayloadLen]
	for i := range fauxCompressed[:n] {
		fauxCompressed[i] = byte(rng.Uint32())
	}

	v, err := decompress(Zstd, fauxCompressed)
	t.Log(err)
	require.Error(t, err)
	require.Nil(t, v)
}

// decompress decompresses an sstable block into memory manually allocated with
// `cache.Alloc`.  NB: If Decompress returns (nil, nil), no decompression was
// necessary and the caller may use `b` directly.
func decompress(algo Algorithm, b []byte) ([]byte, error) {
	decompressor := GetDecompressor(algo)
	defer decompressor.Close()
	// first obtain the decoded length.
	decodedLen, err := decompressor.DecompressedLen(b)
	if err != nil {
		return nil, err
	}
	// Allocate sufficient space from the cache.
	decodedBuf := make([]byte, decodedLen)
	if err := decompressor.DecompressInto(decodedBuf, b); err != nil {
		return nil, err
	}
	return decodedBuf, nil
}
