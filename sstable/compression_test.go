// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/stretchr/testify/require"
)

func TestCompressionRoundtrip(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed %d", seed)
	rng := rand.New(rand.NewSource(seed))
	c := cache.New(128 << 10 /* 128 KiB */)
	defer c.Unref()

	for compression := DefaultCompression + 1; compression < NCompression; compression++ {
		t.Run(compression.String(), func(t *testing.T) {
			payload := make([]byte, rng.Intn(10<<10 /* 10 KiB */))
			rng.Read(payload)
			// Create a randomly-sized buffer to house the compressed output. If it's
			// not sufficient, compressBlock should allocate one that is.
			compressedBuf := make([]byte, rng.Intn(1<<10 /* 1 KiB */))

			btyp, compressed := compressBlock(compression, payload, compressedBuf)
			v, err := decompressBlock(c, btyp, compressed)
			require.NoError(t, err)
			got := payload
			if v != nil {
				got = v.Buf()
			}
			require.Equal(t, payload, got)
		})
	}
}
