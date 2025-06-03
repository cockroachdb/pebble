// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package block

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBufferRandomized(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	compressor := MakeCompressor(SnappyCompression)
	defer compressor.Close()
	var checksummer Checksummer
	checksummer.Init(ChecksumTypeCRC32c)
	b := NewTempBuffer()
	defer b.Release()
	vbuf := make([]byte, 0, 1<<10) // 1 KiB

	for i := 0; i < 25; i++ {
		t.Run(fmt.Sprintf("iteration %d", i), func(t *testing.T) {
			// Randomly release and reinitialize the buffer.
			if rng.IntN(5) == 1 {
				b.Release()
				b = NewTempBuffer()
			}

			aggregateSizeOfKVs := rng.IntN(4<<20-(1<<10)) + 1<<10 // [1 KiB, 4 MiB)
			size := 0
			for b.Size() < aggregateSizeOfKVs {
				vlen := rng.IntN(aggregateSizeOfKVs-b.Size()) + 1
				if cap(vbuf) < vlen {
					vbuf = make([]byte, vlen)
				} else {
					vbuf = vbuf[:vlen]
				}
				for i := range vbuf {
					vbuf[i] = byte(rng.Uint32())
				}
				b.Append(vbuf)
				size += vlen
				require.Equal(t, size, b.Size())
				s := b.Data()
				require.Equal(t, vbuf, s[len(s)-len(vbuf):])
			}
			_, bh := CompressAndChecksumToTempBuffer(b.Data(), compressor, &checksummer)
			b.Reset()
			bh.Release()
		})
	}
}
