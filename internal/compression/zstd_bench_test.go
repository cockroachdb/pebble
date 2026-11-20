// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package compression

import (
	"fmt"
	"math/rand/v2"
	"testing"
)

// zstdBenchBlock returns a 32 KiB block of key/value-like data that
// compresses roughly as table data blocks do.
func zstdBenchBlock() []byte {
	rng := rand.New(rand.NewPCG(1, 2))
	var b []byte
	for len(b) < 32<<10 {
		b = fmt.Appendf(b, "key/%08d/%04d{\"value\":%.3f,\"ts\":%d}", rng.IntN(1e6), rng.IntN(100),
			rng.Float64()*1000, 1_700_000_000_000+rng.Int64N(1e9))
	}
	return b[:32<<10]
}

// BenchmarkZstd measures compressing and decompressing one block, including
// getting and releasing the codec as the sstable writer and reader do.
func BenchmarkZstd(b *testing.B) {
	block := zstdBenchBlock()
	compressor := GetCompressor(ZstdLevel3)
	compressed, setting := compressor.Compress(nil, block)
	compressed = append([]byte(nil), compressed...)
	compressor.Close()

	b.Run("compress", func(b *testing.B) {
		b.SetBytes(int64(len(block)))
		b.ReportAllocs()
		var buf []byte
		for range b.N {
			c := GetCompressor(ZstdLevel3)
			buf, _ = c.Compress(buf[:0], block)
			c.Close()
		}
	})
	for _, parallel := range []bool{false, true} {
		b.Run(fmt.Sprintf("decompress/parallel=%t", parallel), func(b *testing.B) {
			b.SetBytes(int64(len(block)))
			b.ReportAllocs()
			decompressOne := func(dst []byte) {
				d := GetDecompressor(setting.Algorithm)
				if err := d.DecompressInto(dst, compressed); err != nil {
					b.Fatal(err)
				}
				d.Close()
			}
			if !parallel {
				dst := make([]byte, len(block))
				for range b.N {
					decompressOne(dst)
				}
				return
			}
			b.RunParallel(func(pb *testing.PB) {
				dst := make([]byte, len(block))
				for pb.Next() {
					decompressOne(dst)
				}
			})
		})
	}
}
