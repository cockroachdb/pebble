// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"fmt"
	"io"
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/aligned"
	"golang.org/x/exp/rand"
)

func BenchmarkUnsafeIntegerSlice(b *testing.B) {
	rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
	benchmarkUnsafeIntegerSlice[uint64](b, rng, 1000, 8, math.MaxUint64)
	benchmarkUnsafeIntegerSlice[uint64](b, rng, 1000, 4, math.MaxUint32)
	benchmarkUnsafeIntegerSlice[uint64](b, rng, 1000, 2, math.MaxUint16)
	benchmarkUnsafeIntegerSlice[uint64](b, rng, 1000, 1, math.MaxUint8)
	benchmarkUnsafeIntegerSlice[uint64](b, rng, 1000, 0, 1)
	benchmarkUnsafeIntegerSlice[uint32](b, rng, 1000, 4, math.MaxUint32)
	benchmarkUnsafeIntegerSlice[uint32](b, rng, 1000, 2, math.MaxUint16)
	benchmarkUnsafeIntegerSlice[uint32](b, rng, 1000, 1, math.MaxUint8)
	benchmarkUnsafeIntegerSlice[uint32](b, rng, 1000, 0, 1)
	benchmarkUnsafeIntegerSlice[uint16](b, rng, 1000, 2, math.MaxUint16)
	benchmarkUnsafeIntegerSlice[uint16](b, rng, 1000, 1, math.MaxUint8)
	benchmarkUnsafeIntegerSlice[uint16](b, rng, 1000, 0, 1)
	benchmarkUnsafeIntegerSlice[uint8](b, rng, 1000, 1, math.MaxUint8)
	benchmarkUnsafeIntegerSlice[uint8](b, rng, 1000, 0, 1)
}

func benchmarkUnsafeIntegerSlice[U Uint](
	b *testing.B, rng *rand.Rand, rows, expectedWidth int, max uint64,
) {
	b.Run(fmt.Sprintf("%T,delta%d", U(0), expectedWidth), func(b *testing.B) {
		var ub UintBuilder[U]
		ub.Init()
		for i := 0; i < rows; i++ {
			ub.Set(i, U(rng.Uint64n(max)))
		}

		sz := ub.Size(rows, 0)
		buf := aligned.ByteSlice(int(sz) + 1 /* trailing padding byte */)
		_ = ub.Finish(0, rows, 0, buf)
		s, _ := DecodeUnsafeIntegerSlice[U](buf, 0, rows)
		var reads [256]int
		for i := range reads {
			reads[i] = rng.Intn(rows)
		}
		b.ResetTimer()
		var result uint8
		for i := 0; i < b.N; i++ {
			result ^= uint8(s.At(reads[i&255]))
		}
		b.StopTimer()
		fmt.Fprint(io.Discard, result)
	})
}
