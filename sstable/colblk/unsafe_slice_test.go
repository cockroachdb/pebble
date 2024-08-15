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

func BenchmarkUnsafeUints(b *testing.B) {
	rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
	intRanges := []intRange{
		// const
		{Min: 1, Max: 1, ExpectedEncoding: makeUintEncoding(0, true)},
		// 1b
		{Min: 10, Max: 200, ExpectedEncoding: makeUintEncoding(1, false)},
		// 1b,delta
		{Min: 100, Max: 300, ExpectedEncoding: makeUintEncoding(1, true)},
		// 2b
		{Min: 10, Max: 20_000, ExpectedEncoding: makeUintEncoding(2, false)},
		// 2b,delta
		{Min: 20_000, Max: 80_000, ExpectedEncoding: makeUintEncoding(2, true)},
		// 4b
		{Min: 0, Max: math.MaxUint32, ExpectedEncoding: makeUintEncoding(4, false)},
		// 4b,delta
		{Min: 100_000, Max: math.MaxUint32 + 10, ExpectedEncoding: makeUintEncoding(4, true)},
		// 8b
		{Min: 0, Max: math.MaxUint64, ExpectedEncoding: makeUintEncoding(8, false)},
	}
	for _, r := range intRanges {
		benchmarkUnsafeUints(b, rng, 1000, r)
	}
}

func encodeRandUints(rng *rand.Rand, rows int, intRange intRange) []byte {
	var ub UintBuilder
	ub.Init()
	for i := 0; i < rows; i++ {
		ub.Set(i, intRange.Rand(rng))
	}

	sz := ub.Size(rows, 0)
	buf := aligned.ByteSlice(int(sz) + 1 /* trailing padding byte */)
	_ = ub.Finish(0, rows, 0, buf)
	return buf
}

func benchmarkUnsafeUints(b *testing.B, rng *rand.Rand, rows int, intRange intRange) {
	b.Run(intRange.ExpectedEncoding.String(), func(b *testing.B) {
		buf := encodeRandUints(rng, rows, intRange)
		s, _ := DecodeUnsafeUints(buf, 0, rows)
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

func BenchmarkUnsafeUintOffsets(b *testing.B) {
	rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
	intRanges := []intRange{
		// 2b
		{Min: 0, Max: math.MaxUint16, ExpectedEncoding: makeUintEncoding(2, false)},
		// 4b
		{Min: 0, Max: math.MaxUint32, ExpectedEncoding: makeUintEncoding(4, false)},
	}
	for _, r := range intRanges {
		benchmarkUnsafeOffsets(b, rng, 1000, r)
	}
}

func benchmarkUnsafeOffsets(b *testing.B, rng *rand.Rand, rows int, intRange intRange) {
	b.Run(intRange.ExpectedEncoding.String(), func(b *testing.B) {
		buf := encodeRandUints(rng, rows, intRange)
		s, _ := DecodeUnsafeOffsets(buf, 0, rows)
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
