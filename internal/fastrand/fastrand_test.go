// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package fastrand

import (
	"fmt"
	"io"
	"math/rand/v2"
	"sync"
	"testing"
	"time"
)

type defaultRand struct {
	mu  sync.Mutex
	src rand.PCG
}

func newDefaultRand() *defaultRand {
	r := &defaultRand{}
	r.src.Seed(0, uint64(time.Now().UnixNano()))
	return r
}

func (r *defaultRand) Uint32() uint32 {
	r.mu.Lock()
	i := uint32(r.src.Uint64())
	r.mu.Unlock()
	return i
}

func BenchmarkFastRand(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		var x uint32
		for pb.Next() {
			x ^= Uint32()
		}
		fmt.Fprintf(io.Discard, "%v", x)
	})
}

func BenchmarkRandV2(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		var x uint32
		for pb.Next() {
			x ^= rand.Uint32()
		}
		fmt.Fprintf(io.Discard, "%v", x)
	})
}

func BenchmarkDefaultRand(b *testing.B) {
	r := newDefaultRand()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r.Uint32()
		}
	})
}

// Benchmarks for single-threaded (ST) use of fastrand compared to
// constructing a Rand, which can have heap allocation overhead.

// Global state to disable elision of benchmark code.
var xg uint32

func BenchmarkSTFastRand(b *testing.B) {
	var x uint32
	for i := 0; i < b.N; i++ {
		// Arbitrary constant.
		x ^= Uint32n(2097152)
	}
	fmt.Fprintf(io.Discard, "%v", x)
}

func BenchmarkSTRandV2(b *testing.B) {
	var x uint32
	for i := 0; i < b.N; i++ {
		// Arbitrary constant.
		x ^= rand.Uint32N(2097152)
	}
	fmt.Fprintf(io.Discard, "%v", x)
}

func BenchmarkSTDefaultRand(b *testing.B) {
	for _, newPeriod := range []int{0, 10, 100, 1000} {
		name := "no-new"
		if newPeriod > 0 {
			name = fmt.Sprintf("new-period=%d", newPeriod)
		}
		b.Run(name, func(b *testing.B) {
			r := rand.New(rand.NewPCG(0, uint64(time.Now().UnixNano())))
			b.ResetTimer()
			var x uint32
			for i := 0; i < b.N; i++ {
				if newPeriod > 0 && i%newPeriod == 0 {
					r = rand.New(rand.NewPCG(0, uint64(time.Now().UnixNano())))
				}
				// Arbitrary constant.
				x = uint32(r.Uint64N(2097152))
			}
			xg = x
		})
	}
}
