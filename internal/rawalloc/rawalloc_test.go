// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rawalloc

import "testing"

func benchmarkRawalloc(b *testing.B, n int) {
	for i := 0; i < b.N; i++ {
		_ = New(n, n)
	}
}

func benchmarkMake(b *testing.B, n int) {
	for i := 0; i < b.N; i++ {
		_ = make([]byte, n)
	}
}

func BenchmarkRawalloc16(b *testing.B) {
	benchmarkRawalloc(b, 16)
}

func BenchmarkRawalloc100(b *testing.B) {
	benchmarkRawalloc(b, 100)
}

func BenchmarkRawalloc1K(b *testing.B) {
	benchmarkRawalloc(b, 1024)
}

func BenchmarkRawalloc10K(b *testing.B) {
	benchmarkRawalloc(b, 1024*10)
}

func BenchmarkRawalloc100K(b *testing.B) {
	benchmarkRawalloc(b, 1024*100)
}

func BenchmarkRawalloc1M(b *testing.B) {
	benchmarkRawalloc(b, 1024*1024)
}

func BenchmarkMake16(b *testing.B) {
	benchmarkMake(b, 16)
}

func BenchmarkMake100(b *testing.B) {
	benchmarkMake(b, 100)
}

func BenchmarkMake1K(b *testing.B) {
	benchmarkMake(b, 1024)
}

func BenchmarkMake10K(b *testing.B) {
	benchmarkMake(b, 1024*10)
}

func BenchmarkMake100K(b *testing.B) {
	benchmarkMake(b, 1024*100)
}

func BenchmarkMake1M(b *testing.B) {
	benchmarkMake(b, 1024*1024)
}
