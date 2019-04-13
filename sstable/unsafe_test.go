// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/exp/rand"
)

func TestDecodeVarint(t *testing.T) {
	vals := []uint32{
		0,
		1,
		1 << 7,
		1 << 8,
		1 << 14,
		1 << 15,
		1 << 20,
		1 << 21,
		1 << 28,
		1 << 29,
		1 << 31,
	}
	buf := make([]byte, 5)
	for _, v := range vals {
		binary.PutUvarint(buf, uint64(v))
		u, _ := decodeVarint(unsafe.Pointer(&buf[0]))
		if v != u {
			fmt.Printf("%d %d\n", v, u)
		}
	}
}

func TestPrefixVarint(t *testing.T) {
	buf := make([]byte, 5)
	rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
	for i := 0; i < 100000; i++ {
		v := rng.Uint32()
		n := encodePrefixVarint(buf, v)
		d, _ := decodePrefixVarint(unsafe.Pointer(&buf[0]))
		if v != d {
			fmt.Printf("%d != %d: %08b\n", v, d, buf[:n])
		}
	}
}

func BenchmarkEncodeVarint(b *testing.B) {
	rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))

	for _, bytes := range []int{1, 2, 3, 4, 5} {
		b.Run(fmt.Sprintf("bytes=%d", bytes), func(b *testing.B) {
			vals := make([]uint32, 10000)
			for i := range vals {
				vals[i] = rng.Uint32() % mask[bytes]
			}
			buf := make([]byte, 5)

			b.ResetTimer()
			var t int
			for i, n := 0, 0; i < b.N; i += n {
				n = len(vals)
				if n > b.N-i {
					n = b.N - i
				}
				for j := 0; j < n; j++ {
					t = binary.PutUvarint(buf, uint64(vals[j]))
				}
			}
			if testing.Verbose() {
				fmt.Fprint(ioutil.Discard, t)
			}
		})
	}
}

func BenchmarkDecodeVarint(b *testing.B) {
	rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))

	for _, bytes := range []int{1, 2, 3, 4, 5} {
		b.Run(fmt.Sprintf("bytes=%d", bytes), func(b *testing.B) {
			vals := make([]unsafe.Pointer, 10000)
			for i := range vals {
				buf := make([]byte, 5)
				binary.PutUvarint(buf, uint64(rng.Uint32()&mask[bytes]))
				vals[i] = unsafe.Pointer(&buf[0])
			}

			b.ResetTimer()
			var ptr unsafe.Pointer
			for i, n := 0, 0; i < b.N; i += n {
				n = len(vals)
				if n > b.N-i {
					n = b.N - i
				}
				for j := 0; j < n; j++ {
					_, ptr = decodeVarint(vals[j])
				}
			}
			if testing.Verbose() {
				fmt.Fprint(ioutil.Discard, ptr)
			}
		})
	}
}

func BenchmarkEncodePrefixVarint(b *testing.B) {
	rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))

	for _, bytes := range []int{1, 2, 3, 4, 5} {
		b.Run(fmt.Sprintf("bytes=%d", bytes), func(b *testing.B) {
			vals := make([]uint32, 10000)
			for i := range vals {
				vals[i] = rng.Uint32() % mask[bytes]
			}
			buf := make([]byte, 5)

			b.ResetTimer()
			var t int
			for i, n := 0, 0; i < b.N; i += n {
				n = len(vals)
				if n > b.N-i {
					n = b.N - i
				}
				for j := 0; j < n; j++ {
					t = encodePrefixVarint(buf, vals[j])
				}
			}
			if testing.Verbose() {
				fmt.Fprint(ioutil.Discard, t)
			}
		})
	}
}

func BenchmarkDecodePrefixVarint(b *testing.B) {
	rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))

	for _, bytes := range []int{1, 2, 3, 4, 5} {
		b.Run(fmt.Sprintf("bytes=%d", bytes), func(b *testing.B) {
			vals := make([]unsafe.Pointer, 10000)
			for i := range vals {
				buf := make([]byte, 5)
				encodePrefixVarint(buf, rng.Uint32()%mask[bytes])
				vals[i] = unsafe.Pointer(&buf[0])
			}

			b.ResetTimer()
			var v uint32
			for i, n := 0, 0; i < b.N; i += n {
				n = len(vals)
				if n > b.N-i {
					n = b.N - i
				}
				for j := 0; j < n; j++ {
					v, _ = decodePrefixVarint(vals[j])
				}
			}
			if testing.Verbose() {
				fmt.Fprint(ioutil.Discard, v)
			}
		})
	}
}
