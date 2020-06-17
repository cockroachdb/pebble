// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package fastrand

import (
	"golang.org/x/exp/rand"
	"sync"
	"testing"
	"time"
)

type defaultRand struct {
	mu sync.Mutex
	src rand.PCGSource
}

func newDefaultRand() *defaultRand {
	r := &defaultRand{}
	r.src.Seed(uint64(time.Now().UnixNano()))
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
		for pb.Next() {
			Uint32()
		}
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
