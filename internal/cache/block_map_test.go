// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package cache

import (
	"fmt"
	"io"
	"math/rand/v2"
	"runtime"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/internal/base"
)

// The Pebble block cache can be configured with a capacity of GBs or 10s of
// GBs, but the cache itself is sharded. The sharding is 4*GOMAXPROCS. With
// CockroachDB's default block size of 32KB, a shard with 32K entries can
// cache 1GB worth of blocks. A machine configured with multiple GBs will
// almost certainly have more than one CPU making 32K a reasonable upper limit
// for the number of entries in a shard.
const benchSize = 32 * 1024

func BenchmarkGoMapInsert(b *testing.B) {
	rng := rand.New(rand.NewPCG(0, uint64(time.Now().UnixNano())))
	keys := make([]key, benchSize)
	for i := range keys {
		keys[i].fileNum = base.DiskFileNum(rng.Uint64N(1 << 20))
		keys[i].offset = uint64(rng.IntN(1 << 20))
	}
	b.ResetTimer()

	var m map[key]*entry
	for i, j := 0, 0; i < b.N; i, j = i+1, j+1 {
		if m == nil || j == len(keys) {
			b.StopTimer()
			m = make(map[key]*entry, len(keys))
			j = 0
			b.StartTimer()
		}
		m[keys[j]] = nil
	}
}

func BenchmarkSwissMapInsert(b *testing.B) {
	rng := rand.New(rand.NewPCG(0, uint64(time.Now().UnixNano())))
	keys := make([]key, benchSize)
	for i := range keys {
		keys[i].fileNum = base.DiskFileNum(rng.Uint64N(1 << 20))
		keys[i].offset = uint64(rng.IntN(1 << 20))
	}
	e := &entry{}
	b.ResetTimer()

	var m *blockMap
	for i, j := 0, 0; i < b.N; i, j = i+1, j+1 {
		if m == nil || j == len(keys) {
			b.StopTimer()
			if m != nil {
				m.Close()
			}
			m = newBlockMap(len(keys))
			j = 0
			b.StartTimer()
		}
		m.Put(keys[j], e)
	}

	runtime.KeepAlive(e)
}

func BenchmarkGoMapLookupHit(b *testing.B) {
	rng := rand.New(rand.NewPCG(0, uint64(time.Now().UnixNano())))
	keys := make([]key, benchSize)
	m := make(map[key]*entry, len(keys))
	e := &entry{}
	for i := range keys {
		keys[i].fileNum = base.DiskFileNum(rng.Uint64N(1 << 20))
		keys[i].offset = uint64(rng.IntN(1 << 20))
		m[keys[i]] = e
	}
	b.ResetTimer()

	var p *entry
	for i, j := 0, 0; i < b.N; i, j = i+1, j+1 {
		if j == len(keys) {
			j = 0
		}
		p = m[keys[j]]
	}

	if testing.Verbose() {
		fmt.Fprintln(io.Discard, p)
	}
}

func BenchmarkSwissMapLookupHit(b *testing.B) {
	rng := rand.New(rand.NewPCG(0, uint64(time.Now().UnixNano())))
	keys := make([]key, benchSize)
	m := newBlockMap(len(keys))
	e := &entry{}
	for i := range keys {
		keys[i].fileNum = base.DiskFileNum(rng.Uint64N(1 << 20))
		keys[i].offset = uint64(rng.IntN(1 << 20))
		m.Put(keys[i], e)
	}
	b.ResetTimer()

	var p *entry
	for i, j := 0, 0; i < b.N; i, j = i+1, j+1 {
		if j == len(keys) {
			j = 0
		}
		p, _ = m.Get(keys[j])
	}

	b.StopTimer()
	if testing.Verbose() {
		fmt.Fprintln(io.Discard, p)
	}
	runtime.KeepAlive(e)
	m.Close()
}

func BenchmarkGoMapLookupMiss(b *testing.B) {
	rng := rand.New(rand.NewPCG(0, uint64(time.Now().UnixNano())))
	keys := make([]key, benchSize)
	m := make(map[key]*entry, len(keys))
	e := &entry{}
	for i := range keys {
		keys[i].id = 1
		keys[i].fileNum = base.DiskFileNum(rng.Uint64N(1 << 20))
		keys[i].offset = uint64(rng.IntN(1 << 20))
		m[keys[i]] = e
		keys[i].id = 2
	}
	b.ResetTimer()

	var p *entry
	for i, j := 0, 0; i < b.N; i, j = i+1, j+1 {
		if j == len(keys) {
			j = 0
		}
		p = m[keys[j]]
	}

	if testing.Verbose() {
		fmt.Fprintln(io.Discard, p)
	}
}

func BenchmarkSwissMapLookupMiss(b *testing.B) {
	rng := rand.New(rand.NewPCG(0, uint64(time.Now().UnixNano())))
	keys := make([]key, benchSize)
	m := newBlockMap(len(keys))
	e := &entry{}
	for i := range keys {
		keys[i].id = 1
		keys[i].fileNum = base.DiskFileNum(rng.Uint64N(1 << 20))
		keys[i].offset = uint64(rng.IntN(1 << 20))
		m.Put(keys[i], e)
		keys[i].id = 2
	}
	b.ResetTimer()

	var p *entry
	for i, j := 0, 0; i < b.N; i, j = i+1, j+1 {
		if j == len(keys) {
			j = 0
		}
		p, _ = m.Get(keys[j])
	}

	b.StopTimer()
	if testing.Verbose() {
		fmt.Fprintln(io.Discard, p)
	}
	runtime.KeepAlive(e)
	m.Close()
}
