// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package cache

import (
	"fmt"
	"io"
	"runtime"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"golang.org/x/exp/rand"
)

func TestRobinHoodMap(t *testing.T) {
	rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
	rhMap := newRobinHoodMap(0)
	defer rhMap.free()

	goMap := make(map[key]*entry)

	randomKey := func() key {
		n := rng.Intn(len(goMap))
		for k := range goMap {
			if n == 0 {
				return k
			}
			n--
		}
		return key{}
	}

	ops := 10000 + rng.Intn(10000)
	for i := 0; i < ops; i++ {
		var which float64
		if len(goMap) > 0 {
			which = rng.Float64()
		}

		switch {
		case which < 0.4:
			// 40% insert.
			var k key
			k.id = rng.Uint64()
			k.fileNum = base.FileNum(rng.Uint64()).DiskFileNum()
			k.offset = rng.Uint64()
			e := &entry{}
			goMap[k] = e
			rhMap.Put(k, e)
			if len(goMap) != rhMap.Count() {
				t.Fatalf("map sizes differ: %d != %d", len(goMap), rhMap.Count())
			}

		case which < 0.1:
			// 10% overwrite.
			k := randomKey()
			e := &entry{}
			goMap[k] = e
			rhMap.Put(k, e)
			if len(goMap) != rhMap.Count() {
				t.Fatalf("map sizes differ: %d != %d", len(goMap), rhMap.Count())
			}

		case which < 0.75:
			// 25% delete.
			k := randomKey()
			delete(goMap, k)
			rhMap.Delete(k)
			if len(goMap) != rhMap.Count() {
				t.Fatalf("map sizes differ: %d != %d", len(goMap), rhMap.Count())
			}

		default:
			// 25% lookup.
			k := randomKey()
			v := goMap[k]
			u := rhMap.Get(k)
			if v != u {
				t.Fatalf("%s: expected %p, but found %p", k, v, u)
			}
		}
	}

	t.Logf("map size: %d", len(goMap))
}

const benchSize = 1 << 20

func BenchmarkGoMapInsert(b *testing.B) {
	rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
	keys := make([]key, benchSize)
	for i := range keys {
		keys[i].fileNum = base.FileNum(rng.Uint64n(1 << 20)).DiskFileNum()
		keys[i].offset = uint64(rng.Intn(1 << 20))
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

func BenchmarkRobinHoodInsert(b *testing.B) {
	rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
	keys := make([]key, benchSize)
	for i := range keys {
		keys[i].fileNum = base.FileNum(rng.Uint64n(1 << 20)).DiskFileNum()
		keys[i].offset = uint64(rng.Intn(1 << 20))
	}
	e := &entry{}
	b.ResetTimer()

	var m *robinHoodMap
	for i, j := 0, 0; i < b.N; i, j = i+1, j+1 {
		if m == nil || j == len(keys) {
			b.StopTimer()
			m = newRobinHoodMap(len(keys))
			j = 0
			b.StartTimer()
		}
		m.Put(keys[j], e)
	}

	runtime.KeepAlive(e)
}

func BenchmarkGoMapLookupHit(b *testing.B) {
	rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
	keys := make([]key, benchSize)
	m := make(map[key]*entry, len(keys))
	e := &entry{}
	for i := range keys {
		keys[i].fileNum = base.FileNum(rng.Uint64n(1 << 20)).DiskFileNum()
		keys[i].offset = uint64(rng.Intn(1 << 20))
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

func BenchmarkRobinHoodLookupHit(b *testing.B) {
	rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
	keys := make([]key, benchSize)
	m := newRobinHoodMap(len(keys))
	e := &entry{}
	for i := range keys {
		keys[i].fileNum = base.FileNum(rng.Uint64n(1 << 20)).DiskFileNum()
		keys[i].offset = uint64(rng.Intn(1 << 20))
		m.Put(keys[i], e)
	}
	b.ResetTimer()

	var p *entry
	for i, j := 0, 0; i < b.N; i, j = i+1, j+1 {
		if j == len(keys) {
			j = 0
		}
		p = m.Get(keys[j])
	}

	if testing.Verbose() {
		fmt.Fprintln(io.Discard, p)
	}
	runtime.KeepAlive(e)
}

func BenchmarkGoMapLookupMiss(b *testing.B) {
	rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
	keys := make([]key, benchSize)
	m := make(map[key]*entry, len(keys))
	e := &entry{}
	for i := range keys {
		keys[i].id = 1
		keys[i].fileNum = base.FileNum(rng.Uint64n(1 << 20)).DiskFileNum()
		keys[i].offset = uint64(rng.Intn(1 << 20))
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

func BenchmarkRobinHoodLookupMiss(b *testing.B) {
	rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
	keys := make([]key, benchSize)
	m := newRobinHoodMap(len(keys))
	e := &entry{}
	for i := range keys {
		keys[i].id = 1
		keys[i].fileNum = base.FileNum(rng.Uint64n(1 << 20)).DiskFileNum()
		keys[i].offset = uint64(rng.Intn(1 << 20))
		m.Put(keys[i], e)
		keys[i].id = 2
	}
	b.ResetTimer()

	var p *entry
	for i, j := 0, 0; i < b.N; i, j = i+1, j+1 {
		if j == len(keys) {
			j = 0
		}
		p = m.Get(keys[j])
	}

	if testing.Verbose() {
		fmt.Fprintln(io.Discard, p)
	}
	runtime.KeepAlive(e)
}
