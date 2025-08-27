// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package cache

import (
	"fmt"
	"os"
	"unsafe"

	"github.com/cockroachdb/pebble/v2/internal/invariants"
	"github.com/cockroachdb/pebble/v2/internal/manual"
	"github.com/cockroachdb/swiss"
)

func fibonacciHash(k *key, seed uintptr) uintptr {
	// See https://probablydance.com/2018/06/16/fibonacci-hashing-the-optimization-that-the-world-forgot-or-a-better-alternative-to-integer-modulo/
	const m = 11400714819323198485
	h := uint64(seed)
	h ^= uint64(k.id) * m
	h ^= uint64(k.fileNum) * m
	h ^= k.offset * m
	return uintptr(h)
}

type blockMapAllocator struct{}

func (blockMapAllocator) Alloc(n int) []swiss.Group[key, *entry] {
	size := uintptr(n) * unsafe.Sizeof(swiss.Group[key, *entry]{})
	buf := manual.New(manual.BlockCacheMap, size)
	return unsafe.Slice((*swiss.Group[key, *entry])(buf.Data()), n)
}

func (blockMapAllocator) Free(v []swiss.Group[key, *entry]) {
	size := uintptr(len(v)) * unsafe.Sizeof(swiss.Group[key, *entry]{})
	buf := manual.MakeBufUnsafe(unsafe.Pointer(unsafe.SliceData(v)), size)
	manual.Free(manual.BlockCacheMap, buf)
}

var blockMapOptions = []swiss.Option[key, *entry]{
	swiss.WithHash[key, *entry](fibonacciHash),
	swiss.WithMaxBucketCapacity[key, *entry](1 << 16),
	swiss.WithAllocator[key, *entry](blockMapAllocator{}),
}

type blockMap struct {
	swiss.Map[key, *entry]
	closed bool
}

func newBlockMap(initialCapacity int) *blockMap {
	m := &blockMap{}
	m.Init(initialCapacity)

	// Note: this is a no-op if invariants are disabled or race is enabled.
	invariants.SetFinalizer(m, func(obj interface{}) {
		m := obj.(*blockMap)
		if !m.closed {
			fmt.Fprintf(os.Stderr, "%p: block-map not closed\n", m)
			os.Exit(1)
		}
	})
	return m
}

func (m *blockMap) Init(initialCapacity int) {
	m.Map.Init(initialCapacity, blockMapOptions...)
}

func (m *blockMap) Close() {
	m.Map.Close()
	m.closed = true
}

func (m *blockMap) findByValue(v *entry) bool {
	var found bool
	m.Map.All(func(_ key, e *entry) bool {
		if v == e {
			found = true
			return false
		}
		return true
	})
	return found
}
