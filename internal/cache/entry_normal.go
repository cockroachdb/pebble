// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.
//
// +build !invariants,!tracing

package cache

import (
	"runtime"
	"sync"
	"unsafe"

	"github.com/cockroachdb/pebble/internal/manual"
)

const (
	entryAllocCacheLimit = 128
)

var entryAllocPool = sync.Pool{
	New: func() interface{} {
		return newEntryAllocCache()
	},
}

func entryAllocNew() *entry {
	a := entryAllocPool.Get().(*entryAllocCache)
	e := a.alloc()
	entryAllocPool.Put(a)
	e.managed = true
	return e
}

func entryAllocFree(e *entry) {
	a := entryAllocPool.Get().(*entryAllocCache)
	a.free(e)
	entryAllocPool.Put(a)
}

type entryAllocCache struct {
	entries []*entry
}

func newEntryAllocCache() *entryAllocCache {
	c := &entryAllocCache{}
	runtime.SetFinalizer(c, freeEntryAllocCache)
	return c
}

func freeEntryAllocCache(obj interface{}) {
	c := obj.(*entryAllocCache)
	for i, e := range c.entries {
		c.dealloc(e)
		c.entries[i] = nil
	}
}

func (c *entryAllocCache) alloc() *entry {
	n := len(c.entries)
	if n == 0 {
		b := manual.New(entrySize)
		return (*entry)(unsafe.Pointer(&b[0]))
	}
	e := c.entries[n-1]
	c.entries = c.entries[:n-1]
	return e
}

func (c *entryAllocCache) dealloc(e *entry) {
	buf := (*[manual.MaxArrayLen]byte)(unsafe.Pointer(e))[:entrySize:entrySize]
	manual.Free(buf)
}

func (c *entryAllocCache) free(e *entry) {
	if len(c.entries) == entryAllocCacheLimit {
		c.dealloc(e)
		return
	}
	c.entries = append(c.entries, e)
}
