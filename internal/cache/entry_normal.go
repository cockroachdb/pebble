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

	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/manual"
)

const (
	entrySize            = int(unsafe.Sizeof(entry{}))
	entryAllocCacheLimit = 128
	entriesGoAllocated   = false
)

var entryAllocPool = sync.Pool{
	New: func() interface{} {
		return newEntryAllocCache()
	},
}

func entryManualAlloc() *entry {
	b := manual.New(entrySize)
	return (*entry)(unsafe.Pointer(&b[0]))
}

func entryManualFree(e *entry) {
	buf := (*[manual.MaxArrayLen]byte)(unsafe.Pointer(e))[:entrySize:entrySize]
	manual.Free(buf)
}

func entryAllocNew() *entry {
	if invariants.RaceEnabled {
		// We don't use the entry alloc cache in race builds because doing so
		// requires the use of runtime.SetFinalizer which triggers a bug in the
		// go1.15 and earlier race detector.
		return entryManualAlloc()
	}

	a := entryAllocPool.Get().(*entryAllocCache)
	e := a.alloc()
	entryAllocPool.Put(a)
	return e
}

func entryAllocFree(e *entry) {
	if invariants.RaceEnabled {
		entryManualFree(e)
		return
	}

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
		entryManualFree(e)
		c.entries[i] = nil
	}
}

func (c *entryAllocCache) alloc() *entry {
	n := len(c.entries)
	if n == 0 {
		return entryManualAlloc()
	}
	e := c.entries[n-1]
	c.entries = c.entries[:n-1]
	return e
}

func (c *entryAllocCache) free(e *entry) {
	if len(c.entries) == entryAllocCacheLimit {
		entryManualFree(e)
		return
	}
	c.entries = append(c.entries, e)
}
