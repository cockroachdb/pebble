// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package cache

import (
	"sync/atomic"
	"unsafe"
)

type entryType int8

const (
	etTest entryType = iota
	etCold
	etHot
)

func (p entryType) String() string {
	switch p {
	case etTest:
		return "test"
	case etCold:
		return "cold"
	case etHot:
		return "hot"
	}
	return "unknown"
}

// entry holds the metadata for a cache entry. If the value stored in an entry
// is manually managed, the entry will also use manual memory management.
//
// Using manual memory management for entries is technically a volation of the
// Cgo pointer rules:
//
//   https://golang.org/cmd/cgo/#hdr-Passing_pointers
//
// Specifically, Go pointers should not be stored in C allocated memory. The
// reason for this rule is that the Go GC will not look at C allocated memory
// to find pointers to Go objects. If the only reference to a Go object is
// stored in C allocated memory, the object will be reclaimed. The blockLink,
// fileLink, and shard fields of the entry struct may all point to Go objects,
// thus the violation. What makes this "safe" is that the Cache guarantees that
// there are other pointers to the entry and shard which will keep them
// alive. In particular, every Go allocated entry in the cache is referenced by
// the shard.entries map. And every shard is referenced by the Cache.shards
// map.
type entry struct {
	key       key
	val       unsafe.Pointer
	blockLink struct {
		next *entry
		prev *entry
	}
	fileLink struct {
		next *entry
		prev *entry
	}
	size  int64
	ptype entryType
	// Can the entry hold a manual Value? Only a manually managed entry can store
	// manually managed values (Value.manual() is true).
	manual bool
	// Was the entry allocated using the Go allocator or the manual
	// allocator. This can differ from the setting of the manual field due when
	// the "invariants" build tag is set.
	managed bool
	// referenced is atomically set to indicate that this entry has been accessed
	// since the last time one of the clock hands swept it.
	referenced int32
	shard      *shard
}

const entrySize = int(unsafe.Sizeof(entry{}))

func newEntry(s *shard, key key, size int64, manual bool) *entry {
	var e *entry
	if manual {
		e = entryAllocNew()
	} else {
		e = &entry{}
	}
	*e = entry{
		key:     key,
		size:    size,
		ptype:   etCold,
		manual:  manual,
		managed: e.managed,
		shard:   s,
	}
	e.blockLink.next = e
	e.blockLink.prev = e
	e.fileLink.next = e
	e.fileLink.prev = e
	return e
}

func (e *entry) free() {
	if e.manual {
		*e = entry{}
		entryAllocFree(e)
	}
}

func (e *entry) next() *entry {
	if e == nil {
		return nil
	}
	return e.blockLink.next
}

func (e *entry) prev() *entry {
	if e == nil {
		return nil
	}
	return e.blockLink.prev
}

func (e *entry) link(s *entry) {
	s.blockLink.prev = e.blockLink.prev
	s.blockLink.prev.blockLink.next = s
	s.blockLink.next = e
	s.blockLink.next.blockLink.prev = s
}

func (e *entry) unlink() *entry {
	next := e.blockLink.next
	e.blockLink.prev.blockLink.next = e.blockLink.next
	e.blockLink.next.blockLink.prev = e.blockLink.prev
	e.blockLink.prev = e
	e.blockLink.next = e
	return next
}

func (e *entry) linkFile(s *entry) {
	s.fileLink.prev = e.fileLink.prev
	s.fileLink.prev.fileLink.next = s
	s.fileLink.next = e
	s.fileLink.next.fileLink.prev = s
}

func (e *entry) unlinkFile() *entry {
	next := e.fileLink.next
	e.fileLink.prev.fileLink.next = e.fileLink.next
	e.fileLink.next.fileLink.prev = e.fileLink.prev
	e.fileLink.prev = e
	e.fileLink.next = e
	return next
}

func (e *entry) setValue(v *Value) {
	if old := e.getValue(); old != nil {
		old.release()
	}
	atomic.StorePointer(&e.val, unsafe.Pointer(v))
}

func (e *entry) getValue() *Value {
	return (*Value)(atomic.LoadPointer(&e.val))
}
