// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package cache

import "sync/atomic"

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

// entry holds the metadata for a cache entry. The memory for an entry is
// allocated from manually managed memory.
//
// Using manual memory management for entries may seem to be a violation of
// the Cgo pointer rules:
//
//	https://golang.org/cmd/cgo/#hdr-Passing_pointers
//
// Specifically, Go pointers should not be stored in C allocated memory. The
// reason for this rule is that the Go GC will not look at C allocated memory
// to find pointers to Go objects. If the only reference to a Go object is
// stored in C allocated memory, the object will be reclaimed. The entry
// contains various pointers to other entries. This does not violate the Go
// pointer rules because either all entries are manually allocated or none
// are. Also, even if we had a mix of C and Go allocated memory, which would
// violate the rule, we would not have this reclamation problem since the
// lifetime of the entry is managed by the shard containing it, and not
// reliant on the entry pointers.
type entry struct {
	key key
	// The value associated with the entry. The entry holds a reference on the
	// value which is maintained by entry.setValue().
	val       *Value
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
	// referenced is atomically set to indicate that this entry has been accessed
	// since the last time one of the clock hands swept it.
	referenced atomic.Bool
}

func newEntry(key key, size int64) *entry {
	e := entryAllocNew()
	*e = entry{
		key:   key,
		size:  size,
		ptype: etCold,
	}
	e.blockLink.next = e
	e.blockLink.prev = e
	e.fileLink.next = e
	e.fileLink.prev = e
	return e
}

func (e *entry) free() {
	e.setValue(nil)
	entryAllocFree(e)
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
	if v != nil {
		v.acquire()
	}
	old := e.val
	e.val = v
	old.release()
}

func (e *entry) acquireValue() *Value {
	v := e.val
	if v != nil {
		v.acquire()
	}
	return v
}
