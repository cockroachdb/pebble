// Copyright 2011 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pebble // import "github.com/petermattis/pebble"

import (
	"github.com/petermattis/pebble/arenaskl"
	"github.com/petermattis/pebble/db"
)

// memTable is a memory-backed implementation of the db.Reader interface.
//
// It is safe to call Get, Set, and Find concurrently.
//
// A memTable's memory consumption increases monotonically, even if keys are
// deleted or values are updated with shorter slices. Users are responsible for
// explicitly compacting a memTable into a separate DB (whether in-memory or
// on-disk) when appropriate.
type memTable struct {
	cmp       db.Compare
	skl       arenaskl.Skiplist
	emptySize uint32
}

// newMemTable returns a new MemTable.
func newMemTable(o *db.Options) *memTable {
	m := &memTable{
		cmp: o.GetComparer().Compare,
	}
	arena := arenaskl.NewArena(4 << 20 /* 4 MiB */)
	m.skl.Reset(arena, m.cmp)
	m.emptySize = m.skl.Size()
	return m
}

// Get implements Reader.Get, as documented in the pebble/db package.
func (m *memTable) Get(key *db.InternalKey, o *db.ReadOptions) (value []byte, err error) {
	it := m.skl.NewIter()
	it.SeekGE(key)
	if !it.Valid() {
		return nil, db.ErrNotFound
	}
	ikey := db.DecodeInternalKey(it.Key())
	if m.cmp(key.UserKey, ikey.UserKey) != 0 {
		return nil, db.ErrNotFound
	}
	if ikey.Kind() == db.InternalKeyKindDelete {
		return nil, db.ErrNotFound
	}
	return it.Value(), nil
}

// Set implements DB.Set, as documented in the pebble/db package.
func (m *memTable) Set(key *db.InternalKey, value []byte, o *db.WriteOptions) error {
	return m.skl.Add(key, value)
}

// NewIter implements Reader.NewIter, as documented in the pebble/db package.
func (m *memTable) NewIter(o *db.ReadOptions) db.InternalIterator {
	return &memTableIter{
		cmp:  m.cmp,
		iter: m.skl.NewIter(),
	}
}

// Close implements Reader.Close, as documented in the pebble/db package.
func (m *memTable) Close() error {
	return nil
}

// ApproximateMemoryUsage returns the approximate memory usage of the MemTable.
func (m *memTable) ApproximateMemoryUsage() int {
	return int(m.skl.Size())
}

// Empty returns whether the MemTable has no key/value pairs.
func (m *memTable) Empty() bool {
	return m.skl.Size() == m.emptySize
}

// memTableIter is a MemTable memTableIter that buffers upcoming results, so
// that it does not have to acquire the MemTable's mutex on each Next call.
type memTableIter struct {
	cmp  db.Compare
	iter arenaskl.Iterator
	ikey db.InternalKey
}

// memTableIter implements the db.InternalIterator interface.
var _ db.InternalIterator = (*memTableIter)(nil)

func (t *memTableIter) SeekGE(key *db.InternalKey) {
	t.iter.SeekGE(key)
}

func (t *memTableIter) SeekLE(key *db.InternalKey) {
	t.iter.SeekLE(key)
}

func (t *memTableIter) First() {
	t.iter.First()
}

func (t *memTableIter) Last() {
	t.iter.Last()
}

func (t *memTableIter) Next() bool {
	return t.iter.Next()
}

func (t *memTableIter) NextUserKey() bool {
	if !t.iter.Valid() {
		return false
	}
	key := db.DecodeInternalKey(t.iter.Key())
	for t.Next() {
		if t.cmp(key.UserKey, t.Key().UserKey) < 0 {
			return true
		}
	}
	return false
}

func (t *memTableIter) Prev() bool {
	return t.iter.Prev()
}

func (t *memTableIter) PrevUserKey() bool {
	if !t.iter.Valid() {
		return false
	}
	key := db.DecodeInternalKey(t.iter.Key())
	for t.Prev() {
		if t.cmp(key.UserKey, t.Key().UserKey) > 0 {
			return true
		}
	}
	return false
}

func (t *memTableIter) Key() *db.InternalKey {
	// TODO(peter): Perform the decoding during iteration.
	t.ikey = db.DecodeInternalKey(t.iter.Key())
	return &t.ikey
}

func (t *memTableIter) Value() []byte {
	return t.iter.Value()
}

func (t *memTableIter) Valid() bool {
	return t.iter.Valid()
}

func (t *memTableIter) Error() error {
	return nil
}

func (t *memTableIter) Close() error {
	return t.iter.Close()
}
