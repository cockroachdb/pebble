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
	skl       arenaskl.Skiplist
	emptySize uint32
}

// memTable implements the db.InternalReader interface.
var _ db.InternalReader = (*memTable)(nil)

// newMemTable returns a new MemTable.
func newMemTable(o *db.Options) *memTable {
	m := &memTable{}
	arena := arenaskl.NewArena(4 << 20 /* 4 MiB */)
	m.skl.Reset(arena, o.GetComparer().Compare)
	m.emptySize = m.skl.Size()
	return m
}

// Get implements Reader.Get, as documented in the pebble/db package.
func (m *memTable) Get(key *db.InternalKey, o *db.ReadOptions) (value []byte, err error) {
	it := m.skl.NewIter()
	if !it.SeekGE(key) {
		return nil, db.ErrNotFound
	}
	return it.Value(), nil
}

// Set implements DB.Set, as documented in the pebble/db package.
func (m *memTable) Set(key *db.InternalKey, value []byte, o *db.WriteOptions) error {
	return m.skl.Add(key, value)
}

// Find implements Reader.Find, as documented in the pebble/db package.
func (m *memTable) Find(key *db.InternalKey, o *db.ReadOptions) db.InternalIterator {
	t := m.NewIter(o)
	t.SeekGE(key)
	// TODO(peter): remove this oddity.
	t.Prev()
	return t
}

// NewIter implements Reader.NewIter, as documented in the pebble/db package.
func (m *memTable) NewIter(o *db.ReadOptions) db.InternalIterator {
	return &memTableIter{
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
	iter arenaskl.Iterator
	ikey db.InternalKey
}

// memTableIter implements the db.Iterator interface.
var _ db.InternalIterator = (*memTableIter)(nil)

// SeekGE moves the iterator to the first entry whose key is greater than or
// equal to the given key. Returns true if the given key exists and false
// otherwise.
func (t *memTableIter) SeekGE(key *db.InternalKey) (found bool) {
	return t.iter.SeekGE(key)
}

// SeekLE moves the iterator to the first entry whose key is less than or equal
// to the given key. Returns true if the given key exists and false otherwise.
func (t *memTableIter) SeekLE(key *db.InternalKey) (found bool) {
	return t.iter.SeekLE(key)
}

// First seeks position at the first entry in list. Final state of iterator is
// Valid() iff list is not empty.
func (t *memTableIter) First() bool {
	return t.iter.First()
}

// Last seeks position at the last entry in list. Final state of iterator is
// Valid() iff list is not empty.
func (t *memTableIter) Last() bool {
	return t.iter.Last()
}

// Next advances to the next position. If there are no following nodes, then
// Valid() will be false after this call.
func (t *memTableIter) Next() bool {
	return t.iter.Next()
}

// Prev moves to the previous position. If there are no previous nodes, then
// Valid() will be false after this call.
func (t *memTableIter) Prev() bool {
	return t.iter.Prev()
}

// Key returns the key at the current position.
func (t *memTableIter) Key() *db.InternalKey {
	// TODO(peter): Perform the decoding during iteration.
	t.ikey = db.DecodeInternalKey(t.iter.Key())
	return &t.ikey
}

// Value returns the value at the current position.
func (t *memTableIter) Value() []byte {
	return t.iter.Value()
}

// Valid returns true iff the iterator is positioned at a valid node.
func (t *memTableIter) Valid() bool {
	return t.iter.Valid()
}

// Close ...
func (t *memTableIter) Close() error {
	return t.iter.Close()
}
