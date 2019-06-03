// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

// Snapshot provides a read-only point-in-time view of the DB state.
type Snapshot struct {
	// The db the snapshot was created from.
	db     *DB
	seqNum uint64

	// The list the snapshot is linked into.
	list *snapshotList

	// The next/prev link for the snapshotList doubly-linked list of snapshots.
	prev, next *Snapshot
}

var _ Reader = (*Snapshot)(nil)

// Get gets the value for the given key. It returns ErrNotFound if the DB does
// not contain the key.
//
// The caller should not modify the contents of the returned slice, but it is
// safe to modify the contents of the argument after Get returns.
func (s *Snapshot) Get(key []byte) ([]byte, error) {
	if s.db == nil {
		panic(ErrClosed)
	}
	return s.db.getInternal(key, nil /* batch */, s)
}

// NewIter returns an iterator that is unpositioned (Iterator.Valid() will
// return false). The iterator can be positioned via a call to SeekGE,
// SeekLT, First or Last.
func (s *Snapshot) NewIter(o *IterOptions) *Iterator {
	if s.db == nil {
		panic(ErrClosed)
	}
	return s.db.newIterInternal(nil /* batchIter */, nil /* batchRangeDelIter */, s, o)
}

// Close closes the snapshot, releasing its resources. Close must be
// called. Failure to do so while result in a tiny memory leak, and a large
// leak of resources on disk due to the entries the snapshot is preventing from
// being deleted.
func (s *Snapshot) Close() error {
	if s.db == nil {
		panic(ErrClosed)
	}
	s.db.mu.Lock()
	s.db.mu.snapshots.remove(s)
	s.db.mu.Unlock()
	s.db = nil
	return nil
}

type snapshotList struct {
	root Snapshot
}

func (l *snapshotList) init() {
	l.root.next = &l.root
	l.root.prev = &l.root
}

func (l *snapshotList) empty() bool {
	return l.root.next == &l.root
}

func (l *snapshotList) toSlice() []uint64 {
	if l.empty() {
		return nil
	}
	var results []uint64
	for i := l.root.next; i != &l.root; i = i.next {
		results = append(results, i.seqNum)
	}
	return results
}

func (l *snapshotList) pushBack(s *Snapshot) {
	if s.list != nil || s.prev != nil || s.next != nil {
		panic("pebble: snapshot list is inconsistent")
	}
	s.prev = l.root.prev
	s.prev.next = s
	s.next = &l.root
	s.next.prev = s
	s.list = l
}

func (l *snapshotList) remove(s *Snapshot) {
	if s == &l.root {
		panic("pebble: cannot remove snapshot list root node")
	}
	if s.list != l {
		panic("pebble: snapshot list is inconsistent")
	}
	s.prev.next = s.next
	s.next.prev = s.prev
	s.next = nil // avoid memory leaks
	s.prev = nil // avoid memory leaks
	s.list = nil // avoid memory leaks
}
