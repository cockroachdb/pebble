// Copyright 2011 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package memdb provides a memory-backed implementation of the db.DB
// interface.
//
// A MemDB's memory consumption increases monotonically, even if keys are
// deleted or values are updated with shorter slices. Callers of the package
// are responsible for explicitly compacting a MemDB into a separate DB
// (whether in-memory or on-disk) when appropriate.
package memdb

import (
	"os"
	"rand"
	"sync"

	"leveldb-go.googlecode.com/hg/leveldb/db"
	"snappy-go.googlecode.com/hg/varint"
)

const (
	// maxHeight is the maximum height of a MemDB's skiplist.
	maxHeight = 12

	// Nodes hold offsets into a MemDB's data slice that stores varint-prefixed
	// strings: the node's key and value. A negative offset means a zero-length
	// string, whether explicitly set to empty or implicitly set by deletion.
	offsetEmptySlice  = -1
	offsetDeletedNode = -2
)

// node is a node in a skiplist. It holds a key/value pair (as offsets into
// a MemDB's data field) and a variable-length list of next nodes.
//
// TODO: instead of allocating lots of little node structs (whose next field
// is mostly empty), a MemDB could have an []int buffer (similar to its "data
// []byte" buffer) and a node could just be an offset into this buffer.
// A node would be between 2+1 and 2+maxHeight ints: kOff, vOff and a variable
// number of next node offsets. Note that a node's height is unmodified after
// construction (other than the artificial head node), and a node of a given
// height will never appear in a list of higher height.
type node struct {
	// kOff is the data offset of the node's key.
	kOff int
	// vOff is the data offset of the node's value. A value of offsetDeletedNode
	// means that the key has been deleted from the skiplist.
	vOff int
	// next[i] is the next node in the linked list at height i.
	next [maxHeight]*node
}

// MemDB is a memory-backed implementation of the db.DB interface.
//
// It is safe to call Get, Set, Delete and Find concurrently.
type MemDB struct {
	mutex sync.RWMutex
	// head is an artificial node that holds the start of each layer of
	// a skiplist.
	head node
	// height is the number of such lists, which can increase over time.
	height int
	// cmp defines an ordering on keys.
	cmp db.Comparer
	// data is an append-only buffer that holds varint-prefixed strings.
	data []byte
}

// MemDB implements the db.DB interface.
var _ db.DB = &MemDB{}

// load loads a []byte from m.data.
func (m *MemDB) load(offset int) (b []byte) {
	if offset < 0 {
		return nil
	}
	bLen, n := varint.Decode(m.data[offset:])
	return m.data[offset+n : offset+n+int(bLen)]
}

// save saves a []byte to m.data.
func (m *MemDB) save(b []byte) (offset int) {
	if len(b) == 0 {
		return offsetEmptySlice
	}
	offset = len(m.data)
	var buf [varint.MaxLen]byte
	length := varint.Encode(buf[:], uint64(len(b)))
	m.data = append(m.data, buf[:length]...)
	m.data = append(m.data, b...)
	return offset
}

// findNode returns the first node n whose key is >= the given key (or nil if
// there is no such node) and whether n's key equals key. The search is based
// solely on the contents of a node's key. Whether or not that key was
// previously deleted from the MemDB is not relevant.
//
// If prev is non-nil, it also sets the first s.height elements of prev to the
// preceding node at each height.
func (m *MemDB) findNode(key []byte, prev *[maxHeight]*node) (n *node, exactMatch bool) {
	for h, p := m.height-1, &m.head; h >= 0; h-- {
		// Walk the skiplist at height h until we find either a nil node
		// or one whose key is >= the given key.
		n = p.next[h]
		for {
			if n == nil {
				exactMatch = false
				break
			}
			if c := m.cmp.Compare(m.load(n.kOff), key); c >= 0 {
				exactMatch = c == 0
				break
			}
			p, n = n, n.next[h]
		}
		if prev != nil {
			(*prev)[h] = p
		}
	}
	return n, exactMatch
}

// Get implements DB.Get, as documented in the leveldb/db package.
func (m *MemDB) Get(key []byte) (value []byte, err os.Error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	n, exactMatch := m.findNode(key, nil)
	if !exactMatch || n.vOff == offsetDeletedNode {
		return nil, db.ErrNotFound
	}
	return m.load(n.vOff), nil
}

// Set implements DB.Set, as documented in the leveldb/db package.
func (m *MemDB) Set(key, value []byte) os.Error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	// Find the node, and its predecessors at all heights.
	var prev [maxHeight]*node
	n, exactMatch := m.findNode(key, &prev)
	if exactMatch {
		n.vOff = m.save(value)
		return nil
	}
	// Choose the new node's height, branching with 25% probability.
	h := 1
	for h < maxHeight && rand.Intn(4) == 0 {
		h++
	}
	// Raise the skiplist's height to the node's height, if necessary.
	if m.height < h {
		for i := m.height; i < h; i++ {
			prev[i] = &m.head
		}
		m.height = h
	}
	// Insert the new node.
	n1 := &node{
		kOff: m.save(key),
		vOff: m.save(value),
	}
	for i := 0; i < h; i++ {
		n1.next[i] = prev[i].next[i]
		prev[i].next[i] = n1
	}
	return nil
}

// Delete implements DB.Delete, as documented in the leveldb/db package.
func (m *MemDB) Delete(key []byte) os.Error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	n, exactMatch := m.findNode(key, nil)
	if !exactMatch || n.vOff == offsetDeletedNode {
		return db.ErrNotFound
	}
	n.vOff = offsetDeletedNode
	return nil
}

// Find implements DB.Find, as documented in the leveldb/db package.
func (m *MemDB) Find(key []byte) db.Iterator {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	n, _ := m.findNode(key, nil)
	for n != nil && n.vOff == offsetDeletedNode {
		n = n.next[0]
	}
	t := &iterator{
		m:       m,
		restart: n,
	}
	t.fill()
	// The iterator is positioned at the first node >= key. The iterator API
	// requires that the caller the Next first, so we set t.i0 to -1.
	t.i0 = -1
	return t
}

// Close implements DB.Close, as documented in the leveldb/db package.
func (m *MemDB) Close() os.Error {
	return nil
}

// ApproximateMemoryUsage returns the approximate memory usage of the MemDB.
func (m *MemDB) ApproximateMemoryUsage() int {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return len(m.data)
}

// New returns a new MemDB.
func New(o *db.Options) *MemDB {
	return &MemDB{
		head: node{
			kOff: offsetEmptySlice,
			vOff: offsetEmptySlice,
		},
		height: 1,
		cmp:    o.GetComparer(),
		data:   make([]byte, 0, 4096),
	}
}

// iterator is a MemDB iterator that buffers upcoming results, so that it does
// not have to acquire the MemDB's mutex on each Next call.
type iterator struct {
	m       *MemDB
	// restart is the node to start refilling the buffer from.
	restart *node
	// i0 is the current iterator position with respect to buf. A value of -1
	// means that the iterator is either at the start or end of the iteration.
	// i1 is the number of buffered entries.
	// Invariant: -1 <= i0 && i0 < i1 && i1 <= len(buf).
	i0, i1 int
	// buf buffers up to 32 key/value pairs.
	buf [32][2][]byte
}

// iterator implements the db.Iterator interface.
var _ db.Iterator = &iterator{}

// fill fills the iterator's buffer with key/value pairs from the MemDB.
func (t *iterator) fill() {
	i, n := 0, t.restart
	for i < len(t.buf) && n != nil {
		if n.vOff != offsetDeletedNode {
			t.buf[i][0] = t.m.load(n.kOff)
			t.buf[i][1] = t.m.load(n.vOff)
			i++
		}
		n = n.next[0]
	}
	if i == 0 {
		// There were no non-deleted nodes on or after t.restart.
		// The iterator is exhausted.
		t.i0 = -1
	} else {
		t.i0 = 0
	}
	t.i1 = i
	t.restart = n
}

// Next implements Iterator.Next, as documented in the leveldb/db package.
func (t *iterator) Next() bool {
	t.i0++
	if t.i0 < t.i1 {
		return true
	}
	if t.restart == nil {
		t.i0 = -1
		t.i1 = 0
		return false
	}
	t.m.mutex.RLock()
	defer t.m.mutex.RUnlock()
	t.fill()
	return true
}

// Key implements Iterator.Key, as documented in the leveldb/db package.
func (t *iterator) Key() []byte {
	if t.i0 < 0 {
		return nil
	}
	return t.buf[t.i0][0]
}

// Value implements Iterator.Value, as documented in the leveldb/db package.
func (t *iterator) Value() []byte {
	if t.i0 < 0 {
		return nil
	}
	return t.buf[t.i0][1]
}

// Close implements Iterator.Close, as documented in the leveldb/db package.
func (t *iterator) Close() os.Error {
	return nil
}
