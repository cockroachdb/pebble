// Copyright 2013 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pebble

import (
	"sync"

	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/sstable"
	"github.com/petermattis/pebble/storage"
)

type tableCache struct {
	dirname string
	fs      storage.Storage
	opts    *db.Options
	size    int

	mu    sync.Mutex
	nodes map[uint64]*tableCacheNode
	dummy tableCacheNode
}

func (c *tableCache) init(dirname string, fs storage.Storage, opts *db.Options, size int) {
	c.dirname = dirname
	c.fs = fs
	c.opts = opts
	c.size = size
	c.nodes = make(map[uint64]*tableCacheNode)
	c.dummy.next = &c.dummy
	c.dummy.prev = &c.dummy
}

func (c *tableCache) newIter(fileNum uint64) (db.InternalIterator, error) {
	// Calling findNode gives us the responsibility of decrementing n's
	// refCount. If opening the underlying table resulted in error, then we
	// decrement this straight away. Otherwise, we pass that responsibility
	// to the tableCacheIter, which decrements when it is closed.
	n := c.findNode(fileNum)
	x := <-n.result
	if x.err != nil {
		c.mu.Lock()
		n.refCount--
		if n.refCount == 0 {
			go n.release()
		}
		c.mu.Unlock()

		// Try loading the table again; the error may be transient.
		go n.load(c)
		return nil, x.err
	}
	n.result <- x
	return &tableCacheIter{
		InternalIterator: x.reader.NewIter(nil),
		cache:            c,
		node:             n,
	}, nil
}

// releaseNode releases a node from the tableCache.
//
// c.mu must be held when calling this.
func (c *tableCache) releaseNode(n *tableCacheNode) {
	delete(c.nodes, n.fileNum)
	n.next.prev = n.prev
	n.prev.next = n.next
	n.refCount--
	if n.refCount == 0 {
		go n.release()
	}
}

// findNode returns the node for the table with the given file number, creating
// that node if it didn't already exist. The caller is responsible for
// decrementing the returned node's refCount.
func (c *tableCache) findNode(fileNum uint64) *tableCacheNode {
	c.mu.Lock()
	defer c.mu.Unlock()

	n := c.nodes[fileNum]
	if n == nil {
		n = &tableCacheNode{
			fileNum:  fileNum,
			refCount: 1,
			result:   make(chan tableReaderOrError, 1),
		}
		c.nodes[fileNum] = n
		if len(c.nodes) > c.size {
			// Release the tail node.
			c.releaseNode(c.dummy.prev)
		}
		go n.load(c)
	} else {
		// Remove n from the doubly-linked list.
		n.next.prev = n.prev
		n.prev.next = n.next
	}
	// Insert n at the front of the doubly-linked list.
	n.next = c.dummy.next
	n.prev = &c.dummy
	n.next.prev = n
	n.prev.next = n
	// The caller is responsible for decrementing the refCount.
	n.refCount++
	return n
}

func (c *tableCache) evict(fileNum uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if n := c.nodes[fileNum]; n != nil {
		c.releaseNode(n)
	}
}

func (c *tableCache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for n := c.dummy.next; n != &c.dummy; n = n.next {
		n.refCount--
		if n.refCount == 0 {
			go n.release()
		}
	}
	c.nodes = nil
	c.dummy.next = nil
	c.dummy.prev = nil
	return nil
}

type tableReaderOrError struct {
	reader *sstable.Reader
	err    error
}

type tableCacheNode struct {
	fileNum uint64
	result  chan tableReaderOrError

	// The remaining fields are protected by the tableCache mutex.

	next, prev *tableCacheNode
	refCount   int
}

func (n *tableCacheNode) load(c *tableCache) {
	// Try opening the fileTypeTable first.
	f, err := c.fs.Open(dbFilename(c.dirname, fileTypeTable, n.fileNum))
	if err != nil {
		n.result <- tableReaderOrError{err: err}
		return
	}
	n.result <- tableReaderOrError{reader: sstable.NewReader(f, n.fileNum, c.opts)}
}

func (n *tableCacheNode) release() {
	x := <-n.result
	if x.err != nil {
		return
	}
	x.reader.Close()
}

type tableCacheIter struct {
	db.InternalIterator
	cache    *tableCache
	node     *tableCacheNode
	closeErr error
	closed   bool
}

func (i *tableCacheIter) Close() error {
	if i.closed {
		return i.closeErr
	}
	i.closed = true

	i.cache.mu.Lock()
	i.node.refCount--
	if i.node.refCount == 0 {
		go i.node.release()
	}
	i.cache.mu.Unlock()

	i.closeErr = i.InternalIterator.Close()
	return i.closeErr
}
