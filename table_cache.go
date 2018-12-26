// Copyright 2013 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/sstable"
	"github.com/petermattis/pebble/storage"
)

type tableCache struct {
	dirname string
	fs      storage.Storage
	opts    *db.Options
	size    int

	mu struct {
		sync.Mutex
		cond      sync.Cond
		nodes     map[uint64]*tableCacheNode
		iterCount int32
		iters     map[*sstable.Iterator][]byte
		dummy     tableCacheNode
		releasing int
	}
}

func (c *tableCache) init(dirname string, fs storage.Storage, opts *db.Options, size int) {
	c.dirname = dirname
	c.fs = fs
	c.opts = opts
	c.size = size
	c.mu.cond.L = &c.mu.Mutex
	c.mu.nodes = make(map[uint64]*tableCacheNode)
	c.mu.dummy.next = &c.mu.dummy
	c.mu.dummy.prev = &c.mu.dummy

	if raceEnabled {
		c.mu.iters = make(map[*sstable.Iterator][]byte)
	}
}

func (c *tableCache) newIters(meta *fileMetadata) (internalIterator, internalIterator, error) {
	// Calling findNode gives us the responsibility of decrementing n's
	// refCount. If opening the underlying table resulted in error, then we
	// decrement this straight away. Otherwise, we pass that responsibility to
	// the sstable iterator, which decrements when it is closed.
	n := c.findNode(meta)
	x := <-n.result
	if x.err != nil {
		if !c.unrefNode(n) {
			// Try loading the table again; the error may be transient.
			//
			// TODO(peter): This could loop forever. That doesn't seem right.
			go n.load(c)
		}
		return nil, nil, x.err
	}
	n.result <- x

	iter := x.reader.NewIter(nil)
	atomic.AddInt32(&c.mu.iterCount, 1)
	if raceEnabled {
		c.mu.Lock()
		c.mu.iters[iter] = debug.Stack()
		c.mu.Unlock()
	}

	iter.SetCloseHook(func() error {
		c.mu.Lock()
		atomic.AddInt32(&c.mu.iterCount, -1)
		if raceEnabled {
			delete(c.mu.iters, iter)
		}
		n.refCount--
		if n.refCount == 0 {
			c.mu.releasing++
			go n.release(c)
		}
		c.mu.Unlock()
		return nil
	})

	// NB: range-del iterator does not maintain a reference to the table, nor
	// does it need to read from it after creation.
	if rangeDelIter := x.reader.NewRangeDelIter(); rangeDelIter != nil {
		return iter, rangeDelIter, nil
	}
	// NB: Translate a nil range-del iterator into a nil interface.
	return iter, nil, nil
}

// releaseNode releases a node from the tableCache.
//
// c.mu must be held when calling this.
func (c *tableCache) releaseNode(n *tableCacheNode) {
	delete(c.mu.nodes, n.meta.fileNum)
	n.next.prev = n.prev
	n.prev.next = n.next
	n.refCount--
	if n.refCount == 0 {
		c.mu.releasing++
		go n.release(c)
	}
}

// findNode returns the node for the table with the given file number, creating
// that node if it didn't already exist. The caller is responsible for
// decrementing the returned node's refCount.
func (c *tableCache) findNode(meta *fileMetadata) *tableCacheNode {
	c.mu.Lock()
	defer c.mu.Unlock()

	n := c.mu.nodes[meta.fileNum]
	if n == nil {
		n = &tableCacheNode{
			meta:     meta,
			refCount: 1,
			result:   make(chan tableReaderOrError, 1),
		}
		c.mu.nodes[meta.fileNum] = n
		if len(c.mu.nodes) > c.size {
			// Release the tail node.
			c.releaseNode(c.mu.dummy.prev)
		}
		go n.load(c)
	} else {
		// Remove n from the doubly-linked list.
		n.next.prev = n.prev
		n.prev.next = n.next
	}
	// Insert n at the front of the doubly-linked list.
	n.next = c.mu.dummy.next
	n.prev = &c.mu.dummy
	n.next.prev = n
	n.prev.next = n
	// The caller is responsible for decrementing the refCount.
	n.refCount++
	return n
}

// unrefNode decrements the reference count for the specified node, releasing
// it if the reference count fell to 0. Note that the node has a reference if
// it is present in tableCache.mu.nodes, so a reference count of 0 means the
// node has already been removed from that map.
//
// Returns true if the node was released and false otherwise.
func (c *tableCache) unrefNode(n *tableCacheNode) bool {
	c.mu.Lock()
	n.refCount--
	res := false
	if n.refCount == 0 {
		c.mu.releasing++
		go n.release(c)
		res = true
	}
	c.mu.Unlock()
	return res
}

func (c *tableCache) evict(fileNum uint64) {
	c.mu.Lock()
	if n := c.mu.nodes[fileNum]; n != nil {
		c.releaseNode(n)
	}
	c.mu.Unlock()

	c.opts.Cache.EvictFile(fileNum)
}

func (c *tableCache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if v := atomic.LoadInt32(&c.mu.iterCount); v > 0 {
		if !raceEnabled {
			return fmt.Errorf("leaked iterators: %d", v)
		}
		var buf bytes.Buffer
		fmt.Fprintf(&buf, "leaked iterators: %d\n", v)
		for _, stack := range c.mu.iters {
			fmt.Fprintf(&buf, "%s\n", stack)
		}
		return errors.New(buf.String())
	}

	for n := c.mu.dummy.next; n != &c.mu.dummy; n = n.next {
		n.refCount--
		if n.refCount == 0 {
			c.mu.releasing++
			go n.release(c)
		}
	}
	c.mu.nodes = nil
	c.mu.dummy.next = nil
	c.mu.dummy.prev = nil

	for c.mu.releasing > 0 {
		c.mu.cond.Wait()
	}
	return nil
}

type tableReaderOrError struct {
	reader *sstable.Reader
	err    error
}

type tableCacheNode struct {
	meta   *fileMetadata
	result chan tableReaderOrError

	// The remaining fields are protected by the tableCache mutex.

	next, prev *tableCacheNode
	refCount   int
}

func (n *tableCacheNode) load(c *tableCache) {
	// Try opening the fileTypeTable first.
	f, err := c.fs.Open(dbFilename(c.dirname, fileTypeTable, n.meta.fileNum))
	if err != nil {
		n.result <- tableReaderOrError{err: err}
		return
	}
	r := sstable.NewReader(f, n.meta.fileNum, c.opts)
	if n.meta.smallestSeqNum == n.meta.largestSeqNum {
		r.Properties.GlobalSeqNum = n.meta.largestSeqNum
	}
	n.result <- tableReaderOrError{reader: r}
}

func (n *tableCacheNode) release(c *tableCache) {
	x := <-n.result
	if x.err == nil {
		// Nothing to be done about an error at this point.
		_ = x.reader.Close()
	}
	c.mu.Lock()
	c.mu.releasing--
	c.mu.Unlock()
	c.mu.cond.Signal()
}
