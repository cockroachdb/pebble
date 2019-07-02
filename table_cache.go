// Copyright 2013 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"github.com/petermattis/pebble/sstable"
	"github.com/petermattis/pebble/vfs"
)

var emptyIter = &errorIter{err: nil}

type tableCache struct {
	shards []tableCacheShard
}

func (c *tableCache) init(dirname string, fs vfs.FS, opts *Options, size int) {
	c.shards = make([]tableCacheShard, runtime.NumCPU())
	for i := range c.shards {
		c.shards[i].init(dirname, fs, opts, size/len(c.shards))
	}
}

func (c *tableCache) getShard(fileNum uint64) *tableCacheShard {
	return &c.shards[fileNum%uint64(len(c.shards))]
}

func (c *tableCache) newIters(
	meta *fileMetadata, opts *IterOptions,
) (internalIterator, internalIterator, error) {
	return c.getShard(meta.fileNum).newIters(meta, opts)
}

func (c *tableCache) evict(fileNum uint64) {
	c.getShard(fileNum).evict(fileNum)
}

func (c *tableCache) Close() error {
	for i := range c.shards {
		err := c.shards[i].Close()
		if err != nil {
			return err
		}
	}
	return nil
}

type tableCacheShard struct {
	dirname string
	fs      vfs.FS
	opts    *Options
	size    int

	mu struct {
		sync.Mutex
		cond      sync.Cond
		nodes     map[uint64]*tableCacheNode
		iterCount int32
		iters     map[*sstable.Iterator][]byte
		dummy     tableCacheNode
		releasing int32
	}
}

func (c *tableCacheShard) init(dirname string, fs vfs.FS, opts *Options, size int) {
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

func (c *tableCacheShard) newIters(
	meta *fileMetadata, opts *IterOptions,
) (internalIterator, internalIterator, error) {
	// Calling findNode gives us the responsibility of decrementing n's
	// refCount. If opening the underlying table resulted in error, then we
	// decrement this straight away. Otherwise, we pass that responsibility to
	// the sstable iterator, which decrements when it is closed.
	n := c.findNode(meta)
	<-n.loaded
	if n.err != nil {
		c.unrefNode(n)
		return nil, nil, n.err
	}

	if opts != nil &&
		opts.TableFilter != nil &&
		!opts.TableFilter(n.reader.Properties.UserProperties) {
		// Return the empty iterator. This iterator has no mutable state, so
		// using a singleton is fine.
		return emptyIter, nil, nil
	}

	iter := n.reader.NewIter(opts.GetLowerBound(), opts.GetUpperBound())
	atomic.AddInt32(&c.mu.iterCount, 1)
	if raceEnabled {
		c.mu.Lock()
		c.mu.iters[iter] = debug.Stack()
		c.mu.Unlock()
	}

	iter.SetCloseHook(func() error {
		if raceEnabled {
			c.mu.Lock()
			delete(c.mu.iters, iter)
			c.mu.Unlock()
		}
		c.unrefNode(n)
		atomic.AddInt32(&c.mu.iterCount, -1)
		return nil
	})

	// NB: range-del iterator does not maintain a reference to the table, nor
	// does it need to read from it after creation.
	if rangeDelIter := n.reader.NewRangeDelIter(); rangeDelIter != nil {
		return iter, rangeDelIter, nil
	}
	// NB: Translate a nil range-del iterator into a nil interface.
	return iter, nil, nil
}

// releaseNode releases a node from the tableCacheShard.
//
// c.mu must be held when calling this.
func (c *tableCacheShard) releaseNode(n *tableCacheNode) {
	delete(c.mu.nodes, n.meta.fileNum)
	n.next.prev = n.prev
	n.prev.next = n.next
	c.unrefNode(n)
}

// findNode returns the node for the table with the given file number, creating
// that node if it didn't already exist. The caller is responsible for
// decrementing the returned node's refCount.
func (c *tableCacheShard) findNode(meta *fileMetadata) *tableCacheNode {
	// TODO(peter): Experimentation shows that this lock is still a
	// bottleneck. Investigate

	c.mu.Lock()
	defer c.mu.Unlock()

	n := c.mu.nodes[meta.fileNum]
	if n == nil {
		n = &tableCacheNode{
			meta:     meta,
			refCount: 1,
			loaded:   make(chan struct{}),
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
	atomic.AddInt32(&n.refCount, 1)
	return n
}

// unrefNode decrements the reference count for the specified node, releasing
// it if the reference count fell to 0. Note that the node has a reference if
// it is present in tableCacheShard.mu.nodes, so a reference count of 0 means the
// node has already been removed from that map.
//
// Returns true if the node was released and false otherwise.
func (c *tableCacheShard) unrefNode(n *tableCacheNode) {
	if atomic.AddInt32(&n.refCount, -1) == 0 {
		atomic.AddInt32(&c.mu.releasing, 1)
		go n.release(c)
	}
}

func (c *tableCacheShard) evict(fileNum uint64) {
	c.mu.Lock()
	if n := c.mu.nodes[fileNum]; n != nil {
		c.releaseNode(n)
	}
	c.mu.Unlock()

	c.opts.Cache.EvictFile(fileNum)
}

func (c *tableCacheShard) Close() error {
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
		if atomic.AddInt32(&n.refCount, -1) == 0 {
			atomic.AddInt32(&c.mu.releasing, 1)
			go n.release(c)
		}
	}
	c.mu.nodes = nil
	c.mu.dummy.next = nil
	c.mu.dummy.prev = nil

	// While c.mu.releasing is updated atomically, we use a condition variable to
	// signal when it changes. Note that all iterators must have been closed
	// before tableCacheShard.Close was called, so there will be no concurrent
	// increments of c.mu.releasing at this point.
	for atomic.LoadInt32(&c.mu.releasing) > 0 {
		c.mu.cond.Wait()
	}
	return nil
}

type tableCacheNode struct {
	meta   *fileMetadata
	reader *sstable.Reader
	err    error
	loaded chan struct{}

	// The remaining fields are protected by the tableCache mutex.

	next, prev *tableCacheNode
	refCount   int32
}

func (n *tableCacheNode) load(c *tableCacheShard) {
	// Try opening the fileTypeTable first.
	f, err := c.fs.Open(dbFilename(c.dirname, fileTypeTable, n.meta.fileNum))
	if err != nil {
		n.err = err
		close(n.loaded)
		return
	}
	r := sstable.NewReader(f, n.meta.fileNum, c.opts)
	if n.meta.smallestSeqNum == n.meta.largestSeqNum {
		r.Properties.GlobalSeqNum = n.meta.largestSeqNum
	}
	n.reader = r
	close(n.loaded)
}

func (n *tableCacheNode) release(c *tableCacheShard) {
	<-n.loaded
	// Nothing to be done about an error at this point. Close the reader if it is
	// open.
	if n.reader != nil {
		_ = n.reader.Close()
	}
	// Update c.mu.releasing atomically, but also under the c.mu lock. This is
	// required for correct operation of the condition variable.
	c.mu.Lock()
	atomic.AddInt32(&c.mu.releasing, -1)
	c.mu.Unlock()
	c.mu.cond.Signal()
}
