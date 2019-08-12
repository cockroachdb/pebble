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

const defaultTableCacheHitBuffer = 64

var emptyIter = &errorIter{err: nil}

type tableCache struct {
	shards []tableCacheShard
}

func (c *tableCache) init(
	dbNum uint64, dirname string, fs vfs.FS, opts *Options, size, hitBuffer int,
) {
	c.shards = make([]tableCacheShard, runtime.NumCPU())
	for i := range c.shards {
		c.shards[i].init(dbNum, dirname, fs, opts, size/len(c.shards), hitBuffer)
	}
}

func (c *tableCache) getShard(fileNum uint64) *tableCacheShard {
	return &c.shards[fileNum%uint64(len(c.shards))]
}

func (c *tableCache) newIters(
	meta *fileMetadata, opts *IterOptions, bytesIterated *uint64,
) (internalIterator, internalIterator, error) {
	return c.getShard(meta.fileNum).newIters(meta, opts, bytesIterated)
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
	dbNum   uint64
	dirname string
	fs      vfs.FS
	opts    *Options
	size    int

	mu struct {
		sync.RWMutex
		nodes map[uint64]*tableCacheNode
		// The iters map is only created and populated in race builds.
		iters map[sstable.Iterator][]byte
		lru   tableCacheNode
	}

	iterCount int32
	releasing sync.WaitGroup
	hitsPool  *sync.Pool
}

func (c *tableCacheShard) init(
	dbNum uint64, dirname string, fs vfs.FS, opts *Options, size, hitBuffer int,
) {
	c.dbNum = dbNum
	c.dirname = dirname
	c.fs = fs
	c.opts = opts
	c.size = size
	c.mu.nodes = make(map[uint64]*tableCacheNode)
	c.mu.lru.next = &c.mu.lru
	c.mu.lru.prev = &c.mu.lru
	c.hitsPool = &sync.Pool{
		New: func() interface{} {
			return &tableCacheHits{
				hits:  make([]*tableCacheNode, 0, hitBuffer),
				shard: c,
			}
		},
	}

	if raceEnabled {
		c.mu.iters = make(map[sstable.Iterator][]byte)
	}
}

func (c *tableCacheShard) newIters(
	meta *fileMetadata, opts *IterOptions, bytesIterated *uint64,
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
	var iter sstable.Iterator
	if bytesIterated != nil {
		iter = n.reader.NewCompactionIter(bytesIterated)
	} else {
		iter = n.reader.NewIter(opts.GetLowerBound(), opts.GetUpperBound())
	}
	atomic.AddInt32(&c.iterCount, 1)
	if raceEnabled {
		c.mu.Lock()
		c.mu.iters[iter] = debug.Stack()
		c.mu.Unlock()
	}
	iter.SetCloseHook(n.closeHook)

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
	n.prev = nil
	n.next = nil
	c.unrefNode(n)
}

// unrefNode decrements the reference count for the specified node, releasing
// it if the reference count fell to 0. Note that the node has a reference if
// it is present in tableCacheShard.mu.nodes, so a reference count of 0 means the
// node has already been removed from that map.
//
// Returns true if the node was released and false otherwise.
func (c *tableCacheShard) unrefNode(n *tableCacheNode) {
	if atomic.AddInt32(&n.refCount, -1) == 0 {
		c.releasing.Add(1)
		go n.release(c)
	}
}

// findNode returns the node for the table with the given file number, creating
// that node if it didn't already exist. The caller is responsible for
// decrementing the returned node's refCount.
func (c *tableCacheShard) findNode(meta *fileMetadata) *tableCacheNode {
	// Fast-path for a hit in the cache. We grab the lock in shared mode, and use
	// a batching mechanism to perform updates to the LRU list.
	c.mu.RLock()
	if n := c.mu.nodes[meta.fileNum]; n != nil {
		// The caller is responsible for decrementing the refCount.
		atomic.AddInt32(&n.refCount, 1)
		c.mu.RUnlock()

		// Record a hit for the node. This has to be done with tableCacheShard.mu
		// unlocked as it might result in a call to
		// tableCacheShard.recordHits. Note that the sync.Pool acts as a
		// thread-local cache of the accesses. This is lossy (a GC can result in
		// the sync.Pool be cleared), but that is ok as we don't need perfect
		// accuracy for the LRU list.
		hits := c.hitsPool.Get().(*tableCacheHits)
		hits.recordHit(n)
		c.hitsPool.Put(hits)
		return n
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	{
		// Flush the thread-local hits buffer as we already have the shard locked
		// exclusively.
		hits := c.hitsPool.Get().(*tableCacheHits)
		hits.flushLocked()
		c.hitsPool.Put(hits)
	}

	n := c.mu.nodes[meta.fileNum]
	if n == nil {
		n = &tableCacheNode{
			// Cache the closure invoked when an iterator is closed. This avoids an
			// allocation on every call to newIters.
			closeHook: func(i sstable.Iterator) error {
				if raceEnabled {
					c.mu.Lock()
					delete(c.mu.iters, i)
					c.mu.Unlock()
				}
				c.unrefNode(n)
				atomic.AddInt32(&c.iterCount, -1)
				return nil
			},
			meta:     meta,
			refCount: 1,
			loaded:   make(chan struct{}),
		}
		c.mu.nodes[meta.fileNum] = n
		if len(c.mu.nodes) > c.size {
			// Release the tail node.
			c.releaseNode(c.mu.lru.prev)
		}
		go n.load(c)
	} else {
		// Remove n from the doubly-linked list.
		n.next.prev = n.prev
		n.prev.next = n.next
	}
	// Insert n at the front of the doubly-linked list.
	n.next = c.mu.lru.next
	n.prev = &c.mu.lru
	n.next.prev = n
	n.prev.next = n
	// The caller is responsible for decrementing the refCount.
	atomic.AddInt32(&n.refCount, 1)
	return n
}

func (c *tableCacheShard) evict(fileNum uint64) {
	c.mu.Lock()
	if n := c.mu.nodes[fileNum]; n != nil {
		c.releaseNode(n)
	}
	c.mu.Unlock()

	c.opts.Cache.EvictFile(c.dbNum, fileNum)
}

func (c *tableCacheShard) recordHits(hits []*tableCacheNode) {
	c.mu.Lock()
	c.recordHitsLocked(hits)
	c.mu.Unlock()
}

func (c *tableCacheShard) recordHitsLocked(hits []*tableCacheNode) {
	for _, n := range hits {
		if n.next == nil || n.prev == nil {
			// The node is no longer on the LRU list.
			continue
		}
		// Remove n from the doubly-linked list.
		n.next.prev = n.prev
		n.prev.next = n.next
		// Insert n at the front of the doubly-linked list.
		n.next = c.mu.lru.next
		n.prev = &c.mu.lru
		n.next.prev = n
		n.prev.next = n
	}
}

func (c *tableCacheShard) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if v := atomic.LoadInt32(&c.iterCount); v > 0 {
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

	for n := c.mu.lru.next; n != &c.mu.lru; n = n.next {
		if atomic.AddInt32(&n.refCount, -1) == 0 {
			c.releasing.Add(1)
			go n.release(c)
		}
	}
	c.mu.nodes = nil
	c.mu.lru.next = nil
	c.mu.lru.prev = nil

	c.releasing.Wait()
	return nil
}

type tableCacheNode struct {
	closeHook func(i sstable.Iterator) error

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
	f, err := c.fs.Open(dbFilename(c.dirname, fileTypeTable, n.meta.fileNum), vfs.RandomReadsOption)
	if err != nil {
		n.err = err
		close(n.loaded)
		return
	}
	r := sstable.NewReader(f, c.dbNum, n.meta.fileNum, c.opts)
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
	c.releasing.Done()
}

// tableCacheHits batches a set of node accesses in order to amortize exclusive
// lock acquisition.
type tableCacheHits struct {
	hits  []*tableCacheNode
	shard *tableCacheShard
}

func (f *tableCacheHits) recordHit(n *tableCacheNode) {
	f.hits = append(f.hits, n)
	if len(f.hits) == cap(f.hits) {
		f.shard.recordHits(f.hits)
		f.hits = f.hits[:0]
	}
}

func (f *tableCacheHits) flushLocked() {
	f.shard.recordHitsLocked(f.hits)
	f.hits = f.hits[:0]
}
