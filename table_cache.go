// Copyright 2013 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/private"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

const defaultTableCacheHitBuffer = 64

var emptyIter = &errorIter{err: nil}

var tableCacheLabels = pprof.Labels("pebble", "table-cache")

type tableCache struct {
	cache         *Cache
	shards        []tableCacheShard
	filterMetrics FilterMetrics
}

func (c *tableCache) init(
	cacheID uint64, dirname string, fs vfs.FS, opts *Options, size, hitBuffer int,
) {
	c.cache = opts.Cache
	c.cache.Ref()

	c.shards = make([]tableCacheShard, runtime.NumCPU())
	for i := range c.shards {
		c.shards[i].init(cacheID, dirname, fs, opts, size/len(c.shards), hitBuffer)
		c.shards[i].filterMetrics = &c.filterMetrics
	}
}

func (c *tableCache) getShard(fileNum uint64) *tableCacheShard {
	return &c.shards[fileNum%uint64(len(c.shards))]
}

func (c *tableCache) newIters(
	meta *fileMetadata, opts *IterOptions, bytesIterated *uint64,
) (internalIterator, internalIterator, error) {
	return c.getShard(meta.FileNum).newIters(meta, opts, bytesIterated)
}

func (c *tableCache) evict(fileNum uint64) {
	c.getShard(fileNum).evict(fileNum)
}

func (c *tableCache) metrics() (CacheMetrics, FilterMetrics) {
	var m CacheMetrics
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.RLock()
		m.Count += int64(len(s.mu.nodes))
		s.mu.RUnlock()
		m.Hits += atomic.LoadInt64(&s.hits)
		m.Misses += atomic.LoadInt64(&s.misses)
	}
	m.Size = m.Count * int64(unsafe.Sizeof(sstable.Reader{}))
	f := FilterMetrics{
		Hits:   atomic.LoadInt64(&c.filterMetrics.Hits),
		Misses: atomic.LoadInt64(&c.filterMetrics.Misses),
	}
	return m, f
}

// Assumes there is at least partial overlap, i.e., `[start, end]` falls neither
// completely before nor completely after the file's range.
func (c *tableCache) estimateDiskUsage(meta *fileMetadata, start, end []byte) (uint64, error) {
	return c.getShard(meta.FileNum).estimateDiskUsage(meta, start, end)
}

func (c *tableCache) iterCount() int64 {
	var n int64
	for i := range c.shards {
		n += int64(atomic.LoadInt32(&c.shards[i].iterCount))
	}
	return n
}

func (c *tableCache) Close() error {
	var err error
	for i := range c.shards {
		err = firstError(err, c.shards[i].Close())
	}
	c.cache.Unref()
	return err
}

type tableCacheShard struct {
	logger  Logger
	cacheID uint64
	dirname string
	fs      vfs.FS
	opts    sstable.ReaderOptions
	size    int

	mu struct {
		sync.RWMutex
		nodes map[uint64]*tableCacheNode
		// The iters map is only created and populated in race builds.
		iters map[sstable.Iterator][]byte
		lru   tableCacheNode
	}

	hits          int64
	misses        int64
	iterCount     int32
	releasing     sync.WaitGroup
	hitsPool      *sync.Pool
	filterMetrics *FilterMetrics
}

func (c *tableCacheShard) init(
	cacheID uint64, dirname string, fs vfs.FS, opts *Options, size, hitBuffer int,
) {
	c.logger = opts.Logger
	c.cacheID = cacheID
	c.dirname = dirname
	c.fs = fs
	c.opts = opts.MakeReaderOptions()
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

	if invariants.RaceEnabled {
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
		c.unrefNode(n)
		return emptyIter, nil, nil
	}
	var iter sstable.Iterator
	if bytesIterated != nil {
		iter = n.reader.NewCompactionIter(bytesIterated)
	} else {
		iter = n.reader.NewIter(opts.GetLowerBound(), opts.GetUpperBound())
	}
	atomic.AddInt32(&c.iterCount, 1)
	if invariants.RaceEnabled {
		c.mu.Lock()
		c.mu.iters[iter] = debug.Stack()
		c.mu.Unlock()
	}
	iter.SetCloseHook(n.closeHook)
	// Check for errors during iterator creation.
	if iter.Error() != nil {
		// `Close()` returns the same error as `Error()`.
		c.unrefNode(n)
		return nil, nil, iter.Close()
	}

	// NB: range-del iterator does not maintain a reference to the table, nor
	// does it need to read from it after creation.
	rangeDelIter, err := n.reader.NewRangeDelIter()
	if err != nil {
		iter.Close()
		c.unrefNode(n)
		return nil, nil, err
	}
	if rangeDelIter != nil {
		return iter, rangeDelIter, nil
	}
	// NB: Translate a nil range-del iterator into a nil interface.
	return iter, nil, nil
}

// releaseNode releases a node from the tableCacheShard.
//
// c.mu must be held when calling this.
func (c *tableCacheShard) releaseNode(n *tableCacheNode) {
	c.unlinkNode(n)
	c.unrefNode(n)
}

// unlinkNode removes a node from the tableCacheShard, leaving the shard
// reference in place.
//
// c.mu must be held when calling this.
func (c *tableCacheShard) unlinkNode(n *tableCacheNode) {
	delete(c.mu.nodes, n.meta.FileNum)
	n.next.prev = n.prev
	n.prev.next = n.next
	n.prev = nil
	n.next = nil
}

// unrefNode decrements the reference count for the specified node, releasing
// it if the reference count fell to 0. Note that the node has a reference if
// it is present in tableCacheShard.mu.nodes, so a reference count of 0 means the
// node has already been removed from that map.
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
	if n := c.mu.nodes[meta.FileNum]; n != nil {
		// Fast-path hit.

		// The caller is responsible for decrementing the refCount.
		atomic.AddInt32(&n.refCount, 1)
		c.mu.RUnlock()
		atomic.AddInt64(&c.hits, 1)

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

	n := c.mu.nodes[meta.FileNum]
	missed := n == nil
	if missed {
		// Slow-path miss.
		atomic.AddInt64(&c.misses, 1)
		n = &tableCacheNode{
			// Cache the closure invoked when an iterator is closed. This avoids an
			// allocation on every call to newIters.
			closeHook: func(i sstable.Iterator) error {
				if invariants.RaceEnabled {
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
		c.mu.nodes[meta.FileNum] = n
		if len(c.mu.nodes) > c.size {
			// Release the tail node.
			c.releaseNode(c.mu.lru.prev)
		}
	} else {
		// Slow-path hit.
		atomic.AddInt64(&c.hits, 1)
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
	if missed {
		// Note adding to the doubly-linked list must complete before we begin
		// asynchronously loading a `tableCacheNode`. This is because the load's
		// failure handling unlinks the node.
		go func() {
			pprof.Do(context.Background(), tableCacheLabels, func(context.Context) {
				n.load(c)
			})
		}()
	}
	return n
}

func (c *tableCacheShard) evict(fileNum uint64) {
	c.mu.Lock()

	n := c.mu.nodes[fileNum]
	if n != nil {
		// NB: This is equivalent to tableCacheShard.releaseNode(), but we perform
		// the tableCacheNode.release() call synchronously below to ensure the
		// sstable file descriptor is closed before returning. Note that
		// tableCacheShard.releasing needs to be incremented while holding
		// tableCacheShard.mu in order to avoid a race with Close()
		c.unlinkNode(n)
		if v := atomic.AddInt32(&n.refCount, -1); v != 0 {
			c.logger.Fatalf("table refcount is not zero: %d", v)
		}
		c.releasing.Add(1)
	}

	c.mu.Unlock()

	if n != nil {
		n.release(c)
	}

	c.opts.Cache.EvictFile(c.cacheID, fileNum)
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

// Assumes there is at least partial overlap, i.e., `[start, end]` falls neither
// completely before nor completely after the file's range.
func (c *tableCacheShard) estimateDiskUsage(meta *fileMetadata, start, end []byte) (uint64, error) {
	n := c.findNode(meta)
	<-n.loaded
	defer c.unrefNode(n)
	if n.err != nil {
		return 0, n.err
	}
	return n.reader.EstimateDiskUsage(start, end)
}

func (c *tableCacheShard) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check for leaked iterators. Note that we'll still perform cleanup below in
	// the case that there are leaked iterators.
	var err error
	if v := atomic.LoadInt32(&c.iterCount); v > 0 {
		if !invariants.RaceEnabled {
			err = fmt.Errorf("leaked iterators: %d", v)
		} else {
			var buf bytes.Buffer
			fmt.Fprintf(&buf, "leaked iterators: %d\n", v)
			for _, stack := range c.mu.iters {
				fmt.Fprintf(&buf, "%s\n", stack)
			}
			err = errors.New(buf.String())
		}
	}

	for n := c.mu.lru.next; n != &c.mu.lru; n = n.next {
		if atomic.AddInt32(&n.refCount, -1) == 0 {
			c.releasing.Add(1)
			go func(n *tableCacheNode) {
				pprof.Do(context.Background(), tableCacheLabels, func(context.Context) {
					n.release(c)
				})
			}(n)
		}
	}
	c.mu.nodes = nil
	c.mu.lru.next = nil
	c.mu.lru.prev = nil

	c.releasing.Wait()
	return err
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
	var f vfs.File
	f, n.err = c.fs.Open(base.MakeFilename(c.fs, c.dirname, fileTypeTable, n.meta.FileNum),
		vfs.RandomReadsOption)
	if n.err == nil {
		cacheOpts := private.SSTableCacheOpts(c.cacheID, n.meta.FileNum).(sstable.ReaderOption)
		n.reader, n.err = sstable.NewReader(f, c.opts, cacheOpts, c.filterMetrics)
	}
	if n.err == nil {
		if n.meta.SmallestSeqNum == n.meta.LargestSeqNum {
			n.reader.Properties.GlobalSeqNum = n.meta.LargestSeqNum
		}
	}
	if n.err != nil {
		c.mu.Lock()
		defer c.mu.Unlock()
		// Since loading happens outside the lock there is a chance that `releaseNode()`
		// has already been called. In that case, `n.next` will be nil.
		if n.next != nil {
			c.releaseNode(n)
		}
	}
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
