// Copyright 2018. All rights reserved. Use of this source code is governed by
// an MIT-style license that can be found in the LICENSE file.

// Package cache implements the CLOCK-Pro caching algorithm.
//
// CLOCK-Pro is a patent-free alternative to the Adaptive Replacement Cache,
// https://en.wikipedia.org/wiki/Adaptive_replacement_cache.
// It is an approximation of LIRS ( https://en.wikipedia.org/wiki/LIRS_caching_algorithm ),
// much like the CLOCK page replacement algorithm is an approximation of LRU.
//
// This implementation is based on the python code from https://bitbucket.org/SamiLehtinen/pyclockpro .
//
// Slides describing the algorithm: http://fr.slideshare.net/huliang64/clockpro
//
// The original paper: http://static.usenix.org/event/usenix05/tech/general/full_papers/jiang/jiang_html/html.html
//
// It is MIT licensed, like the original.
package cache // import "github.com/cockroachdb/pebble/internal/cache"

import (
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
)

// key is associated with a specific block.
type key struct {
	// id is the namespace for fileNums.
	id      handleID
	fileNum base.DiskFileNum
	offset  uint64
}

func makeKey(id handleID, fileNum base.DiskFileNum, offset uint64) key {
	return key{
		id:      id,
		fileNum: fileNum,
		offset:  offset,
	}
}

// shardIdx determines the shard index for the given key.
func (k *key) shardIdx(numShards int) int {
	if k.id == 0 {
		panic("pebble: 0 cache handleID is invalid")
	}
	// Same as fibonacciHash() but without the cast to uintptr.
	const m = 11400714819323198485
	h := uint64(k.id) * m
	h ^= uint64(k.fileNum) * m
	h ^= k.offset * m

	// We need a 32-bit value below; we use the upper bits as per
	// https://probablydance.com/2018/06/16/fibonacci-hashing-the-optimization-that-the-world-forgot-or-a-better-alternative-to-integer-modulo/
	h >>= 32

	// This is a better alternative to (h % numShards); see
	// https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
	return int(h * uint64(numShards) >> 32)
}

// file returns the "file key" for the receiver. This is the key used for the
// shard.files map.
func (k key) file() key {
	k.offset = 0
	return k
}

func (k key) String() string {
	return fmt.Sprintf("%d/%d/%d", k.id, k.fileNum, k.offset)
}

type shard struct {
	hits   atomic.Int64
	misses atomic.Int64

	mu sync.RWMutex

	reservedSize int64
	maxSize      int64
	coldTarget   int64
	blocks       blockMap // fileNum+offset -> block
	files        blockMap // fileNum -> list of blocks

	// The blocks and files maps store values in manually managed memory that is
	// invisible to the Go GC. This is fine for Value and entry objects that are
	// stored in manually managed memory, but when the "invariants" build tag is
	// set, all Value and entry objects are Go allocated and the entries map will
	// contain a reference to every entry.
	entries map[*entry]struct{}

	handHot  *entry
	handCold *entry
	handTest *entry

	sizeHot  int64
	sizeCold int64
	sizeTest int64

	// The count fields are used exclusively for asserting expectations.
	// We've seen infinite looping (cockroachdb/cockroach#70154) that
	// could be explained by a corrupted sizeCold. Through asserting on
	// these fields, we hope to gain more insight from any future
	// reproductions.
	countHot  int64
	countCold int64
	countTest int64

	// Some fields in readShard are protected by mu. See comments in declaration
	// of readShard.
	readShard readShard
}

func (c *shard) init(maxSize int64) {
	*c = shard{
		maxSize:    maxSize,
		coldTarget: maxSize,
	}
	if entriesCanBeGoAllocated && entriesGoAllocated {
		c.entries = make(map[*entry]struct{})
	}
	c.blocks.Init(16)
	c.files.Init(16)
	c.readShard.Init(c)
}

// getWithMaybeReadEntry is the internal helper for implementing
// Cache.{Get,GetWithReadHandle}. When desireReadEntry is true, and the block
// is not in the cache (nil Value), a non-nil readEntry is returned (in which
// case the caller is responsible to dereference the entry, via one of
// unrefAndTryRemoveFromMap(), setReadValue(), setReadError()).
func (c *shard) getWithMaybeReadEntry(k key, desireReadEntry bool) (*Value, *readEntry) {
	c.mu.RLock()
	var value *Value
	if e, _ := c.blocks.Get(k); e != nil {
		value = e.acquireValue()
		// Note: we Load first to avoid an atomic XCHG when not necessary.
		if value != nil && !e.referenced.Load() {
			e.referenced.Store(true)
		}
	}
	var re *readEntry
	if value == nil && desireReadEntry {
		re = c.readShard.acquireReadEntry(k)
	}
	c.mu.RUnlock()
	if value == nil {
		c.misses.Add(1)
	} else {
		c.hits.Add(1)
	}
	return value, re
}

func (c *shard) set(k key, value *Value, markAccessed bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	e, _ := c.blocks.Get(k)

	switch {
	case e == nil:
		// no cache entry? add it
		e = newEntry(k, int64(len(value.buf)))
		e.setValue(value)
		if markAccessed {
			e.referenced.Store(true)
		}
		if c.metaAdd(k, e) {
			value.ref.trace("add-cold")
			c.sizeCold += e.size
			c.countCold++
		} else {
			value.ref.trace("skip-cold")
			e.free()
			e = nil
		}

	case e.val != nil:
		// cache entry was a hot or cold page
		e.setValue(value)
		e.referenced.Store(true)
		delta := int64(len(value.buf)) - e.size
		e.size = int64(len(value.buf))
		if e.ptype == etHot {
			value.ref.trace("add-hot")
			c.sizeHot += delta
		} else {
			// TODO(sumeer): unclear why we don't set e.ptype to etHot on this path.
			// In the default case below, where the state is etTest we set it to
			// etHot. But etTest is "colder" than etCold, since the only transition
			// into etTest is etCold => etTest, so since etTest transitions to
			// etHot, then etCold should also transition.
			value.ref.trace("add-cold")
			c.sizeCold += delta
		}
		c.evict()

	default:
		// cache entry was a test page
		c.sizeTest -= e.size
		c.countTest--
		v := c.metaDel(e)
		if invariants.Enabled && v != nil {
			panic("value should be nil")
		}
		c.metaCheck(e)

		e.size = int64(len(value.buf))
		c.coldTarget += e.size
		if c.coldTarget > c.targetSize() {
			c.coldTarget = c.targetSize()
		}

		e.referenced.Store(markAccessed)
		e.setValue(value)
		e.ptype = etHot
		if c.metaAdd(k, e) {
			value.ref.trace("add-hot")
			c.sizeHot += e.size
			c.countHot++
		} else {
			value.ref.trace("skip-hot")
			e.free()
			e = nil
		}
	}

	c.checkConsistency()
}

func (c *shard) checkConsistency() {
	// See the comment above the count{Hot,Cold,Test} fields.
	switch {
	case c.sizeHot < 0 || c.sizeCold < 0 || c.sizeTest < 0 || c.countHot < 0 || c.countCold < 0 || c.countTest < 0:
		panic(fmt.Sprintf("pebble: unexpected negative: %d (%d bytes) hot, %d (%d bytes) cold, %d (%d bytes) test",
			c.countHot, c.sizeHot, c.countCold, c.sizeCold, c.countTest, c.sizeTest))
	case c.sizeHot > 0 && c.countHot == 0:
		panic(fmt.Sprintf("pebble: mismatch %d hot size, %d hot count", c.sizeHot, c.countHot))
	case c.sizeCold > 0 && c.countCold == 0:
		panic(fmt.Sprintf("pebble: mismatch %d cold size, %d cold count", c.sizeCold, c.countCold))
	case c.sizeTest > 0 && c.countTest == 0:
		panic(fmt.Sprintf("pebble: mismatch %d test size, %d test count", c.sizeTest, c.countTest))
	}
}

// Delete deletes the cached value for the specified file and offset.
func (c *shard) delete(k key) {
	// The common case is there is nothing to delete, so do a quick check with
	// shared lock.
	c.mu.RLock()
	_, exists := c.blocks.Get(k)
	c.mu.RUnlock()
	if !exists {
		return
	}

	var deletedValue *Value
	func() {
		c.mu.Lock()
		defer c.mu.Unlock()

		e, _ := c.blocks.Get(k)
		if e == nil {
			return
		}
		deletedValue = c.metaEvict(e)
		c.checkConsistency()
	}()
	// Now that the mutex has been dropped, release the reference which will
	// potentially free the memory associated with the previous cached value.
	deletedValue.Release()
}

// EvictFile evicts all of the cache values for the specified file.
func (c *shard) evictFile(id handleID, fileNum base.DiskFileNum) {
	fkey := makeKey(id, fileNum, 0)
	for c.evictFileRun(fkey) {
		// Sched switch to give another goroutine an opportunity to acquire the
		// shard mutex.
		runtime.Gosched()
	}
}

func (c *shard) evictFileRun(fkey key) (moreRemaining bool) {
	// If most of the file's blocks are held in the block cache, evicting all
	// the blocks may take a while. We don't want to block the entire cache
	// shard, forcing concurrent readers to wait until we're finished. We drop
	// the mutex every [blocksPerMutexAcquisition] blocks to give other
	// goroutines an opportunity to make progress.
	const blocksPerMutexAcquisition = 5
	c.mu.Lock()

	// Releasing a value may result in free-ing it back to the memory allocator.
	// This can have a nontrivial cost that we'd prefer to not pay while holding
	// the shard mutex, so we collect the evicted values in a local slice and
	// only release them in a defer after dropping the cache mutex.
	var obsoleteValuesAlloc [blocksPerMutexAcquisition]*Value
	obsoleteValues := obsoleteValuesAlloc[:0]
	defer func() {
		c.mu.Unlock()
		for _, v := range obsoleteValues {
			v.Release()
		}
	}()

	blocks, _ := c.files.Get(fkey)
	if blocks == nil {
		// No blocks for this file.
		return false
	}

	// b is the current head of the doubly linked list, and n is the entry after b.
	for b, n := blocks, (*entry)(nil); len(obsoleteValues) < cap(obsoleteValues); b = n {
		n = b.fileLink.next
		obsoleteValues = append(obsoleteValues, c.metaEvict(b))
		if b == n {
			// b == n represents the case where b was the last entry remaining
			// in the doubly linked list, which is why it pointed at itself. So
			// no more entries left.
			c.checkConsistency()
			return false
		}
	}
	// Exhausted blocksPerMutexAcquisition.
	return true
}

func (c *shard) Free() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// NB: we use metaDel rather than metaEvict in order to avoid the expensive
	// metaCheck call when the "invariants" build tag is specified.
	for c.handHot != nil {
		e := c.handHot
		c.metaDel(c.handHot).Release()
		e.free()
	}

	c.blocks.Close()
	c.files.Close()
}

func (c *shard) Reserve(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reservedSize += int64(n)

	// Changing c.reservedSize will either increase or decrease
	// the targetSize. But we want coldTarget to be in the range
	// [0, targetSize]. So, if c.targetSize decreases, make sure
	// that the coldTarget fits within the limits.
	targetSize := c.targetSize()
	if c.coldTarget > targetSize {
		c.coldTarget = targetSize
	}

	c.evict()
	c.checkConsistency()
}

// Size returns the current space used by the cache.
func (c *shard) Size() int64 {
	c.mu.RLock()
	size := c.sizeHot + c.sizeCold
	c.mu.RUnlock()
	return size
}

func (c *shard) targetSize() int64 {
	target := c.maxSize - c.reservedSize
	// Always return a positive integer for targetSize. This is so that we don't
	// end up in an infinite loop in evict(), in cases where reservedSize is
	// greater than or equal to maxSize.
	if target < 1 {
		return 1
	}
	return target
}

// Add the entry to the cache, returning true if the entry was added and false
// if it would not fit in the cache.
func (c *shard) metaAdd(key key, e *entry) bool {
	c.evict()
	if e.size > c.targetSize() {
		// The entry is larger than the target cache size.
		return false
	}

	c.blocks.Put(key, e)
	if entriesCanBeGoAllocated && entriesGoAllocated {
		// Go allocated entries need to be referenced from Go memory. The entries
		// map provides that reference.
		c.entries[e] = struct{}{}
	}

	if c.handHot == nil {
		// first element
		c.handHot = e
		c.handCold = e
		c.handTest = e
	} else {
		c.handHot.link(e)
	}

	if c.handCold == c.handHot {
		c.handCold = c.handCold.prev()
	}

	fkey := key.file()
	if fileBlocks, _ := c.files.Get(fkey); fileBlocks == nil {
		c.files.Put(fkey, e)
	} else {
		fileBlocks.linkFile(e)
	}
	return true
}

// Remove the entry from the cache. This removes the entry from the blocks map,
// the files map, and ensures that hand{Hot,Cold,Test} are not pointing at the
// entry. Returns the deleted value that must be released, if any.
func (c *shard) metaDel(e *entry) (deletedValue *Value) {
	if value := e.val; value != nil {
		value.ref.trace("metaDel")
	}
	// Remove the pointer to the value.
	deletedValue = e.val
	e.val = nil

	c.blocks.Delete(e.key)
	if entriesCanBeGoAllocated && entriesGoAllocated {
		// Go allocated entries need to be referenced from Go memory. The entries
		// map provides that reference.
		delete(c.entries, e)
	}

	if e == c.handHot {
		c.handHot = c.handHot.prev()
	}
	if e == c.handCold {
		c.handCold = c.handCold.prev()
	}
	if e == c.handTest {
		c.handTest = c.handTest.prev()
	}

	if e.unlink() == e {
		// This was the last entry in the cache.
		c.handHot = nil
		c.handCold = nil
		c.handTest = nil
	}

	fkey := e.key.file()
	if next := e.unlinkFile(); e == next {
		c.files.Delete(fkey)
	} else {
		c.files.Put(fkey, next)
	}
	return deletedValue
}

// Check that the specified entry is not referenced by the cache.
func (c *shard) metaCheck(e *entry) {
	if invariants.Enabled && invariants.Sometimes(1) {
		if _, ok := c.entries[e]; ok {
			fmt.Fprintf(os.Stderr, "%p: %s unexpectedly found in entries map\n%s",
				e, e.key, debug.Stack())
			os.Exit(1)
		}
		if c.blocks.findByValue(e) {
			fmt.Fprintf(os.Stderr, "%p: %s unexpectedly found in blocks map\n%#v\n%s",
				e, e.key, &c.blocks, debug.Stack())
			os.Exit(1)
		}
		if c.files.findByValue(e) {
			fmt.Fprintf(os.Stderr, "%p: %s unexpectedly found in files map\n%#v\n%s",
				e, e.key, &c.files, debug.Stack())
			os.Exit(1)
		}
		// NB: c.hand{Hot,Cold,Test} are pointers into a single linked list. We
		// only have to traverse one of them to check all of them.
		var countHot, countCold, countTest int64
		var sizeHot, sizeCold, sizeTest int64
		for t := c.handHot.next(); t != nil; t = t.next() {
			// Recompute count{Hot,Cold,Test} and size{Hot,Cold,Test}.
			switch t.ptype {
			case etHot:
				countHot++
				sizeHot += t.size
			case etCold:
				countCold++
				sizeCold += t.size
			case etTest:
				countTest++
				sizeTest += t.size
			}
			if e == t {
				fmt.Fprintf(os.Stderr, "%p: %s unexpectedly found in blocks list\n%s",
					e, e.key, debug.Stack())
				os.Exit(1)
			}
			if t == c.handHot {
				break
			}
		}
		if countHot != c.countHot || countCold != c.countCold || countTest != c.countTest ||
			sizeHot != c.sizeHot || sizeCold != c.sizeCold || sizeTest != c.sizeTest {
			fmt.Fprintf(os.Stderr, `divergence of Hot,Cold,Test statistics
				cache's statistics: hot %d, %d, cold %d, %d, test %d, %d
				recalculated statistics: hot %d, %d, cold %d, %d, test %d, %d\n%s`,
				c.countHot, c.sizeHot, c.countCold, c.sizeCold, c.countTest, c.sizeTest,
				countHot, sizeHot, countCold, sizeCold, countTest, sizeTest,
				debug.Stack())
			os.Exit(1)
		}
	}
}

func (c *shard) metaEvict(e *entry) (evictedValue *Value) {
	switch e.ptype {
	case etHot:
		c.sizeHot -= e.size
		c.countHot--
	case etCold:
		c.sizeCold -= e.size
		c.countCold--
	case etTest:
		c.sizeTest -= e.size
		c.countTest--
	}
	evictedValue = c.metaDel(e)
	c.metaCheck(e)
	e.free()
	return evictedValue
}

func (c *shard) evict() {
	for c.targetSize() <= c.sizeHot+c.sizeCold && c.handCold != nil {
		c.runHandCold(c.countCold, c.sizeCold)
	}
}

func (c *shard) runHandCold(countColdDebug, sizeColdDebug int64) {
	// countColdDebug and sizeColdDebug should equal c.countCold and
	// c.sizeCold. They're parameters only to aid in debugging of
	// cockroachdb/cockroach#70154. Since they're parameters, their
	// arguments will appear within stack traces should we encounter
	// a reproduction.
	if c.countCold != countColdDebug || c.sizeCold != sizeColdDebug {
		panic(fmt.Sprintf("runHandCold: cold count and size are %d, %d, arguments are %d and %d",
			c.countCold, c.sizeCold, countColdDebug, sizeColdDebug))
	}

	e := c.handCold
	if e.ptype == etCold {
		if e.referenced.Load() {
			e.referenced.Store(false)
			e.ptype = etHot
			c.sizeCold -= e.size
			c.countCold--
			c.sizeHot += e.size
			c.countHot++
		} else {
			e.setValue(nil)
			e.ptype = etTest
			c.sizeCold -= e.size
			c.countCold--
			c.sizeTest += e.size
			c.countTest++
			for c.targetSize() < c.sizeTest && c.handTest != nil {
				c.runHandTest()
			}
		}
	}

	c.handCold = c.handCold.next()

	for c.targetSize()-c.coldTarget <= c.sizeHot && c.handHot != nil {
		c.runHandHot()
	}
}

func (c *shard) runHandHot() {
	if c.handHot == c.handTest && c.handTest != nil {
		c.runHandTest()
		if c.handHot == nil {
			return
		}
	}

	e := c.handHot
	if e.ptype == etHot {
		if e.referenced.Load() {
			e.referenced.Store(false)
		} else {
			e.ptype = etCold
			c.sizeHot -= e.size
			c.countHot--
			c.sizeCold += e.size
			c.countCold++
		}
	}

	c.handHot = c.handHot.next()
}

func (c *shard) runHandTest() {
	if c.sizeCold > 0 && c.handTest == c.handCold && c.handCold != nil {
		// sizeCold is > 0, so assert that countCold == 0. See the
		// comment above count{Hot,Cold,Test}.
		if c.countCold == 0 {
			panic(fmt.Sprintf("pebble: mismatch %d cold size, %d cold count", c.sizeCold, c.countCold))
		}

		c.runHandCold(c.countCold, c.sizeCold)
		if c.handTest == nil {
			return
		}
	}

	e := c.handTest
	if e.ptype == etTest {
		c.sizeTest -= e.size
		c.countTest--
		c.coldTarget -= e.size
		if c.coldTarget < 0 {
			c.coldTarget = 0
		}
		c.metaDel(e).Release()
		c.metaCheck(e)
		e.free()
	}

	c.handTest = c.handTest.next()
}
