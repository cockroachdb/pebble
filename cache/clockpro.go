// Copyright 2018. All rights reserved. Use of this source code is governed by
// an MIT-style license that can be found in the LICENSE file.

// Package cache implements the CLOCK-Pro caching algorithm.
/*

CLOCK-Pro is a patent-free alternative to the Adaptive Replacement Cache,
https://en.wikipedia.org/wiki/Adaptive_replacement_cache.
It is an approximation of LIRS ( https://en.wikipedia.org/wiki/LIRS_caching_algorithm ),
much like the CLOCK page replacement algorithm is an approximation of LRU.

This implementation is based on the python code from https://bitbucket.org/SamiLehtinen/pyclockpro .

Slides describing the algorithm: http://fr.slideshare.net/huliang64/clockpro

The original paper: http://static.usenix.org/event/usenix05/tech/general/full_papers/jiang/jiang_html/html.html

It is MIT licensed, like the original.
*/
package cache

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

type entryType int8

const (
	etTest entryType = iota
	etCold
	etHot
)

func (p entryType) String() string {
	switch p {
	case etTest:
		return "test"
	case etCold:
		return "cold"
	case etHot:
		return "hot"
	}
	return "unknown"
}

type key struct {
	fileNum uint64
	offset  uint64
}

type value struct {
	ptr unsafe.Pointer
}

func (v *value) set(b []byte) {
	atomic.StorePointer(&v.ptr, unsafe.Pointer(&b))
}

func (v *value) get() []byte {
	p := (*[]byte)(atomic.LoadPointer(&v.ptr))
	if p == nil {
		return nil
	}
	return *p
}

type entry struct {
	key      key
	val      value
	nextLink *entry
	prevLink *entry
	size     int64
	ptype    entryType
	ref      int32
}

func (e *entry) init() *entry {
	e.nextLink = e
	e.prevLink = e
	return e
}

func (e *entry) next() *entry {
	if e.nextLink == nil {
		return e.init()
	}
	return e.nextLink
}

func (e *entry) prev() *entry {
	if e.prevLink == nil {
		return e.init()
	}
	return e.prevLink
}

func (e *entry) link(s *entry) *entry {
	n := e.next()
	if s != nil {
		p := s.prev()
		// Note: Cannot use multiple assignment because
		// evaluation order of LHS is not specified.
		e.nextLink = s
		s.prevLink = e
		n.prevLink = p
		p.nextLink = n
	}
	return n
}

func (e *entry) move(n int) *entry {
	if e.nextLink == nil {
		return e.init()
	}
	switch {
	case n < 0:
		for ; n < 0; n++ {
			e = e.prevLink
		}
	case n > 0:
		for ; n > 0; n-- {
			e = e.nextLink
		}
	}
	return e
}

func (e *entry) unlink(n int) *entry {
	if n <= 0 {
		return nil
	}
	return e.link(e.move(n + 1))
}

func (e *entry) Get() []byte {
	b := e.val.get()
	if b == nil {
		return nil
	}
	atomic.StoreInt32(&e.ref, 1)
	return b
}

// WeakHandle provides a "weak" reference to an entry in the cache. A weak
// reference allows the entry to be evicted, but also provides fast access
type WeakHandle interface {
	// Get retrieves the value associated with the weak handle, returning nil if
	// no value is present.
	Get() []byte
}

// Cache ...
type Cache struct {
	mu sync.Mutex

	maxSize  int64
	coldSize int64
	keys     map[key]*entry

	handHot  *entry
	handCold *entry
	handTest *entry

	countHot  int64
	countCold int64
	countTest int64
}

// New creates a new cache of the specified size. Memory for the cache is
// allocated on demand, not during initialization.
func New(size int64) *Cache {
	return &Cache{
		maxSize:  size,
		coldSize: size,
		keys:     make(map[key]*entry),
	}
}

// Get retrieves the cache value for the specified file and offset, returning
// nil if no value is present.
func (c *Cache) Get(fileNum, offset uint64) []byte {
	if c == nil {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	e := c.keys[key{fileNum: fileNum, offset: offset}]
	if e == nil {
		return nil
	}
	return e.Get()
}

// Set sets the cache value for the specified file and offset, overwriting an
// existing value if present. A WeakHandle is returned which provides faster
// retrieval of the cached value than Get (lock-free and avoidance of the map
// lookup).
func (c *Cache) Set(fileNum, offset uint64, value []byte) WeakHandle {
	if c == nil {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	k := key{fileNum: fileNum, offset: offset}
	e := c.keys[k]
	if e == nil {
		// no cache entry? add it
		e = &entry{ptype: etCold, key: k, size: int64(len(value))}
		e.val.set(value)
		c.metaAdd(k, e)
		c.countCold += e.size
		return e
	}

	if e.val.get() != nil {
		// cache entry was a hot or cold page
		e.val.set(value)
		atomic.StoreInt32(&e.ref, 1)
		delta := int64(len(value)) - e.size
		e.size = int64(len(value))
		if e.ptype == etHot {
			c.countHot += delta
		} else {
			c.countCold += delta
		}
		c.evict()
		return e
	}

	// cache entry was a test page
	c.coldSize += e.size
	if c.coldSize > c.maxSize {
		c.coldSize = c.maxSize
	}
	atomic.StoreInt32(&e.ref, 0)
	e.val.set(value)
	e.ptype = etHot
	c.countTest -= e.size
	c.metaDel(e)
	c.metaAdd(k, e)
	c.countHot += e.size
	return e
}

// EvictFiles evicts all of the cache values for the specified files. This is
// expensive as it walks over the entire cache.
//
// TODO(peter): Is this too expensive?
func (c *Cache) EvictFiles(files map[uint64]struct{}) {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	for k, e := range c.keys {
		if _, ok := files[k.fileNum]; !ok {
			continue
		}
		switch e.ptype {
		case etHot:
			c.countHot -= e.size
		case etCold:
			c.countCold -= e.size
		case etTest:
			c.countTest -= e.size
		}
		c.metaDel(e)
	}
}

// MaxSize returns the max size of the cache.
func (c *Cache) MaxSize() int64 {
	if c == nil {
		return 0
	}
	return c.maxSize
}

// Size returns the current space used by the cache.
func (c *Cache) Size() int64 {
	c.mu.Lock()
	size := c.countHot + c.countCold
	c.mu.Unlock()
	return size
}

func (c *Cache) metaAdd(key key, e *entry) {
	c.evict()

	c.keys[key] = e
	e.link(c.handHot)

	if c.handHot == nil {
		// first element
		c.handHot = e
		c.handCold = e
		c.handTest = e
	}

	if c.handCold == c.handHot {
		c.handCold = c.handCold.prev()
	}
}

func (c *Cache) metaDel(e *entry) {
	delete(c.keys, e.key)

	if e == c.handHot {
		c.handHot = c.handHot.prev()
	}
	if e == c.handCold {
		c.handCold = c.handCold.prev()
	}
	if e == c.handTest {
		c.handTest = c.handTest.prev()
	}

	e.prev().unlink(1)
}

func (c *Cache) evict() {
	for c.maxSize <= c.countHot+c.countCold {
		c.runHandCold()
	}
}

func (c *Cache) runHandCold() {
	e := c.handCold
	if e.ptype == etCold {
		if atomic.LoadInt32(&e.ref) == 1 {
			atomic.StoreInt32(&e.ref, 0)
			e.ptype = etHot
			c.countCold -= e.size
			c.countHot += e.size
		} else {
			e.val.set(nil)
			e.ptype = etTest
			c.countCold -= e.size
			c.countTest += e.size
			for c.maxSize < c.countTest {
				c.runHandTest()
			}
		}
	}

	c.handCold = c.handCold.next()

	for c.maxSize-c.coldSize <= c.countHot {
		c.runHandHot()
	}
}

func (c *Cache) runHandHot() {
	if c.handHot == c.handTest {
		c.runHandTest()
	}

	e := c.handHot
	if e.ptype == etHot {
		if atomic.LoadInt32(&e.ref) == 1 {
			atomic.StoreInt32(&e.ref, 0)
		} else {
			e.ptype = etCold
			c.countHot -= e.size
			c.countCold += e.size
		}
	}

	c.handHot = c.handHot.next()
}

func (c *Cache) runHandTest() {
	if c.countCold > 0 && c.handTest == c.handCold {
		c.runHandCold()
	}

	e := c.handTest
	if e.ptype == etTest {
		prev := c.handTest.prev()
		c.metaDel(c.handTest)
		c.handTest = prev

		c.countTest -= e.size
		c.coldSize -= e.size
		if c.coldSize < 0 {
			c.coldSize = 0
		}
	}

	c.handTest = c.handTest.next()
}
