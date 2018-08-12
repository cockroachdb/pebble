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
)

type pageType int8

const (
	ptTest pageType = iota
	ptCold
	ptHot
)

func (p pageType) String() string {
	switch p {
	case ptTest:
		return "test"
	case ptCold:
		return "cold"
	case ptHot:
		return "hot"
	}
	return "unknown"
}

type key struct {
	fileNum uint64
	offset  uint64
}

type entry struct {
	key   key
	val   []byte
	next  *entry
	prev  *entry
	size  int64
	ptype pageType
	ref   bool
}

func (e *entry) init() *entry {
	e.next = e
	e.prev = e
	return e
}

func (e *entry) Next() *entry {
	if e.next == nil {
		return e.init()
	}
	return e.next
}

func (e *entry) Prev() *entry {
	if e.prev == nil {
		return e.init()
	}
	return e.prev
}

func (e *entry) Link(s *entry) *entry {
	n := e.Next()
	if s != nil {
		p := s.Prev()
		// Note: Cannot use multiple assignment because
		// evaluation order of LHS is not specified.
		e.next = s
		s.prev = e
		n.prev = p
		p.next = n
	}
	return n
}

func (e *entry) Move(n int) *entry {
	if e.next == nil {
		return e.init()
	}
	switch {
	case n < 0:
		for ; n < 0; n++ {
			e = e.prev
		}
	case n > 0:
		for ; n > 0; n-- {
			e = e.next
		}
	}
	return e
}

func (e *entry) Unlink(n int) *entry {
	if n <= 0 {
		return nil
	}
	return e.Link(e.Move(n + 1))
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

// New ...
func New(size int64) *Cache {
	return &Cache{
		maxSize:  size,
		coldSize: size,
		keys:     make(map[key]*entry),
	}
}

// Get ...
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
	if e.val == nil {
		return nil
	}
	e.ref = true
	return e.val
}

// Set ...
func (c *Cache) Set(fileNum, offset uint64, value []byte) {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	k := key{fileNum: fileNum, offset: offset}
	e := c.keys[k]
	if e == nil {
		// no cache entry? add it
		e = &entry{val: value, ptype: ptCold, key: k, size: int64(len(value))}
		c.metaAdd(k, e)
		c.countCold += e.size
		return
	}

	if e.val != nil {
		// cache entry was a hot or cold page
		e.val = value
		e.ref = true
		delta := int64(len(value)) - e.size
		e.size = int64(len(value))
		if e.ptype == ptHot {
			c.countHot += delta
		} else {
			c.countCold += delta
		}
		c.evict()
		return
	}

	// cache entry was a test page
	c.coldSize += e.size
	if c.coldSize > c.maxSize {
		c.coldSize = c.maxSize
	}
	e.ref = false
	e.val = value
	e.ptype = ptHot
	c.countTest -= e.size
	c.metaDel(e)
	c.metaAdd(k, e)
	c.countHot += e.size
}

func (c *Cache) metaAdd(key key, e *entry) {
	c.evict()

	c.keys[key] = e
	e.Link(c.handHot)

	if c.handHot == nil {
		// first element
		c.handHot = e
		c.handCold = e
		c.handTest = e
	}

	if c.handCold == c.handHot {
		c.handCold = c.handCold.Prev()
	}
}

func (c *Cache) metaDel(e *entry) {
	delete(c.keys, e.key)

	if e == c.handHot {
		c.handHot = c.handHot.Prev()
	}
	if e == c.handCold {
		c.handCold = c.handCold.Prev()
	}
	if e == c.handTest {
		c.handTest = c.handTest.Prev()
	}

	e.Prev().Unlink(1)
}

func (c *Cache) evict() {
	for c.maxSize <= c.countHot+c.countCold {
		c.runHandCold()
	}
}

func (c *Cache) runHandCold() {
	e := c.handCold
	if e.ptype == ptCold {
		if e.ref {
			e.ref = false
			e.ptype = ptHot
			c.countCold -= e.size
			c.countHot += e.size
		} else {
			e.val = nil
			e.ptype = ptTest
			c.countCold -= e.size
			c.countTest += e.size
			for c.maxSize < c.countTest {
				c.runHandTest()
			}
		}
	}

	c.handCold = c.handCold.Next()

	for c.maxSize-c.coldSize <= c.countHot {
		c.runHandHot()
	}
}

func (c *Cache) runHandHot() {
	if c.handHot == c.handTest {
		c.runHandTest()
	}

	e := c.handHot
	if e.ptype == ptHot {
		if e.ref {
			e.ref = false
		} else {
			e.ptype = ptCold
			c.countHot -= e.size
			c.countCold += e.size
		}
	}

	c.handHot = c.handHot.Next()
}

func (c *Cache) runHandTest() {
	if c.handTest == c.handCold {
		c.runHandCold()
	}

	e := c.handTest
	if e.ptype == ptTest {
		prev := c.handTest.Prev()
		c.metaDel(c.handTest)
		c.handTest = prev

		c.countTest -= e.size
		c.coldSize -= e.size
		if c.coldSize < 0 {
			c.coldSize = 0
		}
	}

	c.handTest = c.handTest.Next()
}
