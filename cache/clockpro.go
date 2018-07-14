// Package cache implements the CLOCK-Pro caching algorithm.
/*

CLOCK-Pro is a patent-free alternative to the Adaptive Replacement Cache,
https://en.wikipedia.org/wiki/Adaptive_replacement_cache .
It is an approximation of LIRS ( https://en.wikipedia.org/wiki/LIRS_caching_algorithm ),
much like the CLOCK page replacement algorithm is an approximation of LRU.

This implementation is based on the python code from https://bitbucket.org/SamiLehtinen/pyclockpro .

Slides describing the algorithm: http://fr.slideshare.net/huliang64/clockpro

The original paper: http://static.usenix.org/event/usenix05/tech/general/full_papers/jiang/jiang_html/html.html

It is MIT licensed, like the original.
*/
package cache

import (
	"container/ring"
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

type entry2 struct {
	key   key
	val   interface{}
	link  ring.Ring
	ptype pageType
	ref   bool
}

// Cache ...
type Cache struct {
	mu sync.Mutex

	maxSize  int64
	coldSize int64
	keys     map[key]*entry2

	handHot  *ring.Ring
	handCold *ring.Ring
	handTest *ring.Ring

	countHot  int64
	countCold int64
	countTest int64
}

// New ...
func New(size int64) *Cache {
	return &Cache{
		maxSize:  size,
		coldSize: size,
		keys:     make(map[key]*entry2),
	}
}

// Get ...
func (c *Cache) Get(fileNum, offset uint64) interface{} {
	c.mu.Lock()
	defer c.mu.Unlock()

	k := key{FileNum: fileNum, Offset: offset}
	e := c.keys[k]
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
func (c *Cache) Set(fileNum, offset uint64, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	k := key{FileNum: fileNum, Offset: offset}
	e := c.keys[k]
	if e == nil {
		// no cache entry? add it
		e = &entry2{ref: false, val: value, ptype: ptCold, key: k}
		e.link.Value = e
		c.metaAdd(k, e)
		c.countCold++
		return
	}

	if e.val != nil {
		// cache entry was a hot or cold page
		e.val = value
		e.ref = true
		return
	}

	// cache entry was a test page
	if c.coldSize < c.maxSize {
		c.coldSize++
	}
	e.ref = false
	e.val = value
	e.ptype = ptHot
	c.countTest--
	c.metaDel(&e.link)
	c.metaAdd(k, e)
	c.countHot++
}

func (c *Cache) metaAdd(key key, e *entry2) {
	c.evict()

	c.keys[key] = e
	e.link.Link(c.handHot)

	if c.handHot == nil {
		// first element
		c.handHot = &e.link
		c.handCold = &e.link
		c.handTest = &e.link
	}

	if c.handCold == c.handHot {
		c.handCold = c.handCold.Prev()
	}
}

func (c *Cache) metaDel(r *ring.Ring) {
	delete(c.keys, r.Value.(*entry2).key)

	if r == c.handHot {
		c.handHot = c.handHot.Prev()
	}
	if r == c.handCold {
		c.handCold = c.handCold.Prev()
	}
	if r == c.handTest {
		c.handTest = c.handTest.Prev()
	}

	r.Prev().Unlink(1)
}

func (c *Cache) evict() {
	for c.maxSize <= c.countHot+c.countCold {
		c.runHandCold()
	}
}

func (c *Cache) runHandCold() {
	mentry := c.handCold.Value.(*entry2)
	if mentry.ptype == ptCold {
		if mentry.ref {
			mentry.ptype = ptHot
			mentry.ref = false
			c.countCold--
			c.countHot++
		} else {
			mentry.ptype = ptTest
			mentry.val = nil
			c.countCold--
			c.countTest++
			for c.maxSize < c.countTest {
				c.runHandTest()
			}
		}
	}

	c.handCold = c.handCold.Next()

	for c.maxSize-c.coldSize < c.countHot {
		c.runHandHot()
	}
}

func (c *Cache) runHandHot() {
	if c.handHot == c.handTest {
		c.runHandTest()
	}

	mentry := c.handHot.Value.(*entry2)
	if mentry.ptype == ptHot {
		if mentry.ref {
			mentry.ref = false
		} else {
			mentry.ptype = ptCold
			c.countHot--
			c.countCold++
		}
	}

	c.handHot = c.handHot.Next()
}

func (c *Cache) runHandTest() {
	if c.handTest == c.handCold {
		c.runHandCold()
	}

	mentry := c.handTest.Value.(*entry2)
	if mentry.ptype == ptTest {
		prev := c.handTest.Prev()
		c.metaDel(c.handTest)
		c.handTest = prev

		c.countTest--
		if c.coldSize > 1 {
			c.coldSize--
		}
	}

	c.handTest = c.handTest.Next()
}
