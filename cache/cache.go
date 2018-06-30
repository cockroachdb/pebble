package cache

import (
	"fmt"
	"sync"
)

type key struct {
	FileNum uint64
	Offset  uint64
}

func (k key) String() string {
	return fmt.Sprintf("%d.%d", k.FileNum, k.Offset)
}

type entry struct {
	key        key
	data       []byte
	next, prev *entry
}

func (e entry) String() string {
	return e.key.String()
}

// entryList is a double-linked circular list of *entry elements. The code is
// derived from the stdlib container/list but customized to entry in order to
// avoid a separate allocation for every element.
type entryList struct {
	root entry
}

func (l *entryList) init() {
	l.root.next = &l.root
	l.root.prev = &l.root
}

func (l *entryList) empty() bool {
	return l.root.next == &l.root
}

func (l *entryList) back() *entry {
	return l.root.prev
}

func (l *entryList) insertAfter(e, at *entry) {
	n := at.next
	at.next = e
	e.prev = at
	e.next = n
	n.prev = e
}

func (l *entryList) insertBefore(e, mark *entry) {
	l.insertAfter(e, mark.prev)
}

func (l *entryList) remove(e *entry) *entry {
	if e == &l.root {
		panic("cannot remove root list node")
	}
	e.prev.next = e.next
	e.next.prev = e.prev
	e.next = nil // avoid memory leaks
	e.prev = nil // avoid memory leaks
	return e
}

func (l *entryList) pushFront(e *entry) {
	l.insertAfter(e, &l.root)
}

func (l *entryList) moveToFront(e *entry) {
	if l.root.next == e {
		return
	}
	l.insertAfter(l.remove(e), &l.root)
}

// BlockCache ...
type BlockCache struct {
	maxSize int64

	mu   sync.Mutex
	m    map[key]*entry
	size int64
	lru  entryList
}

// NewBlockCache ...
func NewBlockCache(maxSize int64) *BlockCache {
	c := &BlockCache{
		maxSize: maxSize,
		m:       make(map[key]*entry),
	}
	c.lru.init()
	return c
}

// Get ...
func (c *BlockCache) Get(fileNum, offset uint64) []byte {
	if c == nil {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	k := key{FileNum: fileNum, Offset: offset}
	if e := c.m[k]; e != nil {
		c.lru.moveToFront(e)
		return e.data
	}
	return nil
}

// Insert ...
func (c *BlockCache) Insert(fileNum, offset uint64, data []byte) []byte {
	if c == nil {
		return data
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	k := key{FileNum: fileNum, Offset: offset}
	if e := c.m[k]; e != nil {
		return e.data
	}
	e := &entry{
		key:  k,
		data: data,
	}
	c.m[k] = e
	c.lru.pushFront(e)
	c.size += int64(len(e.data))
	c.evict()
	return e.data
}

func (c *BlockCache) evict() {
	for c.size > c.maxSize && !c.lru.empty() {
		e := c.lru.back()
		c.lru.remove(e)
		delete(c.m, e.key)
		c.size -= int64(len(e.data))
	}
}
