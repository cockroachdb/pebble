package pebble

import (
	"container/heap"

	"github.com/petermattis/pebble/db"
)

type errorIter struct {
	err error
}

var _ db.Iterator = (*errorIter)(nil)

func newErrorIter(err error) *errorIter {
	return &errorIter{err: err}
}

func (c *errorIter) SeekGE(key []byte) bool {
	return false
}

func (c *errorIter) SeekLE(key []byte) bool {
	return false
}

func (c *errorIter) First() bool {
	return false
}

func (c *errorIter) Last() bool {
	return false
}

func (c *errorIter) Next() bool {
	return false
}

func (c *errorIter) Prev() bool {
	return false
}

func (c *errorIter) Key() []byte {
	return nil
}

func (c *errorIter) Value() []byte {
	return nil
}

func (c *errorIter) Valid() bool {
	return false
}

func (c *errorIter) Close() error {
	return c.err
}

// newConcatenatingIterator returns an iterator that concatenates its input.
// Walking the resultant iterator will walk each input iterator in turn,
// exhausting each input before moving on to the next.
//
// The sequence of the combined inputs' keys are assumed to be in strictly
// increasing order: iters[i]'s last key is less than iters[i+1]'s first key.
//
// None of the iters may be nil.
func newConcatenatingIterator(iters ...db.InternalIterator) db.InternalIterator {
	for len(iters) > 0 {
		if iters[0].Valid() {
			break
		}
		iters = iters[1:]
	}
	if len(iters) == 1 {
		return iters[0]
	}
	return &concatenatingIter{
		iters: iters,
	}
}

type concatenatingIter struct {
	iters []db.InternalIterator
	err   error
}

func (c *concatenatingIter) SeekGE(key *db.InternalKey) {
	panic("pebble: Seek unimplemented")
}

func (c *concatenatingIter) SeekLE(key *db.InternalKey) {
	panic("pebble: RSeek unimplemented")
}

func (c *concatenatingIter) First() {
	panic("pebble: First unimplemented")
}

func (c *concatenatingIter) Last() {
	panic("pebble: Last unimplemented")
}

func (c *concatenatingIter) Next() bool {
	if c.err != nil {
		return false
	}
	if len(c.iters) > 0 {
		if c.iters[0].Next() {
			return true
		}
	}

	for len(c.iters) > 0 {
		if c.iters[0].Valid() {
			return true
		}
		c.err = c.iters[0].Close()
		if c.err != nil {
			return false
		}
		c.iters = c.iters[1:]
	}
	return false
}

func (c *concatenatingIter) Prev() bool {
	panic("pebble: Prev unimplemented")
}

func (c *concatenatingIter) Key() *db.InternalKey {
	if len(c.iters) == 0 || c.err != nil {
		return nil
	}
	return c.iters[0].Key()
}

func (c *concatenatingIter) Value() []byte {
	if len(c.iters) == 0 || c.err != nil {
		return nil
	}
	return c.iters[0].Value()
}

func (c *concatenatingIter) Valid() bool {
	if len(c.iters) == 0 || c.err != nil {
		return false
	}
	return c.iters[0].Valid()
}

// Error implements Iterator.Error, as documented in the pebble/db package.
func (c *concatenatingIter) Error() error {
	if len(c.iters) == 0 || c.err != nil {
		return c.err
	}
	return c.iters[0].Error()
}

func (c *concatenatingIter) Close() error {
	for _, t := range c.iters {
		err := t.Close()
		if c.err == nil {
			c.err = err
		}
	}
	c.iters = nil
	return c.err
}

type mergingIterItem struct {
	iter  db.InternalIterator
	key   *db.InternalKey
	index int
}

type mergingIterHeap struct {
	cmp   db.Compare
	items []mergingIterItem
}

func (h *mergingIterHeap) Len() int {
	return len(h.items)
}

func (h *mergingIterHeap) Less(i, j int) bool {
	return db.InternalCompare(h.cmp, *h.items[i].key, *h.items[j].key) < 0
}

func (h *mergingIterHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *mergingIterHeap) Push(x interface{}) {
	n := len(h.items)
	item := x.(*mergingIterItem)
	item.index = n
	h.items = append(h.items, *item)
}

func (h *mergingIterHeap) Pop() interface{} {
	n := len(h.items)
	item := &h.items[n-1]
	item.index = -1 // for safety
	h.items = h.items[:n-1]
	return item
}

type mergingIter struct {
	iters []db.InternalIterator
	heap  mergingIterHeap
	err   error
}

// newMergingIterator returns an iterator that merges its input. Walking the
// resultant iterator will return all key/value pairs of all input iterators
// in strictly increasing key order, as defined by cmp.
//
// The input's key ranges may overlap, but there are assumed to be no duplicate
// keys: if iters[i] contains a key k then iters[j] will not contain that key k.
//
// None of the iters may be nil.
func newMergingIterator(cmp db.Compare, iters ...db.InternalIterator) db.InternalIterator {
	m := &mergingIter{
		iters: iters,
	}
	m.heap.cmp = cmp
	m.heap.items = make([]mergingIterItem, 0, len(iters))
	for _, t := range iters {
		if t.Valid() {
			m.heap.items = append(m.heap.items, mergingIterItem{
				iter: t,
				key:  t.Key(),
			})
		}
	}
	heap.Init(&m.heap)
	return m
}

func (m *mergingIter) SeekGE(key *db.InternalKey) {
	panic("pebble: Seek unimplemented")
}

func (m *mergingIter) SeekLE(key *db.InternalKey) {
	panic("pebble: RSeek unimplemented")
}

func (m *mergingIter) First() {
	panic("pebble: First unimplemented")
}

func (m *mergingIter) Last() {
	panic("pebble: Last unimplemented")
}

func (m *mergingIter) Next() bool {
	if m.err != nil {
		return false
	}
	item := heap.Pop(&m.heap).(*mergingIterItem)
	if item.iter.Next() {
		item.key = item.iter.Key()
		heap.Push(&m.heap, item)
		return true
	}
	m.err = item.iter.Error()
	if m.err != nil {
		return false
	}
	return m.heap.Len() > 0
}

func (m *mergingIter) Prev() bool {
	panic("pebble: Prev unimplemented")
}

func (m *mergingIter) Key() *db.InternalKey {
	if m.heap.Len() == 0 || m.err != nil {
		return nil
	}
	return m.heap.items[0].key
}

func (m *mergingIter) Value() []byte {
	if m.heap.Len() == 0 || m.err != nil {
		return nil
	}
	return m.heap.items[0].iter.Value()
}

func (m *mergingIter) Valid() bool {
	if m.heap.Len() == 0 || m.err != nil {
		return false
	}
	return true
}

// Error implements Iterator.Error, as documented in the pebble/db package.
func (m *mergingIter) Error() error {
	if m.heap.Len() == 0 || m.err != nil {
		return m.err
	}
	return m.heap.items[0].iter.Error()
}

func (m *mergingIter) Close() error {
	for i := range m.iters {
		m.iters[i].Close()
	}
	m.iters = nil
	m.heap.items = nil
	return m.err
}
