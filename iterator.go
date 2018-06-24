package pebble

import (
	"bytes"
	"container/heap"
	"fmt"

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

type concatenatingIter struct {
	iters []db.InternalIterator
	err   error
}

// concatenatingIter implements the db.InternalIterator interface.
var _ db.InternalIterator = (*concatenatingIter)(nil)

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

func (c *concatenatingIter) SeekGE(key *db.InternalKey) {
	panic("pebble: SeekGE unimplemented")
}

func (c *concatenatingIter) SeekLE(key *db.InternalKey) {
	panic("pebble: SeekLE unimplemented")
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
	cmp     db.Compare
	reverse bool
	items   []mergingIterItem
}

func (h *mergingIterHeap) Len() int {
	return len(h.items)
}

func (h *mergingIterHeap) Less(i, j int) bool {
	if h.reverse {
		i, j = j, i
	}
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
	dir   int
	iters []db.InternalIterator
	heap  mergingIterHeap
	err   error
}

// mergingIter implements the db.InternalIterator interface.
var _ db.InternalIterator = (*mergingIter)(nil)

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
	m.initMinHeap()
	return m
}

func (m *mergingIter) initHeap() {
	m.heap.items = m.heap.items[:0]
	for _, t := range m.iters {
		if t.Valid() {
			m.heap.items = append(m.heap.items, mergingIterItem{
				iter: t,
				key:  t.Key(),
			})
		}
	}
	heap.Init(&m.heap)
}

func (m *mergingIter) initMinHeap() {
	m.dir = 1
	m.heap.reverse = false
	m.initHeap()
}

func (m *mergingIter) initMaxHeap() {
	m.dir = -1
	m.heap.reverse = true
	m.initHeap()
}

func (m *mergingIter) switchToMinHeap() {
	if m.heap.Len() == 0 {
		m.First()
		return
	}

	// We're switching from using a max heap to a min heap. We need to advance
	// any iterator that is less than or equal to the current key. Consider the
	// scenario where we have 2 iterators being merged (user-key:seqnum):
	//
	// i1:     *a:2     b:2
	// i2: a:1      b:1
	//
	// The current key is a:2 and i2 is pointed at a:1. When we switch to forward
	// iteration, we want to return a key that is greater than a:2.

	key := m.heap.items[0].key
	cur := m.heap.items[0].iter

	for _, i := range m.iters {
		if i == cur {
			continue
		}
		if !i.Valid() {
			i.Next()
		}
		for ; i.Valid(); i.Next() {
			if db.InternalCompare(m.heap.cmp, *key, *i.Key()) < 0 {
				// key < iter-key
				break
			}
			// key >= iter-key
		}
	}

	// Special handling for the current iterator because we were using its key
	// above.
	cur.Next()
	m.initMinHeap()
}

func (m *mergingIter) switchToMaxHeap() {
	if m.heap.Len() == 0 {
		m.Last()
		return
	}

	// We're switching from using a min heap to a max heap. We need to backup any
	// iterator that is greater than or equal to the current key. Consider the
	// scenario where we have 2 iterators being merged (user-key:seqnum):
	//
	// i1: a:2     *b:2
	// i2:     a:1      b:1
	//
	// The current key is b:2 and i2 is pointing at b:1. When we switch to
	// reverse iteration, we want to return a key that is less than b:2.
	key := m.heap.items[0].key
	cur := m.heap.items[0].iter
	for _, i := range m.iters {
		if i == cur {
			continue
		}
		if !i.Valid() {
			i.Prev()
		}
		for ; i.Valid(); i.Prev() {
			if db.InternalCompare(m.heap.cmp, *key, *i.Key()) > 0 {
				break
			}
		}
	}
	// Special handling for the current iterator because we were using its key
	// above.
	cur.Prev()
	m.initMaxHeap()
}

func (m *mergingIter) SeekGE(key *db.InternalKey) {
	for _, t := range m.iters {
		t.SeekGE(key)
	}
	m.initMinHeap()
}

func (m *mergingIter) SeekLE(key *db.InternalKey) {
	for _, t := range m.iters {
		t.SeekLE(key)
	}
	m.initMaxHeap()
}

func (m *mergingIter) First() {
	for _, t := range m.iters {
		t.First()
	}
	m.initMinHeap()
}

func (m *mergingIter) Last() {
	for _, t := range m.iters {
		t.Last()
	}
	m.initMaxHeap()
}

func (m *mergingIter) Next() bool {
	if m.err != nil {
		return false
	}

	if m.dir != 1 {
		m.switchToMinHeap()
		return m.heap.Len() > 0
	}

	if m.heap.Len() == 0 {
		return false
	}

	item := &m.heap.items[0]
	if item.iter.Next() {
		item.key = item.iter.Key()
		heap.Fix(&m.heap, 0)
		return true
	}

	m.err = item.iter.Error()
	if m.err != nil {
		return false
	}

	heap.Pop(&m.heap)
	return m.heap.Len() > 0
}

func (m *mergingIter) Prev() bool {
	if m.err != nil {
		return false
	}

	if m.dir != -1 {
		m.switchToMaxHeap()
		return m.heap.Len() > 0
	}

	if m.heap.Len() == 0 {
		return false
	}

	item := &m.heap.items[0]
	if item.iter.Prev() {
		item.key = item.iter.Key()
		heap.Fix(&m.heap, 0)
		return true
	}

	m.err = item.iter.Error()
	if m.err != nil {
		return false
	}

	heap.Pop(&m.heap)
	return m.heap.Len() > 0
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

func (m *mergingIter) DebugString() string {
	var buf bytes.Buffer
	sep := ""
	for m.heap.Len() > 0 {
		item := heap.Pop(&m.heap).(*mergingIterItem)
		fmt.Fprintf(&buf, "%s%s:%d", sep, item.key.UserKey, item.key.Seqnum())
		sep = " "
	}
	if m.dir == 1 {
		m.initMinHeap()
	} else {
		m.initMaxHeap()
	}
	return buf.String()
}
