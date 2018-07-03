package pebble

import (
	"bytes"
	"fmt"

	"github.com/petermattis/pebble/db"
)

type mergingIterItem struct {
	iter db.InternalIterator
	key  db.InternalKey
}

type mergingIterHeap struct {
	cmp     db.Compare
	reverse bool
	items   []mergingIterItem
}

func (h *mergingIterHeap) len() int {
	return len(h.items)
}

func (h *mergingIterHeap) less(i, j int) bool {
	ikey, jkey := h.items[i].key, h.items[j].key
	if c := h.cmp(ikey.UserKey, jkey.UserKey); c != 0 {
		if h.reverse {
			return c > 0
		}
		return c < 0
	}
	return ikey.Trailer > jkey.Trailer
}

func (h *mergingIterHeap) swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

// init, up and down are copied from the go stdlib.
func (h *mergingIterHeap) init() {
	// heapify
	n := h.len()
	for i := n/2 - 1; i >= 0; i-- {
		h.down(i, n)
	}
}

func (h *mergingIterHeap) fix(i int) {
	if !h.down(i, h.len()) {
		h.up(i)
	}
}

func (h *mergingIterHeap) pop() *mergingIterItem {
	n := h.len() - 1
	h.swap(0, n)
	h.down(0, n)
	item := &h.items[n]
	h.items = h.items[:n]
	return item
}

func (h *mergingIterHeap) up(j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !h.less(j, i) {
			break
		}
		h.swap(i, j)
		j = i
	}
}

func (h *mergingIterHeap) down(i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && h.less(j2, j1) {
			j = j2 // = 2*i + 2  // right child
		}
		if !h.less(j, i) {
			break
		}
		h.swap(i, j)
		i = j
	}
	return i > i0
}

type mergingIter struct {
	dir   int
	iters []db.InternalIterator
	heap  mergingIterHeap
	err   error
}

// mergingIter implements the db.InternalIterator interface.
var _ db.InternalIterator = (*mergingIter)(nil)

// newMergingIter returns an iterator that merges its input. Walking the
// resultant iterator will return all key/value pairs of all input iterators
// in strictly increasing key order, as defined by cmp.
//
// The input's key ranges may overlap, but there are assumed to be no duplicate
// keys: if iters[i] contains a key k then iters[j] will not contain that key k.
//
// None of the iters may be nil.
func newMergingIter(cmp db.Compare, iters ...db.InternalIterator) db.InternalIterator {
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
	m.heap.init()
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
	if m.heap.len() == 0 {
		m.First()
		return
	}

	// We're switching from using a max heap to a min heap. We need to advance
	// any iterator that is less than or equal to the current key. Consider the
	// scenario where we have 2 iterators being merged (user-key:seq-num):
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
			if db.InternalCompare(m.heap.cmp, key, i.Key()) < 0 {
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
	if m.heap.len() == 0 {
		m.Last()
		return
	}

	// We're switching from using a min heap to a max heap. We need to backup any
	// iterator that is greater than or equal to the current key. Consider the
	// scenario where we have 2 iterators being merged (user-key:seq-num):
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
			c := m.heap.cmp(key.UserKey, i.Key().UserKey)
			if c > 0 {
				// key > iter-key
				break
			}
			if c == 0 && key.Trailer > i.Key().Trailer {
				// key > iter-key
				break
			}
			// key <= iter-key
		}
	}

	// Special handling for the current iterator because we were using its key
	// above.
	cur.Prev()
	m.initMaxHeap()
}

func (m *mergingIter) SeekGE(key []byte) {
	for _, t := range m.iters {
		t.SeekGE(key)
	}
	m.initMinHeap()
}

func (m *mergingIter) SeekLT(key []byte) {
	for _, t := range m.iters {
		t.SeekLT(key)
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
		return m.heap.len() > 0
	}

	if m.heap.len() == 0 {
		return false
	}

	item := &m.heap.items[0]
	if item.iter.Next() {
		item.key = item.iter.Key()
		m.heap.fix(0)
		return true
	}

	m.err = item.iter.Error()
	if m.err != nil {
		return false
	}

	m.heap.pop()
	return m.heap.len() > 0
}

func (m *mergingIter) NextUserKey() bool {
	if m.err != nil {
		return false
	}

	if m.dir != 1 {
		// We're switching directions from reverse iteration to forward iteration.
		if m.heap.len() == 0 {
			m.First()
			return m.heap.len() > 0
		}

		key := m.heap.items[0].key
		cur := m.heap.items[0].iter

		for _, i := range m.iters {
			if i == cur {
				continue
			}
			if !i.Valid() {
				i.NextUserKey()
			}
			for ; i.Valid(); i.NextUserKey() {
				if m.heap.cmp(key.UserKey, i.Key().UserKey) < 0 {
					// key < iter-key
					break
				}
				// key >= iter-key
			}
		}

		// Special handling for the current iterator because we were using its key
		// above.
		cur.NextUserKey()
		m.initMinHeap()
		return m.heap.len() > 0
	}

	if m.heap.len() == 0 {
		return false
	}

	key := m.heap.items[0].key
	cur := m.heap.items[0].iter

	var reheap bool
	for _, i := range m.iters {
		if i == cur {
			continue
		}
		if i.Valid() && m.heap.cmp(key.UserKey, i.Key().UserKey) == 0 {
			i.NextUserKey()
			reheap = true
		}
	}

	// Special handling for the current iterator because we were using its key
	// above.
	if reheap {
		cur.NextUserKey()
		m.initMinHeap()
	} else if cur.NextUserKey() {
		m.heap.items[0].key = cur.Key()
		m.heap.fix(0)
	} else {
		m.heap.pop()
	}
	return m.heap.len() > 0
}

func (m *mergingIter) Prev() bool {
	if m.err != nil {
		return false
	}

	if m.dir != -1 {
		m.switchToMaxHeap()
		return m.heap.len() > 0
	}

	if m.heap.len() == 0 {
		return false
	}

	item := &m.heap.items[0]
	if item.iter.Prev() {
		item.key = item.iter.Key()
		m.heap.fix(0)
		return true
	}

	m.err = item.iter.Error()
	if m.err != nil {
		return false
	}

	m.heap.pop()
	return m.heap.len() > 0
}

func (m *mergingIter) PrevUserKey() bool {
	if m.err != nil {
		return false
	}

	if m.dir != -1 {
		// We're switching directions from forward iteration to reverse iteration.
		if m.heap.len() == 0 {
			// If the iterators are all existed, move to the last entry.
			m.Last()
			return m.heap.len() > 0
		}

		// The current heap is organized from smallest to larger user-key and from
		// larger to smaller sequence:
		//
		//   a:2 a:1 *b:2 b:1 c:2 c:1
		//
		// This needs to be flipped to sort from larger to smaller user-key and
		// from larger to smaller sequence:
		//
		//   a:1 *a:2 b:1 b:2 c:1 c:2
		key := m.heap.items[0].key
		cur := m.heap.items[0].iter

		for _, i := range m.iters {
			if i == cur {
				continue
			}
			if !i.Valid() {
				i.PrevUserKey()
			}
			for ; i.Valid(); i.PrevUserKey() {
				if m.heap.cmp(key.UserKey, i.Key().UserKey) > 0 {
					// key > iter-key
					break
				}
				// key <= iter-key
			}
		}

		// Special handling for the current iterator because we were using its key
		// above.
		cur.PrevUserKey()
		m.initMaxHeap()
		return m.heap.len() > 0
	}

	if m.heap.len() == 0 {
		return false
	}

	key := m.heap.items[0].key
	cur := m.heap.items[0].iter

	var reheap bool
	for _, i := range m.iters {
		if i == cur {
			continue
		}
		if i.Valid() && m.heap.cmp(key.UserKey, i.Key().UserKey) == 0 {
			i.PrevUserKey()
			reheap = true
		}
	}

	// Special handling for the current iterator because we were using its key
	// above.
	if reheap {
		cur.PrevUserKey()
		m.initMaxHeap()
	} else if cur.PrevUserKey() {
		m.heap.items[0].key = cur.Key()
		m.heap.fix(0)
	} else {
		m.heap.pop()
	}
	return m.heap.len() > 0
}

func (m *mergingIter) Key() db.InternalKey {
	if m.heap.len() == 0 || m.err != nil {
		return db.InvalidInternalKey
	}
	return m.heap.items[0].key
}

func (m *mergingIter) Value() []byte {
	if m.heap.len() == 0 || m.err != nil {
		return nil
	}
	return m.heap.items[0].iter.Value()
}

func (m *mergingIter) Valid() bool {
	if m.heap.len() == 0 || m.err != nil {
		return false
	}
	return true
}

func (m *mergingIter) Error() error {
	if m.heap.len() == 0 || m.err != nil {
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
	for m.heap.len() > 0 {
		item := m.heap.pop()
		fmt.Fprintf(&buf, "%s%s:%d", sep, item.key.UserKey, item.key.SeqNum())
		sep = " "
	}
	if m.dir == 1 {
		m.initMinHeap()
	} else {
		m.initMaxHeap()
	}
	return buf.String()
}
