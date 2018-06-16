package pebble

import "github.com/petermattis/pebble/db"

// NewConcatenatingIterator returns an iterator that concatenates its input.
// Walking the resultant iterator will walk each input iterator in turn,
// exhausting each input before moving on to the next.
//
// The sequence of the combined inputs' keys are assumed to be in strictly
// increasing order: iters[i]'s last key is less than iters[i+1]'s first key.
//
// None of the iters may be nil.
func NewConcatenatingIterator(iters ...db.InternalIterator) db.InternalIterator {
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

func (c *concatenatingIter) SeekGE(key *db.InternalKey) bool {
	panic("pebble: Seek unimplemented")
}

func (c *concatenatingIter) SeekLE(key *db.InternalKey) bool {
	panic("pebble: RSeek unimplemented")
}

func (c *concatenatingIter) First() bool {
	panic("pebble: First unimplemented")
}

func (c *concatenatingIter) Last() bool {
	panic("pebble: Last unimplemented")
}

func (c *concatenatingIter) Next() bool {
	if c.err != nil {
		return false
	}
	for len(c.iters) > 0 {
		if c.iters[0].Next() {
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

// NewMergingIterator returns an iterator that merges its input. Walking the
// resultant iterator will return all key/value pairs of all input iterators
// in strictly increasing key order, as defined by cmp.
//
// The input's key ranges may overlap, but there are assumed to be no duplicate
// keys: if iters[i] contains a key k then iters[j] will not contain that key k.
//
// None of the iters may be nil.
func NewMergingIterator(cmp db.Compare, iters ...db.InternalIterator) db.InternalIterator {
	return &mergingIter{
		iters: iters,
		cmp:   cmp,
		keys:  make([]*db.InternalKey, len(iters)),
		index: -1,
	}
}

type mergingIter struct {
	// iters are the input iterators. An element is set to nil when that
	// input iterator is done.
	iters []db.InternalIterator
	err   error
	cmp   db.Compare
	// keys[i] is the current key for iters[i].
	keys []*db.InternalKey
	// index is:
	//   - -2 if the mergingIter is done,
	//   - -1 if the mergingIter has not yet started,
	//   - otherwise, the index (in iters and in keys) of the smallest key.
	index int
}

// close records that the i'th input iterator is done.
func (m *mergingIter) close(i int) error {
	t := m.iters[i]
	if t == nil {
		return nil
	}
	err := t.Close()
	if m.err == nil {
		m.err = err
	}
	m.iters[i] = nil
	m.keys[i] = nil
	return err
}

func (m *mergingIter) SeekGE(key *db.InternalKey) bool {
	panic("pebble: Seek unimplemented")
}

func (m *mergingIter) SeekLE(key *db.InternalKey) bool {
	panic("pebble: RSeek unimplemented")
}

func (m *mergingIter) First() bool {
	panic("pebble: First unimplemented")
}

func (m *mergingIter) Last() bool {
	panic("pebble: Last unimplemented")
}

func (m *mergingIter) Next() bool {
	if m.err != nil {
		return false
	}
	switch m.index {
	case -2:
		return false
	case -1:
		for i, t := range m.iters {
			if t.Next() {
				m.keys[i] = t.Key()
			} else if m.close(i) != nil {
				return false
			}
		}
	default:
		t := m.iters[m.index]
		if t.Next() {
			m.keys[m.index] = t.Key()
		} else if m.close(m.index) != nil {
			return false
		}
	}
	// Find the smallest key. We could maintain a heap instead of doing
	// a linear scan, but len(iters) is typically small.
	m.index = -2
	for i, t := range m.iters {
		if t == nil {
			continue
		}
		if m.index < 0 {
			m.index = i
			continue
		}
		if db.InternalCompare(m.cmp, *m.keys[i], *m.keys[m.index]) < 0 {
			m.index = i
		}
	}
	return m.index >= 0
}

func (m *mergingIter) Prev() bool {
	panic("pebble: Prev unimplemented")
}

func (m *mergingIter) Key() *db.InternalKey {
	if m.index < 0 || m.err != nil {
		return nil
	}
	return m.keys[m.index]
}

func (m *mergingIter) Value() []byte {
	if m.index < 0 || m.err != nil {
		return nil
	}
	return m.iters[m.index].Value()
}

func (m *mergingIter) Valid() bool {
	if m.index < 0 || m.err != nil {
		return false
	}
	return m.iters[m.index].Valid()
}

func (m *mergingIter) Close() error {
	for i := range m.iters {
		m.close(i)
	}
	m.index = -2
	return m.err
}
