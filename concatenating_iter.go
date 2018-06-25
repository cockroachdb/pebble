package pebble

import "github.com/petermattis/pebble/db"

type concatenatingIter struct {
	iters []db.InternalIterator
	err   error
}

// concatenatingIter implements the db.InternalIterator interface.
var _ db.InternalIterator = (*concatenatingIter)(nil)

// newConcatenatingIter returns an iterator that concatenates its input.
// Walking the resultant iterator will walk each input iterator in turn,
// exhausting each input before moving on to the next.
//
// The sequence of the combined inputs' keys are assumed to be in strictly
// increasing order: iters[i]'s last key is less than iters[i+1]'s first key.
//
// None of the iters may be nil.
func newConcatenatingIter(iters ...db.InternalIterator) db.InternalIterator {
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
	panic("pebble.concatenatingIter: SeekGE unimplemented")
}

func (c *concatenatingIter) SeekLE(key *db.InternalKey) {
	panic("pebble.concatenatingIter: SeekLE unimplemented")
}

func (c *concatenatingIter) First() {
	panic("pebble.concatenatingIter: First unimplemented")
}

func (c *concatenatingIter) Last() {
	panic("pebble.concatenatingIter: Last unimplemented")
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

func (c *concatenatingIter) NextUserKey() bool {
	panic("pebble.concatenatingIter: NextUserKey unimplemented")
}

func (c *concatenatingIter) Prev() bool {
	panic("pebble.concatenatingIter: Prev unimplemented")
}

func (c *concatenatingIter) PrevUserKey() bool {
	panic("pebble.concatenatingIter: PrevUserKey unimplemented")
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
