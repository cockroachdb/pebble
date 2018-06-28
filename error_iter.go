package pebble

import "github.com/petermattis/pebble/db"

type errorIter struct {
	err error
}

var _ db.InternalIterator = (*errorIter)(nil)

func newErrorIter(err error) *errorIter {
	return &errorIter{err: err}
}

func (c *errorIter) SeekGE(key *db.InternalKey) {
}

func (c *errorIter) SeekLT(key *db.InternalKey) {
}

func (c *errorIter) First() {
}

func (c *errorIter) Last() {
}

func (c *errorIter) Next() bool {
	return false
}

func (c *errorIter) NextUserKey() bool {
	return false
}

func (c *errorIter) Prev() bool {
	return false
}

func (c *errorIter) PrevUserKey() bool {
	return false
}

func (c *errorIter) Key() db.InternalKey {
	return db.InvalidInternalKey
}

func (c *errorIter) Value() []byte {
	return nil
}

func (c *errorIter) Valid() bool {
	return false
}

func (c *errorIter) Error() error {
	return c.err
}

func (c *errorIter) Close() error {
	return c.err
}
