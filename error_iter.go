package pebble

import "github.com/petermattis/pebble/db"

type errorIter struct {
	err error
}

var _ db.Iterator = (*errorIter)(nil)

func newErrorIter(err error) *errorIter {
	return &errorIter{err: err}
}

func (c *errorIter) SeekGE(key []byte) {
}

func (c *errorIter) SeekLE(key []byte) {
}

func (c *errorIter) First() {
}

func (c *errorIter) Last() {
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

func (c *errorIter) Error() error {
	return c.err
}

func (c *errorIter) Close() error {
	return c.err
}
