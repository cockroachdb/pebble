package pebble

import "github.com/petermattis/pebble/db"

type dbIter struct {
	iter db.InternalIterator
	err  error

	levelsBuf [numLevels]levelIter
	itersBuf  [2 + numLevels]db.InternalIterator
}

var _ db.Iterator = (*dbIter)(nil)

func (c *dbIter) SeekGE(key []byte) {
}

func (c *dbIter) SeekLE(key []byte) {
}

func (c *dbIter) First() {
}

func (c *dbIter) Last() {
}

func (c *dbIter) Next() bool {
	return false
}

func (c *dbIter) Prev() bool {
	return false
}

func (c *dbIter) Key() []byte {
	return nil
}

func (c *dbIter) Value() []byte {
	return nil
}

func (c *dbIter) Valid() bool {
	return false
}

func (c *dbIter) Error() error {
	return c.err
}

func (c *dbIter) Close() error {
	return c.err
}
