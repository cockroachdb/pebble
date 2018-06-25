package pebble

import "github.com/petermattis/pebble/db"

type levelIter struct {
	files      []fileMetadata
	tableCache *tableCache
	err        error
}

// levelIter implements the db.InternalIterator interface.
var _ db.InternalIterator = (*levelIter)(nil)

func (l *levelIter) SeekGE(key *db.InternalKey) {
	panic("pebble: SeekGE unimplemented")
}

func (l *levelIter) SeekLE(key *db.InternalKey) {
	panic("pebble: SeekLE unimplemented")
}

func (l *levelIter) First() {
	panic("pebble: First unimplemented")
}

func (l *levelIter) Last() {
	panic("pebble: Last unimplemented")
}

func (l *levelIter) Next() bool {
	panic("pebble: Next unimplemented")
}

func (l *levelIter) Prev() bool {
	panic("pebble: Prev unimplemented")
}

func (l *levelIter) Key() *db.InternalKey {
	return nil
}

func (l *levelIter) Value() []byte {
	return nil
}

func (l *levelIter) Valid() bool {
	return false
}

func (l *levelIter) Error() error {
	return l.err
}

func (l *levelIter) Close() error {
	return l.err
}
