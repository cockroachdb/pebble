package pebble

import "github.com/petermattis/pebble/db"

// truncatedRangeDelIter is a wrapper around an internalIterator which
// truncates the upper bound of range tombstones to the specified key.
//
// TODO(peter): This needs to be tested.
type truncatedRangeDelIter struct {
	cmp        db.Compare
	wrapped    internalIterator
	upperBound []byte
}

// SeekGE implements internalIterator.SeekGE, as documented in the pebble
// package.
func (i *truncatedRangeDelIter) SeekGE(key []byte) bool {
	return i.wrapped.SeekGE(key)
}

// SeekLT implements internalIterator.SeekLT, as documented in the pebble
// package.
func (i *truncatedRangeDelIter) SeekLT(key []byte) bool {
	return i.wrapped.SeekLT(key)
}

// First implements internalIterator.First, as documented in the pebble
// package.
func (i *truncatedRangeDelIter) First() bool {
	return i.wrapped.First()
}

// Last implements internalIterator.Last, as documented in the pebble
// package.
func (i *truncatedRangeDelIter) Last() bool {
	return i.wrapped.Last()
}

// Next implements internalIterator.Next, as documented in the pebble
// package.
func (i *truncatedRangeDelIter) Next() bool {
	return i.wrapped.Next()
}

// Prev implements internalIterator.Prev, as documented in the pebble
// package.
func (i *truncatedRangeDelIter) Prev() bool {
	return i.wrapped.Prev()
}

// Key implements internalIterator.Key, as documented in the pebble
// package.
func (i *truncatedRangeDelIter) Key() db.InternalKey {
	key := i.wrapped.Key()
	if i.cmp(key.UserKey, i.upperBound) >= 0 {
		panic("truncation resulted in empty range tombstone")
	}
	return key
}

// Value implements internalIterator.Value, as documented in the pebble
// package.
func (i *truncatedRangeDelIter) Value() []byte {
	end := i.wrapped.Value()
	if i.cmp(end, i.upperBound) > 0 {
		end = i.upperBound
	}
	return end
}

// Valid implements internalIterator.Valid, as documented in the pebble
// package.
func (i *truncatedRangeDelIter) Valid() bool {
	return i.wrapped.Valid()
}

// Error implements internalIterator.Error, as documented in the pebble
// package.
func (i *truncatedRangeDelIter) Error() error {
	return i.wrapped.Error()
}

// Close implements internalIterator.Close, as documented in the pebble
// package.
func (i *truncatedRangeDelIter) Close() error {
	return i.wrapped.Close()
}
