// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

// InternalIterator iterates over a DB's key/value pairs in key order. Unlike
// the Iterator interface, the returned keys are InternalKeys composed of the
// user-key, a sequence number and a key kind. In forward iteration, key/value
// pairs for identical user-keys are returned in descending sequence order. In
// reverse iteration, key/value pairs for identical user-keys are returned in
// ascending sequence order. Bounds, [lower, upper), can be set on iterators,
// either using the SetBounds() function in the interface, or in implementation
// specific ways.
//
// An iterator must be closed after use, but it is not necessary to read an
// iterator until exhaustion.
//
// An iterator is not necessarily goroutine-safe, but it is safe to use
// multiple iterators concurrently, with each in a dedicated goroutine.
//
// It is also safe to use an iterator concurrently with modifying its
// underlying DB, if that DB permits modification. However, the resultant
// key/value pairs are not guaranteed to be a consistent snapshot of that DB
// at a particular point in time.
type InternalIterator interface {
	// SeekGE moves the iterator to the first key/value pair whose key is greater
	// than or equal to the given key. Returns the key and value if the iterator
	// is pointing at a valid entry, and (nil, nil) otherwise. Note that SeekGE
	// only checks the upper bound. It is up to the caller to ensure that key
	// is greater than or equal to the lower bound.
	SeekGE(key []byte) (*InternalKey, []byte)

	// SeekPrefixGE moves the iterator to the first key/value pair whose key is
	// greater than or equal to the given key. Returns the key and value if the
	// iterator is pointing at a valid entry, and (nil, nil) otherwise. Note that
	// SeekPrefixGE only checks the upper bound. It is up to the caller to ensure
	// that key is greater than or equal to the lower bound.
	//
	// The prefix argument is used by some InternalIterator implementations (e.g.
	// sstable.Reader) to avoid expensive operations. A user-defined Split
	// function must be supplied to the Comparer for the DB. The supplied prefix
	// will be the prefix of the given key returned by that Split function. If
	// the iterator is able to determine that no key with the prefix exists, it
	// can return (nil,nil). Unlike SeekGE, this is not an indication that
	// iteration is exhausted.
	//
	// Note that the iterator may return keys not matching the prefix. It is up
	// to the user to check if the prefix matches.
	SeekPrefixGE(prefix, key []byte) (*InternalKey, []byte)

	// SeekLT moves the iterator to the last key/value pair whose key is less
	// than the given key. Returns the key and value if the iterator is pointing
	// at a valid entry, and (nil, nil) otherwise. Note that SeekLT only checks
	// the lower bound. It is up to the caller to ensure that key is less than
	// the upper bound.
	SeekLT(key []byte) (*InternalKey, []byte)

	// First moves the iterator the the first key/value pair. Returns the key and
	// value if the iterator is pointing at a valid entry, and (nil, nil)
	// otherwise. Note that First only checks the upper bound. It is up to the
	// caller to ensure that key is greater than or equal to the lower bound
	// (e.g. via a call to SeekGE(lower)).
	First() (*InternalKey, []byte)

	// Last moves the iterator the the last key/value pair. Returns the key and
	// value if the iterator is pointing at a valid entry, and (nil, nil)
	// otherwise. Note that Last only checks the lower bound. It is up to the
	// caller to ensure that key is less than the upper bound (e.g. via a call
	// to SeekLT(upper))
	Last() (*InternalKey, []byte)

	// Next moves the iterator to the next key/value pair. Returns the key and
	// value if the iterator is pointing at a valid entry, and (nil, nil)
	// otherwise. Note that Next only checks the upper bound. It is up to the
	// caller to ensure that key is greater than or equal to the lower bound.
	Next() (*InternalKey, []byte)

	// Prev moves the iterator to the previous key/value pair. Returns the key
	// and value if the iterator is pointing at a valid entry, and (nil, nil)
	// otherwise. Note that Prev only checks the lower bound. It is up to the
	// caller to ensure that key is less than the upper bound.
	Prev() (*InternalKey, []byte)

	// Error returns any accumulated error.
	Error() error

	// Close closes the iterator and returns any accumulated error. Exhausting
	// all the key/value pairs in a table is not considered to be an error.
	// It is valid to call Close multiple times. Other methods should not be
	// called after the iterator has been closed.
	Close() error

	// SetBounds sets the lower and upper bounds for the iterator. Note that the
	// result of Next and Prev will be undefined until the iterator has been
	// repositioned with SeekGE, SeekPrefixGE, SeekLT, First, or Last.
	SetBounds(lower, upper []byte)
}
