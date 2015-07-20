// Copyright 2011 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package db defines the interfaces for a key/value store.
//
// A DB's basic operations (Get, Set, Delete) should be self-explanatory. Get
// and Delete will return ErrNotFound if the requested key is not in the store.
// Callers are free to ignore this error.
//
// A DB also allows for iterating over the key/value pairs in key order. If d
// is a DB, the code below prints all key/value pairs whose keys are 'greater
// than or equal to' k:
//
//	iter := d.Find(k, readOptions)
//	for iter.Next() {
//		fmt.Printf("key=%q value=%q\n", iter.Key(), iter.Value())
//	}
//	return iter.Close()
//
// Other leveldb packages provide implementations of these interfaces. The
// Options struct in this package holds the optional parameters for these
// implementations, including a Comparer to define a 'less than' relationship
// over keys. It is always valid to pass a nil *Options, which means to use
// the default parameter values. Any zero field of a non-nil *Options also
// means to use the default value for that parameter. Thus, the code below
// uses a custom Comparer, but the default values for every other parameter:
//
//	db := memdb.New(&db.Options{
//		Comparer: myComparer,
//	})
package db

import (
	"errors"
)

// ErrNotFound means that a get or delete call did not find the requested key.
var ErrNotFound = errors.New("leveldb/db: not found")

// Iterator iterates over a DB's key/value pairs in key order.
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
type Iterator interface {
	// Next moves the iterator to the next key/value pair.
	// It returns whether the iterator is exhausted.
	Next() bool

	// Key returns the key of the current key/value pair, or nil if done.
	// The caller should not modify the contents of the returned slice, and
	// its contents may change on the next call to Next.
	Key() []byte

	// Value returns the value of the current key/value pair, or nil if done.
	// The caller should not modify the contents of the returned slice, and
	// its contents may change on the next call to Next.
	Value() []byte

	// Close closes the iterator and returns any accumulated error. Exhausting
	// all the key/value pairs in a table is not considered to be an error.
	// It is valid to call Close multiple times. Other methods should not be
	// called after the iterator has been closed.
	Close() error
}

// DB is a key/value store.
//
// It is safe to call Get and Find from concurrent goroutines. It is not
// necessarily safe to do so for Set and Delete.
//
// Some implementations may impose additional restrictions. For example:
//   - Set calls may need to be in increasing key order.
//   - a DB may be read-only or write-only.
type DB interface {
	// Get gets the value for the given key. It returns ErrNotFound if the DB
	// does not contain the key.
	//
	// The caller should not modify the contents of the returned slice, but
	// it is safe to modify the contents of the argument after Get returns.
	Get(key []byte, o *ReadOptions) (value []byte, err error)

	// Set sets the value for the given key. It overwrites any previous value
	// for that key; a DB is not a multi-map.
	//
	// It is safe to modify the contents of the arguments after Set returns.
	Set(key, value []byte, o *WriteOptions) error

	// Delete deletes the value for the given key. It returns ErrNotFound if
	// the DB does not contain the key.
	//
	// It is safe to modify the contents of the arguments after Delete returns.
	Delete(key []byte, o *WriteOptions) error

	// Find returns an iterator positioned before the first key/value pair
	// whose key is 'greater than or equal to' the given key. There may be no
	// such pair, in which case the iterator will return false on Next.
	//
	// Any error encountered will be implicitly returned via the iterator. An
	// error-iterator will yield no key/value pairs and closing that iterator
	// will return that error.
	//
	// It is safe to modify the contents of the argument after Find returns.
	Find(key []byte, o *ReadOptions) Iterator

	// Close closes the DB. It may or may not close any underlying io.Reader
	// or io.Writer, depending on how the DB was created.
	//
	// It is not safe to close a DB until all outstanding iterators are closed.
	// It is valid to call Close multiple times. Other methods should not be
	// called after the DB has been closed.
	Close() error
}

// NewConcatenatingIterator returns an iterator that concatenates its input.
// Walking the resultant iterator will walk each input iterator in turn,
// exhausting each input before moving on to the next.
//
// The sequence of the combined inputs' keys are assumed to be in strictly
// increasing order: iters[i]'s last key is less than iters[i+1]'s first key.
//
// None of the iters may be nil.
func NewConcatenatingIterator(iters ...Iterator) Iterator {
	if len(iters) == 1 {
		return iters[0]
	}
	return &concatenatingIter{
		iters: iters,
	}
}

type concatenatingIter struct {
	iters []Iterator
	err   error
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

func (c *concatenatingIter) Key() []byte {
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
func NewMergingIterator(cmp Comparer, iters ...Iterator) Iterator {
	if len(iters) == 1 {
		return iters[0]
	}
	return &mergingIter{
		iters: iters,
		cmp:   cmp,
		keys:  make([][]byte, len(iters)),
		index: -1,
	}
}

type mergingIter struct {
	// iters are the input iterators. An element is set to nil when that
	// input iterator is done.
	iters []Iterator
	err   error
	cmp   Comparer
	// keys[i] is the current key for iters[i].
	keys [][]byte
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
		if m.cmp.Compare(m.keys[i], m.keys[m.index]) < 0 {
			m.index = i
		}
	}
	return m.index >= 0
}

func (m *mergingIter) Key() []byte {
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

func (m *mergingIter) Close() error {
	for i := range m.iters {
		m.close(i)
	}
	m.index = -2
	return m.err
}
