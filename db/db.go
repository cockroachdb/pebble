// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

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
//	iter := d.NewIter(readOptions)
//	for iter.SeekGE(k); iter.Valid(); iter.Next() {
//		fmt.Printf("key=%q value=%q\n", iter.Key(), iter.Value())
//	}
//	return iter.Close()
//
// Other pebble packages provide implementations of these interfaces. The
// Options struct in this package holds the optional parameters for these
// implementations, including a Comparer to define a 'less than' relationship
// over keys. It is always valid to pass a nil *Options, which means to use the
// default parameter values. Any zero field of a non-nil *Options also means to
// use the default value for that parameter. Thus, the code below uses a custom
// Comparer, but the default values for every other parameter:
//
//	db := pebble.NewMemTable(&db.Options{
//		Comparer: myComparer,
//	})
package db // import "github.com/petermattis/pebble/db"

import (
	"errors"
)

// ErrNotFound means that a get or delete call did not find the requested key.
var ErrNotFound = errors.New("pebble/db: not found")

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
	// SeekGE moves the iterator to the first key/value pair whose key is greater
	// than or equal to the given key.
	SeekGE(key []byte)

	// SeekLT moves the iterator to the last key/value pair whose key is less
	// than the given key.
	SeekLT(key []byte)

	// First moves the iterator the the first key/value pair.
	First()

	// Last moves the iterator the the last key/value pair.
	Last()

	// Next moves the iterator to the next key/value pair.
	// It returns whether the iterator is exhausted.
	Next() bool

	// Prev moves the iterator to the previous key/value pair.
	// It returns whether the iterator is exhausted.
	Prev() bool

	// Key returns the key of the current key/value pair, or nil if done.
	// The caller should not modify the contents of the returned slice, and
	// its contents may change on the next call to Next.
	Key() []byte

	// Value returns the value of the current key/value pair, or nil if done.
	// The caller should not modify the contents of the returned slice, and
	// its contents may change on the next call to Next.
	Value() []byte

	// Valid returns true if the iterator is positioned at a valid key/value pair
	// and false otherwise.
	Valid() bool

	// Error returns any accumulated error.
	Error() error

	// Close closes the iterator and returns any accumulated error. Exhausting
	// all the key/value pairs in a table is not considered to be an error.
	// It is valid to call Close multiple times. Other methods should not be
	// called after the iterator has been closed.
	Close() error
}
