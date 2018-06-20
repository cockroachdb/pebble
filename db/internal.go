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
	"bytes"
	"encoding/binary"
)

type InternalKeyKind uint8

// These constants are part of the file format, and should not be changed.
const (
	InternalKeyKindDelete  InternalKeyKind = 0
	InternalKeyKindSet                     = 1
	InternalKeyKindMerge                   = 2
	InternalKeyKindLogData                 = 3
	// InternalKeyKindColumnFamilyDeletion                     = 4
	// InternalKeyKindColumnFamilyValue                        = 5
	// InternalKeyKindColumnFamilyMerge                        = 6
	// InternalKeyKindSingleDelete                             = 7
	// InternalKeyKindColumnFamilySingleDelete                 = 8
	// InternalKeyKindBeginPrepareXID                          = 9
	// InternalKeyKindEndPrepareXID                            = 10
	// InternalKeyKindCommitXID                                = 11
	// InternalKeyKindRollbackXID                              = 12
	// InternalKeyKindNoop                                     = 13
	// InternalKeyKindColumnFamilyRangeDelete                  = 14
	InternalKeyKindRangeDelete = 15
	// InternalKeyKindColumnFamilyBlobIndex                    = 16
	// InternalKeyKindBlobIndex                                = 17

	// This maximum value isn't part of the file format. It's unlikely,
	// but future extensions may increase this value.
	//
	// When constructing an internal key to pass to DB.Find, internalKeyComparer
	// sorts decreasing by kind (after sorting increasing by user key and
	// decreasing by sequence number). Thus, use InternalKeyKindMax, which sorts
	// 'less than or equal to' any other valid internalKeyKind, when searching
	// for any kind of internal key formed by a certain user key and seqNum.
	InternalKeyKindMax InternalKeyKind = 15

	// A marker for an invalid key.
	InternalKeyKindInvalid InternalKeyKind = 255

	// InternalKeySeqNumMax is the largest valid sequence number.
	InternalKeySeqNumMax = uint64(1<<56 - 1)
)

// InternalKey is a key used for the in-memory and on-disk partial DBs that
// make up a pebble DB.
//
// It consists of the user key (as given by the code that uses package pebble)
// followed by 8-bytes of metadata:
//   - 1 byte for the type of internal key: delete or set,
//   - 7 bytes for a uint56 sequence number, in little-endian format.
type InternalKey struct {
	UserKey []byte
	trailer uint64
}

// MakeInternalKey ...
func MakeInternalKey(userKey []byte, seqnum uint64, kind InternalKeyKind) InternalKey {
	return InternalKey{
		UserKey: userKey,
		trailer: (seqnum << 8) | uint64(kind),
	}
}

// DecodeInternalKey ...
func DecodeInternalKey(encodedKey []byte) InternalKey {
	n := len(encodedKey) - 8
	if n < 0 {
		return MakeInternalKey(encodedKey, 0, InternalKeyKindInvalid)
	}
	return InternalKey{
		UserKey: encodedKey[:n:n],
		trailer: binary.LittleEndian.Uint64(encodedKey[n:]),
	}
}

// InternalCompare ...
func InternalCompare(userCmp func(a, b []byte) int, a, b InternalKey) int {
	if !a.Valid() {
		if b.Valid() {
			return -1
		}
		return bytes.Compare(a.UserKey, b.UserKey)
	}
	if !b.Valid() {
		return 1
	}
	if x := userCmp(a.UserKey, b.UserKey); x != 0 {
		return x
	}
	if a.trailer < b.trailer {
		return 1
	}
	if a.trailer > b.trailer {
		return -1
	}
	return 0
}

// Compare ...
func (k *InternalKey) Compare(userCmp func(a, b []byte) int, other []byte) int {
	if k == nil {
		return -1
	}
	return InternalCompare(userCmp, *k, DecodeInternalKey(other))
}

// Encode ...
func (k *InternalKey) Encode(buf []byte) {
	i := copy(buf, k.UserKey)
	binary.LittleEndian.PutUint64(buf[i:], k.trailer)
}

// EncodeTrailer ...
func (k *InternalKey) EncodeTrailer() [8]byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], k.trailer)
	return buf
}

// Size ...
func (k *InternalKey) Size() int {
	return len(k.UserKey) + 8
}

// Seqnum ...
func (k *InternalKey) Seqnum() uint64 {
	return k.trailer >> 8
}

// Kind ...
func (k *InternalKey) Kind() InternalKeyKind {
	return InternalKeyKind(k.trailer & 0xff)
}

// Valid returns true if the key has a valid kind.
func (k *InternalKey) Valid() bool {
	return k.Kind() <= InternalKeyKindMax
}

// Clone ...
func (k InternalKey) Clone() InternalKey {
	return InternalKey{
		UserKey: append([]byte(nil), k.UserKey...),
		trailer: k.trailer,
	}
}

// InternalKeyCoder ...
type InternalKeyCoder struct{}

// Size ...
func (InternalKeyCoder) Size(key *InternalKey) int {
	return key.Size()
}

// Encode ...
func (InternalKeyCoder) Encode(key *InternalKey, buf []byte) {
	i := copy(buf, key.UserKey)
	binary.LittleEndian.PutUint64(buf[i:], key.trailer)
}

// Decode ...
func (InternalKeyCoder) Decode(buf []byte) InternalKey {
	return DecodeInternalKey(buf)
}

// InternalIterator iterates over a DB's key/value pairs in key order.
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
	// than or equal to the given key.
	SeekGE(key *InternalKey)

	// SeekLE moves the iterator to the first key/value pair whose key is less
	// than or equal to the given key.
	SeekLE(key *InternalKey)

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

	// Key returns the encoded internal key of the current key/value pair, or nil
	// if done.  The caller should not modify the contents of the returned slice,
	// and its contents may change on the next call to Next.
	Key() *InternalKey

	// Value returns the value of the current key/value pair, or nil if done.
	// The caller should not modify the contents of the returned slice, and
	// its contents may change on the next call to Next.
	Value() []byte

	// Valid returns true if the iterator is positioned at a valid key/value pair
	// and false otherwise.
	Valid() bool

	// Close closes the iterator and returns any accumulated error. Exhausting
	// all the key/value pairs in a table is not considered to be an error.
	// It is valid to call Close multiple times. Other methods should not be
	// called after the iterator has been closed.
	Close() error
}

// InternalReader is a readable key/value store.
//
// It is safe to call Get and Find from concurrent goroutines.
type InternalReader interface {
	// Get gets the value for the given key. It returns ErrNotFound if the DB
	// does not contain the key.
	//
	// The caller should not modify the contents of the returned slice, but
	// it is safe to modify the contents of the argument after Get returns.
	Get(key *InternalKey, o *ReadOptions) (value []byte, err error)

	// Find returns an iterator positioned before the first key/value pair whose
	// key is greater than or equal to the given key. There may be no such pair,
	// in which case the iterator will return false on Next.
	//
	// Any error encountered will be implicitly returned via the iterator. An
	// error-iterator will yield no key/value pairs and closing that iterator
	// will return that error.
	//
	// Equivalent to NewIter(o) -> SeekGE(key).
	//
	// It is safe to modify the contents of the argument after Find returns.
	Find(key *InternalKey, o *ReadOptions) InternalIterator

	// NewIter returns an iterator that is unpositioned (Iterator.Valid() will
	// return false). The iterator can be positioned via a call to Seek, RSeek,
	// First or Last.
	NewIter(o *ReadOptions) InternalIterator

	// Close closes the Reader. It may or may not close any underlying io.Reader
	// or io.Writer, depending on how the DB was created.
	//
	// It is not safe to close a DB until all outstanding iterators are closed.
	// It is valid to call Close multiple times. Other methods should not be
	// called after the DB has been closed.
	Close() error
}

// InternalSetter is a basic writable key/value store.
//
// Goroutine safety is dependent on the specific implementation.
//
// Some implementations may impose additional restrictions. For example:
//   - Set calls may need to be in increasing key order.
type InternalSetter interface {
	// Set sets the value for the given key. It overwrites any previous value
	// for that key; a DB is not a multi-map.
	//
	// It is safe to modify the contents of the arguments after Set returns.
	Set(key *InternalKey, value []byte, o *WriteOptions) error
}

// InternalWriter is a writable key/value store.
//
// Goroutine safety is dependent on the specific implementation.
type InternalWriter interface {
	Setter

	// Apply the operations contain in the batch to the store.
	//
	// It is safe to modify the contents of the arguments after Apply returns.
	Apply(batch []byte, o *WriteOptions) error
}
