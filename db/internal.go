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
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
)

// InternalKeyKind enumerates the kind of key: a deletion tombstone, a set
// value, a merged value, etc.
type InternalKeyKind uint8

// These constants are part of the file format, and should not be changed.
const (
	InternalKeyKindDelete InternalKeyKind = 0
	InternalKeyKindSet                    = 1
	InternalKeyKindMerge                  = 2
	// InternalKeyKindLogData                                  = 3
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
	// When constructing an internal key to pass to DB.Seek{GE,LE},
	// internalKeyComparer sorts decreasing by kind (after sorting increasing by
	// user key and decreasing by sequence number). Thus, use InternalKeyKindMax,
	// which sorts 'less than or equal to' any other valid internalKeyKind, when
	// searching for any kind of internal key formed by a certain user key and
	// seqNum.
	InternalKeyKindMax InternalKeyKind = 17

	// A marker for an invalid key.
	InternalKeyKindInvalid InternalKeyKind = 255

	// InternalKeySeqNumBatch is a bit that is set on batch sequence numbers
	// which prevents those entries from being excluded from iteration.
	InternalKeySeqNumBatch = uint64(1 << 55)

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
	Trailer uint64
}

// InvalidInternalKey is an invalid internal key for which Valid() will return
// false.
var InvalidInternalKey = MakeInternalKey(nil, 0, InternalKeyKindInvalid)

// MakeInternalKey constructs an internal key from a specified user key,
// sequence number and kind.
func MakeInternalKey(userKey []byte, seqNum uint64, kind InternalKeyKind) InternalKey {
	return InternalKey{
		UserKey: userKey,
		Trailer: (seqNum << 8) | uint64(kind),
	}
}

// MakeSearchKey constructs an internal key that is appropriate for searching
// for a the specified user key. The search key contain the maximual sequence
// number and kind ensuring that it sorts before any other internal keys for
// the same user key.
func MakeSearchKey(userKey []byte) InternalKey {
	return InternalKey{
		UserKey: userKey,
		Trailer: (InternalKeySeqNumMax << 8) | uint64(InternalKeyKindMax),
	}
}

var kindsMap = map[string]InternalKeyKind{
	"DEL":      InternalKeyKindDelete,
	"RANGEDEL": InternalKeyKindRangeDelete,
	"SET":      InternalKeyKindSet,
	"MERGE":    InternalKeyKindMerge,
	"INVALID":  InternalKeyKindInvalid,
	"MAX":      InternalKeyKindMax,
}

// ParseInternalKey parses the string representation of an internal key. The
// format is <user-key>.<kind>.<seq-num>. If the seq-num starts with a "b" it
// is marked as a batch-seq-num (i.e. the InternalKeySeqNumBatch bit is set).
func ParseInternalKey(s string) InternalKey {
	x := strings.Split(s, ".")
	ukey := x[0]
	kind := kindsMap[x[1]]
	j := 0
	if x[2][0] == 'b' {
		j = 1
	}
	seqNum, _ := strconv.ParseUint(x[2][j:], 10, 64)
	if x[2][0] == 'b' {
		seqNum |= InternalKeySeqNumBatch
	}
	return MakeInternalKey([]byte(ukey), seqNum, kind)
}

// DecodeInternalKey decodes an encoded internal key. See InternalKey.Encode().
func DecodeInternalKey(encodedKey []byte) InternalKey {
	n := len(encodedKey) - 8
	var trailer uint64
	if n >= 0 {
		trailer = binary.LittleEndian.Uint64(encodedKey[n:])
		encodedKey = encodedKey[:n:n]
	} else {
		trailer = uint64(InternalKeyKindInvalid)
	}
	return InternalKey{
		UserKey: encodedKey,
		Trailer: trailer,
	}
}

// InternalCompare compares two internal keys using the specified comparison
// function. For equal user keys, internal keys compare in descending sequence
// number order. For equal user keys and sequence numbers, internal keys
// compare in descending kind order (though this should never happen in
// practice).
func InternalCompare(userCmp Compare, a, b InternalKey) int {
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
	if a.Trailer < b.Trailer {
		return 1
	}
	if a.Trailer > b.Trailer {
		return -1
	}
	return 0
}

// Encode encodes the receiver into the buffer. The buffer must be large enough
// to hold the encoded data. See InternalKey.Size().
func (k InternalKey) Encode(buf []byte) {
	i := copy(buf, k.UserKey)
	binary.LittleEndian.PutUint64(buf[i:], k.Trailer)
}

// EncodeTrailer returns the trailer encoded to an 8-byte array.
func (k InternalKey) EncodeTrailer() [8]byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], k.Trailer)
	return buf
}

// Separator returns a separator key such that k <= x && x < other, where less
// than is consistent with the Compare function. The buf parameter may be used
// to store the returned InternalKey.UserKey, though it is valid to pass a
// nil. See the Separator type for details on separator keys.
func (k InternalKey) Separator(
	cmp Compare,
	sep Separator,
	buf []byte,
	other InternalKey,
) InternalKey {
	buf = sep(buf, k.UserKey, other.UserKey)
	if len(buf) <= len(k.UserKey) && cmp(k.UserKey, buf) < 0 {
		// The separator user key is physically shorter that k.UserKey, but
		// logically after. Tack on the max sequence number to the shortened user
		// key.
		return MakeInternalKey(buf, InternalKeySeqNumMax, InternalKeyKindMax)
	}
	return k
}

// Successor returns a successor key such that k <= x. A simple implementation
// may return k unchanged. The buf parameter may be used to store the returned
// InternalKey.UserKey, though it is valid to pass a nil.
func (k InternalKey) Successor(cmp Compare, succ Successor, buf []byte) InternalKey {
	buf = succ(buf, k.UserKey)
	if len(buf) <= len(k.UserKey) && cmp(k.UserKey, buf) < 0 {
		// The successor user key is physically shorter that k.UserKey, but
		// logically after. Tack on the max sequence number to the shortened user
		// key.
		return MakeInternalKey(buf, InternalKeySeqNumMax, InternalKeyKindMax)
	}
	return k
}

// Size returns the encoded size of the key.
func (k InternalKey) Size() int {
	return len(k.UserKey) + 8
}

// SetSeqNum sets the sequence number component of the key.
func (k *InternalKey) SetSeqNum(seqNum uint64) {
	k.Trailer = (seqNum << 8) | (k.Trailer & 0xff)
}

// SeqNum returns the sequence number component of the key.
func (k InternalKey) SeqNum() uint64 {
	return k.Trailer >> 8
}

// SetKind sets the kind component of the key.
func (k *InternalKey) SetKind(kind InternalKeyKind) {
	k.Trailer = (k.Trailer &^ 0xff) | uint64(kind)
}

// Kind returns the kind compoment of the key.
func (k InternalKey) Kind() InternalKeyKind {
	return InternalKeyKind(k.Trailer & 0xff)
}

// Valid returns true if the key has a valid kind.
func (k InternalKey) Valid() bool {
	return k.Kind() <= InternalKeyKindMax
}

// Clone clones the storage for the UserKey component of the key.
func (k InternalKey) Clone() InternalKey {
	return InternalKey{
		UserKey: append([]byte(nil), k.UserKey...),
		Trailer: k.Trailer,
	}
}

// String returns a string representation of the key.
func (k InternalKey) String() string {
	return fmt.Sprintf("%s#%d,%d", k.UserKey, k.SeqNum(), k.Kind())
}

// Pretty returns a pretty-printed string representation of the key.
func (k InternalKey) Pretty(f func([]byte) string) string {
	return fmt.Sprintf("%s#%d,%d", f(k.UserKey), k.SeqNum(), k.Kind())
}

// InternalIterator iterates over a DB's key/value pairs in key order. Unlike
// the Iterator interface, the returned keys are InternalKeys composed of the
// user-key, a sequence number and a key kind. In both forward and reverse
// iteration, key/value pairs for identical user-keys are returned in
// descending sequence order: newer keys are returned before older keys.
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

	// NextUserKey moves the iterator to the next key/value pair with a user-key
	// greater than the current user key.
	// It returns whether the iterator is exhausted.
	NextUserKey() bool

	// Prev moves the iterator to the previous key/value pair.
	// It returns whether the iterator is exhausted.
	Prev() bool

	// PrevUserKey moves the iterator to the previous key/value pair with a
	// user-key less than the current user key.
	// It returns whether the iterator is exhausted.
	PrevUserKey() bool

	// Key returns the encoded internal key of the current key/value pair, or nil
	// if done.  The caller should not modify the contents of the returned slice,
	// and its contents may change on the next call to Next.
	Key() InternalKey

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
