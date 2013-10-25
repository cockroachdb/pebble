// Copyright 2012 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package leveldb

import (
	"bytes"

	"code.google.com/p/leveldb-go/leveldb/db"
)

// internalKey is a key used for the in-memory and on-disk partial DBs that
// make up a leveldb DB.
//
// It consists of the user key (as given by the arbitrary code that uses
// package leveldb) followed by an 8-byte trailer:
//   - 1 byte for the kind of internal key: delete or set,
//   - 7 bytes for a uint56 sequence number, in little-endian format.
type internalKey []byte

type internalKeyKind uint8

const (
	// These constants are part of the file format, and should not be changed.
	internalKeyKindDelete internalKeyKind = 0
	internalKeyKindSet    internalKeyKind = 1

	// This maximum value isn't part of the file format. It's unlikely,
	// but future extensions may increase this value.
	//
	// When constructing an internal key to pass to DB.Find, internalKeyComparer
	// sorts decreasing by kind (after sorting increasing by user key and
	// decreasing by sequence number). Thus, use internalKeyKindMax, which sorts
	// 'less than or equal to' any other valid internalKeyKind, when searching
	// for any kind of internal key formed by a certain user key and seqNum.
	internalKeyKindMax internalKeyKind = 1
)

// internalKeySeqNumMax is the largest valid sequence number.
const internalKeySeqNumMax = uint64(1<<56 - 1)

// makeInternalKey makes an internalKey from a user key, a kind, and a sequence
// number. The return value may be a slice of dst[:cap(dst)] if it is large
// enough. Otherwise, it may be a slice of a newly allocated buffer. In any
// case, all of dst[:cap(dst)] may be overwritten.
func makeInternalKey(dst internalKey, ukey []byte, kind internalKeyKind, seqNum uint64) internalKey {
	if cap(dst) < len(ukey)+8 {
		n := 256
		for n < len(ukey)+8 {
			n *= 2
		}
		dst = make(internalKey, n)
	}
	ikey := dst[:len(ukey)+8]
	i := copy(ikey, ukey)
	ikey[i+0] = uint8(kind)
	ikey[i+1] = uint8(seqNum)
	ikey[i+2] = uint8(seqNum >> 8)
	ikey[i+3] = uint8(seqNum >> 16)
	ikey[i+4] = uint8(seqNum >> 24)
	ikey[i+5] = uint8(seqNum >> 32)
	ikey[i+6] = uint8(seqNum >> 40)
	ikey[i+7] = uint8(seqNum >> 48)
	return ikey
}

// valid returns whether k is a valid internal key.
func (k internalKey) valid() bool {
	i := len(k) - 8
	return i >= 0 && internalKeyKind(k[i]) <= internalKeyKindMax
}

// ukey returns the user key portion of an internal key.
// ukey may panic if k is not valid.
func (k internalKey) ukey() []byte {
	return []byte(k[:len(k)-8])
}

// kind returns the kind of an internal key.
// kind may panic if k is not valid.
func (k internalKey) kind() internalKeyKind {
	return internalKeyKind(k[len(k)-8])
}

// seqNum returns the sequence number of an internal key.
// seqNum may panic if k is not valid.
func (k internalKey) seqNum() uint64 {
	i := len(k) - 7
	n := uint64(k[i+0])
	n |= uint64(k[i+1]) << 8
	n |= uint64(k[i+2]) << 16
	n |= uint64(k[i+3]) << 24
	n |= uint64(k[i+4]) << 32
	n |= uint64(k[i+5]) << 40
	n |= uint64(k[i+6]) << 48
	return n
}

// clone returns an internalKey that has the same contents but is backed by a
// different array.
func (k internalKey) clone() internalKey {
	x := make(internalKey, len(k))
	copy(x, k)
	return x
}

// internalKeyComparer is a db.Comparer that wraps another db.Comparer.
//
// It compares internal keys first by their user keys (as ordered by userCmp),
// then by sequence number (decreasing), then by kind (decreasing). The last
// step is only for completeness; for a given leveldb DB, no two internal keys
// should have the same sequence number.
//
// This ordering is designed so that when iterating through an internal table
// starting at (ukey0, seqNum0), one first encounters those entries with the
// same user key and lower sequence number (i.e. sets or deletes from earlier
// in time), followed by those entries with 'greater' user keys (where
// 'greater' is defined by userCmp). Specifically, one does not encounter
// entries with the same user key and higher sequence number (i.e. sets or
// deletes for ukey0 from the 'future' relative to the particular snapshot
// seqNum0 of the DB).
type internalKeyComparer struct {
	userCmp db.Comparer
}

var _ db.Comparer = internalKeyComparer{}

func (c internalKeyComparer) Compare(a, b []byte) int {
	ak, bk := internalKey(a), internalKey(b)
	if !ak.valid() {
		if bk.valid() {
			return -1
		}
		return bytes.Compare(a, b)
	}
	if !bk.valid() {
		return 1
	}
	if x := c.userCmp.Compare(ak.ukey(), bk.ukey()); x != 0 {
		return x
	}
	if an, bn := ak.seqNum(), bk.seqNum(); an < bn {
		return +1
	} else if an > bn {
		return -1
	}
	if ai, bi := ak.kind(), bk.kind(); ai < bi {
		return +1
	} else if ai > bi {
		return -1
	}
	return 0
}

func (c internalKeyComparer) Name() string {
	// This is the same name given by the C++ leveldb's InternalKeyComparator class.
	return "leveldb.InternalKeyComparator"
}

func (c internalKeyComparer) AppendSeparator(dst, a, b []byte) []byte {
	// TODO: this could be more sophisticated.
	return append(dst, a...)
}
