// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package db

import (
	"bytes"
)

// Compare returns -1, 0, or +1 depending on whether a is 'less than',
// 'equal to' or 'greater than' b. The two arguments can only be 'equal'
// if their contents are exactly equal. Furthermore, the empty slice
// must be 'less than' any non-empty slice.
type Compare func(a, b []byte) int

// Equal returns true if a and b are equivalent. For a given comparer,
// Equal(a,b) must return the same result as Compare(a,b)==0. For most
// comparers, Equal can be set to bytes.Equal. In rare cases, byte equality
// differs from logical equality (e.g. a hidden suffix on keys that is not used
// for comparison purposes).
type Equal func(a, b []byte) bool

// InlineKey returns a fixed length prefix of a user key such that InlineKey(a)
// < InlineKey(b) iff a < b and InlineKey(a) > InlineKey(b) iff a > b. If
// InlineKey(a) == InlineKey(b) an additional comparison is required to
// determine if the two keys are actually equal.
type InlineKey func(key []byte) uint64

// Separator appends a sequence of bytes x to dst such that
// a <= x && x < b, where 'less than' is consistent with Compare.
// It returns the enlarged slice, like the built-in append function.
//
// Precondition: either a is 'less than' b, or b is an empty slice.
// In the latter case, empty means 'positive infinity', and appending any
// x such that a <= x will be valid.
//
// An implementation may simply be "return append(dst, a...)" but appending
// fewer bytes will result in smaller tables.
//
// For example, if dst, a and b are the []byte equivalents of the strings
// "aqua", "black" and "blue", then the result may be "aquablb".
// Similarly, if the arguments were "aqua", "green" and "", then the result
// may be "aquah".
type Separator func(dst, a, b []byte) []byte

// Successor returns a successor key such that k <= a. A simple implementation
// may return a unchanged. The dst parameter may be used to store the returned
// key, though it is valid to pass a nil.
type Successor func(dst, a []byte) []byte

// Comparer defines a total ordering over the space of []byte keys: a 'less
// than' relationship.
type Comparer struct {
	Compare   Compare
	Equal     Equal
	InlineKey InlineKey
	Separator Separator
	Successor Successor

	// Name is the name of the comparer.
	//
	// The Level-DB on-disk format stores the comparer name, and opening a
	// database with a different comparer from the one it was created with
	// will result in an error.
	Name string
}

// DefaultComparer is the default implementation of the Comparer interface.
// It uses the natural ordering, consistent with bytes.Compare.
var DefaultComparer = &Comparer{
	Compare: bytes.Compare,
	Equal:   bytes.Equal,

	InlineKey: func(key []byte) uint64 {
		var v uint64
		n := 8
		if n > len(key) {
			n = len(key)
		}
		for _, b := range key[:n] {
			v <<= 8
			v |= uint64(b)
		}
		return v
	},

	Separator: func(dst, a, b []byte) []byte {
		i, n := SharedPrefixLen(a, b), len(dst)
		dst = append(dst, a...)

		min := len(a)
		if min > len(b) {
			min = len(b)
		}
		if i >= min {
			// Do not shorten if one string is a prefix of the other.
			return dst
		}

		if a[i] >= b[i] {
			// b is smaller than a or a is already the shortest possible.
			return dst
		}

		if i < len(b)-1 || a[i]+1 < b[i] {
			i += n
			dst[i]++
			return dst[:i+1]
		}

		i += n + 1
		for ; i < len(dst); i++ {
			if dst[i] != 0xff {
				dst[i]++
				return dst[:i+1]
			}
		}
		return dst
	},

	Successor: func(dst, a []byte) []byte {
		for i := 0; i < len(a); i++ {
			if a[i] != 0xff {
				dst = append(dst, a[:i+1]...)
				dst[len(dst)-1]++
				return dst
			}
		}
		return append(dst, a...)
	},

	// This name is part of the C++ Level-DB implementation's default file
	// format, and should not be changed.
	Name: "leveldb.BytewiseComparator",
}

// SharedPrefixLen returns the largest i such that a[:i] equals b[:i].
// This function can be useful in implementing the Comparer interface.
func SharedPrefixLen(a, b []byte) int {
	i, n := 0, len(a)
	if n > len(b) {
		n = len(b)
	}
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}
