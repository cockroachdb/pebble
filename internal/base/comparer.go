// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"
	"strconv"
	"unicode/utf8"

	"github.com/cockroachdb/errors"
)

// Compare returns -1, 0, or +1 depending on whether a is 'less than', 'equal
// to' or 'greater than' b.
//
// Both a and b must be valid keys.
//
// A key a is less than b if a's prefix is byte-wise less than b's prefix, or if
// the prefixes are equal and a's suffix is less than b's suffix (according to
// CompareSuffixes).
//
// In other words, if prefix(a) = a[:Split(a)] and suffix(a) = a[Split(a):]:
//
//	  Compare(a, b) = bytes.Compare(prefix(a), prefix(b)) if not 0,
//		                otherwise either 0 or CompareSuffixes(suffix(a), suffix(b))
//
// The "either 0" part is because CompareSuffixes is allowed to be more strict;
// see CompareSuffixes.
//
// Compare defaults to using the formula above but it can be customized if there
// is a (potentially faster) specialization or it has to compare suffixes
// differently.
type Compare func(a, b []byte) int

// CompareSuffixes compares two key suffixes and returns -1, 0, or +1.
//
// For historical reasons (see
// https://github.com/cockroachdb/cockroach/issues/130533 for a summary), we
// allow this function to be more strict than Compare. Specifically, Compare may
// treat two suffixes as equal whereas CompareSuffixes might not.
//
// The empty slice suffix must be 'less than' any non-empty suffix.
//
// A full key k is composed of a prefix k[:Split(k)] and suffix k[Split(k):].
// Suffixes are compared to break ties between equal prefixes.
type CompareSuffixes func(a, b []byte) int

// defaultCompare implements Compare in terms of Split and CompareSuffixes, as
// mentioned above.
func defaultCompare(split Split, compareSuffixes CompareSuffixes, a, b []byte) int {
	an := split(a)
	bn := split(b)
	if prefixCmp := bytes.Compare(a[:an], b[:bn]); prefixCmp != 0 {
		return prefixCmp
	}
	return compareSuffixes(a[an:], b[bn:])
}

// Equal returns true if a and b are equivalent.
//
// For a given Compare, Equal(a,b)=true iff Compare(a,b)=0; that is, Equal is a
// (potentially faster) specialization of Compare.
type Equal func(a, b []byte) bool

// AbbreviatedKey returns a fixed length prefix of a user key such that
//
//	AbbreviatedKey(a) < AbbreviatedKey(b) implies a < b, and
//	AbbreviatedKey(a) > AbbreviatedKey(b) implies a > b.
//
// If AbbreviatedKey(a) == AbbreviatedKey(b), an additional comparison is
// required to determine if the two keys are actually equal.
//
// This helps optimize indexed batch comparisons for cache locality. If a Split
// function is specified, AbbreviatedKey usually returns the first eight bytes
// of the user key prefix in the order that gives the correct ordering.
type AbbreviatedKey func(key []byte) uint64

// FormatKey returns a formatter for the user key.
type FormatKey func(key []byte) fmt.Formatter

// DefaultFormatter is the default implementation of user key formatting:
// non-ASCII data is formatted as escaped hexadecimal values.
var DefaultFormatter FormatKey = func(key []byte) fmt.Formatter {
	return FormatBytes(key)
}

// FormatValue returns a formatter for the user value. The key is also specified
// for the value formatter in order to support value formatting that is
// dependent on the key.
type FormatValue func(key, value []byte) fmt.Formatter

// Separator is used to construct SSTable index blocks. A trivial implementation
// is `return append(dst, a...)`, but appending fewer bytes leads to smaller
// SSTables.
//
// Given keys a, b for which Compare(a, b) < 0, Separator produces a key k such
// that:
//
// 1. Compare(a, k) <= 0, and
// 2. Compare(k, b) < 0.
//
// For example, if a and b are the []byte equivalents of the strings "black" and
// "blue", then the function may append "blb" to dst.
type Separator func(dst, a, b []byte) []byte

// Successor appends to dst a shortened key k given a key a such that
// Compare(a, k) <= 0. A simple implementation may return a unchanged.
// The appended key k must be valid to pass to Compare.
type Successor func(dst, a []byte) []byte

// ImmediateSuccessor is invoked with a prefix key ([Split(a) == len(a)]) and
// appends to dst the smallest prefix key that is larger than the given prefix a.
//
// ImmediateSuccessor must generate a prefix key k such that:
//
//	Split(k) == len(k) and Compare(a, k) < 0
//
// and there exists no representable prefix key k2 such that:
//
//	Split(k2) == len(k2) and Compare(a, k2) < 0 and Compare(k2, k) < 0
//
// As an example, an implementation built on the natural byte ordering using
// bytes.Compare could append a `\0` to `a`.
//
// The appended key must be valid to pass to Compare.
type ImmediateSuccessor func(dst, a []byte) []byte

// Split returns the length of the prefix of the user key that corresponds to
// the key portion of an MVCC encoding scheme to enable the use of prefix bloom
// filters.
//
// The method will only ever be called with valid MVCC keys, that is, keys that
// the user could potentially store in the database. Pebble does not know which
// keys are MVCC keys and which are not, and may call Split on both MVCC keys
// and non-MVCC keys.
//
// A trivial MVCC scheme is one in which Split() returns len(a). This
// corresponds to assigning a constant version to each key in the database. For
// performance reasons, it is preferable to use a `nil` split in this case.
//
// Let prefix(a) = a[:Split(a)] and suffix(a) = a[Split(a):]. The following
// properties must hold:
//
//  1. A key consisting of just a prefix must sort before all other keys with
//     that prefix:
//
//     If len(suffix(a)) > 0, then Compare(prefix(a), a) < 0.
//
//  2. Prefixes must be used to order keys before suffixes:
//
//     If Compare(a, b) <= 0, then Compare(prefix(a), prefix(b)) <= 0.
//     If Compare(prefix(a), prefix(b)) < 0, then Compare(a, b) < 0
//
//  3. Suffixes themselves must be valid keys and comparable, respecting the same
//     ordering as within a key:
//
//     If Compare(prefix(a), prefix(b)) = 0, then Compare(a, b) = Compare(suffix(a), suffix(b)).
type Split func(a []byte) int

// Prefix returns the prefix of the key k, using s to split the key.
func (s Split) Prefix(k []byte) []byte {
	i := s(k)
	return k[:i:i]
}

// DefaultSplit is a trivial implementation of Split which always returns the
// full key.
var DefaultSplit Split = func(key []byte) int { return len(key) }

// Comparer defines a total ordering over the space of []byte keys: a 'less
// than' relationship.
type Comparer struct {
	// The following must always be specified.
	AbbreviatedKey AbbreviatedKey
	Separator      Separator
	Successor      Successor

	// ImmediateSuccessor must be specified if range keys are used.
	ImmediateSuccessor ImmediateSuccessor

	// Split defaults to a trivial implementation that returns the full key length
	// if it is not specified.
	Split Split

	// CompareSuffixes defaults to bytes.Compare if it is not specified.
	CompareSuffixes CompareSuffixes

	// Compare defaults to a generic implementation that uses Split,
	// bytes.Compare, and CompareSuffixes if it is not specified.
	Compare Compare
	// Equal defaults to using Compare() == 0 if it is not specified.
	Equal Equal
	// FormatKey defaults to the DefaultFormatter if it is not specified.
	FormatKey FormatKey

	// FormatValue is optional.
	FormatValue FormatValue

	// Name is the name of the comparer.
	//
	// The on-disk format stores the comparer name, and opening a database with a
	// different comparer from the one it was created with will result in an
	// error.
	Name string
}

// EnsureDefaults ensures that all non-optional fields are set.
//
// If c is nil, returns DefaultComparer.
//
// If any fields need to be set, returns a modified copy of c.
func (c *Comparer) EnsureDefaults() *Comparer {
	if c == nil {
		return DefaultComparer
	}
	if c.AbbreviatedKey == nil || c.Separator == nil || c.Successor == nil || c.Name == "" {
		panic("invalid Comparer: mandatory field not set")
	}
	if c.CompareSuffixes != nil && c.Compare != nil && c.Equal != nil && c.Split != nil && c.FormatKey != nil {
		return c
	}
	n := &Comparer{}
	*n = *c

	if n.Split == nil {
		n.Split = DefaultSplit
	}
	if n.CompareSuffixes == nil && n.Compare == nil && n.Equal == nil {
		n.CompareSuffixes = bytes.Compare
		n.Compare = bytes.Compare
		n.Equal = bytes.Equal
	} else {
		if n.CompareSuffixes == nil {
			n.CompareSuffixes = bytes.Compare
		}
		if n.Compare == nil {
			n.Compare = func(a, b []byte) int {
				return defaultCompare(n.Split, n.CompareSuffixes, a, b)
			}
		}
		if n.Equal == nil {
			n.Equal = func(a, b []byte) bool {
				return n.Compare(a, b) == 0
			}
		}
	}
	if n.FormatKey == nil {
		n.FormatKey = DefaultFormatter
	}
	return n
}

// DefaultComparer is the default implementation of the Comparer interface.
// It uses the natural ordering, consistent with bytes.Compare.
var DefaultComparer = &Comparer{
	CompareSuffixes: bytes.Compare,
	Compare:         bytes.Compare,
	Equal:           bytes.Equal,

	AbbreviatedKey: func(key []byte) uint64 {
		if len(key) >= 8 {
			return binary.BigEndian.Uint64(key)
		}
		var v uint64
		for _, b := range key {
			v <<= 8
			v |= uint64(b)
		}
		return v << uint(8*(8-len(key)))
	},

	Split: DefaultSplit,

	FormatKey: DefaultFormatter,

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

	Successor: func(dst, a []byte) (ret []byte) {
		for i := 0; i < len(a); i++ {
			if a[i] != 0xff {
				dst = append(dst, a[:i+1]...)
				dst[len(dst)-1]++
				return dst
			}
		}
		// a is a run of 0xffs, leave it alone.
		return append(dst, a...)
	},

	ImmediateSuccessor: func(dst, a []byte) (ret []byte) {
		return append(append(dst, a...), 0x00)
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
	asUint64 := func(c []byte, i int) uint64 {
		return binary.LittleEndian.Uint64(c[i:])
	}
	for i < n-7 && asUint64(a, i) == asUint64(b, i) {
		i += 8
	}
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

// MinUserKey returns the smaller of two user keys. If one of the keys is nil,
// the other one is returned.
func MinUserKey(cmp Compare, a, b []byte) []byte {
	if a != nil && (b == nil || cmp(a, b) < 0) {
		return a
	}
	return b
}

// FormatBytes formats a byte slice using hexadecimal escapes for non-ASCII
// data.
type FormatBytes []byte

const lowerhex = "0123456789abcdef"

// Format implements the fmt.Formatter interface.
func (p FormatBytes) Format(s fmt.State, c rune) {
	buf := make([]byte, 0, len(p))
	for _, b := range p {
		if b < utf8.RuneSelf && strconv.IsPrint(rune(b)) {
			buf = append(buf, b)
			continue
		}
		buf = append(buf, `\x`...)
		buf = append(buf, lowerhex[b>>4])
		buf = append(buf, lowerhex[b&0xF])
	}
	s.Write(buf)
}

// MakeAssertComparer creates a Comparer that is the same with the given
// Comparer except that it asserts that the Compare and Equal functions adhere
// to their specifications.
func MakeAssertComparer(c Comparer) Comparer {
	return Comparer{
		Compare: func(a []byte, b []byte) int {
			res := c.Compare(a, b)
			// Verify that Compare is consistent with the default implementation.
			if expected := defaultCompare(c.Split, c.CompareSuffixes, a, b); res != expected {
				panic(AssertionFailedf("%s: Compare(%s, %s)=%d, expected %d",
					c.Name, c.FormatKey(a), c.FormatKey(b), res, expected))
			}
			return res
		},

		Equal: func(a []byte, b []byte) bool {
			eq := c.Equal(a, b)
			// Verify that Equal is consistent with Compare.
			if expected := c.Compare(a, b); eq != (expected == 0) {
				panic("Compare and Equal are not consistent")
			}
			return eq
		},

		// TODO(radu): add more checks.
		CompareSuffixes:    c.CompareSuffixes,
		AbbreviatedKey:     c.AbbreviatedKey,
		Separator:          c.Separator,
		Successor:          c.Successor,
		ImmediateSuccessor: c.ImmediateSuccessor,
		FormatKey:          c.FormatKey,
		Split:              c.Split,
		FormatValue:        c.FormatValue,
		Name:               c.Name,
	}
}

// CheckComparer is a mini test suite that verifies a comparer implementation.
//
// It takes lists of valid prefixes and suffixes. It is recommended that both
// lists have at least three elements.
func CheckComparer(c *Comparer, prefixes [][]byte, suffixes [][]byte) error {
	// Empty slice is always a valid suffix.
	suffixes = append(suffixes, nil)

	// Verify the suffixes have a consistent ordering.
	slices.SortFunc(suffixes, c.CompareSuffixes)
	if !slices.IsSortedFunc(suffixes, c.CompareSuffixes) {
		return errors.Errorf("CompareSuffixes is inconsistent")
	}

	// Check the split function.
	for _, p := range prefixes {
		for _, s := range suffixes {
			key := slices.Concat(p, s)
			if n := c.Split(key); n != len(p) {
				return errors.Errorf("incorrect Split result %d on '%x' (prefix '%x' suffix '%x')", n, key, p, s)
			}
		}
		for i := 1; i < len(suffixes); i++ {
			a := slices.Concat(p, suffixes[i-1])
			b := slices.Concat(p, suffixes[i])
			// Make sure the Compare function agrees with CompareSuffixes, with the
			// caveat that it can consider some consecutive suffixes to be equal.
			if cmp := c.Compare(a, b); cmp > 0 {
				return errors.Errorf("Compare(%s, %s)=%d, expected <= 0", c.FormatKey(a), c.FormatKey(b), cmp)
			}
		}
	}

	// Check the Compare/Equals functions on all possible combinations.
	for _, ap := range prefixes {
		for _, as := range suffixes {
			a := slices.Concat(ap, as)
			for _, bp := range prefixes {
				for _, bs := range suffixes {
					b := slices.Concat(bp, bs)
					result := c.Compare(a, b)
					if (result == 0) != c.Equal(a, b) {
						return errors.Errorf("Equal(%s, %s) doesn't agree with Compare", c.FormatKey(a), c.FormatKey(b))
					}

					if prefixCmp := bytes.Compare(ap, bp); prefixCmp != 0 {
						if result != prefixCmp {
							return errors.Errorf("Compare(%s, %s)=%d, expected %d", c.FormatKey(a), c.FormatKey(b), result, prefixCmp)
						}
					} else {
						// We allow Compare to return equality for suffixes even when
						// CompareSuffixes does not.
						if result != 0 {
							if suffixCmp := c.CompareSuffixes(as, bs); result != suffixCmp {
								return errors.Errorf("Compare(%s, %s)=%d, expected %d", c.FormatKey(a), c.FormatKey(b), result, suffixCmp)
							}
						}
					}
				}
			}
		}
	}

	// TODO(radu): check more methods.
	return nil
}
