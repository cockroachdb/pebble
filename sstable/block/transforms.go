// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package block

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/pebble/internal/base"
)

// IterTransforms allow on-the-fly transformation of data at iteration time.
//
// These transformations could in principle be implemented as block transforms
// (at least for non-virtual sstables), but applying them during iteration is
// preferable.
type IterTransforms struct {
	// SyntheticSeqNum, if set, overrides the sequence number in all keys. It is
	// set if the sstable was ingested or it is foreign.
	SyntheticSeqNum SyntheticSeqNum
	// HideObsoletePoints, if true, skips over obsolete points during iteration.
	// This is the norm when the sstable is foreign or the largest sequence number
	// of the sstable is below the one we are reading.
	HideObsoletePoints bool
	SyntheticPrefix    SyntheticPrefix
	SyntheticSuffix    SyntheticSuffix
}

// NoTransforms is the default value for IterTransforms.
var NoTransforms = IterTransforms{}

// NoTransforms returns true if there are no transforms enabled.
func (t *IterTransforms) NoTransforms() bool {
	return t.SyntheticSeqNum == 0 &&
		!t.HideObsoletePoints &&
		!t.SyntheticPrefix.IsSet() &&
		!t.SyntheticSuffix.IsSet()
}

// FragmentIterTransforms allow on-the-fly transformation of range deletion or
// range key data at iteration time.
type FragmentIterTransforms struct {
	SyntheticSeqNum SyntheticSeqNum
	SyntheticPrefix SyntheticPrefix
	SyntheticSuffix SyntheticSuffix
}

// NoTransforms returns true if there are no transforms enabled.
func (t *FragmentIterTransforms) NoTransforms() bool {
	// NoTransforms returns true if there are no transforms enabled.
	return t.SyntheticSeqNum == 0 &&
		!t.SyntheticPrefix.IsSet() &&
		!t.SyntheticSuffix.IsSet()
}

// NoFragmentTransforms is the default value for IterTransforms.
var NoFragmentTransforms = FragmentIterTransforms{}

// SyntheticSeqNum is used to override all sequence numbers in a table. It is
// set to a non-zero value when the table was created externally and ingested
// whole.
type SyntheticSeqNum base.SeqNum

// NoSyntheticSeqNum is the default zero value for SyntheticSeqNum, which
// disables overriding the sequence number.
const NoSyntheticSeqNum SyntheticSeqNum = 0

// SyntheticSuffix will replace every suffix of every point key surfaced during
// block iteration. A synthetic suffix can be used if:
//  1. no two keys in the sst share the same prefix; and
//  2. pebble.Compare(prefix + replacementSuffix, prefix + originalSuffix) < 0,
//     for all keys in the backing sst which have a suffix (i.e. originalSuffix
//     is not empty).
//
// Range dels are not supported when synthetic suffix is used.
//
// For range keys, the synthetic suffix applies to the suffix that is part of
// RangeKeySet - if it is non-empty, it is replaced with the SyntheticSuffix.
// RangeKeyUnset keys are not supported when a synthetic suffix is used.
type SyntheticSuffix []byte

// IsSet returns true if the synthetic suffix is not empty.
func (ss SyntheticSuffix) IsSet() bool {
	return len(ss) > 0
}

// SyntheticPrefix represents a byte slice that is implicitly prepended to every
// key in a file being read or accessed by a reader. Note that since the byte
// slice is prepended to every KV rather than replacing a byte prefix, the
// result of prepending the synthetic prefix must be a full, valid key while the
// partial key physically stored within the sstable need not be a valid key
// according to user key semantics.
//
// Note that elsewhere we use the language of 'prefix' to describe the user key
// portion of a MVCC key, as defined by the Comparer's base.Split method. The
// SyntheticPrefix is related only in that it's a byte prefix that is
// incorporated into the logical MVCC prefix.
//
// The table's bloom filters are constructed only on the partial keys physically
// stored in the table, but interactions with the file including seeks and
// reads will all behave as if the file had been constructed from keys that
// include the synthetic prefix. Note that all Compare operations will act on a
// partial key (before any prepending), so the Comparer must support comparing
// these partial keys.
//
// The synthetic prefix will never modify key metadata stored in the key suffix.
//
// NB: Since this transformation currently only applies to point keys, a block
// with range keys cannot be iterated over with a synthetic prefix.
type SyntheticPrefix []byte

// IsSet returns true if the synthetic prefix is not enpty.
func (sp SyntheticPrefix) IsSet() bool {
	return len(sp) > 0
}

// Apply prepends the synthetic prefix to a key.
func (sp SyntheticPrefix) Apply(key []byte) []byte {
	res := make([]byte, 0, len(sp)+len(key))
	res = append(res, sp...)
	res = append(res, key...)
	return res
}

// Invert removes the synthetic prefix from a key.
func (sp SyntheticPrefix) Invert(key []byte) []byte {
	res, ok := bytes.CutPrefix(key, sp)
	if !ok {
		panic(fmt.Sprintf("unexpected prefix: %s", key))
	}
	return res
}
