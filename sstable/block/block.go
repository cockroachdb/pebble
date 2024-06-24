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
	SyntheticSeqNum    SyntheticSeqNum
	HideObsoletePoints bool
	SyntheticPrefix    SyntheticPrefix
	SyntheticSuffix    SyntheticSuffix
}

// NoTransforms is the default value for IterTransforms.
var NoTransforms = IterTransforms{}

// SyntheticSeqNum is used to override all sequence numbers in a table. It is
// set to a non-zero value when the table was created externally and ingested
// whole.
type SyntheticSeqNum base.SeqNum

// NoSyntheticSeqNum is the default zero value for SyntheticSeqNum, which
// disables overriding the sequence number.
const NoSyntheticSeqNum SyntheticSeqNum = 0

// SyntheticSuffix will replace every suffix of every key surfaced during block
// iteration. A synthetic suffix can be used if:
//  1. no two keys in the sst share the same prefix; and
//  2. pebble.Compare(prefix + replacementSuffix, prefix + originalSuffix) < 0,
//     for all keys in the backing sst which have a suffix (i.e. originalSuffix
//     is not empty).
type SyntheticSuffix []byte

// IsSet returns true if the synthetic suffix is not enpty.
func (ss SyntheticSuffix) IsSet() bool {
	return len(ss) > 0
}

// SyntheticPrefix represents a byte slice that is implicitly prepended to every
// key in a file being read or accessed by a reader.  Note that the table is
// assumed to contain "prefix-less" keys that become full keys when prepended
// with the synthetic prefix. The table's bloom filters are constructed only on
// the "prefix-less" keys in the table, but interactions with the file including
// seeks and reads, will all behave as if the file had been constructed from
// keys that did include the prefix. Note that all Compare operations may act on
// a prefix-less key as the synthetic prefix will never modify key metadata
// stored in the key suffix.
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
