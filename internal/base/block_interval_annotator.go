// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import (
	"bytes"
	"encoding/binary"
	"github.com/cockroachdb/errors"
)

// Block interval annotations are an optional user-facing feature that can be
// used to filter data blocks from an Iterator before they are loaded.
//
// Interval annotations are of the form [lower,upper) where both lower and
// upper are uint64, and represent a set such that the block can contain keys
// that belong to this set, but no keys outside this set. That is, the set is
// not necessarily tight.
//
// The interval [0,x) for any x is reserved and represents the universal set.
// A [0,x) annotation is not actually written out to the sstable, and is
// useful in two cases:
// - These annotations are written as part of the BlockHandle in index blocks
//   (first level and second level), where they refer to either data blocks
//   (when written in a first level index block) or first level index blocks
//   (when written in a second level index block). BlockHandles for other
//   kinds of blocks default to the [0,0) annotation and avoid writing
//   anything for the annotation.
// - Tables written prior to this feature also default to blocks having a
//   [0,0) annotation, which is correctly interpreted as the universal set
//   since we do not have any useful filtering information.
//
// The implementation requires [lower,upper) to satisfy upper >= lower. And
// when lower > 0, these are encoded as either:
// - lower, upper-lower when upper-lower > 0
// - lower
//
// The implementation does not require any particular lower value for
// representation of the empty set [lower,lower), but from an encoding
// perspective [1,1) is the most efficient.

// BlockIntervalAnnotatorFunc creates a new BlockIntervalAnnotator. A
// previously created one can be passed in for reuse. These annotators should
// only be created for data blocks.
type BlockIntervalAnnotatorFunc func(reuse BlockIntervalAnnotator) BlockIntervalAnnotator

// BlockIntervalAnnotator is given the keys that are being added to the data
// block, and tracks the [lower,upper) interval for that block.
type BlockIntervalAnnotator interface {
	// AddKey is passed each key being added to the data block.
	AddKey(key []byte) error
	// Finish returns the interval for this block.
	Finish() (BlockInterval, error)
}

// BlockInterval is the interval for a data block or a first level index
// block. Represents [Lower, Upper).
type BlockInterval struct {
	Lower uint64
	Upper uint64
}

// Union merges x into the given BlockInterval. Initialization cannot be done
// via unioning into a default initialized BlockInterval since that represents
// the universal set.
func (bi *BlockInterval) Union(x BlockInterval) {
	if bi.Lower == 0 {
		// Already the universal set.
		return
	}
	if x.Lower == 0 {
		// Change to universal set.
		*bi = BlockInterval{Lower: 0, Upper: 0}
		return
	}
	// Neither is the universal set.
	if x.Lower == x.Upper {
		// x is the empty set.
		return
	}
	if bi.Lower > x.Lower {
		bi.Lower = x.Lower
	}
	if bi.Upper < x.Upper {
		bi.Upper = x.Upper
	}
}

// BlockIntervalsIntersect compares the block intervals for an iterator and a
// block in an sstable. This is not precisely set intersection (see the code
// below).
func BlockIntervalsIntersect(iter, block BlockInterval) bool {
	if isEmptySet(iter) {
		// Bizarre iterator that is interested in nothing.
		return false
	}
	if iter.Lower == 0 {
		// Universal set. We don't care whether block is empty or not, since
		// this iterator wants to see everything.
		return true
	}
	// INVARIANT: iter is a non-empty and non-universal set.
	if block.Lower == 0 {
		// Universal set, so must intersect.
		return true
	}
	if isEmptySet(block) {
		return false
	}
	// INVARIANT: Neither set is empty or universal set.
	return !(iter.Upper <= block.Lower || iter.Lower >= block.Upper)
}

func isEmptySet(x BlockInterval) bool {
	return x.Lower != 0 && x.Lower == x.Upper
}

// Following will move to the CockroachDB code. It is here only for
// illustration.

func crdbBIAF(reuse BlockIntervalAnnotator) BlockIntervalAnnotator {
	if reuse == nil {
		return &crdbBIA{}
	}
	bia := reuse.(*crdbBIA)
	*bia = crdbBIA{}
	return bia
}

type crdbBIA struct {
	// Keep the encoded timestamps in min, max and decode in Finish for
	// constructing the BlockInterval.
	min, max []byte
	err error
}

const engineKeyVersionWallTimeLen = 8
const engineKeyVersionWallAndLogicalTimeLen = 12
const engineKeyVersionWallLogicalAndSyntheticTimeLen = 13

func (ia *crdbBIA) AddKey(key []byte) error {
	if len(key) == 0 {
		return nil
	}
	// Last byte is the version length + 1 when there is a version,
	// else it is 0.
	versionLen := int(key[len(key)-1])
	// keyPartEnd points to the sentinel byte.
	keyPartEnd := len(key) - 1 - versionLen
	if keyPartEnd < 0 {
		ia.err = errors.Errorf("invalid key")
		return ia.err
	}
	if versionLen > 0 && (versionLen == engineKeyVersionWallTimeLen ||
		versionLen == engineKeyVersionWallAndLogicalTimeLen ||
		versionLen == engineKeyVersionWallLogicalAndSyntheticTimeLen) {
		// Version consists of the bytes after the sentinel and before the length.
		key = key[keyPartEnd+1 : len(key)-1]
		if len(ia.min) == 0 || bytes.Compare(key, ia.min) < 0 {
			ia.min = append(ia.min[:0], key...)
		}
		if len(ia.max) == 0 || bytes.Compare(key, ia.max) > 0 {
			ia.max = append(ia.max[:0], key...)
		}
	}
	return nil
}

func decodeWallTime(ts []byte) uint64 {
	return binary.BigEndian.Uint64(ts[0:8])
}

func (ia *crdbBIA) Finish() (BlockInterval, error) {
	if ia.err != nil {
		// Universal set interval.
		return BlockInterval{}, ia.err
	}
	if len(ia.min) == 0 {
		// No calls to AddKey that contained a timestamped key. [1, 1) is the
		// empty interval that is cheap to encode.
		return BlockInterval{Lower: 1, Upper: 1}, nil
	}

	var interval BlockInterval
	interval.Lower = decodeWallTime(ia.min)
	// The actual value encoded into walltime is an int64, so +1 will not
	// overflow.
	interval.Upper = decodeWallTime(ia.max) + 1
	// Check validity of [lower, upper).
	if interval.Lower >= interval.Upper {
		// Universal set interval.
		return BlockInterval{}, errors.Errorf(
			"unexpected lower %d >= upper %d", interval.Lower, interval.Upper)
	}
	return interval, nil
}

