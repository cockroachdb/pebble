// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import "fmt"

// This file defines sequence numbers that are reserved for foreign keys i.e.
// internal keys coming from other Pebble instances and existing in shared
// storage, as those will "slot below" any internal keys added by our own Pebble
// instance. Any keys created by this Pebble instance need to be greater than
// all reserved sequence numbers (i.e. >= SeqNumStart).
const (
	// SeqNumZero is the zero sequence number, set by compactions if they can
	// guarantee there are no keys underneath an internal key.
	SeqNumZero = uint64(0)

	// SeqNumL6 is the sequence number reserved for foreign keys in L6. This seqnum
	// will be used to expose any range key sets as well as point keys in L6. Range
	// deletes do not need to be exposed in L6.
	SeqNumL6 = uint64(1)

	// SeqNumL5 is the sequence number reserved for foreign keys in L5. This seqnum
	// needs to be greater than SeqNumL6, so that range deletes in L5 can be
	// exposed at this level and will correctly delete covering points in L6. Also
	// range key unsets/dels will be exposed at this seqnum and will need to shadow
	// overlapping range keys in L6.
	//
	// Note that we can use the same sequence number for all exposed keys in L5 as
	// range dels do not delete points at the same seqnum, and range key
	// unsets/deletes do not coalesce with range key sets at the same seqnum. Range
	// key masking does not care about the sequence number of overlapping points
	// (rather, it applies to points based on suffixes), so we can use this seqnum
	// for all L5 keys exposed.
	SeqNumL5 = uint64(2)

	// Sequence numbers 3-9 are reserved for future use.

	// SeqNumStart is the first sequence number assigned to a key written by
	// ourselves.
	SeqNumStart = uint64(10)
)

// SeqNumForLevel returns the appropriate reserved sequence number for keys in
// foreign sstables at the specified level.
func SeqNumForLevel(level int) uint64 {
	switch level {
	case 5:
		return SeqNumL5
	case 6:
		return SeqNumL6
	default:
		panic(fmt.Sprintf("unexpected foreign sstable at level %d", level))
	}
}
