// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

// This file defines sequence numbers that are reserved for foreign keys i.e.
// internal keys coming from other Pebble instances and existing in shared
// storage, as those will "slot below" any internal keys added by our own Pebble
// instance. Any keys created by this Pebble instance need to be greater than
// all reserved sequence numbers (i.e. >= SeqNumStart).
const (
	// SeqNumZero is the zero sequence number, set by compactions if they can
	// guarantee there are no keys underneath an internal key.
	SeqNumZero = uint64(0)

	// SeqNumL6Point is the sequence number reserved for foreign point keys in L6.
	// This sequence number must be lower than SeqNumL6RangeKey for range key
	// masking to work correctly.
	SeqNumL6Point = uint64(1)

	// SeqNumL6RangeKey is the sequence number reserved for foreign range keys in
	// L6. Only RangeKeySets are expected at this level.
	SeqNumL6RangeKey = uint64(2)

	// SeqNumL5RangeDel is the sequence number reserved for foreign range deletes
	// in L5. These keys could delete L6 points.
	SeqNumL5RangeDel = uint64(3)

	// SeqNumL5Point is the sequence number reserved for foreign point keys in L5.
	// Any sst-local range deletions would have already been applied to these keys,
	// so they can safely get a sequence number higher than SeqNumL5RangeDel.
	// However they must have a sequence number lower than SeqNumL5RangeKey* for
	// range key masking to work correctly.
	SeqNumL5Point = uint64(4)

	// SeqNumL5RangeKeyUnsetDel is the sequence number reserved for foreign
	// range key unsets/deletes in L5. These operations could apply to L6
	// RangeKeySets, so this sequence number must be > SeqNumL6RangeKey.
	SeqNumL5RangeKeyUnsetDel = uint64(5)

	// SeqNumL5RangeKeySet is the sequence number reserved for foreign range key
	// Sets in L5. These operations could apply to L6 RangeKeySets, so this
	// sequence number must be > SeqNumL6RangeKey. Any SST-local rangekey
	// unsets/dels have already been applied to them, so their sequence number must
	// be > SeqNumL5RangeKeyUnsetDel.
	SeqNumL5RangeKeySet = uint64(6)

	// Sequence numbers 7-9 are reserved for future use.

	// SeqNumStart is the first sequence number assigned to a key written by
	// ourselves.
	SeqNumStart = uint64(10)
)
