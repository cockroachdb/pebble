// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/sstable/block"
)

// SeqNum exports the base.SeqNum type.
type SeqNum = base.SeqNum

// InternalKeyKind exports the base.InternalKeyKind type.
type InternalKeyKind = base.InternalKeyKind

// These constants are part of the file format, and should not be changed.
const (
	InternalKeyKindDelete         = base.InternalKeyKindDelete
	InternalKeyKindSet            = base.InternalKeyKindSet
	InternalKeyKindMerge          = base.InternalKeyKindMerge
	InternalKeyKindLogData        = base.InternalKeyKindLogData
	InternalKeyKindSingleDelete   = base.InternalKeyKindSingleDelete
	InternalKeyKindSyntheticKey   = base.InternalKeyKindSyntheticKey
	InternalKeyKindRangeDelete    = base.InternalKeyKindRangeDelete
	InternalKeyKindMax            = base.InternalKeyKindMax
	InternalKeyKindSetWithDelete  = base.InternalKeyKindSetWithDelete
	InternalKeyKindRangeKeySet    = base.InternalKeyKindRangeKeySet
	InternalKeyKindRangeKeyUnset  = base.InternalKeyKindRangeKeyUnset
	InternalKeyKindRangeKeyDelete = base.InternalKeyKindRangeKeyDelete
	InternalKeyKindRangeKeyMin    = base.InternalKeyKindRangeKeyMin
	InternalKeyKindRangeKeyMax    = base.InternalKeyKindRangeKeyMax
	InternalKeyKindIngestSST      = base.InternalKeyKindIngestSST
	InternalKeyKindDeleteSized    = base.InternalKeyKindDeleteSized
	InternalKeyKindExcise         = base.InternalKeyKindExcise
	InternalKeyKindInvalid        = base.InternalKeyKindInvalid
)

// InternalKeyTrailer exports the base.InternalKeyTrailer type.
type InternalKeyTrailer = base.InternalKeyTrailer

// InternalKey exports the base.InternalKey type.
type InternalKey = base.InternalKey

// KeyRange exports the base.KeyRange type.
type KeyRange = base.KeyRange

// MakeInternalKey constructs an internal key from a specified user key,
// sequence number and kind.
func MakeInternalKey(userKey []byte, seqNum SeqNum, kind InternalKeyKind) InternalKey {
	return base.MakeInternalKey(userKey, seqNum, kind)
}

// MakeInternalKeyTrailer constructs a trailer from a specified sequence number
// and kind.
func MakeInternalKeyTrailer(seqNum SeqNum, kind InternalKeyKind) InternalKeyTrailer {
	return base.MakeTrailer(seqNum, kind)
}

type internalIterator = base.InternalIterator

type topLevelIterator = base.TopLevelIterator

// IsCorruptionError returns true if the given error indicates database
// corruption.
func IsCorruptionError(err error) bool {
	return base.IsCorruptionError(err)
}

// ErrCorruption is a marker to indicate that data in a file (WAL, MANIFEST,
// sstable) isn't in the expected format.
// DEPRECATED: should use IsCorruptionError() instead.
var ErrCorruption = base.ErrCorruption

// AttributeAndLen exports the base.AttributeAndLen type.
type AttributeAndLen = base.AttributeAndLen

// ShortAttribute exports the base.ShortAttribute type.
type ShortAttribute = base.ShortAttribute

// LazyFetcher exports the base.LazyFetcher type. This export is needed since
// LazyValue.Clone requires a pointer to a LazyFetcher struct to avoid
// allocations. No code outside Pebble needs to peer into a LazyFetcher.
type LazyFetcher = base.LazyFetcher

type CompressionCounters = block.CompressionCounters
