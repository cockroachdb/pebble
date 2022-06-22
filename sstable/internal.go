// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/rangekey"
)

// InternalKeyKind exports the base.InternalKeyKind type.
type InternalKeyKind = base.InternalKeyKind

// These constants are part of the file format, and should not be changed.
const (
	InternalKeyKindDelete          = base.InternalKeyKindDelete
	InternalKeyKindSet             = base.InternalKeyKindSet
	InternalKeyKindMerge           = base.InternalKeyKindMerge
	InternalKeyKindLogData         = base.InternalKeyKindLogData
	InternalKeyKindRangeDelete     = base.InternalKeyKindRangeDelete
	InternalKeyKindMax             = base.InternalKeyKindMax
	InternalKeyKindInvalid         = base.InternalKeyKindInvalid
	InternalKeySeqNumBatch         = base.InternalKeySeqNumBatch
	InternalKeySeqNumMax           = base.InternalKeySeqNumMax
	InternalKeyRangeDeleteSentinel = base.InternalKeyRangeDeleteSentinel
)

// InternalKey exports the base.InternalKey type.
type InternalKey = base.InternalKey

// IsRangeKey returns if this InternalKey is a range key. Alias for
// rangekey.IsRangeKey.
func IsRangeKey(ik InternalKey) bool {
	return rangekey.IsRangeKey(ik.Kind())
}

// DecodeRangeKey decodes an InternalKey into a keyspan.Span, if it is a range
// key. If keysDst is provided, keys will be appended to keysDst to reduce
// allocations.
func DecodeRangeKey(ik InternalKey, val []byte, keysDst []keyspan.Key) (keyspan.Span, error) {
	return rangekey.Decode(ik, val, keysDst)
}
