// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"github.com/cockroachdb/pebble/v2/internal/base"
	"github.com/cockroachdb/pebble/v2/internal/keyspan"
	"github.com/cockroachdb/pebble/v2/sstable/blob"
	"github.com/cockroachdb/pebble/v2/sstable/valblk"
)

// These constants are part of the file format, and should not be changed.
const (
	InternalKeyKindDelete        = base.InternalKeyKindDelete
	InternalKeyKindSet           = base.InternalKeyKindSet
	InternalKeyKindMerge         = base.InternalKeyKindMerge
	InternalKeyKindLogData       = base.InternalKeyKindLogData
	InternalKeyKindSingleDelete  = base.InternalKeyKindSingleDelete
	InternalKeyKindRangeDelete   = base.InternalKeyKindRangeDelete
	InternalKeyKindSetWithDelete = base.InternalKeyKindSetWithDelete
	InternalKeyKindDeleteSized   = base.InternalKeyKindDeleteSized
	InternalKeyKindMax           = base.InternalKeyKindMax
	InternalKeyKindInvalid       = base.InternalKeyKindInvalid
)

// InternalKey exports the base.InternalKey type.
type InternalKey = base.InternalKey

// Span exports the keyspan.Span type.
type Span = keyspan.Span

const valueBlocksIndexHandleMaxLen = blockHandleMaxLenWithoutProperties + 3

// Assert blockHandleLikelyMaxLen >= valueBlocksIndexHandleMaxLen.
const _ = uint(blockHandleLikelyMaxLen - valueBlocksIndexHandleMaxLen)

// Assert blockHandleLikelyMaxLen >= (valblk.HandleMaxLen+1).
//
// The additional 1 is for the 'valuePrefix' byte which prefaces values in
// recent SSTable versions.
const _ = uint(blockHandleLikelyMaxLen - valblk.HandleMaxLen - 1)

// Assert blockHandleLikelyMaxLen >= (blob.MaxInlineHandleLength+1).
//
// The additional 1 is for the 'valuePrefix' byte which prefaces values in recent
// SSTable versions.
const _ = uint(blockHandleLikelyMaxLen - blob.MaxInlineHandleLength - 1)
