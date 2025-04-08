// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package blob

import (
	"encoding/binary"
	"unsafe"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/sstable/valblk"
	"github.com/cockroachdb/redact"
)

// MaxInlineHandleLength is the maximum length of an inline blob handle.
//
// Handle fields are varint encoded, so maximum 5 bytes each.
const MaxInlineHandleLength = 4 * binary.MaxVarintLen32

// Handle describes the location of a value stored within a blob file.
type Handle struct {
	FileNum       base.DiskFileNum
	BlockNum      uint32
	OffsetInBlock uint32
	ValueLen      uint32
}

// String implements the fmt.Stringer interface.
func (h Handle) String() string {
	return redact.StringWithoutMarkers(h)
}

// SafeFormat implements redact.SafeFormatter.
func (h Handle) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("(%s,blk%d[%d:%d])",
		h.FileNum, h.BlockNum, h.OffsetInBlock, h.OffsetInBlock+h.ValueLen)
}

// TODO(jackson): Consider encoding the handle's data using columnar block
// primitives, rather than a variable-width encoding in the value column.

// InlineHandle describes a handle as it is encoded within a sstable block. The
// inline handle does not encode the blob file number outright. Instead it
// encodes an index into the containing sstable's BlobReferences.
//
// The inline handle is composed of two parts: a preface (InlineHandlePreface)
// and a suffix (HandleSuffix). The preface is eagerly decoded from the encoded
// handle when returning an InternalValue to higher layers. The remaining bits
// (the suffix) are decoded only when the value is being fetched from the blob
// file.
type InlineHandle struct {
	InlineHandlePreface
	HandleSuffix
}

// ReferenceID identifies a particular blob reference within a table. It's
// implemented as an index into the slice of the BlobReferences recorded in the
// manifest.
type ReferenceID uint32

// InlineHandlePreface is the prefix of an inline handle. It's eagerly decoded
// when returning an InternalValue to higher layers.
type InlineHandlePreface struct {
	ReferenceID ReferenceID
	ValueLen    uint32
}

// HandleSuffix is the suffix of an inline handle. It's decoded only when the
// value is being fetched from the blob file.
type HandleSuffix struct {
	BlockNum      uint32
	OffsetInBlock uint32
}

// Encode encodes the handle suffix into the provided buffer, returning the
// number of bytes encoded.
func (h HandleSuffix) Encode(b []byte) int {
	n := 0
	n += binary.PutUvarint(b[n:], uint64(h.BlockNum))
	n += binary.PutUvarint(b[n:], uint64(h.OffsetInBlock))
	return n
}

// String implements the fmt.Stringer interface.
func (h InlineHandle) String() string {
	return redact.StringWithoutMarkers(h)
}

// SafeFormat implements redact.SafeFormatter.
func (h InlineHandle) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("(%d, blk%d[%d:%d])",
		h.ReferenceID, h.BlockNum, h.OffsetInBlock, h.OffsetInBlock+h.ValueLen)
}

// Encode encodes the inline handle into the provided buffer, returning the
// number of bytes encoded.
func (h InlineHandle) Encode(b []byte) int {
	n := 0
	n += binary.PutUvarint(b[n:], uint64(h.ReferenceID))
	n += valblk.EncodeHandle(b[n:], valblk.Handle{
		BlockNum:      h.BlockNum,
		OffsetInBlock: h.OffsetInBlock,
		ValueLen:      h.ValueLen,
	})
	return n
}

// DecodeInlineHandlePreface decodes the blob reference index and value length
// from the beginning of a variable-width encoded InlineHandle.
func DecodeInlineHandlePreface(src []byte) (InlineHandlePreface, []byte) {
	ptr := unsafe.Pointer(&src[0])
	var refIdx uint32
	if a := *((*uint8)(ptr)); a < 128 {
		refIdx = uint32(a)
		src = src[1:]
	} else if a, b := a&0x7f, *((*uint8)(unsafe.Add(ptr, 1))); b < 128 {
		refIdx = uint32(b)<<7 | uint32(a)
		src = src[2:]
	} else if b, c := b&0x7f, *((*uint8)(unsafe.Add(ptr, 2))); c < 128 {
		refIdx = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		src = src[3:]
	} else if c, d := c&0x7f, *((*uint8)(unsafe.Add(ptr, 3))); d < 128 {
		refIdx = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		src = src[4:]
	} else {
		d, e := d&0x7f, *((*uint8)(unsafe.Add(ptr, 4)))
		refIdx = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		src = src[5:]
	}

	ptr = unsafe.Pointer(&src[0])
	var valueLen uint32
	if a := *((*uint8)(ptr)); a < 128 {
		valueLen = uint32(a)
		src = src[1:]
	} else if a, b := a&0x7f, *((*uint8)(unsafe.Add(ptr, 1))); b < 128 {
		valueLen = uint32(b)<<7 | uint32(a)
		src = src[2:]
	} else if b, c := b&0x7f, *((*uint8)(unsafe.Add(ptr, 2))); c < 128 {
		valueLen = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		src = src[3:]
	} else if c, d := c&0x7f, *((*uint8)(unsafe.Add(ptr, 3))); d < 128 {
		valueLen = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		src = src[4:]
	} else {
		d, e := d&0x7f, *((*uint8)(unsafe.Add(ptr, 4)))
		valueLen = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		src = src[5:]
	}

	return InlineHandlePreface{
		ReferenceID: ReferenceID(refIdx),
		ValueLen:    valueLen,
	}, src
}

// DecodeHandleSuffix decodes the block number and offset in block from the
// encoded handle.
func DecodeHandleSuffix(src []byte) HandleSuffix {
	h := valblk.DecodeRemainingHandle(src)
	return HandleSuffix{
		BlockNum:      h.BlockNum,
		OffsetInBlock: h.OffsetInBlock,
	}
}
