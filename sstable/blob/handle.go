// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package blob

import (
	"encoding/binary"
	"unsafe"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/redact"
)

// MaxInlineHandleLength is the maximum length of an inline blob handle.
//
// Handle fields are varint encoded, so maximum 5 bytes each.
const MaxInlineHandleLength = 4 * binary.MaxVarintLen32

// BlockValueID identifies a value within a block of a blob file. The
// BlockValueID is local to the block.
type BlockValueID uint32

// BlockID identifies a block within a blob file. If a blob file has not been
// rewritten, the block ID is simply an index of the block within the file. If
// the blob file has been rewritten to reclaim disk space, the rewritten blob
// file will contain fewer blocks than the original. The rewritten blob file's
// index block contains a column mapping the original block ID to the index of
// the block in the new blob file containing the original block's data.
type BlockID uint32

// Handle describes the location of a value stored within a blob file.
type Handle struct {
	FileNum  base.DiskFileNum
	ValueLen uint32
	// BlockID identifies the block within the blob file containing the value.
	BlockID BlockID
	// ValueID identifies the value within the block identified by BlockID.
	ValueID BlockValueID
}

// String implements the fmt.Stringer interface.
func (h Handle) String() string {
	return redact.StringWithoutMarkers(h)
}

// SafeFormat implements redact.SafeFormatter.
func (h Handle) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("(%s,blk%d,id%d,len%d)", h.FileNum, h.BlockID, h.ValueID, h.ValueLen)
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
	BlockID BlockID
	ValueID BlockValueID
}

// Encode encodes the handle suffix into the provided buffer, returning the
// number of bytes encoded.
func (h HandleSuffix) Encode(b []byte) int {
	n := binary.PutUvarint(b, uint64(h.BlockID))
	n += binary.PutUvarint(b[n:], uint64(h.ValueID))
	return n
}

// String implements the fmt.Stringer interface.
func (h InlineHandle) String() string {
	return redact.StringWithoutMarkers(h)
}

// SafeFormat implements redact.SafeFormatter.
func (h InlineHandle) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("(f%d,blk%d,id%d,len%d)", h.ReferenceID, h.BlockID, h.ValueID, h.ValueLen)
}

// Encode encodes the inline handle into the provided buffer, returning the
// number of bytes encoded.
func (h InlineHandle) Encode(b []byte) int {
	n := 0
	n += binary.PutUvarint(b[n:], uint64(h.ReferenceID))
	n += binary.PutUvarint(b[n:], uint64(h.ValueLen))
	n += h.HandleSuffix.Encode(b[n:])
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

// DecodeHandleSuffix decodes the HandleSuffix from the provided buffer.
func DecodeHandleSuffix(src []byte) HandleSuffix {
	var vs HandleSuffix
	ptr := unsafe.Pointer(&src[0])
	// Manually inlined uvarint decoding. Saves ~25% in benchmarks. Unrolling
	// a loop for i:=0; i<2; i++, saves ~6%.
	var v uint32
	if a := *((*uint8)(ptr)); a < 128 {
		v = uint32(a)
		ptr = unsafe.Add(ptr, 1)
	} else if a, b := a&0x7f, *((*uint8)(unsafe.Add(ptr, 1))); b < 128 {
		v = uint32(b)<<7 | uint32(a)
		ptr = unsafe.Add(ptr, 2)
	} else if b, c := b&0x7f, *((*uint8)(unsafe.Add(ptr, 2))); c < 128 {
		v = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Add(ptr, 3)
	} else if c, d := c&0x7f, *((*uint8)(unsafe.Add(ptr, 3))); d < 128 {
		v = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Add(ptr, 4)
	} else {
		d, e := d&0x7f, *((*uint8)(unsafe.Add(ptr, 4)))
		v = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Add(ptr, 5)
	}
	vs.BlockID = BlockID(v)

	if a := *((*uint8)(ptr)); a < 128 {
		v = uint32(a)
	} else if a, b := a&0x7f, *((*uint8)(unsafe.Add(ptr, 1))); b < 128 {
		v = uint32(b)<<7 | uint32(a)
	} else if b, c := b&0x7f, *((*uint8)(unsafe.Add(ptr, 2))); c < 128 {
		v = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
	} else if c, d := c&0x7f, *((*uint8)(unsafe.Add(ptr, 3))); d < 128 {
		v = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
	} else {
		d, e := d&0x7f, *((*uint8)(unsafe.Add(ptr, 4)))
		v = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
	}
	vs.ValueID = BlockValueID(v)
	return vs
}
