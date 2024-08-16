// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package block

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/crc"
)

// Handle is the file offset and length of a block.
type Handle struct {
	Offset, Length uint64
}

// EncodeVarints encodes the block handle into dst using a variable-width
// encoding and returns the number of bytes written.
func (h Handle) EncodeVarints(dst []byte) int {
	n := binary.PutUvarint(dst, h.Offset)
	m := binary.PutUvarint(dst[n:], h.Length)
	return n + m
}

// HandleWithProperties is used for data blocks and first/lower level index
// blocks, since they can be annotated using BlockPropertyCollectors.
type HandleWithProperties struct {
	Handle
	Props []byte
}

// EncodeVarints encodes the block handle and properties into dst using a
// variable-width encoding and returns the number of bytes written.
func (h HandleWithProperties) EncodeVarints(dst []byte) []byte {
	n := h.Handle.EncodeVarints(dst)
	dst = append(dst[:n], h.Props...)
	return dst
}

// DecodeHandle returns the block handle encoded in a variable-width encoding at
// the start of src, as well as the number of bytes it occupies. It returns zero
// if given invalid input. A block handle for a data block or a first/lower
// level index block should not be decoded using DecodeHandle since the caller
// may validate that the number of bytes decoded is equal to the length of src,
// which will be false if the properties are not decoded. In those cases the
// caller should use DecodeHandleWithProperties.
func DecodeHandle(src []byte) (Handle, int) {
	offset, n := binary.Uvarint(src)
	length, m := binary.Uvarint(src[n:])
	if n == 0 || m == 0 {
		return Handle{}, 0
	}
	return Handle{Offset: offset, Length: length}, n + m
}

// DecodeHandleWithProperties returns the block handle and properties encoded in
// a variable-width encoding at the start of src. src needs to be exactly the
// length that was encoded. This method must be used for data block and
// first/lower level index blocks. The properties in the block handle point to
// the bytes in src.
func DecodeHandleWithProperties(src []byte) (HandleWithProperties, error) {
	bh, n := DecodeHandle(src)
	if n == 0 {
		return HandleWithProperties{}, errors.Errorf("invalid block.Handle")
	}
	return HandleWithProperties{
		Handle: bh,
		Props:  src[n:],
	}, nil
}

// TrailerLen is the length of the trailer at the end of a block.
const TrailerLen = 5

// Trailer is the trailer at the end of a block, encoding the block type
// (compression) and a checksum.
type Trailer = [TrailerLen]byte

// MakeTrailer constructs a trailer from a block type and a checksum.
func MakeTrailer(blockType byte, checksum uint32) (t Trailer) {
	t[0] = blockType
	binary.LittleEndian.PutUint32(t[1:5], checksum)
	return t
}

// ChecksumType specifies the checksum used for blocks.
type ChecksumType byte

// The available checksum types. These values are part of the durable format and
// should not be changed.
const (
	ChecksumTypeNone     ChecksumType = 0
	ChecksumTypeCRC32c   ChecksumType = 1
	ChecksumTypeXXHash   ChecksumType = 2
	ChecksumTypeXXHash64 ChecksumType = 3
)

// String implements fmt.Stringer.
func (t ChecksumType) String() string {
	switch t {
	case ChecksumTypeCRC32c:
		return "crc32c"
	case ChecksumTypeNone:
		return "none"
	case ChecksumTypeXXHash:
		return "xxhash"
	case ChecksumTypeXXHash64:
		return "xxhash64"
	default:
		panic(errors.Newf("sstable: unknown checksum type: %d", t))
	}
}

// A Checksummer calculates checksums for blocks.
type Checksummer struct {
	Type     ChecksumType
	xxHasher *xxhash.Digest
}

// Checksum computes a checksum over the provided block and block type.
func (c *Checksummer) Checksum(block []byte, blockType []byte) (checksum uint32) {
	// Calculate the checksum.
	switch c.Type {
	case ChecksumTypeCRC32c:
		checksum = crc.New(block).Update(blockType).Value()
	case ChecksumTypeXXHash64:
		if c.xxHasher == nil {
			c.xxHasher = xxhash.New()
		} else {
			c.xxHasher.Reset()
		}
		c.xxHasher.Write(block)
		c.xxHasher.Write(blockType)
		checksum = uint32(c.xxHasher.Sum64())
	default:
		panic(errors.Newf("unsupported checksum type: %d", c.Type))
	}
	return checksum
}

// DataBlockIterator is a type constraint for implementations of block iterators
// over data blocks. It's currently satisifed by the *rowblk.Iter type.
//
// DataBlockIterator requires that the type be a pointer to its type parameter,
// D, to allow sstable iterators embed the block iterator within its struct. See
// this example from the Go generics proposal:
// https://go.googlesource.com/proposal/+/refs/heads/master/design/43651-type-parameters.md#pointer-method-example
type DataBlockIterator[D any] interface {
	base.InternalIterator

	// Handle returns the handle to the block.
	Handle() BufferHandle
	// InitHandle initializes the block from the provided buffer handle.
	InitHandle(base.Compare, base.Split, BufferHandle, IterTransforms) error
	// Valid returns true if the iterator is currently positioned at a valid KV.
	Valid() bool
	// KV returns the key-value pair at the current iterator position. The
	// iterator must be Valid().
	KV() *base.InternalKV
	// ResetForReuse resets the iterator so that it may be used for iteration
	// over a new block. It returns the non-pointer D type to allow resetting
	// while initializing the containing struct, eg::
	//   iter = sstableIter{dataBlockIter: iter.dataBlockIter.ResetForReuse()}
	ResetForReuse() D
	// FirstUserKey returns the first user key contained within the data block.
	FirstUserKey() []byte
	// Invalidate invalidates the block iterator, removing references to the block
	// it was initialized with.
	Invalidate()
	// IsDataInvalidated returns true when the iterator has been invalidated
	// using an Invalidate call.
	//
	// NB: this is different from Valid which indicates whether the current *KV*
	// is valid.
	IsDataInvalidated() bool

	*D // non-interface type constraint element
}

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

// FragmentIterTransforms allow on-the-fly transformation of range deletion or
// range key data at iteration time.
type FragmentIterTransforms struct {
	SyntheticSeqNum SyntheticSeqNum
	// ElideSameSeqNum, if true, returns only the first-occurring (in forward
	// order) keyspan.Key for each sequence number.
	ElideSameSeqNum bool
	SyntheticPrefix SyntheticPrefix
	SyntheticSuffix SyntheticSuffix
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
