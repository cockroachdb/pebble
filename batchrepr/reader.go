// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package batchrepr provides interfaces for reading and writing the binary
// batch representation. This batch representation is used in-memory while
// constructing a batch and on-disk within the write-ahead log.
package batchrepr

import (
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/pkg/errors"
)

// ErrInvalidBatch indicates that a batch is invalid or otherwise corrupted.
var ErrInvalidBatch = base.MarkCorruptionError(errors.New("pebble: invalid batch"))

const (
	// HeaderLen is the length of the batch header in bytes.
	HeaderLen = 12
	// countOffset is the index into the batch representation where the
	// count is stored, encoded as a little-endian uint32.
	countOffset = 8
)

// IsEmpty returns true iff the batch contains zero keys.
func IsEmpty(repr []byte) bool {
	return len(repr) <= HeaderLen
}

// ReadHeader reads the contents of the batch header. If the repr is too small
// to contain a valid batch header, ReadHeader returns ok=false.
func ReadHeader(repr []byte) (h Header, ok bool) {
	if len(repr) < HeaderLen {
		return h, false
	}
	return Header{
		SeqNum: ReadSeqNum(repr),
		Count:  binary.LittleEndian.Uint32(repr[countOffset:HeaderLen]),
	}, true
}

// Header describes the contents of a batch header.
type Header struct {
	// SeqNum is the sequence number at which the batch is committed. A batch
	// that has not yet committed will have a zero sequence number.
	SeqNum base.SeqNum
	// Count is the count of keys written to the batch.
	Count uint32
}

// String returns a string representation of the header's contents.
func (h Header) String() string {
	return fmt.Sprintf("[seqNum=%d,count=%d]", h.SeqNum, h.Count)
}

// ReadSeqNum reads the sequence number encoded within the batch. ReadSeqNum
// does not validate that the repr is valid. It's exported only for very
// performance sensitive code paths that should not necessarily read the rest of
// the header as well.
func ReadSeqNum(repr []byte) base.SeqNum {
	return base.SeqNum(binary.LittleEndian.Uint64(repr[:countOffset]))
}

// Read constructs a Reader from an encoded batch representation, ignoring the
// contents of the Header.
func Read(repr []byte) (r Reader) {
	if len(repr) <= HeaderLen {
		return nil
	}
	return repr[HeaderLen:]
}

// Reader iterates over the entries contained in a batch.
type Reader []byte

// Next returns the next entry in this batch, if there is one. If the reader has
// reached the end of the batch, Next returns ok=false and a nil error. If the
// batch is corrupt and the next entry is illegible, Next returns ok=false and a
// non-nil error.
func (r *Reader) Next() (kind base.InternalKeyKind, ukey []byte, value []byte, ok bool, err error) {
	if len(*r) == 0 {
		return 0, nil, nil, false, nil
	}
	kind = base.InternalKeyKind((*r)[0])
	if kind > base.InternalKeyKindMax {
		return 0, nil, nil, false, errors.Wrapf(ErrInvalidBatch, "invalid key kind 0x%x", (*r)[0])
	}
	*r, ukey, ok = DecodeStr((*r)[1:])
	if !ok {
		return 0, nil, nil, false, errors.Wrapf(ErrInvalidBatch, "decoding user key")
	}
	switch kind {
	case base.InternalKeyKindSet, base.InternalKeyKindMerge, base.InternalKeyKindRangeDelete,
		base.InternalKeyKindRangeKeySet, base.InternalKeyKindRangeKeyUnset, base.InternalKeyKindRangeKeyDelete,
		base.InternalKeyKindDeleteSized, base.InternalKeyKindExcise, base.InternalKeyKindIngestSSTWithBlobs:
		*r, value, ok = DecodeStr(*r)
		if !ok {
			return 0, nil, nil, false, errors.Wrapf(ErrInvalidBatch, "decoding %s value", kind)
		}
	}
	return kind, ukey, value, true, nil
}

// DecodeStr decodes a varint encoded string from data, returning the remainder
// of data and the decoded string. It returns ok=false if the varint is invalid.
//
// TODO(jackson): This should be unexported once pebble package callers have
// been updated to use appropriate abstractions.
func DecodeStr(data []byte) (odata []byte, s []byte, ok bool) {
	// TODO(jackson): This will index out of bounds if there's no varint or an
	// invalid varint (eg, a single 0xff byte). Correcting will add a bit of
	// overhead. We could avoid that overhead whenever len(data) >=
	// binary.MaxVarint32?

	var v uint32
	var n int
	ptr := unsafe.Pointer(&data[0])
	if a := *((*uint8)(ptr)); a < 128 {
		v = uint32(a)
		n = 1
	} else if a, b := a&0x7f, *((*uint8)(unsafe.Add(ptr, 1))); b < 128 {
		v = uint32(b)<<7 | uint32(a)
		n = 2
	} else if b, c := b&0x7f, *((*uint8)(unsafe.Add(ptr, 2))); c < 128 {
		v = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		n = 3
	} else if c, d := c&0x7f, *((*uint8)(unsafe.Add(ptr, 3))); d < 128 {
		v = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		n = 4
	} else {
		d, e := d&0x7f, *((*uint8)(unsafe.Add(ptr, 4)))
		v = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		n = 5
	}

	data = data[n:]
	if v > uint32(len(data)) {
		return nil, nil, false
	}
	return data[v:], data[:v], true
}

// DecodeBlobFileIDs decodes blob file IDs from bytes.
// The format expected is: count (varint) + blob file IDs (varints).
func DecodeBlobFileIDs(value []byte) (blobIDs []base.BlobFileID, ok bool) {
	blobCount, n := binary.Uvarint(value)
	if n <= 0 {
		return nil, false
	}
	blobIDs = make([]base.BlobFileID, 0, blobCount)
	for i := uint64(0); i < blobCount; i++ {
		blobID, m := binary.Uvarint(value[n:])
		if m <= 0 {
			return nil, false
		}
		blobIDs = append(blobIDs, base.BlobFileID(blobID))
		n += m
	}
	return blobIDs, true
}
