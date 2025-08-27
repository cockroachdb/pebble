// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package valblk implements value blocks for sstables.
//
// Value blocks are supported in TableFormatPebblev3.
//
// 1. Motivation and overview
//
// Value blocks are a mechanism designed for sstables storing MVCC data, where
// there can be many versions of a key that need to be kept, but only the
// latest value is typically read (see the documentation for Comparer.Split
// regarding MVCC keys). The goal is faster reads. Unlike Pebble versions,
// which can be eagerly thrown away (except when there are snapshots), MVCC
// versions are long-lived (e.g. default CockroachDB garbage collection
// threshold for older versions is 24 hours) and can significantly slow down
// reads. We have seen CockroachDB production workloads with very slow reads
// due to:
// - 100s of versions for each key in a table.
//
//   - Tables with mostly MVCC garbage consisting of 2 versions per key -- a
//     real key-value pair, followed by a key-value pair whose value (usually
//     with zero byte length) indicates it is an MVCC tombstone.
//
// The value blocks mechanism attempts to improve read throughput in these
// cases when the key size is smaller than the value sizes of older versions.
// This is done by moving the value of an older version to a value block in a
// different part of the sstable. This improves spatial locality of the data
// being read by the workload, which increases caching effectiveness.
//
// Additionally, even when the key size is not smaller than the value of older
// versions (e.g. secondary indexes in CockroachDB), TableFormatPebblev3
// stores the result of key comparisons done at write time inside the sstable,
// which makes stepping from one key prefix to the next prefix (i.e., skipping
// over older versions of a MVCC key) more efficient by avoiding key
// comparisons and key decoding. See the results in
// https://github.com/cockroachdb/pebble/v2/pull/2149 and more details in the
// comment inside BenchmarkIteratorScanNextPrefix. These improvements are also
// visible in end-to-end CockroachDB tests, as outlined in
// https://github.com/cockroachdb/cockroach/pull/96652.
//
// In TableFormatPebblev3, each SET has a one byte value prefix that tells us
// whether the value is in-place or in a value block. This 1 byte prefix
// encodes additional information:
//
//   - ShortAttribute: This is an attribute of the value. Currently, CockroachDB
//     uses it to represent whether the value is a tombstone or not. This avoids
//     the need to fetch a value from the value block if the caller only wants
//     to figure out whether it is an MVCC tombstone. The length of the value is
//     another attribute that the caller can be interested in, and it is also
//     accessible without reading the value in the value block (see the value
//     handle in the details section).
//
//   - SET-same-prefix: this enables the aforementioned optimization when
//     stepping from one key prefix to the next key prefix.
//
// We further optimize this iteration over prefixes by using the restart
// points in a block to encode whether the SET at a restart point has the same
// prefix since the last restart point. This allows us to skip over restart
// points within the same block. See the comment in blockWriter, and how both
// SET-same-prefix and the restart point information is used in
// blockIter.nextPrefixV3.
//
// This flexibility of values that are in-place or in value blocks requires
// flexibility in the iterator interface. The InternalIterator interface
// returns a LazyValue instead of a byte slice. Additionally, pebble.Iterator
// allows the caller to ask for a LazyValue. See lazy_value.go for details,
// including the memory lifetime management.
//
// For historical discussions about this feature, see the issue
// https://github.com/cockroachdb/pebble/v2/issues/1170 and the prototype in
// https://github.com/cockroachdb/pebble/v2/pull/1443.
//
// The code in this file mainly covers value block and related encodings. We
// discuss these in the next section.
//
// 2. Details
//
// Note that the notion of the latest value is local to the sstable. It is
// possible that that latest value has been deleted by a sstable in a higher
// level, and what is the latest value from the perspective of the whole LSM
// is an older MVCC version. This only affects performance and not
// correctness. This local knowledge is also why we continue to store these
// older versions in the same sstable -- we need to be able to conveniently
// read them. The code in this file is agnostic to the policy regarding what
// should be stored in value blocks -- it allows even the latest MVCC version
// to be stored in a value block. The policy decision in made in the
// sstable.Writer. See Writer.makeAddPointDecisionV3.
//
// Data blocks contain two kinds of SET keys: those with in-place values and
// those with a value handle. To distinguish these two cases we use a single
// byte prefix (ValuePrefix). This single byte prefix is split into multiple
// parts, where nb represents information that is encoded in n bits.
//
// +---------------+--------------------+-----------+--------------------+
// | value-kind 2b | SET-same-prefix 1b | unused 2b | short-attribute 3b |
// +---------------+--------------------+-----------+--------------------+
//
// The 2 bit value-kind specifies whether this is an in-place value or a value
// handle pointing to a value block. We use 2 bits here for future
// representation of values that are in separate files. The 1 bit
// SET-same-prefix is true if this key is a SET and is immediately preceded by
// a SET that shares the same prefix. The 3 bit short-attribute is described
// in base.ShortAttribute -- it stores user-defined attributes about the
// value. It is unused for in-place values.
//
// Value Handle and Value Blocks:
// valueHandles refer to values in value blocks. Value blocks are simpler than
// normal data blocks (that contain key-value pairs, and allow for binary
// search), which makes them cheap for value retrieval purposes. A valueHandle
// is a tuple (valueLen, blockNum, offsetInBlock), where blockNum is the 0
// indexed value block number and offsetInBlock is the byte offset in that
// block containing the value. The valueHandle.valueLen is included since
// there are multiple use cases in CockroachDB that need the value length but
// not the value, for which we can avoid reading the value in the value block
// (see
// https://github.com/cockroachdb/pebble/v2/issues/1170#issuecomment-958203245).
//
// A value block has a checksum like other blocks, and is optionally
// compressed. An uncompressed value block is a sequence of values with no
// separator or length (we rely on the valueHandle to demarcate). The
// valueHandle.offsetInBlock points to the value, of length
// valueHandle.valueLen. While writing a sstable, all the (possibly
// compressed) value blocks need to be held in-memory until they can be
// written. Value blocks are placed after the "meta rangedel" and "meta range
// key" blocks since value blocks are considered less likely to be read.
//
// Meta Value Index Block:
// Since the (key, valueHandle) pair are written before there is any knowledge
// of the byte offset of the value block in the file, or its compressed
// length, we need another lookup to map the valueHandle.blockNum to the
// information needed to read it from the file. This information is provided
// by the "value index block". The "value index block" is referred to by the
// metaindex block. The design intentionally avoids making the "value index
// block" a general purpose key-value block, since each caller wants to lookup
// the information for a particular blockNum (there is no need for SeekGE
// etc.). Instead, this index block stores a sequence of (blockNum,
// blockOffset, blockLength) tuples, where the blockNums are consecutive
// integers, and the tuples are encoded with a fixed width encoding. This
// allows a reader to find the tuple for block K by looking at the offset
// K*fixed-width. The fixed width for each field is decided by looking at the
// maximum value of each of these fields. As a concrete example of a large
// sstable with many value blocks, we constructed a 100MB sstable with many
// versions and had 2475 value blocks (~32KB each). This sstable had this
// tuple encoded using 2+4+2=8 bytes, which means the uncompressed value index
// block was 2475*8=~19KB, which is modest. Therefore, we don't support more
// than one value index block. Consider the example of 2 byte blockNum, 4 byte
// blockOffset and 2 byte blockLen. The value index block will look like:
//
//	+---------------+------------------+---------------+
//	| blockNum (2B) | blockOffset (4B) | blockLen (2B) |
//	+---------------+------------------+---------------+
//	|       0       |    7,123,456     |  30,000       |
//	+---------------+------------------+---------------+
//	|       1       |    7,153,456     |  20,000       |
//	+---------------+------------------+---------------+
//	|       2       |    7,173,456     |  25,567       |
//	+---------------+------------------+---------------+
//	|     ....      |      ...         |    ...        |
//
// The metaindex block contains the valueBlocksIndexHandle which in addition
// to the BlockHandle also specifies the widths of these tuple fields. In the
// above example, the
// valueBlockIndexHandle.{blockNumByteLength,blockOffsetByteLength,blockLengthByteLength}
// will be (2,4,2).
package valblk

import (
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2/internal/base"
	"github.com/cockroachdb/pebble/v2/sstable/block"
)

// Handle is stored with a key when the value is in a value block. This
// handle is the pointer to that value.
type Handle struct {
	ValueLen      uint32
	BlockNum      uint32
	OffsetInBlock uint32
}

// HandleMaxLen is the maximum length of a variable-width encoded Handle.
//
// Handle fields are varint encoded, so maximum 5 bytes each. This could
// alternatively be group varint encoded, but experiments were inconclusive
// (https://github.com/cockroachdb/pebble/v2/pull/1443#issuecomment-1270298802).
const HandleMaxLen = 3 * binary.MaxVarintLen32

// EncodeHandle encodes the Handle into dst in a variable-width encoding and
// returns the number of bytes encoded.
func EncodeHandle(dst []byte, v Handle) int {
	n := 0
	n += binary.PutUvarint(dst[n:], uint64(v.ValueLen))
	n += binary.PutUvarint(dst[n:], uint64(v.BlockNum))
	n += binary.PutUvarint(dst[n:], uint64(v.OffsetInBlock))
	return n
}

// DecodeLenFromHandle decodes the value length from the beginning of a
// variaible-width encoded Handle.
func DecodeLenFromHandle(src []byte) (uint32, []byte) {
	ptr := unsafe.Pointer(&src[0])
	var v uint32
	if a := *((*uint8)(ptr)); a < 128 {
		v = uint32(a)
		src = src[1:]
	} else if a, b := a&0x7f, *((*uint8)(unsafe.Add(ptr, 1))); b < 128 {
		v = uint32(b)<<7 | uint32(a)
		src = src[2:]
	} else if b, c := b&0x7f, *((*uint8)(unsafe.Add(ptr, 2))); c < 128 {
		v = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		src = src[3:]
	} else if c, d := c&0x7f, *((*uint8)(unsafe.Add(ptr, 3))); d < 128 {
		v = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		src = src[4:]
	} else {
		d, e := d&0x7f, *((*uint8)(unsafe.Add(ptr, 4)))
		v = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		src = src[5:]
	}
	return v, src
}

// DecodeRemainingHandle decodes the BlockNum and OffsetInBlock from a
// variable-width encoded Handle, assuming the ValueLen prefix has already been
// stripped from src.
func DecodeRemainingHandle(src []byte) Handle {
	var vh Handle
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
	vh.BlockNum = v

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
	vh.OffsetInBlock = v

	return vh
}

// DecodeHandle decodes a Handle from a variable-length encoding.
func DecodeHandle(src []byte) Handle {
	valLen, src := DecodeLenFromHandle(src)
	vh := DecodeRemainingHandle(src)
	vh.ValueLen = valLen
	return vh
}

// IndexHandle is placed in the metaindex if there are any value blocks. If
// there are no value blocks, there is no value blocks index, and no entry in
// the metaindex. Note that the lack of entry in the metaindex should not be
// used to ascertain whether the values are prefixed, since the former is an
// emergent property of the data that was written and not known until all the
// key-value pairs in the sstable are written.
type IndexHandle struct {
	Handle                block.Handle
	BlockNumByteLength    uint8
	BlockOffsetByteLength uint8
	BlockLengthByteLength uint8
}

// String returns a string representation of the IndexHandle.
func (h IndexHandle) String() string {
	return fmt.Sprintf("{Handle: {%d,%d}, DataLens:(%d,%d,%d)}",
		h.Handle.Offset, h.Handle.Length, h.BlockNumByteLength, h.BlockOffsetByteLength, h.BlockLengthByteLength)
}

// RowWidth returns the number of bytes that the referenced index block uses per
// value block.
func (h IndexHandle) RowWidth() int {
	return int(h.BlockNumByteLength + h.BlockOffsetByteLength + h.BlockLengthByteLength)
}

// EncodeIndexHandle encodes the IndexHandle into dst and returns the number of
// bytes written.
func EncodeIndexHandle(dst []byte, v IndexHandle) int {
	n := v.Handle.EncodeVarints(dst)
	dst[n] = v.BlockNumByteLength
	n++
	dst[n] = v.BlockOffsetByteLength
	n++
	dst[n] = v.BlockLengthByteLength
	n++
	return n
}

// DecodeIndexHandle decodes an IndexHandle from src and returns the number of
// bytes read.
func DecodeIndexHandle(src []byte) (IndexHandle, int, error) {
	var vbih IndexHandle
	var n int
	vbih.Handle, n = block.DecodeHandle(src)
	if n <= 0 {
		return vbih, 0, errors.Errorf("bad BlockHandle %x", src)
	}
	if len(src) != n+3 {
		return vbih, 0, errors.Errorf("bad BlockHandle %x", src)
	}
	vbih.BlockNumByteLength = src[n]
	vbih.BlockOffsetByteLength = src[n+1]
	vbih.BlockLengthByteLength = src[n+2]
	return vbih, n + 3, nil
}

// DecodeIndex decodes the entirety of a value block index, returning a slice of
// the contained block handles.
func DecodeIndex(data []byte, vbih IndexHandle) ([]block.Handle, error) {
	var valueBlocks []block.Handle
	indexEntryLen := int(vbih.BlockNumByteLength + vbih.BlockOffsetByteLength +
		vbih.BlockLengthByteLength)
	i := 0
	for len(data) != 0 {
		if len(data) < indexEntryLen {
			return nil, errors.Errorf(
				"remaining value index block %d does not contain a full entry of length %d",
				len(data), indexEntryLen)
		}
		n := int(vbih.BlockNumByteLength)
		bn := int(littleEndianGet(data, n))
		if bn != i {
			return nil, errors.Errorf("unexpected block num %d, expected %d",
				bn, i)
		}
		i++
		data = data[n:]
		n = int(vbih.BlockOffsetByteLength)
		blockOffset := littleEndianGet(data, n)
		data = data[n:]
		n = int(vbih.BlockLengthByteLength)
		blockLen := littleEndianGet(data, n)
		data = data[n:]
		valueBlocks = append(valueBlocks, block.Handle{Offset: blockOffset, Length: blockLen})
	}
	return valueBlocks, nil
}

// DecodeBlockHandleFromIndex decodes the block handle for the given block
// number from the provided index block.
func DecodeBlockHandleFromIndex(
	vbiBlock []byte, blockNum uint32, indexHandle IndexHandle,
) (block.Handle, error) {
	w := indexHandle.RowWidth()
	off := w * int(blockNum)
	if len(vbiBlock) < off+w {
		return block.Handle{}, base.CorruptionErrorf(
			"index entry out of bounds: offset %d length %d block length %d",
			off, w, len(vbiBlock))
	}
	b := vbiBlock[off : off+w]
	n := int(indexHandle.BlockNumByteLength)
	bn := littleEndianGet(b, n)
	if uint32(bn) != blockNum {
		return block.Handle{},
			errors.Errorf("expected block num %d but found %d", blockNum, bn)
	}
	b = b[n:]
	n = int(indexHandle.BlockOffsetByteLength)
	blockOffset := littleEndianGet(b, n)
	b = b[n:]
	n = int(indexHandle.BlockLengthByteLength)
	blockLen := littleEndianGet(b, n)
	return block.Handle{Offset: blockOffset, Length: blockLen}, nil
}

// littleEndianPut writes v to b using little endian encoding, under the
// assumption that v can be represented using n bytes.
func littleEndianPut(v uint64, b []byte, n int) {
	_ = b[n-1] // bounds check
	for i := 0; i < n; i++ {
		b[i] = byte(v)
		v = v >> 8
	}
}

// lenLittleEndian returns the minimum number of bytes needed to encode v
// using little endian encoding.
func lenLittleEndian(v uint64) int {
	n := 0
	for i := 0; i < 8; i++ {
		n++
		v = v >> 8
		if v == 0 {
			break
		}
	}
	return n
}

func littleEndianGet(b []byte, n int) uint64 {
	_ = b[n-1] // bounds check
	v := uint64(b[0])
	for i := 1; i < n; i++ {
		v |= uint64(b[i]) << (8 * i)
	}
	return v
}
