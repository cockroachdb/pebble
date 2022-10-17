// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"encoding/binary"
	"io"
	"sync"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"golang.org/x/exp/rand"
)

// Value blocks are supported in TableFormatPebblev3.
//
// Value blocks are a mechanism designed for sstables storing MVCC data, where
// there can be many versions of a key that need to be kept, but only the
// latest value is typically read. See the documentation for Comparer.Split
// regarding MVCC keys.
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
// sstable.Writer. See Writer.makeAddPointDecision.
//
// Data blocks contain two kinds of SET keys: those with in-place values and
// those with a value handle. To distinguish these two cases we use a single
// byte prefix (valuePrefix). This single byte prefix is split into multiple
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
// https://github.com/cockroachdb/pebble/issues/1170#issuecomment-958203245).
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
//   +---------------+------------------+---------------+
//   | blockNum (2B) | blockOffset (4B) | blockLen (2B) |
//   +---------------+------------------+---------------+
//   |       0       |    7,123,456     |  30,000       |
//   +---------------+------------------+---------------+
//   |       1       |    7,153,456     |  20,000       |
//   +---------------+------------------+---------------+
//   |       2       |    7,173,456     |  25,567       |
//   +---------------+------------------+---------------+
//   |     ....      |      ...         |    ...        |
//
//
// The metaindex block contains the valueBlocksIndexHandle which in addition
// to the BlockHandle also specifies the widths of these tuple fields. In the
// above example, the
// valueBlockIndexHandle.{blockNumByteLength,blockOffsetByteLength,blockLengthByteLength}
// will be (2,4,2).

// valueHandle is stored with a key when the value is in a value block. This
// handle is the pointer to that value.
type valueHandle struct {
	valueLen      uint32
	blockNum      uint32
	offsetInBlock uint32
}

// valuePrefix is the single byte prefix for either the in-place value or the
// encoded valueHandle. It encoded multiple kinds of information.
type valuePrefix byte

const (
	// 2 most-significant bits of valuePrefix encodes the value-kind.
	valueKindMask           valuePrefix = '\xC0'
	valueKindIsValueHandle  valuePrefix = '\x80'
	valueKindIsInPlaceValue valuePrefix = '\x00'

	// 1 bit indicates SET has same key prefix as immediately preceding key that
	// is also a SET. This is optional, in that if this bit is false, there is
	// no information.
	//
	// Note that the current policy of only storing older MVCC versions in value
	// blocks means that valueKindIsValueHandle => SET has same prefix. But no
	// code should rely on this behavior.
	setHasSameKeyPrefixMask valuePrefix = '\x20'

	// 3 least-significant bits for the user-defined base.ShortAttribute.
	// Undefined for valueKindIsInPlaceValue.
	userDefinedShortAttributeMask valuePrefix = '\x07'
)

// valueHandle fields are varint encoded, so maximum 5 bytes each, plus 1 byte
// for the valuePrefix. This could alternatively be group varint encoded, but
// experiments were inconclusive
// (https://github.com/cockroachdb/pebble/pull/1443#issuecomment-1270298802).
const valueHandleMaxLen = 5*3 + 1

// Assert blockHandleLikelyMaxLen >= valueHandleMaxLen.
const _ = uint(blockHandleLikelyMaxLen - valueHandleMaxLen)

func encodeValueHandle(dst []byte, v valueHandle) int {
	n := 0
	n += binary.PutUvarint(dst[n:], uint64(v.valueLen))
	n += binary.PutUvarint(dst[n:], uint64(v.blockNum))
	n += binary.PutUvarint(dst[n:], uint64(v.offsetInBlock))
	return n
}

func makePrefixForValueHandle(setHasSameKeyPrefix bool, attribute base.ShortAttribute) valuePrefix {
	prefix := valueKindIsValueHandle | valuePrefix(attribute)
	if setHasSameKeyPrefix {
		prefix = prefix | setHasSameKeyPrefixMask
	}
	return prefix
}

func makePrefixForInPlaceValue(setHasSameKeyPrefix bool) valuePrefix {
	prefix := valueKindIsInPlaceValue
	if setHasSameKeyPrefix {
		prefix = prefix | setHasSameKeyPrefixMask
	}
	return prefix
}

func isValueHandle(b valuePrefix) bool {
	return b&valueKindMask == valueKindIsValueHandle
}

// REQUIRES: isValueHandle(b)
func getShortAttribute(b valuePrefix) base.ShortAttribute {
	return base.ShortAttribute(b & userDefinedShortAttributeMask)
}

func setHasSamePrefix(b valuePrefix) bool {
	return b&setHasSameKeyPrefixMask == setHasSameKeyPrefixMask
}

func decodeValueHandle(src []byte) (valueHandle, error) {
	var vh valueHandle
	ptr := unsafe.Pointer(&src[0])
	for i := 0; i < 3; i++ {
		// Manually inlined uvarint decoding. Saves ~25% in
		// BenchmarkValueBlocks/valueSize=100/versions=10/needValue=true/hasCache=true.
		// If needed we can also unroll the loop, as done in blockIter.readEntry.
		var v uint32
		if a := *((*uint8)(ptr)); a < 128 {
			v = uint32(a)
			ptr = unsafe.Pointer(uintptr(ptr) + 1)
		} else if a, b := a&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 1))); b < 128 {
			v = uint32(b)<<7 | uint32(a)
			ptr = unsafe.Pointer(uintptr(ptr) + 2)
		} else if b, c := b&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 2))); c < 128 {
			v = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
			ptr = unsafe.Pointer(uintptr(ptr) + 3)
		} else if c, d := c&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 3))); d < 128 {
			v = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
			ptr = unsafe.Pointer(uintptr(ptr) + 4)
		} else {
			d, e := d&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 4)))
			v = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
			ptr = unsafe.Pointer(uintptr(ptr) + 5)
		}
		switch i {
		case 0:
			vh.valueLen = v
		case 1:
			vh.blockNum = v
		case 2:
			vh.offsetInBlock = v
		}
	}
	return vh, nil
}

// valueBlocksIndexHandle is placed in the metaindex if there are any value
// blocks. If there are no value blocks, there is no value blocks index, and
// no entry in the metaindex. Note that the lack of entry in the metaindex
// should not be used to ascertain whether the values are prefixed, since the
// former is an emergent property of the data that was written and not known
// until all the key-value pairs in the sstable are written.
type valueBlocksIndexHandle struct {
	h                     BlockHandle
	blockNumByteLength    uint8
	blockOffsetByteLength uint8
	blockLengthByteLength uint8
}

const valueBlocksIndexHandleMaxLen = blockHandleMaxLenWithoutProperties + 3

// Assert blockHandleLikelyMaxLen >= valueBlocksIndexHandleMaxLen.
const _ = uint(blockHandleLikelyMaxLen - valueBlocksIndexHandleMaxLen)

func encodeValueBlocksIndexHandle(dst []byte, v valueBlocksIndexHandle) int {
	n := encodeBlockHandle(dst, v.h)
	dst[n] = v.blockNumByteLength
	n++
	dst[n] = v.blockOffsetByteLength
	n++
	dst[n] = v.blockLengthByteLength
	n++
	return n
}

func decodeValueBlocksIndexHandle(src []byte) (valueBlocksIndexHandle, int, error) {
	var vbih valueBlocksIndexHandle
	var n int
	vbih.h, n = decodeBlockHandle(src)
	if n <= 0 {
		return vbih, 0, errors.Errorf("bad BlockHandle %x", src)
	}
	if len(src) != n+3 {
		return vbih, 0, errors.Errorf("bad BlockHandle %x", src)
	}
	vbih.blockNumByteLength = src[n]
	vbih.blockOffsetByteLength = src[n+1]
	vbih.blockLengthByteLength = src[n+2]
	return vbih, n + 3, nil
}

// TODO(sumeer): Incorporate into Pebble metrics.
type valueBlocksAndIndexStats struct {
	numValueBlocks         uint64
	numValuesInValueBlocks uint64
	// Includes both value blocks and value index block.
	valueBlocksAndIndexSize uint64
}

// valueBlockWriter writes a sequence of value blocks, and the value blocks
// index, for a sstable.
type valueBlockWriter struct {
	// The configured uncompressed block size and size threshold
	blockSize, blockSizeThreshold int
	// Configured compression.
	compression Compression
	// checksummer with configured checksum type.
	checksummer checksummer
	// Block finished callback.
	blockFinishedFunc func(compressedSize int)

	// buf is the current block being written to (uncompressed).
	buf *blockBuffer
	// compressedBuf is used for compressing the block.
	compressedBuf *blockBuffer
	// Sequence of blocks that are finished.
	blocks []blockAndHandle
	// Cumulative value block bytes written so far.
	totalBlockBytes uint64
	numValues       uint64
}

type blockAndHandle struct {
	block      *blockBuffer
	handle     BlockHandle
	compressed bool
}

type blockBuffer struct {
	b []byte
}

// Pool of block buffers that should be roughly the blockSize.
var uncompressedValueBlockBufPool = sync.Pool{
	New: func() interface{} {
		return &blockBuffer{}
	},
}

// Pool of block buffers for compressed value blocks. These may widely vary in
// size based on compression ratios.
var compressedValueBlockBufPool = sync.Pool{
	New: func() interface{} {
		return &blockBuffer{}
	},
}

func releaseToValueBlockBufPool(pool *sync.Pool, b *blockBuffer) {
	// Don't pool buffers larger than 128KB, in case we had some rare large
	// values.
	if len(b.b) > 128*1024 {
		return
	}
	if invariants.Enabled {
		// Set the bytes to a random value.
		b.b = b.b[:cap(b.b)]
		rand.Read(b.b)
	}
	pool.Put(b)
}

var valueBlockWriterPool = sync.Pool{
	New: func() interface{} {
		return &valueBlockWriter{}
	},
}

func newValueBlockWriter(
	blockSize int,
	blockSizeThreshold int,
	compression Compression,
	checksumType ChecksumType,
	// compressedSize should exclude the block trailer.
	blockFinishedFunc func(compressedSize int),
) *valueBlockWriter {
	w := valueBlockWriterPool.Get().(*valueBlockWriter)
	*w = valueBlockWriter{
		blockSize:          blockSize,
		blockSizeThreshold: blockSizeThreshold,
		compression:        compression,
		checksummer: checksummer{
			checksumType: checksumType,
		},
		blockFinishedFunc: blockFinishedFunc,
		buf:               uncompressedValueBlockBufPool.Get().(*blockBuffer),
		compressedBuf:     compressedValueBlockBufPool.Get().(*blockBuffer),
		blocks:            w.blocks[:0],
	}
	w.buf.b = w.buf.b[:0]
	w.compressedBuf.b = w.compressedBuf.b[:0]
	return w
}

func releaseValueBlockWriter(w *valueBlockWriter) {
	for i := range w.blocks {
		if w.blocks[i].compressed {
			releaseToValueBlockBufPool(&compressedValueBlockBufPool, w.blocks[i].block)
		} else {
			releaseToValueBlockBufPool(&uncompressedValueBlockBufPool, w.blocks[i].block)
		}
		w.blocks[i].block = nil
	}
	if w.buf != nil {
		releaseToValueBlockBufPool(&uncompressedValueBlockBufPool, w.buf)
	}
	if w.compressedBuf != nil {
		releaseToValueBlockBufPool(&compressedValueBlockBufPool, w.compressedBuf)
	}
	*w = valueBlockWriter{
		blocks: w.blocks[:0],
	}
	valueBlockWriterPool.Put(w)
}

func (w *valueBlockWriter) addValue(v []byte) (valueHandle, error) {
	w.numValues++
	blockLen := len(w.buf.b)
	valueLen := len(v)
	if blockLen >= w.blockSize ||
		(blockLen > w.blockSizeThreshold && blockLen+valueLen > w.blockSize) {
		// Block is not currently empty and adding this value will become too big,
		// so finish this block.
		w.compressAndFlush()
		blockLen = len(w.buf.b)
		if invariants.Enabled && blockLen != 0 {
			panic("blockLen of new block should be 0")
		}
	}
	vh := valueHandle{
		valueLen:      uint32(valueLen),
		blockNum:      uint32(len(w.blocks)),
		offsetInBlock: uint32(blockLen),
	}
	blockLen = int(vh.offsetInBlock + vh.valueLen)
	if cap(w.buf.b) < blockLen {
		size := w.blockSize + w.blockSize/2
		if size < blockLen {
			size = blockLen + blockLen/2
		}
		buf := make([]byte, blockLen, size)
		_ = copy(buf, w.buf.b)
		w.buf.b = buf
	} else {
		w.buf.b = w.buf.b[:blockLen]
	}
	buf := w.buf.b[vh.offsetInBlock:]
	n := copy(buf, v)
	if n != len(buf) {
		panic("incorrect length computation")
	}
	return vh, nil
}

func (w *valueBlockWriter) compressAndFlush() {
	// Compress the buffer, discarding the result if the improvement isn't at
	// least 12.5%.
	blockType := noCompressionBlockType
	b := w.buf
	if w.compression != NoCompression {
		blockType, w.compressedBuf.b =
			compressBlock(w.compression, w.buf.b, w.compressedBuf.b[:cap(w.compressedBuf.b)])
		if len(w.compressedBuf.b) < len(w.buf.b)-len(w.buf.b)/8 {
			b = w.compressedBuf
		} else {
			blockType = noCompressionBlockType
		}
	}
	n := len(b.b)
	allocated := false
	if n+blockTrailerLen > cap(b.b) {
		block := make([]byte, n+blockTrailerLen)
		copy(block, b.b)
		b.b = block
		allocated = true
	} else {
		b.b = b.b[:n+blockTrailerLen]
	}
	b.b[n] = byte(blockType)
	w.computeChecksum(b.b)
	bh := BlockHandle{Offset: w.totalBlockBytes, Length: uint64(n)}
	w.totalBlockBytes += uint64(len(b.b))
	// blockFinishedFunc length excludes the block trailer.
	w.blockFinishedFunc(n)
	compressed := blockType != noCompressionBlockType
	w.blocks = append(w.blocks, blockAndHandle{
		block:      b,
		handle:     bh,
		compressed: compressed,
	})
	if !allocated {
		// Handed off a buffer to w.blocks, so need get a new one.
		if compressed {
			w.compressedBuf = compressedValueBlockBufPool.Get().(*blockBuffer)
		} else {
			w.buf = uncompressedValueBlockBufPool.Get().(*blockBuffer)
		}
	}
	w.buf.b = w.buf.b[:0]
}

func (w *valueBlockWriter) computeChecksum(block []byte) {
	n := len(block) - blockTrailerLen
	checksum := w.checksummer.checksum(block[:n], block[n:n+1])
	binary.LittleEndian.PutUint32(block[n+1:], checksum)
}

func (w *valueBlockWriter) finish(
	writer io.Writer, fileOffset uint64,
) (valueBlocksIndexHandle, valueBlocksAndIndexStats, error) {
	if len(w.buf.b) > 0 {
		w.compressAndFlush()
	}
	n := len(w.blocks)
	if n == 0 {
		return valueBlocksIndexHandle{}, valueBlocksAndIndexStats{}, nil
	}
	largestOffset := uint64(0)
	largestLength := uint64(0)
	for i := range w.blocks {
		_, err := writer.Write(w.blocks[i].block.b)
		if err != nil {
			return valueBlocksIndexHandle{}, valueBlocksAndIndexStats{}, err
		}
		w.blocks[i].handle.Offset += fileOffset
		largestOffset = w.blocks[i].handle.Offset
		if largestLength < w.blocks[i].handle.Length {
			largestLength = w.blocks[i].handle.Length
		}
	}
	vbihOffset := fileOffset + w.totalBlockBytes

	vbih := valueBlocksIndexHandle{
		h: BlockHandle{
			Offset: vbihOffset,
		},
		blockNumByteLength:    uint8(lenLittleEndian(uint64(n - 1))),
		blockOffsetByteLength: uint8(lenLittleEndian(largestOffset)),
		blockLengthByteLength: uint8(lenLittleEndian(largestLength)),
	}
	var err error
	if vbih, err = w.writeValueBlocksIndex(writer, vbih); err != nil {
		return valueBlocksIndexHandle{}, valueBlocksAndIndexStats{}, err
	}
	stats := valueBlocksAndIndexStats{
		numValueBlocks:          uint64(n),
		numValuesInValueBlocks:  w.numValues,
		valueBlocksAndIndexSize: w.totalBlockBytes + vbih.h.Length + blockTrailerLen,
	}
	return vbih, stats, err
}

func (w *valueBlockWriter) writeValueBlocksIndex(
	writer io.Writer, h valueBlocksIndexHandle,
) (valueBlocksIndexHandle, error) {
	blockLen :=
		int(h.blockNumByteLength+h.blockOffsetByteLength+h.blockLengthByteLength) * len(w.blocks)
	h.h.Length = uint64(blockLen)
	blockLen += blockTrailerLen
	var buf []byte
	if cap(w.buf.b) < blockLen {
		buf = make([]byte, blockLen)
		w.buf.b = buf
	} else {
		buf = w.buf.b[:blockLen]
	}
	b := buf
	for i := range w.blocks {
		littleEndianPut(uint64(i), b, int(h.blockNumByteLength))
		b = b[int(h.blockNumByteLength):]
		littleEndianPut(w.blocks[i].handle.Offset, b, int(h.blockOffsetByteLength))
		b = b[int(h.blockOffsetByteLength):]
		littleEndianPut(w.blocks[i].handle.Length, b, int(h.blockLengthByteLength))
		b = b[int(h.blockLengthByteLength):]
	}
	if len(b) != blockTrailerLen {
		panic("incorrect length calculation")
	}
	b[0] = byte(noCompressionBlockType)
	w.computeChecksum(buf)
	if _, err := writer.Write(buf); err != nil {
		return valueBlocksIndexHandle{}, err
	}
	return h, nil
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

// UserKeyPrefixBound represents a [Lower,Upper) bound of user key prefixes.
// If both are nil, there is no bound specified. Else, Compare(Lower,Upper)
// must be < 0.
type UserKeyPrefixBound struct {
	// Lower is a lower bound user key prefix.
	Lower []byte
	// Upper is an upper bound user key prefix.
	Upper []byte
}

// IsEmpty returns true iff the bound is empty.
func (ukb *UserKeyPrefixBound) IsEmpty() bool {
	return len(ukb.Lower) == 0 && len(ukb.Upper) == 0
}
