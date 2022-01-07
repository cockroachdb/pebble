// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"encoding/binary"
	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/crc"
	"io"
	"unsafe"
)

// TODO(sumeer): define a new sstable format version that allows for value
// block. Such a format can have value blocks disables, in which case it will
// look identical to its preceding format.

// Value blocks are a mechanism designed for sstables storing MVCC data, where
// there can be many versions of a key that need to be kept, but only the
// latest value is typically read. See the documentation for Comparer.Split
// regarding what are MVCC keys.
//
// Note that the notion of the latest value is local to the sstable. It is
// possible that that latest value has been deleted by a sstable in a higher
// level, and what is the latest value from the perspective of the whole LSM
// is an older MVCC version. This only affects performance and not
// correctness. This local knowledge is also why we continue to store these
// older versions in the same sstable -- we need to be able to conveniently
// read them.
//
// Data blocks contain two kinds of keys: those with inline values and those
// with a value handle. These are distinguished by the first byte in the value
// being either inlineValuePrefix or valueHandlePrefix.
//
// Older versions of a key map to a value which is an encoded valueHandle.
// valueHandles refer to value blocks. Value blocks are simpler than normal
// data blocks (that contain key-value pairs, and allow for binary search),
// which makes them cheap for value retrieval purposes. A valueHandle is a
// tuple (valueLen, blockNum, offsetInBlock), where blockNum is the 0 indexed
// value block number and offsetInBlock is the byte offset in that block
// containing the value. The valueHandle.valueLen is included since there are
// multiple use cases in CockroachDB that need the value length but not the
// value, for which we can avoid reading the value in the value block (see
// https://github.com/cockroachdb/pebble/issues/1170#issuecomment-958203245)
//
// A value block has a checksum like other blocks, and is optionally
// compressed. An uncompressed value block is a sequence of (varint encoded
// value length, value bytes) tuples. The value length is redundant since the
// valueHandle.valueLen already has the length, and is only to allow
// additional validation (we may remove it since varint decoding shows up in
// cpu profiles). The valueHandle.offsetInBlock points to the start of this
// tuple. While writing a sstable, all the (possibly compressed) value blocks
// need to be held in-memory until they can be written. Value blocks are
// placed after the "meta rangedel" and "meta range key" blocks since value
// blocks are considered less likely to be read.
//
// Since the (key, valueHandle) pair are written before there is any knowledge
// of the byte offset of the value block in the file, or its compressed length,
// we need another lookup to map the valueHandle.blockNum to the information
// needed to read it from the file. This information is provided by the
// "value index block". The "value index block" is referred to by the metaindex
// block. The design intentionally avoids making the "value index block" a
// general purpose key-value block, since each caller wants to lookup the information
// for a particular blockNum (there is no need for SeekGE etc.). Instead, this
// index block stores a sequence of (blockNum, blockOffset, blockLength) tuples,
// where the blockNums are consecutive integers, and the tuples are encoded with
// a fixed width encoding. This allows a reader to find the tuple for block K by
// looking at the offset K*fixed-width. The fixed width for each field is decided
// by looking at the maximum value of each of these fields: an extreme case of a
// 100MB sstable with 2475 value blocks (~32KB each), has this tuple encoded using
// 2+4+2=8 bytes, which means the uncompressed value index block is 2475*8=~19KB,
// which is modest. Therefore, we don't support more than one value index block.
// The metaindex block contains the valueBlockIndexHandle which in addition to
// the BlockHandle also specifies the widths of these tuple fields.

// valueHandle is stored with a key when the value is in a value block. This
// handle is the pointer to that value.
type valueHandle struct {
	valueLen      uint32
	blockNum      uint32
	offsetInBlock uint32
}

// valueKind is the single byte prefix used to distinguish an inline value
// from a valueHandle.
type valueKind byte

const (
	valueHandlePrefix valueKind = '\xff'
	inlineValuePrefix valueKind = '\x00'
)

const valueHandleMaxLen = 5*3 + 1

func encodeValueHandle(dst []byte, v valueHandle) int {
	dst[0] = byte(valueHandlePrefix)
	n := 1
	n += binary.PutUvarint(dst[n:], uint64(v.valueLen))
	n += binary.PutUvarint(dst[n:], uint64(v.blockNum))
	n += binary.PutUvarint(dst[n:], uint64(v.offsetInBlock))
	return n
}

func decodeValueHandle(src []byte) (valueHandle, error) {
	var vh valueHandle
	if len(src) == 0 || src[0] != byte(valueHandlePrefix) {
		return vh, errors.Errorf("TODO0")
	}
	ptr := unsafe.Pointer(&src[1])
	for i := 0; i < 3; i++ {
		// Manually inlined uvarint decoding. Saves ~25% in
		// BenchmarkValueBlocks/valueSize=100/versions=10/needValue=true/hasCache=true.
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

// valueBlocksIndexHandle is placed in the metaindex if there are any value blocks.
// If there are no value blocks, there is no value blocks index, and no entry in the
// metaindex. Note that the lack of entry in the metaindex should not be used to ascertain
// whether the values are prefixed, since it is an emergent property of the data that
// was written and not known beforehand.
type valueBlocksIndexHandle struct {
	h                     BlockHandle
	blockNumByteLength    uint8
	blockOffsetByteLength uint8
	blockLengthByteLength uint8
}

const valueBlocksIndexHandleMaxLen = blockHandleMaxLenWithoutProperties + 3

func init() {
	// Const assertions. Is there a better way in Golang?
	if valueHandleMaxLen > blockHandleLikelyMaxLen {
		panic("")
	}
	if valueBlocksIndexHandleMaxLen > blockHandleLikelyMaxLen {
		panic("")
	}
}

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
		return vbih, 0, errors.Errorf("TODO4")
	}
	if len(src) != n+3 {
		return vbih, 0, errors.Errorf("TODO5")
	}
	vbih.blockNumByteLength = src[n]
	vbih.blockOffsetByteLength = src[n+1]
	vbih.blockLengthByteLength = src[n+2]
	return vbih, n + 3, nil
}

type valueBlocksAndIndexStats struct {
	numValueBlocks uint64
	// Includes both value blocks and value blocks index.
	writtenBytes uint64
}

// TODO(sumeer): sync.Pools for valueBlockWriter and blockAndHandle etc.

// valueBlockWriter writes a sequence of value blocks, and the value blocks
// index, for a sstable.
type valueBlockWriter struct {
	blockSize    int
	compression  Compression
	checksumType ChecksumType

	// Internal state.
	buf             []byte
	compressedBuf   []byte
	blocks          []blockAndHandle
	totalBlockBytes uint64

	xxHasher *xxhash.Digest
}

type blockAndHandle struct {
	block  []byte
	handle BlockHandle
}

func (w *valueBlockWriter) addValue(v []byte) (valueHandle, error) {
	vh := valueHandle{
		valueLen:      uint32(len(v)),
		blockNum:      uint32(len(w.blocks)),
		offsetInBlock: uint32(len(w.buf)),
	}
	blockLen := int(vh.offsetInBlock+vh.valueLen) + uvarintLen(vh.valueLen)
	if cap(w.buf) < blockLen {
		size := w.blockSize + w.blockSize/2
		if size < blockLen {
			size = blockLen + blockLen/2
		}
		buf := make([]byte, blockLen, size)
		copy(buf, w.buf)
		w.buf = buf
	} else {
		w.buf = w.buf[:blockLen]
	}
	buf := w.buf[vh.offsetInBlock:]
	n := binary.PutUvarint(buf, uint64(vh.valueLen))
	buf = buf[n:]
	n = copy(buf, v)
	if n != len(buf) {
		panic("incorrect length computation")
	}
	if len(w.buf) > w.blockSize {
		// Compress the block and add to blocks.
		if err := w.compressAndFlush(); err != nil {
			return valueHandle{}, err
		}
	}
	return vh, nil
}

func (w *valueBlockWriter) compressAndFlush() error {
	// Compress the buffer, discarding the result if the improvement isn't at
	// least 12.5%.
	// TODO(sumeer): parallel compression.
	var blockType blockType
	blockType, w.compressedBuf =
		compressBlock(w.compression, w.buf, w.compressedBuf[:cap(w.compressedBuf)])
	var b []byte
	if len(w.compressedBuf) < len(w.buf)-len(w.buf)/8 {
		b = w.compressedBuf
	} else {
		blockType = noCompressionBlockType
		b = w.buf
	}
	// Allocate the exact size needed, to not waste memory.
	n := len(b)
	block := make([]byte, n+blockTrailerLen)
	copy(block, b)
	block[n] = byte(blockType)

	if err := w.computeChecksum(block); err != nil {
		return err
	}
	bh := BlockHandle{Offset: w.totalBlockBytes, Length: uint64(n)}
	w.totalBlockBytes += uint64(len(block))
	w.blocks = append(w.blocks, blockAndHandle{
		block:  block,
		handle: bh,
	})
	w.buf = w.buf[:0]
	return nil
}

func (w *valueBlockWriter) computeChecksum(block []byte) error {
	n := len(block) - 4
	var checksum uint32
	switch w.checksumType {
	case ChecksumTypeCRC32c:
		checksum = crc.New(block[:n]).Value()
	case ChecksumTypeXXHash64:
		if w.xxHasher == nil {
			w.xxHasher = xxhash.New()
		} else {
			w.xxHasher.Reset()
		}
		w.xxHasher.Write(block[:n])
		checksum = uint32(w.xxHasher.Sum64())
	default:
		return errors.Newf("unsupported checksum type: %d", w.checksumType)
	}
	binary.LittleEndian.PutUint32(block[n:], checksum)
	return nil
}

func (w *valueBlockWriter) finish(writer io.Writer, offset uint64) (
	valueBlocksIndexHandle, valueBlocksAndIndexStats, error) {
	if len(w.buf) > 0 {
		if err := w.compressAndFlush(); err != nil {
			return valueBlocksIndexHandle{}, valueBlocksAndIndexStats{}, err
		}
	}
	n := len(w.blocks)
	if n == 0 {
		return valueBlocksIndexHandle{}, valueBlocksAndIndexStats{}, nil
	}
	largestOffset := uint64(0)
	largestLength := uint64(0)
	for i := range w.blocks {
		_, err := writer.Write(w.blocks[i].block)
		if err != nil {
			return valueBlocksIndexHandle{}, valueBlocksAndIndexStats{}, err
		}
		w.blocks[i].handle.Offset += offset
		largestOffset = w.blocks[i].handle.Offset
		if largestLength < w.blocks[i].handle.Length {
			largestLength = w.blocks[i].handle.Length
		}
	}
	vbihOffset := offset + w.totalBlockBytes

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
		numValueBlocks: uint64(n),
		writtenBytes:   vbih.h.Offset + vbih.h.Length + blockTrailerLen - offset,
	}
	return vbih, stats, err
}

func (w *valueBlockWriter) writeValueBlocksIndex(
	writer io.Writer, h valueBlocksIndexHandle) (valueBlocksIndexHandle, error) {
	blockLen :=
		int(h.blockNumByteLength+h.blockOffsetByteLength+h.blockLengthByteLength) * len(w.blocks)
	h.h.Length = uint64(blockLen)
	blockLen += blockTrailerLen
	var buf []byte
	if cap(w.buf) < blockLen {
		buf = make([]byte, blockLen)
		w.buf = buf
	} else {
		buf = w.buf[:blockLen]
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
	if err := w.computeChecksum(buf); err != nil {
		return valueBlocksIndexHandle{}, err
	}
	_, err := writer.Write(buf)
	if err != nil {
		return valueBlocksIndexHandle{}, err
	}
	return h, nil
}

func littleEndianPut(v uint64, b []byte, n int) {
	_ = b[n-1] // bounds check
	for i := 0; i < n; i++ {
		b[i] = byte(v)
		v = v >> 8
	}
}

func littleEndianGet(b []byte, n int) uint64 {
	_ = b[n-1] // bounds check
	v := uint64(b[0])
	for i := 1; i < n; i++ {
		v |= uint64(b[i]) << (8 * i)
	}
	return v
}

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

type blockProvider interface {
	readBlock(h BlockHandle) (cache.Handle, error)
}

// valueBlockReader is used to retrieve both inline values and values in value
// blocks. It is used when the sstable was written with
// Properties.ValueBlocksAreEnabled.
type valueBlockReader struct {
	bp blockProvider
	// Will be an empty handle if the sstable doesn't actually have any values
	// in value blocks.
	vbih valueBlocksIndexHandle
	// The value blocks index is lazily retrieved the first time the reader
	// needs to read a value that resides in a value block. It is then "cached"
	// here.
	vbiBlock []byte
	vbiCache cache.Handle
	// When sequentially iterating through all key-value pairs, the cost of
	// repeatedly getting a block that is already in the cache and releasing the
	// cache.Handle can be ~40% of the cpu overhead. So the reader remembers the
	// last value block it retrieved, in case there is locality of access, and
	// this value block can be used for the next value retrieval.
	valueBlockNum uint32
	valueBlock    []byte
	valueBlockPtr unsafe.Pointer
	valueCache    cache.Handle
}

func (r *valueBlockReader) close() {
	r.vbiBlock = nil
	r.vbiCache.Release()
	r.valueCache.Release()
}

func (r *valueBlockReader) getValue(valueBytes []byte) ([]byte, error) {
	if len(valueBytes) == 0 {
		return nil, errors.Errorf("lacking valueKind prefix")
	}
	prefix := valueKind(valueBytes[0])
	switch prefix {
	case inlineValuePrefix:
		return valueBytes[1:], nil
	case valueHandlePrefix:
	default:
		return nil, errors.Errorf("TODO6")
	}
	// Inlining the decoding here instead of a function call saves 5% in
	// BenchmarkValueBlocks/valueSize=100/versions=10/needValue=true/hasCache=true.
	var vh valueHandle
	{
		ptr := unsafe.Pointer(&valueBytes[1])
		// Manually inlined uvarint decoding. Saves ~25% in
		// BenchmarkValueBlocks/valueSize=100/versions=10/needValue=true/hasCache=true.
		// Unrolling a loop for i:=0; i<3; i++, saves ~6%.
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
		vh.valueLen = v

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
		vh.blockNum = v

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
		vh.offsetInBlock = v
	}
	if r.vbiBlock == nil {
		ch, err := r.bp.readBlock(r.vbih.h)
		if err != nil {
			return nil, err
		}
		r.vbiCache = ch
		r.vbiBlock = ch.Get()
	}
	if r.valueBlock == nil || r.valueBlockNum != vh.blockNum {
		vbh, err := r.getBlockHandle(vh.blockNum)
		if err != nil {
			return nil, err
		}
		vbCacheHandle, err := r.bp.readBlock(vbh)
		if err != nil {
			return nil, err
		}
		r.valueBlockNum = vh.blockNum
		r.valueCache.Release()
		r.valueCache = vbCacheHandle
		r.valueBlock = vbCacheHandle.Get()
		r.valueBlockPtr = unsafe.Pointer(&r.valueBlock[0])
	}
	ptr := unsafe.Pointer(uintptr(r.valueBlockPtr) + uintptr(vh.offsetInBlock))
	var n uint32
	// Manually inlined uvarint decoding. Saves ~6% on
	// BenchmarkValueBlocks/valueSize=100/versions=10/needValue=true/hasCache=true.
	var valueLen uint32
	if a := *((*uint8)(ptr)); a < 128 {
		valueLen = uint32(a)
		n = 1
	} else if a, b := a&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 1))); b < 128 {
		valueLen = uint32(b)<<7 | uint32(a)
		n = 2
	} else if b, c := b&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 2))); c < 128 {
		valueLen = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		n = 3
	} else if c, d := c&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 3))); d < 128 {
		valueLen = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		n = 4
	} else {
		d, e := d&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 4)))
		valueLen = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		n = 5
	}
	n += vh.offsetInBlock
	if valueLen != vh.valueLen {
		return nil, errors.Errorf("TODO8")
	}
	return r.valueBlock[n : n+valueLen], nil
}

func (r *valueBlockReader) getBlockHandle(blockNum uint32) (BlockHandle, error) {
	indexEntryLen :=
		int(r.vbih.blockNumByteLength + r.vbih.blockOffsetByteLength + r.vbih.blockLengthByteLength)
	offsetInIndex := indexEntryLen * int(blockNum)
	if len(r.vbiBlock) < offsetInIndex+indexEntryLen {
		return BlockHandle{}, errors.Errorf("TODO9")
	}
	b := r.vbiBlock[offsetInIndex : offsetInIndex+indexEntryLen]
	n := int(r.vbih.blockNumByteLength)
	bn := littleEndianGet(b, n)
	if uint32(bn) != blockNum {
		return BlockHandle{}, errors.Errorf("TODO10")
	}
	b = b[n:]
	n = int(r.vbih.blockOffsetByteLength)
	blockOffset := littleEndianGet(b, n)
	b = b[n:]
	n = int(r.vbih.blockLengthByteLength)
	blockLen := littleEndianGet(b, n)
	return BlockHandle{Offset: blockOffset, Length: blockLen}, nil
}
