// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package valblk

import (
	"encoding/binary"
	"math/rand/v2"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/sstable/block"
)

// Writer writes a sequence of value blocks, and the value blocks index, for a
// sstable.
type Writer struct {
	flush block.FlushGovernor
	// Configured compression.
	compression block.Compression
	// checksummer with configured checksum type.
	checksummer block.Checksummer
	// Block finished callback.
	blockFinishedFunc func(compressedSize int)

	// buf is the current block being written to (uncompressed).
	buf *blockBuffer
	// compressedBuf is used for compressing the block.
	compressedBuf *blockBuffer
	// Sequence of blocks that are finished.
	blocks []bufferedValueBlock
	// Cumulative value block bytes written so far.
	totalBlockBytes uint64
	numValues       uint64
}

type bufferedValueBlock struct {
	block  block.PhysicalBlock
	buffer *blockBuffer
	handle block.Handle
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
		// Set the bytes to a random value. Cap the number of bytes being
		// randomized to prevent test timeouts.
		length := cap(b.b)
		if length > 1000 {
			length = 1000
		}
		b.b = b.b[:length:length]
		for j := range b.b {
			b.b[j] = byte(rand.Uint32())
		}
	}
	pool.Put(b)
}

var valueBlockWriterPool = sync.Pool{
	New: func() interface{} {
		return &Writer{}
	},
}

// NewWriter creates a new Writer of value blocks and value index blocks.
func NewWriter(
	flushGovernor block.FlushGovernor,
	compression block.Compression,
	checksumType block.ChecksumType,
	// compressedSize should exclude the block trailer.
	blockFinishedFunc func(compressedSize int),
) *Writer {
	w := valueBlockWriterPool.Get().(*Writer)
	*w = Writer{
		flush:       flushGovernor,
		compression: compression,
		checksummer: block.Checksummer{
			Type: checksumType,
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

// AddValue adds a value to the writer, returning a Handle referring to the
// stored value.
func (w *Writer) AddValue(v []byte) (Handle, error) {
	if invariants.Enabled && len(v) == 0 {
		return Handle{}, errors.Errorf("cannot write empty value to value block")
	}
	w.numValues++
	blockLen := len(w.buf.b)
	valueLen := len(v)
	if w.flush.ShouldFlush(blockLen, blockLen+valueLen) {
		// Block is not currently empty and adding this value will become too big,
		// so finish this block.
		w.compressAndFlush()
		blockLen = len(w.buf.b)
		if invariants.Enabled && blockLen != 0 {
			panic("blockLen of new block should be 0")
		}
	}
	vh := Handle{
		ValueLen:      uint32(valueLen),
		BlockNum:      uint32(len(w.blocks)),
		OffsetInBlock: uint32(blockLen),
	}
	blockLen = int(vh.OffsetInBlock + vh.ValueLen)
	if cap(w.buf.b) < blockLen {
		size := 2 * cap(w.buf.b)
		if size < 1024 {
			size = 1024
		}
		for size < blockLen {
			size *= 2
		}
		buf := make([]byte, blockLen, size)
		_ = copy(buf, w.buf.b)
		w.buf.b = buf
	} else {
		w.buf.b = w.buf.b[:blockLen]
	}
	buf := w.buf.b[vh.OffsetInBlock:]
	n := copy(buf, v)
	if n != len(buf) {
		panic("incorrect length computation")
	}
	return vh, nil
}

// Size returns the total size of currently buffered value blocks.
func (w *Writer) Size() uint64 {
	sz := w.totalBlockBytes
	if w.buf != nil {
		sz += uint64(len(w.buf.b))
	}
	return sz
}

func (w *Writer) compressAndFlush() {
	w.compressedBuf.b = w.compressedBuf.b[:cap(w.compressedBuf.b)]
	physicalBlock := block.CompressAndChecksum(&w.compressedBuf.b, w.buf.b, w.compression, &w.checksummer)
	bh := block.Handle{Offset: w.totalBlockBytes, Length: uint64(physicalBlock.LengthWithoutTrailer())}
	w.totalBlockBytes += uint64(physicalBlock.LengthWithTrailer())
	// blockFinishedFunc length excludes the block trailer.
	w.blockFinishedFunc(physicalBlock.LengthWithoutTrailer())
	b := bufferedValueBlock{
		block:  physicalBlock,
		handle: bh,
	}

	// We'll hand off a buffer to w.blocks and get a new one. Which buffer we're
	// handing off depends on the outcome of compression.
	if physicalBlock.IsCompressed() {
		b.buffer = w.compressedBuf
		w.compressedBuf = compressedValueBlockBufPool.Get().(*blockBuffer)
	} else {
		b.buffer = w.buf
		w.buf = uncompressedValueBlockBufPool.Get().(*blockBuffer)
	}
	w.blocks = append(w.blocks, b)
	w.buf.b = w.buf.b[:0]
}

func (w *Writer) computeChecksum(blk []byte) {
	n := len(blk) - block.TrailerLen
	checksum := w.checksummer.Checksum(blk[:n], blk[n])
	binary.LittleEndian.PutUint32(blk[n+1:], checksum)
}

// Finish finishes writing the value blocks and value blocks index, writing the
// buffered blocks out to the provider layout writer.
func (w *Writer) Finish(layout LayoutWriter, fileOffset uint64) (IndexHandle, WriterStats, error) {
	if len(w.buf.b) > 0 {
		w.compressAndFlush()
	}
	n := len(w.blocks)
	if n == 0 {
		return IndexHandle{}, WriterStats{}, nil
	}
	largestOffset := uint64(0)
	largestLength := uint64(0)
	for i := range w.blocks {
		_, err := layout.WriteValueBlock(w.blocks[i].block)
		if err != nil {
			return IndexHandle{}, WriterStats{}, err
		}
		w.blocks[i].handle.Offset += fileOffset
		largestOffset = w.blocks[i].handle.Offset
		if largestLength < w.blocks[i].handle.Length {
			largestLength = w.blocks[i].handle.Length
		}
	}
	vbihOffset := fileOffset + w.totalBlockBytes

	vbih := IndexHandle{
		Handle: block.Handle{
			Offset: vbihOffset,
		},
		BlockNumByteLength:    uint8(lenLittleEndian(uint64(n - 1))),
		BlockOffsetByteLength: uint8(lenLittleEndian(largestOffset)),
		BlockLengthByteLength: uint8(lenLittleEndian(largestLength)),
	}
	var err error
	if n > 0 {
		if vbih, err = w.writeValueBlocksIndex(layout, vbih); err != nil {
			return IndexHandle{}, WriterStats{}, err
		}
	}
	stats := WriterStats{
		NumValueBlocks:          uint64(n),
		NumValuesInValueBlocks:  w.numValues,
		ValueBlocksAndIndexSize: w.totalBlockBytes + vbih.Handle.Length + block.TrailerLen,
	}
	return vbih, stats, err
}

func (w *Writer) writeValueBlocksIndex(layout LayoutWriter, h IndexHandle) (IndexHandle, error) {
	blockLen :=
		int(h.BlockNumByteLength+h.BlockOffsetByteLength+h.BlockLengthByteLength) * len(w.blocks)
	h.Handle.Length = uint64(blockLen)
	blockLen += block.TrailerLen
	var buf []byte
	if cap(w.buf.b) < blockLen {
		buf = make([]byte, blockLen)
		w.buf.b = buf
	} else {
		buf = w.buf.b[:blockLen]
	}
	b := buf
	for i := range w.blocks {
		littleEndianPut(uint64(i), b, int(h.BlockNumByteLength))
		b = b[int(h.BlockNumByteLength):]
		littleEndianPut(w.blocks[i].handle.Offset, b, int(h.BlockOffsetByteLength))
		b = b[int(h.BlockOffsetByteLength):]
		littleEndianPut(w.blocks[i].handle.Length, b, int(h.BlockLengthByteLength))
		b = b[int(h.BlockLengthByteLength):]
	}
	if len(b) != block.TrailerLen {
		panic("incorrect length calculation")
	}
	b[0] = byte(block.NoCompressionIndicator)
	w.computeChecksum(buf)
	if _, err := layout.WriteValueIndexBlock(buf, h); err != nil {
		return IndexHandle{}, err
	}
	return h, nil
}

// Release relinquishes the resources held by the writer and returns the Writer
// to a pool.
func (w *Writer) Release() {
	for i := range w.blocks {
		if w.blocks[i].block.IsCompressed() {
			releaseToValueBlockBufPool(&compressedValueBlockBufPool, w.blocks[i].buffer)
		} else {
			releaseToValueBlockBufPool(&uncompressedValueBlockBufPool, w.blocks[i].buffer)
		}
		w.blocks[i].buffer = nil
	}
	if w.buf != nil {
		releaseToValueBlockBufPool(&uncompressedValueBlockBufPool, w.buf)
	}
	if w.compressedBuf != nil {
		releaseToValueBlockBufPool(&compressedValueBlockBufPool, w.compressedBuf)
	}
	*w = Writer{
		blocks: w.blocks[:0],
	}
	valueBlockWriterPool.Put(w)
}

// WriterStats contains statistics about the value blocks and value index block
// written by a Writer.
type WriterStats struct {
	NumValueBlocks         uint64
	NumValuesInValueBlocks uint64
	// Includes both value blocks and value index block.
	ValueBlocksAndIndexSize uint64
}

// TODO(jackson): Refactor the Writer into an Encoder and move the onus of
// calling into the sstable.layoutWriter onto the sstable.Writer.

// LayoutWriter defines the interface for a writer that writes out serialized
// value and value index blocks.
type LayoutWriter interface {
	// WriteValueBlock writes a pre-finished value block (with the trailer) to
	// the writer.
	WriteValueBlock(blk block.PhysicalBlock) (block.Handle, error)
	// WriteValueBlockIndex writes a pre-finished value block index to the
	// writer.
	WriteValueIndexBlock(blk []byte, vbih IndexHandle) (block.Handle, error)
}
