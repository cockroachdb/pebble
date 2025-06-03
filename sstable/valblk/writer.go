// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package valblk

import (
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/block/blockkind"
)

// Writer writes a sequence of value blocks, and the value blocks index, for a
// sstable.
type Writer struct {
	flush block.FlushGovernor
	// Block finished callback.
	blockFinishedFunc func(compressedSize int)

	compressor  *block.Compressor
	checksummer block.Checksummer
	// buf is the current block being written to (uncompressed).
	buf *block.TempBuffer
	// Sequence of blocks that are finished.
	blocks []bufferedValueBlock
	// Cumulative value block bytes written so far.
	totalBlockBytes uint64
	numValues       uint64
}

type bufferedValueBlock struct {
	block     block.PhysicalBlock
	bufHandle *block.TempBuffer
	handle    block.Handle
}

var valueBlockWriterPool = sync.Pool{
	New: func() interface{} {
		return &Writer{}
	},
}

// NewWriter creates a new Writer of value blocks and value index blocks.
func NewWriter(
	flushGovernor block.FlushGovernor,
	compressor *block.Compressor,
	checksumType block.ChecksumType,
	// compressedSize should exclude the block trailer.
	blockFinishedFunc func(compressedSize int),
) *Writer {
	w := valueBlockWriterPool.Get().(*Writer)
	*w = Writer{
		flush:             flushGovernor,
		compressor:        compressor,
		blockFinishedFunc: blockFinishedFunc,
		blocks:            w.blocks[:0],
	}
	w.checksummer.Init(checksumType)
	w.buf = block.NewTempBuffer()
	return w
}

// AddValue adds a value to the writer, returning a Handle referring to the
// stored value.
func (w *Writer) AddValue(v []byte) (Handle, error) {
	if invariants.Enabled && len(v) == 0 {
		return Handle{}, errors.Errorf("cannot write empty value to value block")
	}
	w.numValues++
	if blockLen := w.buf.Size(); w.flush.ShouldFlush(blockLen, blockLen+len(v)) {
		// Block is not currently empty and adding this value will become too
		// big, so finish this block.
		w.compressAndFlush()
		if invariants.Enabled && w.buf.Size() != 0 {
			panic("buffer should be empty when starting new block")
		}
	}
	vh := Handle{
		ValueLen: uint32(len(v)),
		BlockNum: uint32(len(w.blocks)),
	}
	vh.OffsetInBlock = uint32(w.buf.Append(v))
	return vh, nil
}

// Size returns the total size of currently buffered value blocks.
func (w *Writer) Size() uint64 {
	return w.totalBlockBytes + uint64(w.buf.Size())
}

func (w *Writer) compressAndFlush() {
	physicalBlock, bufHandle := block.CompressAndChecksumToTempBuffer(
		w.buf.Data(), blockkind.SSTableValue, w.compressor, &w.checksummer,
	)
	w.buf.Reset()
	bh := block.Handle{Offset: w.totalBlockBytes, Length: uint64(physicalBlock.LengthWithoutTrailer())}
	w.totalBlockBytes += uint64(physicalBlock.LengthWithTrailer())
	// blockFinishedFunc length excludes the block trailer.
	w.blockFinishedFunc(physicalBlock.LengthWithoutTrailer())
	w.blocks = append(w.blocks, bufferedValueBlock{
		block:     physicalBlock,
		bufHandle: bufHandle,
		handle:    bh,
	})
}

// Finish finishes writing the value blocks and value blocks index, writing the
// buffered blocks out to the provider layout writer.
func (w *Writer) Finish(layout LayoutWriter, fileOffset uint64) (IndexHandle, WriterStats, error) {
	if w.buf.Size() > 0 {
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
	blockLen := h.RowWidth() * len(w.blocks)
	h.Handle.Length = uint64(blockLen)
	w.buf.Resize(blockLen)
	b := w.buf.Data()
	for i := range w.blocks {
		littleEndianPut(uint64(i), b, int(h.BlockNumByteLength))
		b = b[int(h.BlockNumByteLength):]
		littleEndianPut(w.blocks[i].handle.Offset, b, int(h.BlockOffsetByteLength))
		b = b[int(h.BlockOffsetByteLength):]
		littleEndianPut(w.blocks[i].handle.Length, b, int(h.BlockLengthByteLength))
		b = b[int(h.BlockLengthByteLength):]
	}
	if len(b) != 0 {
		panic("incorrect length calculation")
	}
	pb, bufHandle := block.CompressAndChecksumToTempBuffer(w.buf.Data(), blockkind.Metadata, block.NoopCompressor, &w.checksummer)
	if _, err := layout.WriteValueIndexBlock(pb, h); err != nil {
		return IndexHandle{}, err
	}
	bufHandle.Release()
	return h, nil
}

// Release relinquishes the resources held by the writer and returns the Writer
// to a pool.
func (w *Writer) Release() {
	for i := range w.blocks {
		w.blocks[i].bufHandle.Release()
		w.blocks[i] = bufferedValueBlock{}
	}
	w.buf.Release()
	w.buf = nil
	*w = Writer{blocks: w.blocks[:0]}
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
	// WriteValueIndexBlock writes a pre-finished value block index to the
	// writer.
	WriteValueIndexBlock(blk block.PhysicalBlock, vbih IndexHandle) (block.Handle, error)
}
