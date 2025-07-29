// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package block

import (
	"encoding/binary"
	"slices"
	"sync"

	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/objstorage"
)

// PhysicalBlock is a block (possibly compressed) as it will be stored
// physically on disk (including its trailer).
//
// A PhysicalBlock is always backed by a TempBuffer and should be released after
// it is written out.
type PhysicalBlock struct {
	tb *TempBuffer
}

// TrailerLen is the length of the trailer at the end of a block.
const TrailerLen = 5

// Trailer is the trailer at the end of a block, encoding the block type
// (compression) and a checksum.
type Trailer [TrailerLen]byte

// MakeTrailer constructs a trailer from a block type and a checksum.
func MakeTrailer(blockType byte, checksum uint32) (t Trailer) {
	t[0] = blockType
	binary.LittleEndian.PutUint32(t[1:5], checksum)
	return t
}

// LengthWithTrailer returns the length of the data block, including the trailer.
func (b PhysicalBlock) LengthWithTrailer() int {
	return b.tb.Size()
}

// LengthWithoutTrailer returns the length of the data block, excluding the trailer.
func (b PhysicalBlock) LengthWithoutTrailer() int {
	return b.tb.Size() - TrailerLen
}

// Release the underlying TempBuffer. The PhysicalBlock should not be used again.
func (b *PhysicalBlock) Release() {
	b.tb.Release()
	b.tb = nil
}

// AlreadyEncodedPhysicalBlock creates a PhysicalBlock from the provided on-disk
// data; used when copying existing blocks. The given slice is copied into the
// physical block.
func AlreadyEncodedPhysicalBlock(dataWithTrailer []byte) PhysicalBlock {
	tb := NewTempBuffer()
	tb.Append(dataWithTrailer)
	return PhysicalBlock{tb: tb}
}

// WriteTo writes the block (including its trailer) to the provided Writable. If
// err == nil, n is the number of bytes successfully written to the Writable.
//
// WriteTo might mangle the block data.
func (b PhysicalBlock) WriteTo(w objstorage.Writable) (n int, err error) {
	if err := w.Write(b.tb.Data()); err != nil {
		return 0, err
	}

	// WriteTo is allowed to mangle the data. Mangle it ourselves some of the time
	// in invariant builds to catch callers that don't handle this.
	if invariants.Enabled && invariants.Sometimes(1) {
		invariants.Mangle(b.tb.Data())
	}
	return b.tb.Size(), nil
}

// PhysicalBlockMaker is used to create physical blocks from logical block data.
// It takes care of compression, checksum calculation, and trailer encoding.
//
// It is not thread-safe and should not be used concurrently.
type PhysicalBlockMaker struct {
	Compressor  Compressor
	Checksummer Checksummer
}

// PhysicalBlockFlags is a bitmask with flags used when making a physical block.
type PhysicalBlockFlags int

const (
	NoFlags      PhysicalBlockFlags = 0
	DontCompress PhysicalBlockFlags = 1 << (iota - 1)
)

// Init the physical block maker. Closed must be called when no longer needed.
func (p *PhysicalBlockMaker) Init(profile *CompressionProfile, checksumType ChecksumType) {
	p.Compressor = MakeCompressor(profile)
	p.Checksummer.Init(checksumType)
}

// Close must be called when the PhysicalBlockMaker is no longer needed. After
// Close is called, the PhysicalBlockMaker must not be used again (unless it is
// initialized again).
func (p *PhysicalBlockMaker) Close() {
	p.Compressor.Close()
}

// Make creates a PhysicalBlock from the provided uncompressed data. The
// PhysicalBlock should be released to save allocations, at least in the
// "normal" paths.
//
// The uncompressedData slice is never used directly in the PhysicalBlock.
func (p *PhysicalBlockMaker) Make(
	uncompressedData []byte, blockKind Kind, flags PhysicalBlockFlags,
) PhysicalBlock {
	tb := NewTempBuffer()
	ci := NoCompressionIndicator
	if flags&DontCompress == 0 {
		ci, tb.b = p.Compressor.Compress(tb.b[:0], uncompressedData, blockKind)
	} else {
		tb.b = append(tb.b[:0], uncompressedData...)
		p.Compressor.UncompressedBlock(len(uncompressedData), blockKind)
	}
	checksum := p.Checksummer.Checksum(tb.b, byte(ci))
	trailer := MakeTrailer(byte(ci), checksum)
	tb.b = append(tb.b, trailer[:]...)
	return PhysicalBlock{tb: tb}
}

// TempBuffer is a buffer that is used temporarily and is released back to a
// pool for reuse.
type TempBuffer struct {
	b []byte
}

// NewTempBuffer returns a TempBuffer from the pool. The buffer will have zero
// size and length and arbitrary capacity.
func NewTempBuffer() *TempBuffer {
	tb := tempBufferPool.Get().(*TempBuffer)
	if invariants.Enabled && len(tb.b) > 0 {
		panic("NewTempBuffer length not 0")
	}
	return tb
}

// Data returns the byte slice currently backing the Buffer.
func (tb *TempBuffer) Data() []byte {
	return tb.b
}

// Size returns the current size of the buffer.
func (tb *TempBuffer) Size() int {
	return len(tb.b)
}

// Append appends the contents of v to the buffer, growing the buffer if
// necessary. Returns the offset at which it was appended.
func (tb *TempBuffer) Append(v []byte) (startOffset int) {
	startOffset = len(tb.b)
	tb.b = append(tb.b, v...)
	return startOffset
}

// Resize resizes the buffer to the specified length, allocating if necessary.
// If the length is longer than the current length, the values of the new bytes
// are arbitrary.
func (tb *TempBuffer) Resize(length int) {
	if length > cap(tb.b) {
		tb.b = slices.Grow(tb.b, length-len(tb.b))
	}
	tb.b = tb.b[:length]
}

// Reset is equivalent to Resize(0).
func (tb *TempBuffer) Reset() {
	tb.b = tb.b[:0]
}

// Release releases the buffer back to the pool for reuse.
func (tb *TempBuffer) Release() {
	// Note we avoid releasing buffers that are larger than the configured
	// maximum to the pool. This avoids holding on to occasional large buffers
	// necessary for e.g. singular large values.
	if tb.b != nil && len(tb.b) < tempBufferMaxReusedSize {
		invariants.MaybeMangle(tb.b)
		tb.b = tb.b[:0]
		tempBufferPool.Put(tb)
	}
}

// tempBufferPool is a pool of buffers that are used to temporarily hold either
// compressed or uncompressed block data.
var tempBufferPool = sync.Pool{
	New: func() any {
		return &TempBuffer{b: make([]byte, 0, tempBufferInitialSize)}
	},
}

const tempBufferInitialSize = 32 * 1024
const tempBufferMaxReusedSize = 256 * 1024
