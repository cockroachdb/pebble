// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/cockroachdb/pebble/internal/treeprinter"
)

const keyValueBlockCustomHeaderSize = 0

// KeyValueBlockWriter writes key value blocks. The writer is used as a
// drop-in replacement for the metaindex block and the properties block.
// The key value block schema consists of two primary columns:
//   - Key: represented by RawBytes
//   - Value: represented by RawBytes
type KeyValueBlockWriter struct {
	keys   RawBytesBuilder
	values RawBytesBuilder
	rows   int
	enc    blockEncoder
}

const (
	keyValueBlockColumnKey = iota
	keyValueBlockColumnValue
	keyValueBlockColumnCount
)

// Init initializes the key value block writer.
func (w *KeyValueBlockWriter) Init() {
	w.keys.Init()
	w.values.Init()
	w.rows = 0
}

// Reset resets the key value block writer to its initial state, retaining
// buffers.
func (w *KeyValueBlockWriter) Reset() {
	w.keys.Reset()
	w.values.Init()
	w.rows = 0
	w.enc.reset()
}

// Rows returns the number of entries in the key value block so far.
func (w *KeyValueBlockWriter) Rows() int {
	return w.rows
}

// AddBlockHandle adds a new key and value of a block to the key value block.
// Add returns the index of the row.
func (w *KeyValueBlockWriter) AddBlockHandle(key []byte, value []byte) {
	w.keys.Put(key)
	w.values.Put(value)
	w.rows++
}

func (w *KeyValueBlockWriter) size(rows int) int {
	off := blockHeaderSize(keyValueBlockColumnCount, keyValueBlockCustomHeaderSize)
	off = w.keys.Size(rows, off)
	off = w.values.Size(rows, off)
	off++
	return int(off)
}

// Finish serializes the pending key value block.
func (w *KeyValueBlockWriter) Finish(rows int) []byte {
	w.enc.init(w.size(rows), Header{
		Version: Version1,
		Columns: keyValueBlockColumnCount,
		Rows:    uint32(rows),
	}, indexBlockCustomHeaderSize)
	w.enc.encode(rows, &w.keys)
	w.enc.encode(rows, &w.values)
	return w.enc.finish()
}

// KeyValueBlockDecoder reads columnar key value blocks.
type KeyValueBlockDecoder struct {
	keys   RawBytes
	values RawBytes
	bd     BlockDecoder
}

// Init initializes the key value block decoder with the given serialized block.
func (r *KeyValueBlockDecoder) Init(data []byte) {
	r.bd.Init(data, keyValueBlockCustomHeaderSize)
	r.keys = r.bd.RawBytes(keyValueBlockColumnKey)
	r.values = r.bd.RawBytes(keyValueBlockColumnValue)
}

// DebugString prints a human-readable explanation of the block's binary
// representation.
func (r *KeyValueBlockDecoder) DebugString() string {
	f := binfmt.New(r.bd.data).LineWidth(20)
	tp := treeprinter.New()
	r.Describe(f, tp.Child("key-value-block-decoder"))
	return tp.String()
}

// Describe describes the binary format of the key value block, assuming
// f.Offset() is positioned at the beginning of the same key value block
// described by r.
func (r *KeyValueBlockDecoder) Describe(f *binfmt.Formatter, tp treeprinter.Node) {
	// Set the relative offset. When loaded into memory, the beginning of blocks
	// are aligned. Padding that ensures alignment is done relative to the
	// current offset. Setting the relative offset ensures that if we're
	// describing this block within a larger structure (eg, f.Offset()>0), we
	// compute padding appropriately assuming the current byte f.Offset() is
	// aligned.
	f.SetAnchorOffset()

	n := tp.Child("key value block header")
	r.bd.headerToBinFormatter(f, n)
	for i := 0; i < keyValueBlockColumnCount; i++ {
		r.bd.columnToBinFormatter(f, n, i, int(r.bd.header.Rows))
	}
	f.HexBytesln(1, "block padding byte")
	f.ToTreePrinter(n)
}

func (r *KeyValueBlockDecoder) BlockDecoder() *BlockDecoder {
	return &r.bd
}

func (r *KeyValueBlockDecoder) KeyAt(i int) []byte {
	return r.keys.At(i)
}

func (r *KeyValueBlockDecoder) ValueAt(i int) []byte {
	return r.values.At(i)
}
