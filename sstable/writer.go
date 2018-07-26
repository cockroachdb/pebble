// Copyright 2011 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sstable

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/golang/snappy"
	"github.com/petermattis/pebble/crc"
	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/storage"
)

type syncer interface {
	Sync() error
}

// indexEntry is a block handle and the length of the separator key.
type indexEntry struct {
	bh     blockHandle
	keyLen int
}

// filterBaseLog being 11 means that we generate a new filter for every 2KiB of
// data.
//
// It's a little unfortunate that this is 11, whilst the default db.Options
// BlockSize is 1<<12 or 4KiB, so that in practice, every second filter is
// empty, but both values match the C++ code.
const filterBaseLog = 11

type filterWriter struct {
	policy db.FilterPolicy
	// block holds the keys for the current block. The buffers are re-used for
	// each new block.
	block struct {
		data    []byte
		lengths []int
		keys    [][]byte
	}
	// data and offsets are the per-block filters for the overall table.
	data    []byte
	offsets []uint32
}

func (f *filterWriter) hasKeys() bool {
	return len(f.block.lengths) != 0
}

func (f *filterWriter) appendKey(key []byte) {
	f.block.data = append(f.block.data, key...)
	f.block.lengths = append(f.block.lengths, len(key))
}

func (f *filterWriter) appendOffset() error {
	o := len(f.data)
	if uint64(o) > 1<<32-1 {
		return errors.New("pebble/table: filter data is too long")
	}
	f.offsets = append(f.offsets, uint32(o))
	return nil
}

func (f *filterWriter) emit() error {
	if err := f.appendOffset(); err != nil {
		return err
	}
	if !f.hasKeys() {
		return nil
	}

	i, j := 0, 0
	for _, length := range f.block.lengths {
		j += length
		f.block.keys = append(f.block.keys, f.block.data[i:j])
		i = j
	}
	f.data = f.policy.AppendFilter(f.data, f.block.keys)

	// Reset the per-block state.
	f.block.data = f.block.data[:0]
	f.block.lengths = f.block.lengths[:0]
	f.block.keys = f.block.keys[:0]
	return nil
}

func (f *filterWriter) finishBlock(blockOffset uint64) error {
	for i := blockOffset >> filterBaseLog; i > uint64(len(f.offsets)); {
		if err := f.emit(); err != nil {
			return err
		}
	}
	return nil
}

func (f *filterWriter) finish() ([]byte, error) {
	if f.hasKeys() {
		if err := f.emit(); err != nil {
			return nil, err
		}
	}
	if err := f.appendOffset(); err != nil {
		return nil, err
	}

	var b [4]byte
	for _, x := range f.offsets {
		binary.LittleEndian.PutUint32(b[:], x)
		f.data = append(f.data, b[0], b[1], b[2], b[3])
	}
	f.data = append(f.data, filterBaseLog)
	return f.data, nil
}

// Writer is a table writer. It implements the DB interface, as documented
// in the pebble/db package.
type Writer struct {
	writer    io.Writer
	bufWriter *bufio.Writer
	file      storage.File
	stat      os.FileInfo
	err       error
	// The next give fields are copied from a db.Options.
	blockSize    int
	bytesPerSync int
	appendSep    db.AppendSeparator
	compare      db.Compare
	compression  db.Compression
	// A table is a series of blocks and a block's index entry contains a
	// separator key between one block and the next. Thus, a finished block
	// cannot be written until the first key in the next block is seen.
	// pendingBH is the blockHandle of a finished block that is waiting for
	// the next call to Set. If the writer is not in this state, pendingBH
	// is zero.
	pendingBH blockHandle
	// offset is the offset (relative to the table start) of the next block
	// to be written.
	offset     uint64
	syncOffset uint64
	block      blockWriter
	indexBlock blockWriter
	props      Properties
	// compressedBuf is the destination buffer for snappy compression. It is
	// re-used over the lifetime of the writer, avoiding the allocation of a
	// temporary buffer for each block.
	compressedBuf []byte
	// filter accumulates the filter block.
	filter filterWriter
	// tmp is a scratch buffer, large enough to hold either footerLen bytes,
	// blockTrailerLen bytes, or (5 * binary.MaxVarintLen64) bytes.
	tmp [footerLen]byte
}

// Add adds a key/value pair to the table being written. For a given Writer,
// the keys passed to Add must be in increasing order.
func (w *Writer) Add(key db.InternalKey, value []byte) error {
	if w.err != nil {
		return w.err
	}
	prevKey := db.DecodeInternalKey(w.block.curKey)
	if db.InternalCompare(w.compare, prevKey, key) >= 0 {
		w.err = fmt.Errorf("pebble/table: Add called in non-increasing key order: %q, %q", prevKey, key)
		return w.err
	}
	if w.filter.policy != nil {
		w.filter.appendKey(key.UserKey)
	}
	w.flushPendingBH(key)
	w.props.NumEntries++
	w.props.RawKeySize += uint64(key.Size())
	w.props.RawValueSize += uint64(len(value))
	w.block.add(key, value)
	// If the estimated block size is sufficiently large, finish the current block.
	if w.block.estimatedSize() >= w.blockSize {
		bh, err := w.finishBlock(&w.block)
		if err != nil {
			w.err = err
			return w.err
		}
		w.pendingBH = bh
	}
	return nil
}

// flushPendingBH adds any pending block handle to the index entries.
func (w *Writer) flushPendingBH(key db.InternalKey) {
	if w.pendingBH.length == 0 {
		// A valid blockHandle must be non-zero.
		// In particular, it must have a non-zero length.
		return
	}
	ikey := db.DecodeInternalKey(w.block.curKey)
	ikey.UserKey = w.appendSep(nil, ikey.UserKey, key.UserKey)
	n := encodeBlockHandle(w.tmp[:], w.pendingBH)
	w.indexBlock.add(ikey, w.tmp[:n])
	w.pendingBH = blockHandle{}
}

// finishBlock finishes the current block and returns its block handle, which is
// its offset and length in the table.
func (w *Writer) finishBlock(block *blockWriter) (blockHandle, error) {
	// Compress the buffer, discarding the result if the improvement
	// isn't at least 12.5%.
	b := block.finish()
	blockType := byte(noCompressionBlockType)
	if w.compression == db.SnappyCompression {
		compressed := snappy.Encode(w.compressedBuf, b)
		w.compressedBuf = compressed[:cap(compressed)]
		if len(compressed) < len(b)-len(b)/8 {
			blockType = snappyCompressionBlockType
			b = compressed
		}
	}
	bh, err := w.writeRawBlock(b, blockType)

	// Calculate filters.
	if w.filter.policy != nil {
		w.filter.finishBlock(w.offset)
	}

	// Reset the per-block state.
	block.reset()

	return bh, err
}

func (w *Writer) writeRawBlock(b []byte, blockType byte) (blockHandle, error) {
	w.tmp[0] = blockType

	// Calculate the checksum.
	checksum := crc.New(b).Update(w.tmp[:1]).Value()
	binary.LittleEndian.PutUint32(w.tmp[1:5], checksum)

	// Write the bytes to the file.
	if _, err := w.writer.Write(b); err != nil {
		return blockHandle{}, err
	}
	if _, err := w.writer.Write(w.tmp[:5]); err != nil {
		return blockHandle{}, err
	}
	bh := blockHandle{w.offset, uint64(len(b))}
	w.offset += uint64(len(b)) + blockTrailerLen

	// Sync the file periodically to smooth out disk traffic.
	if w.bytesPerSync > 0 && (w.offset-w.syncOffset) >= uint64(w.bytesPerSync) {
		if w.bufWriter != nil {
			if err := w.bufWriter.Flush(); err != nil {
				return blockHandle{}, err
			}
		}
		if err := w.file.Sync(); err != nil {
			return blockHandle{}, err
		}
		w.syncOffset = w.offset
	}
	return bh, nil
}

// Close finishes writing the table and closes the underlying file that the
// table was written to.
func (w *Writer) Close() (err error) {
	defer func() {
		if w.file == nil {
			return
		}
		err1 := w.file.Close()
		if err == nil {
			err = err1
		}
		w.file = nil
	}()
	if w.err != nil {
		return w.err
	}

	// Finish the last data block, or force an empty data block if there
	// aren't any data blocks at all.
	w.flushPendingBH(db.InternalKey{})
	if w.block.nEntries > 0 || w.indexBlock.nEntries == 0 {
		bh, err := w.finishBlock(&w.block)
		if err != nil {
			w.err = err
			return w.err
		}
		w.pendingBH = bh
		w.flushPendingBH(db.InternalKey{})
	}
	w.props.DataSize = w.offset
	w.props.NumDataBlocks = uint64(w.indexBlock.nEntries)

	// Write the filter block.
	var metaindex rawBlockWriter
	metaindex.restartInterval = 1
	if w.filter.policy != nil {
		b, err := w.filter.finish()
		if err != nil {
			w.err = err
			return w.err
		}
		bh, err := w.writeRawBlock(b, noCompressionBlockType)
		if err != nil {
			w.err = err
			return w.err
		}
		n := encodeBlockHandle(w.tmp[:0], bh)
		metaindex.add(db.InternalKey{UserKey: []byte("filter." + w.filter.policy.Name())}, w.tmp[:n])
	}

	// Write the index block.
	indexBlockHandle, err := w.finishBlock(&w.indexBlock)
	if err != nil {
		w.err = err
		return w.err
	}

	// TODO(peter): write the range-del block.

	{
		// Write the properties block.
		var raw rawBlockWriter
		raw.restartInterval = 1
		w.props.IndexSize = indexBlockHandle.length
		w.props.save(&raw)
		bh, err := w.writeRawBlock(raw.finish(), noCompressionBlockType)
		if err != nil {
			w.err = err
			return w.err
		}
		n := encodeBlockHandle(w.tmp[:], bh)
		metaindex.add(db.InternalKey{UserKey: []byte("rocksdb.properties")}, w.tmp[:n])
	}

	// Write the metaindex block. It might be an empty block, if the filter
	// policy is nil.
	metaindexBlockHandle, err := w.finishBlock(&metaindex.blockWriter)
	if err != nil {
		w.err = err
		return w.err
	}

	// Write the table footer.
	footer := w.tmp[:footerLen]
	for i := range footer {
		footer[i] = 0
	}
	footer[0] = checksumCRC32c
	n := 1
	n += encodeBlockHandle(footer[n:], metaindexBlockHandle)
	n += encodeBlockHandle(footer[n:], indexBlockHandle)
	binary.LittleEndian.PutUint32(footer[versionOffset:], formatVersion)
	copy(footer[magicOffset:], magic)
	if _, err := w.writer.Write(footer); err != nil {
		w.err = err
		return w.err
	}

	// Flush the buffer.
	if w.bufWriter != nil {
		if err := w.bufWriter.Flush(); err != nil {
			w.err = err
			return err
		}
	}

	if err := w.file.Sync(); err != nil {
		w.err = err
		return err
	}

	w.stat, err = w.file.Stat()
	if err != nil {
		w.err = err
		return err
	}

	// Make any future calls to Set or Close return an error.
	w.err = errors.New("pebble/table: writer is closed")
	return nil
}

// EstimatedSize ...
func (w *Writer) EstimatedSize() uint64 {
	return w.offset + uint64(w.block.estimatedSize()+w.indexBlock.estimatedSize())
}

// Stat ...
func (w *Writer) Stat() (os.FileInfo, error) {
	if w.file != nil {
		return nil, errors.New("pebble/table: writer is not closed")
	}
	return w.stat, nil
}

// NewWriter returns a new table writer for the file. Closing the writer will
// close the file.
func NewWriter(f storage.File, o *db.Options, lo db.LevelOptions) *Writer {
	o = o.EnsureDefaults()
	lo = *lo.EnsureDefaults()
	w := &Writer{
		file:         f,
		blockSize:    lo.BlockSize,
		bytesPerSync: o.BytesPerSync,
		appendSep:    o.Comparer.AppendSeparator,
		compare:      o.Comparer.Compare,
		compression:  lo.Compression,
		filter: filterWriter{
			policy: lo.FilterPolicy,
		},
		block: blockWriter{
			restartInterval: lo.BlockRestartInterval,
		},
		indexBlock: blockWriter{
			restartInterval: 1,
		},
	}
	if f == nil {
		w.err = errors.New("pebble/table: nil file")
		return w
	}
	// If f does not have a Flush method, do our own buffering.
	type flusher interface {
		Flush() error
	}
	if _, ok := f.(flusher); ok {
		w.writer = f
	} else {
		w.bufWriter = bufio.NewWriter(f)
		w.writer = w.bufWriter
	}
	return w
}
