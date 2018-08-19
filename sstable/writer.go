// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"

	"github.com/golang/snappy"
	"github.com/petermattis/pebble/crc"
	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/storage"
)

// Writer is a table writer. It implements the DB interface, as documented
// in the pebble/db package.
type Writer struct {
	writer    io.Writer
	bufWriter *bufio.Writer
	file      storage.File
	stat      os.FileInfo
	err       error
	// The next give fields are copied from a db.Options.
	blockSize          int
	blockSizeThreshold int
	bytesPerSync       int
	compare            db.Compare
	compression        db.Compression
	separator          db.Separator
	successor          db.Successor
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

	if err := w.maybeFlush(key, value); err != nil {
		return err
	}

	if w.filter != nil {
		w.filter.addKey(key.UserKey)
	}
	w.props.NumEntries++
	w.props.RawKeySize += uint64(key.Size())
	w.props.RawValueSize += uint64(len(value))
	w.block.add(key, value)
	return nil
}

func (w *Writer) maybeFlush(key db.InternalKey, value []byte) error {
	if size := w.block.estimatedSize(); size < w.blockSize {
		// The block is currently smaller than the target size.
		if size <= w.blockSizeThreshold {
			// The block is smaller than the threshold size at which we'll consider
			// flushing it.
			return nil
		}
		newSize := size + key.Size() + len(value)
		if w.block.nEntries&w.block.restartInterval == 0 {
			newSize += 4
		}
		// TODO(peter): newSize += 4                              // varint for shared key bytes
		newSize += uvarintLen(uint32(key.Size())) // varint for unshared key bytes
		newSize += uvarintLen(uint32(len(value))) // varint for value size
		if newSize <= w.blockSize {
			// The block plus the new entry is smaller than the target size.
			return nil
		}
	}

	bh, err := w.finishBlock(&w.block)
	if err != nil {
		w.err = err
		return w.err
	}
	w.pendingBH = bh
	w.flushPendingBH(key)
	return nil
}

// flushPendingBH adds any pending block handle to the index entries.
func (w *Writer) flushPendingBH(key db.InternalKey) {
	if w.pendingBH.length == 0 {
		// A valid blockHandle must be non-zero.
		// In particular, it must have a non-zero length.
		return
	}
	prevKey := db.DecodeInternalKey(w.block.curKey)
	var sep db.InternalKey
	if key.UserKey == nil && key.Trailer == 0 {
		sep = prevKey.Successor(w.compare, w.successor, nil)
	} else {
		sep = prevKey.Separator(w.compare, w.separator, nil, key)
	}
	n := encodeBlockHandle(w.tmp[:], w.pendingBH)
	w.indexBlock.add(sep, w.tmp[:n])
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
	if w.filter != nil {
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
	if w.filter != nil {
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
		n := encodeBlockHandle(w.tmp[:], bh)
		metaindex.add(db.InternalKey{UserKey: []byte(w.filter.metaName())}, w.tmp[:n])
		w.props.FilterPolicyName = w.filter.policyName()
		w.props.FilterSize = bh.length
	}

	// TODO(peter): write the range-del block.

	{
		// Write the properties block.
		var raw rawBlockWriter
		raw.restartInterval = 1
		// NB: RocksDB includes the block trailer length in the index size
		// property, though it doesn't include the trailer in the filter size
		// property.
		w.props.IndexSize = uint64(w.indexBlock.estimatedSize()) + blockTrailerLen
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
	metaindexBH, err := w.finishBlock(&metaindex.blockWriter)
	if err != nil {
		w.err = err
		return w.err
	}

	// Write the index block.
	indexBH, err := w.finishBlock(&w.indexBlock)
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
	n += encodeBlockHandle(footer[n:], metaindexBH)
	n += encodeBlockHandle(footer[n:], indexBH)
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

// EstimatedSize returns the estimated size of the sstable being written if a
// called to Finish() was made without adding additional keys.
func (w *Writer) EstimatedSize() uint64 {
	return w.offset + uint64(w.block.estimatedSize()+w.indexBlock.estimatedSize())
}

// Stat returns the file info for the finished sstable. Only valid to call
// after the sstable has been finished.
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
		file:               f,
		blockSize:          lo.BlockSize,
		blockSizeThreshold: (lo.BlockSize*lo.BlockSizeThreshold + 99) / 100,
		bytesPerSync:       o.BytesPerSync,
		compare:            o.Comparer.Compare,
		compression:        lo.Compression,
		separator:          o.Comparer.Separator,
		successor:          o.Comparer.Successor,
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

	if lo.FilterPolicy != nil {
		switch lo.FilterType {
		case db.BlockFilter:
			w.filter = newBlockFilterWriter(lo.FilterPolicy)
		case db.TableFilter:
			w.filter = newTableFilterWriter(lo.FilterPolicy)
		default:
			panic(fmt.Sprintf("unknown filter type: %v", lo.FilterType))
		}
	}

	w.props.ColumnFamilyID = math.MaxInt32
	w.props.ComparatorName = o.Comparer.Name
	w.props.CompressionName = lo.Compression.String()
	w.props.MergeOperatorName = o.Merger.Name
	w.props.PrefixExtractorName = "nullptr"
	w.props.PropertyCollectorNames = "[]"
	w.props.WholeKeyFiltering = true
	w.props.Version = 2 // TODO(peter): what is this?

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
