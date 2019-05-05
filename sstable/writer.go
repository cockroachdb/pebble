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

	"github.com/golang/snappy"
	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/internal/crc"
	"github.com/petermattis/pebble/internal/rangedel"
	"github.com/petermattis/pebble/vfs"
)

// WriterMetadata holds info about a finished sstable.
type WriterMetadata struct {
	Size           uint64
	SmallestPoint  db.InternalKey
	SmallestRange  db.InternalKey
	LargestPoint   db.InternalKey
	LargestRange   db.InternalKey
	SmallestSeqNum uint64
	LargestSeqNum  uint64
}

func (m *WriterMetadata) updateSeqNum(seqNum uint64) {
	if m.SmallestSeqNum > seqNum {
		m.SmallestSeqNum = seqNum
	}
	if m.LargestSeqNum < seqNum {
		m.LargestSeqNum = seqNum
	}
}

func (m *WriterMetadata) updateLargestPoint(key db.InternalKey) {
	// Avoid the memory allocation in InternalKey.Clone() by reusing the buffer.
	m.LargestPoint.UserKey = append(m.LargestPoint.UserKey[:0], key.UserKey...)
	m.LargestPoint.Trailer = key.Trailer
}

// Smallest returns the smaller of SmallestPoint and SmallestRange.
func (m *WriterMetadata) Smallest(cmp db.Compare) db.InternalKey {
	if m.SmallestPoint.UserKey == nil {
		return m.SmallestRange
	}
	if m.SmallestRange.UserKey == nil {
		return m.SmallestPoint
	}
	if db.InternalCompare(cmp, m.SmallestPoint, m.SmallestRange) < 0 {
		return m.SmallestPoint
	}
	return m.SmallestRange
}

// Largest returns the larget of LargestPoint and LargestRange.
func (m *WriterMetadata) Largest(cmp db.Compare) db.InternalKey {
	if m.LargestPoint.UserKey == nil {
		return m.LargestRange
	}
	if m.LargestRange.UserKey == nil {
		return m.LargestPoint
	}
	if db.InternalCompare(cmp, m.LargestPoint, m.LargestRange) > 0 {
		return m.LargestPoint
	}
	return m.LargestRange
}

// Writer is a table writer. It implements the DB interface, as documented
// in the pebble/db package.
type Writer struct {
	writer    io.Writer
	bufWriter *bufio.Writer
	file      vfs.File
	meta      WriterMetadata
	err       error
	// The following fields are copied from db.Options.
	blockSize          int
	blockSizeThreshold int
	compare            db.Compare

	split db.Split

	compression db.Compression
	separator   db.Separator
	successor   db.Successor
	tableFormat db.TableFormat
	// A table is a series of blocks and a block's index entry contains a
	// separator key between one block and the next. Thus, a finished block
	// cannot be written until the first key in the next block is seen.
	// pendingBH is the blockHandle of a finished block that is waiting for
	// the next call to Set. If the writer is not in this state, pendingBH
	// is zero.
	pendingBH blockHandle
	// offset is the offset (relative to the table start) of the next block
	// to be written.
	offset        uint64
	block         blockWriter
	indexBlock    blockWriter
	rangeDelBlock blockWriter
	props         Properties
	// compressedBuf is the destination buffer for snappy compression. It is
	// re-used over the lifetime of the writer, avoiding the allocation of a
	// temporary buffer for each block.
	compressedBuf []byte
	// filter accumulates the filter block. If populated, the filter ingests
	// either the output of w.split (i.e. a prefix extractor) if w.split is not
	// nil, or the full keys otherwise.
	filter filterWriter
	// tmp is a scratch buffer, large enough to hold either footerLen bytes,
	// blockTrailerLen bytes, or (5 * binary.MaxVarintLen64) bytes.
	tmp [rocksDBFooterLen]byte
}

// Set sets the value for the given key. The sequence number is set to
// 0. Intended for use to externally construct an sstable before ingestion into
// a DB.
//
// TODO(peter): untested
func (w *Writer) Set(key, value []byte) error {
	if w.err != nil {
		return w.err
	}
	return w.addPoint(db.MakeInternalKey(key, 0, db.InternalKeyKindSet), value)
}

// Delete deletes the value for the given key. The sequence number is set to
// 0. Intended for use to externally construct an sstable before ingestion into
// a DB.
//
// TODO(peter): untested
func (w *Writer) Delete(key []byte) error {
	if w.err != nil {
		return w.err
	}
	return w.addPoint(db.MakeInternalKey(key, 0, db.InternalKeyKindDelete), nil)
}

// DeleteRange deletes all of the keys (and values) in the range [start,end)
// (inclusive on start, exclusive on end). The sequence number is set to
// 0. Intended for use to externally construct an sstable before ingestion into
// a DB.
//
// TODO(peter): untested
func (w *Writer) DeleteRange(start, end []byte) error {
	if w.err != nil {
		return w.err
	}
	return w.addTombstone(db.MakeInternalKey(start, 0, db.InternalKeyKindRangeDelete), end)
}

// Merge adds an action to the DB that merges the value at key with the new
// value. The details of the merge are dependent upon the configured merge
// operator. The sequence number is set to 0. Intended for use to externally
// construct an sstable before ingestion into a DB.
//
// TODO(peter): untested
func (w *Writer) Merge(key, value []byte) error {
	if w.err != nil {
		return w.err
	}
	return w.addPoint(db.MakeInternalKey(key, 0, db.InternalKeyKindMerge), value)
}

// Add adds a key/value pair to the table being written. For a given Writer,
// the keys passed to Add must be in increasing order. The exception to this
// rule is range deletion tombstones. Range deletion tombstones need to be
// added ordered by their start key, but they can be added out of order from
// point entries. Additionally, range deletion tombstones must be fragmented
// (i.e. by rangedel.Fragmenter).
func (w *Writer) Add(key db.InternalKey, value []byte) error {
	if w.err != nil {
		return w.err
	}

	if key.Kind() == db.InternalKeyKindRangeDelete {
		return w.addTombstone(key, value)
	}
	return w.addPoint(key, value)
}

func (w *Writer) addPoint(key db.InternalKey, value []byte) error {
	if db.InternalCompare(w.compare, w.meta.LargestPoint, key) >= 0 {
		w.err = fmt.Errorf("pebble: keys must be added in order: %s, %s", w.meta.LargestPoint, key)
		return w.err
	}

	if err := w.maybeFlush(key, value); err != nil {
		return err
	}

	w.meta.updateSeqNum(key.SeqNum())
	w.meta.updateLargestPoint(key)

	w.maybeAddToFilter(key.UserKey)

	if w.props.NumEntries == 0 {
		w.meta.SmallestPoint = key.Clone()
	}
	w.props.NumEntries++
	if key.Kind() == db.InternalKeyKindDelete {
		w.props.NumDeletions++
	}
	w.props.RawKeySize += uint64(key.Size())
	w.props.RawValueSize += uint64(len(value))
	w.block.add(key, value)
	return nil
}

func (w *Writer) addTombstone(key db.InternalKey, value []byte) error {
	if w.rangeDelBlock.nEntries > 0 {
		// Check that tombstones are being added in fragmented order. If the two
		// tombstones overlap, their start and end keys must be identical.
		prevKey := db.DecodeInternalKey(w.rangeDelBlock.curKey)
		switch c := w.compare(prevKey.UserKey, key.UserKey); {
		case c > 0:
			w.err = fmt.Errorf("pebble: keys must be added in order: %s, %s", prevKey, key)
			return w.err
		case c == 0:
			prevValue := w.rangeDelBlock.curValue
			if w.compare(prevValue, value) != 0 {
				w.err = fmt.Errorf("pebble: overlapping tombstones must be fragmented: %s vs %s",
					rangedel.Tombstone{Start: prevKey, End: prevValue},
					rangedel.Tombstone{Start: key, End: value})
				return w.err
			}
			if prevKey.SeqNum() <= key.SeqNum() {
				w.err = fmt.Errorf("pebble: keys must be added in order: %s, %s", prevKey, key)
				return w.err
			}
		default:
			prevValue := w.rangeDelBlock.curValue
			if w.compare(prevValue, key.UserKey) > 0 {
				w.err = fmt.Errorf("pebble: overlapping tombstones must be fragmented: %s vs %s",
					rangedel.Tombstone{Start: prevKey, End: prevValue},
					rangedel.Tombstone{Start: key, End: value})
				return w.err
			}
		}
	}

	w.meta.updateSeqNum(key.SeqNum())

	if w.props.NumRangeDeletions == 0 {
		w.meta.SmallestRange = key.Clone()
	}
	w.props.NumRangeDeletions++
	w.rangeDelBlock.add(key, value)
	return nil
}

func (w *Writer) maybeAddToFilter(key []byte) {
	if w.filter != nil {
		if w.split != nil {
			prefix := key[:w.split(key)]
			w.filter.addKey(prefix)
		} else {
			w.filter.addKey(key)
		}
	}
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
	bh, err := w.writeRawBlock(block.finish(), w.compression)

	// Calculate filters.
	if w.filter != nil {
		w.filter.finishBlock(w.offset)
	}

	// Reset the per-block state.
	block.reset()
	return bh, err
}

func (w *Writer) writeRawBlock(b []byte, compression db.Compression) (blockHandle, error) {
	blockType := noCompressionBlockType
	if compression == db.SnappyCompression {
		// Compress the buffer, discarding the result if the improvement isn't at
		// least 12.5%.
		compressed := snappy.Encode(w.compressedBuf, b)
		w.compressedBuf = compressed[:cap(compressed)]
		if len(compressed) < len(b)-len(b)/8 {
			blockType = snappyCompressionBlockType
			b = compressed
		}
	}
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
		bh, err := w.writeRawBlock(b, db.NoCompression)
		if err != nil {
			w.err = err
			return w.err
		}
		n := encodeBlockHandle(w.tmp[:], bh)
		metaindex.add(db.InternalKey{UserKey: []byte(w.filter.metaName())}, w.tmp[:n])
		w.props.FilterPolicyName = w.filter.policyName()
		w.props.FilterSize = bh.length
	}

	// Write the range-del block.
	if w.props.NumRangeDeletions > 0 {
		// Because the range tombstones are fragmented, the end key of the last
		// added range tombstone will be the largest range tombstone key. Note that
		// we need to make this into a range deletion sentinel because sstable
		// boundaries are inclusive while the end key of a range deletion tombstone
		// is exclusive.
		w.meta.LargestRange = db.MakeRangeDeleteSentinelKey(w.rangeDelBlock.curValue)
		b := w.rangeDelBlock.finish()
		bh, err := w.writeRawBlock(b, w.compression)
		if err != nil {
			w.err = err
			return w.err
		}
		n := encodeBlockHandle(w.tmp[:], bh)
		// The v2 range-del block encoding is backwards compatible with the v1
		// encoding. We add meta-index entries for both the old name and the new
		// name so that old code can continue to find the range-del block and new
		// code knows that the range tombstones in the block are fragmented and
		// sorted.
		metaindex.add(db.InternalKey{UserKey: []byte(metaRangeDelName)}, w.tmp[:n])
		metaindex.add(db.InternalKey{UserKey: []byte(metaRangeDelV2Name)}, w.tmp[:n])
	}

	{
		// Write the properties block.
		var raw rawBlockWriter
		raw.restartInterval = 1
		// NB: RocksDB includes the block trailer length in the index size
		// property, though it doesn't include the trailer in the filter size
		// property.
		w.props.IndexSize = uint64(w.indexBlock.estimatedSize()) + blockTrailerLen
		w.props.save(&raw)
		bh, err := w.writeRawBlock(raw.finish(), db.NoCompression)
		if err != nil {
			w.err = err
			return w.err
		}
		n := encodeBlockHandle(w.tmp[:], bh)
		metaindex.add(db.InternalKey{UserKey: []byte(metaPropertiesName)}, w.tmp[:n])
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
	footer := footer{
		format:      w.tableFormat,
		checksum:    checksumCRC32c,
		metaindexBH: metaindexBH,
		indexBH:     indexBH,
	}
	if _, err := w.writer.Write(footer.encode(w.tmp[:])); err != nil {
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

	stat, err := w.file.Stat()
	if err != nil {
		w.err = err
		return err
	}

	size := stat.Size()
	if size < 0 {
		w.err = fmt.Errorf("pebble: file has negative size %d", size)
		return err
	}
	w.meta.Size = uint64(size)

	// Make any future calls to Set or Close return an error.
	w.err = errors.New("pebble: writer is closed")
	return nil
}

// EstimatedSize returns the estimated size of the sstable being written if a
// called to Finish() was made without adding additional keys.
func (w *Writer) EstimatedSize() uint64 {
	return w.offset + uint64(w.block.estimatedSize()+w.indexBlock.estimatedSize())
}

// Metadata returns the metadata for the finished sstable. Only valid to call
// after the sstable has been finished.
func (w *Writer) Metadata() (*WriterMetadata, error) {
	if w.file != nil {
		return nil, errors.New("pebble: writer is not closed")
	}
	return &w.meta, nil
}

// NewWriter returns a new table writer for the file. Closing the writer will
// close the file.
func NewWriter(f vfs.File, o *db.Options, lo db.LevelOptions) *Writer {
	o = o.EnsureDefaults()
	lo = *lo.EnsureDefaults()

	if f != nil {
		f = vfs.NewSyncingFile(f, vfs.SyncingFileOptions{
			BytesPerSync: o.BytesPerSync,
		})
	}

	w := &Writer{
		file: f,
		meta: WriterMetadata{
			SmallestSeqNum: math.MaxUint64,
		},
		blockSize:          lo.BlockSize,
		blockSizeThreshold: (lo.BlockSize*lo.BlockSizeThreshold + 99) / 100,
		compare:            o.Comparer.Compare,
		split:              o.Comparer.Split,
		compression:        lo.Compression,
		separator:          o.Comparer.Separator,
		successor:          o.Comparer.Successor,
		tableFormat:        o.TableFormat,
		block: blockWriter{
			restartInterval: lo.BlockRestartInterval,
		},
		indexBlock: blockWriter{
			restartInterval: 1,
		},
		rangeDelBlock: blockWriter{
			restartInterval: 1,
		},
	}
	if f == nil {
		w.err = errors.New("pebble: nil file")
		return w
	}

	w.props.PrefixExtractorName = "nullptr"
	if lo.FilterPolicy != nil {
		switch lo.FilterType {
		case db.TableFilter:
			w.filter = newTableFilterWriter(lo.FilterPolicy)
			if w.split != nil {
				w.props.PrefixExtractorName = o.Comparer.Name
				w.props.PrefixFiltering = true
			} else {
				w.props.WholeKeyFiltering = true
			}
		default:
			panic(fmt.Sprintf("unknown filter type: %v", lo.FilterType))
		}
	}

	w.props.ColumnFamilyID = math.MaxInt32
	w.props.ComparatorName = o.Comparer.Name
	w.props.CompressionName = lo.Compression.String()
	w.props.MergeOperatorName = o.Merger.Name
	w.props.PropertyCollectorNames = "[]"
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
