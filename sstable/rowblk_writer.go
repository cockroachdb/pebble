// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/bytealloc"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/rangedel"
	"github.com/cockroachdb/pebble/internal/rangekey"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable/blob"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/rowblk"
	"github.com/cockroachdb/pebble/sstable/valblk"
)

// encodedBHPEstimatedSize estimates the size of the encoded BlockHandleWithProperties.
// It would also be nice to account for the length of the data block properties here,
// but isn't necessary since this is an estimate.
const encodedBHPEstimatedSize = binary.MaxVarintLen64 * 2

var errWriterClosed = errors.New("pebble: writer is closed")

// RawRowWriter is a sstable RawWriter that writes sstables with row-oriented
// blocks. All table formats TableFormatPebblev4 and earlier write row-oriented
// blocks and use RawRowWriter.
type RawRowWriter struct {
	layout     layoutWriter
	meta       WriterMetadata
	err        error
	dataFlush  block.FlushGovernor
	indexFlush block.FlushGovernor
	// The following fields are copied from Options.
	compare              Compare
	pointSuffixCmp       base.ComparePointSuffixes
	split                Split
	formatKey            base.FormatKey
	compression          block.Compression
	separator            Separator
	successor            Successor
	validateKey          base.ValidateKey
	tableFormat          TableFormat
	isStrictObsolete     bool
	writingToLowestLevel bool
	restartInterval      int
	checksumType         block.ChecksumType
	// disableKeyOrderChecks disables the checks that keys are added to an
	// sstable in order. It is intended for internal use only in the construction
	// of invalid sstables for testing. See tool/make_test_sstables.go.
	disableKeyOrderChecks bool
	// With two level indexes, the index/filter of a SST file is partitioned into
	// smaller blocks with an additional top-level index on them. When reading an
	// index/filter, only the top-level index is loaded into memory. The two level
	// index/filter then uses the top-level index to load on demand into the block
	// cache the partitions that are required to perform the index/filter query.
	//
	// Two level indexes are enabled automatically when there is more than one
	// index block.
	//
	// This is useful when there are very large index blocks, which generally occurs
	// with the usage of large keys. With large index blocks, the index blocks fight
	// the data blocks for block cache space and the index blocks are likely to be
	// re-read many times from the disk. The top level index, which has a much
	// smaller memory footprint, can be used to prevent the entire index block from
	// being loaded into the block cache.
	twoLevelIndex       bool
	indexBlock          *indexBlockBuf
	rangeDelBlock       rowblk.Writer
	rangeKeyBlock       rowblk.Writer
	topLevelIndexBlock  rowblk.Writer
	props               Properties
	blockPropCollectors []BlockPropertyCollector
	obsoleteCollector   obsoleteKeyBlockPropertyCollector
	blockPropsEncoder   blockPropertiesEncoder
	// filter accumulates the filter block. If populated, the filter ingests
	// either the output of w.split (i.e. a prefix extractor) if w.split is not
	// nil, or the full keys otherwise.
	filter          filterWriter
	indexPartitions []bufferedIndexBlock
	// indexPartitionsSizeSum is the sum of the sizes of all index blocks in
	// indexPartitions.
	indexPartitionsSizeSum uint64

	// indexBlockAlloc is used to bulk-allocate byte slices used to store index
	// blocks in indexPartitions. These live until the index finishes.
	indexBlockAlloc []byte
	// indexSepAlloc is used to bulk-allocate index block separator slices stored
	// in indexPartitions. These live until the index finishes.
	indexSepAlloc bytealloc.A

	rangeKeyEncoder rangekey.Encoder
	// dataBlockBuf consists of the state which is currently owned by and used by
	// the Writer client goroutine. This state can be handed off to other goroutines.
	dataBlockBuf *dataBlockBuf
	// blockBuf consists of the state which is owned by and used by the Writer client
	// goroutine.
	blockBuf blockBuf

	coordination coordinationState

	// Information (other than the byte slice) about the last point key, to
	// avoid extracting it again.
	lastPointKeyInfo pointKeyInfo

	// For value blocks.
	shortAttributeExtractor   base.ShortAttributeExtractor
	requiredInPlaceValueBound UserKeyPrefixBound
	// When w.tableFormat >= TableFormatPebblev3, valueBlockWriter is nil iff
	// WriterOptions.DisableValueBlocks was true.
	valueBlockWriter *valblk.Writer

	allocatorSizeClasses []int

	numDeletionsThreshold      int
	deletionSizeRatioThreshold float32
}

type pointKeyInfo struct {
	trailer base.InternalKeyTrailer
	// Only computed when w.valueBlockWriter is not nil.
	userKeyLen int
	// prefixLen uses w.split, if not nil. Only computed when w.valueBlockWriter
	// is not nil.
	prefixLen int
	// True iff the point was marked obsolete.
	isObsolete bool
}

type coordinationState struct {
	// writeQueue is used to write data blocks to disk. The writeQueue is primarily
	// used to maintain the order in which data blocks must be written to disk. For
	// this reason, every single data block write must be done through the writeQueue.
	writeQueue *writeQueue

	sizeEstimate dataBlockEstimates
}

func (c *coordinationState) init(writer *RawRowWriter) {
	// writeQueueSize determines the size of the write queue, or the number of
	// items which can be added to the queue without blocking. We always use a
	// writeQueue size of 0.
	c.writeQueue = newWriteQueue(0 /* size */, writer)
}

// sizeEstimate is a general purpose helper for estimating two kinds of sizes:
// A. The compressed sstable size, which is useful for deciding when to start
//
//	a new sstable during flushes or compactions. In practice, we use this in
//	estimating the data size (excluding the index).
//
// B. The size of index blocks to decide when to start a new index block.
//
// There are some terminology peculiarities which are due to the origin of
// sizeEstimate for use case A with parallel compression enabled (for which
// the code has not been merged). Specifically this relates to the terms
// "written" and "compressed".
//   - The notion of "written" for case A is sufficiently defined by saying that
//     the data block is compressed. Waiting for the actual data block write to
//     happen can result in unnecessary estimation, when we already know how big
//     it will be in compressed form. Additionally, with the forthcoming value
//     blocks containing older MVCC values, these compressed block will be held
//     in-memory until late in the sstable writing, and we do want to accurately
//     account for them without waiting for the actual write.
//     For case B, "written" means that the index entry has been fully
//     generated, and has been added to the uncompressed block buffer for that
//     index block. It does not include actually writing a potentially
//     compressed index block.
//   - The notion of "compressed" is to differentiate between a "inflight" size
//     and the actual size, and is handled via computing a compression ratio
//     observed so far (defaults to 1).
//     For case A, this is actual data block compression, so the "inflight" size
//     is uncompressed blocks (that are no longer being written to) and the
//     "compressed" size is after they have been compressed.
//     For case B the inflight size is for a key-value pair in the index for
//     which the value size (the encoded size of the BlockHandleWithProperties)
//     is not accurately known, while the compressed size is the size of that
//     entry when it has been added to the (in-progress) index ssblock.
//
// Usage: To update state, one can optionally provide an inflight write value
// using addInflight (used for case B). When something is "written" the state
// can be updated using either writtenWithDelta or writtenWithTotal, which
// provide the actual delta size or the total size (latter must be
// monotonically non-decreasing). If there were no calls to addInflight, there
// isn't any real estimation happening here. So case A does not do any real
// estimation. However, when we introduce parallel compression, there will be
// estimation in that the client goroutine will call addInFlight and the
// compression goroutines will call writtenWithDelta.
type sizeEstimate struct {
	// emptySize is the size when there is no inflight data, and numEntries is 0.
	// emptySize is constant once set.
	emptySize uint64

	// inflightSize is the estimated size of some inflight data which hasn't
	// been written yet.
	inflightSize uint64

	// totalSize is the total size of the data which has already been written.
	totalSize uint64

	// numWrittenEntries is the total number of entries which have already been
	// written.
	numWrittenEntries uint64
	// numInflightEntries is the total number of entries which are inflight, and
	// haven't been written.
	numInflightEntries uint64

	// maxEstimatedSize stores the maximum result returned from sizeEstimate.size.
	// It ensures that values returned from subsequent calls to Writer.EstimatedSize
	// never decrease.
	maxEstimatedSize uint64

	// We assume that the entries added to the sizeEstimate can be compressed.
	// For this reason, we keep track of a compressedSize and an uncompressedSize
	// to compute a compression ratio for the inflight entries. If the entries
	// aren't being compressed, then compressedSize and uncompressedSize must be
	// equal.
	compressedSize   uint64
	uncompressedSize uint64
}

func (s *sizeEstimate) init(emptySize uint64) {
	s.emptySize = emptySize
}

func (s *sizeEstimate) size() uint64 {
	ratio := float64(1)
	if s.uncompressedSize > 0 {
		ratio = float64(s.compressedSize) / float64(s.uncompressedSize)
	}
	estimatedInflightSize := uint64(float64(s.inflightSize) * ratio)
	total := s.totalSize + estimatedInflightSize
	if total > s.maxEstimatedSize {
		s.maxEstimatedSize = total
	} else {
		total = s.maxEstimatedSize
	}

	if total == 0 {
		return s.emptySize
	}

	return total
}

func (s *sizeEstimate) numTotalEntries() uint64 {
	return s.numWrittenEntries + s.numInflightEntries
}

func (s *sizeEstimate) addInflight(size int) {
	s.numInflightEntries++
	s.inflightSize += uint64(size)
}

func (s *sizeEstimate) writtenWithTotal(newTotalSize uint64, inflightSize int) {
	finalEntrySize := int(newTotalSize - s.totalSize)
	s.writtenWithDelta(finalEntrySize, inflightSize)
}

func (s *sizeEstimate) writtenWithDelta(finalEntrySize int, inflightSize int) {
	if inflightSize > 0 {
		// This entry was previously inflight, so we should decrement inflight
		// entries and update the "compression" stats for future estimation.
		s.numInflightEntries--
		s.inflightSize -= uint64(inflightSize)
		s.uncompressedSize += uint64(inflightSize)
		s.compressedSize += uint64(finalEntrySize)
	}
	s.numWrittenEntries++
	s.totalSize += uint64(finalEntrySize)
}

func (s *sizeEstimate) clear() {
	*s = sizeEstimate{emptySize: s.emptySize}
}

type indexBlockBuf struct {
	// block will only be accessed from the writeQueue.
	block rowblk.Writer

	size struct {
		estimate sizeEstimate
	}

	// restartInterval matches indexBlockBuf.block.restartInterval. We store it twice, because the `block`
	// must only be accessed from the writeQueue goroutine.
	restartInterval int
}

func (i *indexBlockBuf) clear() {
	i.block.Reset()
	i.size.estimate.clear()
	i.restartInterval = 0
}

var indexBlockBufPool = sync.Pool{
	New: func() interface{} {
		return &indexBlockBuf{}
	},
}

const indexBlockRestartInterval = 1

func newIndexBlockBuf() *indexBlockBuf {
	i := indexBlockBufPool.Get().(*indexBlockBuf)
	i.restartInterval = indexBlockRestartInterval
	i.block.RestartInterval = indexBlockRestartInterval
	i.size.estimate.init(rowblk.EmptySize)
	return i
}

func (i *indexBlockBuf) shouldFlush(
	sep InternalKey, valueLen int, flushGovernor *block.FlushGovernor,
) bool {
	nEntries := i.size.estimate.numTotalEntries()
	return shouldFlush(
		sep.Size(), valueLen, i.restartInterval, int(i.size.estimate.size()),
		int(nEntries), flushGovernor)
}

func (i *indexBlockBuf) add(key InternalKey, value []byte, inflightSize int) error {
	if err := i.block.Add(key, value); err != nil {
		return err
	}
	size := i.block.EstimatedSize()
	i.size.estimate.writtenWithTotal(uint64(size), inflightSize)
	return nil
}

func (i *indexBlockBuf) finish() []byte {
	b := i.block.Finish()
	return b
}

func (i *indexBlockBuf) addInflight(inflightSize int) {
	i.size.estimate.addInflight(inflightSize)
}

func (i *indexBlockBuf) estimatedSize() uint64 {
	// Make sure that the size estimation works as expected.
	if invariants.Enabled {
		if i.size.estimate.inflightSize != 0 {
			panic("unexpected inflight entry in index block size estimation")
		}
		if i.size.estimate.size() != uint64(i.block.EstimatedSize()) {
			panic("index block size estimation is incorrect")
		}
	}
	return i.size.estimate.size()
}

// sizeEstimate is used for sstable size estimation. sizeEstimate can be
// accessed by the Writer client and compressionQueue goroutines. Fields
// should only be read/updated through the functions defined on the
// *sizeEstimate type.
type dataBlockEstimates struct {
	estimate sizeEstimate
}

// inflightSize is the uncompressed block size estimate which has been
// previously provided to addInflightDataBlock(). If addInflightDataBlock()
// has not been called, this must be set to 0. compressedSize is the
// compressed size of the block.
func (d *dataBlockEstimates) dataBlockCompressed(compressedSize int, inflightSize int) {
	d.estimate.writtenWithDelta(compressedSize+block.TrailerLen, inflightSize)
}

// size is an estimated size of datablock data which has been written to disk.
func (d *dataBlockEstimates) size() uint64 {
	if invariants.Enabled {
		if d.estimate.inflightSize != 0 {
			panic("unexpected inflight entry in data block size estimation")
		}
	}
	return d.estimate.size()
}

// Avoid linter unused error.
var _ = (&dataBlockEstimates{}).addInflightDataBlock

// NB: unused since no parallel compression.
func (d *dataBlockEstimates) addInflightDataBlock(size int) {
	d.estimate.addInflight(size)
}

var writeTaskPool = sync.Pool{
	New: func() interface{} {
		t := &writeTask{}
		t.compressionDone = make(chan bool, 1)
		return t
	},
}

type blockBuf struct {
	// tmp is a scratch buffer, large enough to hold either footerLen bytes,
	// blockTrailerLen bytes, (5 * binary.MaxVarintLen64) bytes, and most
	// likely large enough for a block handle with properties.
	tmp [blockHandleLikelyMaxLen]byte
	// dataBuf is the destination buffer for compression, or (in some cases where
	// compression is not used) for storing a copy of the data. It is re-used over
	// the lifetime of the blockBuf, avoiding the allocation of a temporary buffer
	// for each block.
	dataBuf     []byte
	checksummer block.Checksummer
}

func (b *blockBuf) clear() {
	b.tmp = [blockHandleLikelyMaxLen]byte{}
	b.dataBuf = b.dataBuf[:0]
}

// A dataBlockBuf holds all the state required to compress and write a data block to disk.
// A dataBlockBuf begins its lifecycle owned by the Writer client goroutine. The Writer
// client goroutine adds keys to the sstable, writing directly into a dataBlockBuf's blockWriter
// until the block is full. Once a dataBlockBuf's block is full, the dataBlockBuf may be passed
// to other goroutines for compression and file I/O.
type dataBlockBuf struct {
	blockBuf
	dataBlock rowblk.Writer

	// uncompressed is a reference to a byte slice which is owned by the dataBlockBuf. It is the
	// next byte slice to be compressed. The uncompressed byte slice will be backed by the
	// dataBlock.buf.
	uncompressed []byte

	// physical holds the (possibly) compressed block and its trailer. The
	// underlying block data's byte slice is owned by the dataBlockBuf. It  may
	// be backed by the dataBlock.buf, or the dataBlockBuf.compressedBuf,
	// depending on whether we use the result of the compression.
	physical block.PhysicalBlock

	// We're making calls to BlockPropertyCollectors from the Writer client goroutine. We need to
	// pass the encoded block properties over to the write queue. To prevent copies, and allocations,
	// we give each dataBlockBuf, a blockPropertiesEncoder.
	blockPropsEncoder blockPropertiesEncoder
	// dataBlockProps is set when Writer.finishDataBlockProps is called. The dataBlockProps slice is
	// a shallow copy of the internal buffer of the dataBlockBuf.blockPropsEncoder.
	dataBlockProps []byte

	// sepScratch is reusable scratch space for computing separator keys.
	sepScratch []byte

	// numDeletions stores the count of point tombstones in this data block.
	// It's used to determine if this data block is considered tombstone-dense
	// for the purposes of compaction.
	numDeletions int
	// deletionSize stores the raw size of point tombstones in this data block.
	// It's used to determine if this data block is considered tombstone-dense
	// for the purposes of compaction.
	deletionSize int
}

func (d *dataBlockBuf) clear() {
	d.blockBuf.clear()
	d.dataBlock.Reset()

	d.uncompressed = nil
	d.physical = block.PhysicalBlock{}
	d.dataBlockProps = nil
	d.sepScratch = d.sepScratch[:0]
}

var dataBlockBufPool = sync.Pool{
	New: func() interface{} {
		return &dataBlockBuf{}
	},
}

func newDataBlockBuf(restartInterval int, checksumType block.ChecksumType) *dataBlockBuf {
	d := dataBlockBufPool.Get().(*dataBlockBuf)
	d.dataBlock.RestartInterval = restartInterval
	d.checksummer.Type = checksumType
	return d
}

func (d *dataBlockBuf) finish() {
	d.uncompressed = d.dataBlock.Finish()
}

func (d *dataBlockBuf) compressAndChecksum(c block.Compression) {
	d.physical = block.CompressAndChecksum(&d.dataBuf, d.uncompressed, c, &d.checksummer)
}

func (d *dataBlockBuf) shouldFlush(
	key InternalKey, valueLen int, flushGovernor *block.FlushGovernor,
) bool {
	return shouldFlush(
		key.Size(), valueLen, d.dataBlock.RestartInterval, d.dataBlock.EstimatedSize(),
		d.dataBlock.EntryCount(), flushGovernor)
}

type bufferedIndexBlock struct {
	nEntries int
	// sep is the last key added to this block, for computing a separator later.
	sep        InternalKey
	properties []byte
	// block is the encoded block produced by blockWriter.finish.
	block []byte
}

// Add must be used when writing a strict-obsolete sstable.
//
// forceObsolete indicates whether the caller has determined that this key is
// obsolete even though it may be the latest point key for this userkey. This
// should be set to true for keys obsoleted by RANGEDELs, and is required for
// strict-obsolete sstables.
//
// Note that there are two properties, S1 and S2 (see comment in format.go)
// that strict-obsolete ssts must satisfy. S2, due to RANGEDELs, is solely the
// responsibility of the caller. S1 is solely the responsibility of the
// callee.
func (w *RawRowWriter) Add(key InternalKey, value []byte, forceObsolete bool) error {
	if w.err != nil {
		return w.err
	}

	switch key.Kind() {
	case InternalKeyKindRangeDelete:
		return w.addTombstone(key, value)
	case base.InternalKeyKindRangeKeyDelete,
		base.InternalKeyKindRangeKeySet,
		base.InternalKeyKindRangeKeyUnset:
		w.err = errors.Errorf(
			"pebble: range keys must be added via one of the RangeKey* functions")
		return w.err
	}
	return w.addPoint(key, value, forceObsolete)
}

// AddWithBlobHandle implements the RawWriter interface. This implementation
// does not support writing blob value handles.
func (w *RawRowWriter) AddWithBlobHandle(
	key InternalKey, h blob.InlineHandle, attr base.ShortAttribute, forceObsolete bool,
) error {
	w.err = errors.Newf("pebble: blob value handles are not supported in %s", w.tableFormat.String())
	return w.err
}

func (w *RawRowWriter) makeAddPointDecisionV2(key InternalKey) error {
	prevTrailer := w.lastPointKeyInfo.trailer
	w.lastPointKeyInfo.trailer = key.Trailer
	if w.dataBlockBuf.dataBlock.EntryCount() == 0 {
		return nil
	}
	if !w.disableKeyOrderChecks {
		prevPointUserKey := w.dataBlockBuf.dataBlock.CurUserKey()
		cmpUser := w.compare(prevPointUserKey, key.UserKey)
		if cmpUser > 0 || (cmpUser == 0 && prevTrailer <= key.Trailer) {
			return errors.Errorf(
				"pebble: keys must be added in strictly increasing order: %s, %s",
				InternalKey{UserKey: prevPointUserKey, Trailer: prevTrailer}.Pretty(w.formatKey),
				key.Pretty(w.formatKey))
		}
	}
	return nil
}

// REQUIRES: at least one point has been written to the Writer.
func (w *RawRowWriter) getLastPointUserKey() []byte {
	if w.dataBlockBuf.dataBlock.EntryCount() == 0 {
		panic(errors.AssertionFailedf("no point keys added to writer"))
	}
	return w.dataBlockBuf.dataBlock.CurUserKey()
}

// REQUIRES: w.tableFormat >= TableFormatPebblev3
func (w *RawRowWriter) makeAddPointDecisionV3(
	key InternalKey, valueLen int,
) (setHasSamePrefix bool, writeToValueBlock bool, isObsolete bool, err error) {
	prevPointKeyInfo := w.lastPointKeyInfo
	w.lastPointKeyInfo.userKeyLen = len(key.UserKey)
	w.lastPointKeyInfo.prefixLen = w.split(key.UserKey)
	w.lastPointKeyInfo.trailer = key.Trailer
	w.lastPointKeyInfo.isObsolete = false
	if !w.meta.HasPointKeys {
		return false, false, false, nil
	}
	keyKind := key.Trailer.Kind()
	prevPointUserKey := w.getLastPointUserKey()
	prevPointKey := InternalKey{UserKey: prevPointUserKey, Trailer: prevPointKeyInfo.trailer}
	prevKeyKind := prevPointKeyInfo.trailer.Kind()
	considerWriteToValueBlock := prevKeyKind == InternalKeyKindSet &&
		keyKind == InternalKeyKindSet
	if considerWriteToValueBlock && !w.requiredInPlaceValueBound.IsEmpty() {
		keyPrefix := key.UserKey[:w.lastPointKeyInfo.prefixLen]
		cmpUpper := w.compare(
			w.requiredInPlaceValueBound.Upper, keyPrefix)
		if cmpUpper <= 0 {
			// Common case for CockroachDB. Make it empty since all future keys in
			// this sstable will also have cmpUpper <= 0.
			w.requiredInPlaceValueBound = UserKeyPrefixBound{}
		} else if w.compare(keyPrefix, w.requiredInPlaceValueBound.Lower) >= 0 {
			considerWriteToValueBlock = false
		}
	}
	// cmpPrefix is initialized iff considerWriteToValueBlock.
	var cmpPrefix int
	var cmpUser int
	if considerWriteToValueBlock {
		// Compare the prefixes.
		cmpPrefix = w.compare(prevPointUserKey[:prevPointKeyInfo.prefixLen],
			key.UserKey[:w.lastPointKeyInfo.prefixLen])
		cmpUser = cmpPrefix
		if cmpPrefix == 0 {
			// Need to compare suffixes to compute cmpUser.
			cmpUser = w.pointSuffixCmp(prevPointUserKey[prevPointKeyInfo.prefixLen:],
				key.UserKey[w.lastPointKeyInfo.prefixLen:])
		}
	} else {
		cmpUser = w.compare(prevPointUserKey, key.UserKey)
	}
	// Ensure that no one adds a point key kind without considering the obsolete
	// handling for that kind.
	switch keyKind {
	case InternalKeyKindSet, InternalKeyKindSetWithDelete, InternalKeyKindMerge,
		InternalKeyKindDelete, InternalKeyKindSingleDelete, InternalKeyKindDeleteSized:
	default:
		panic(errors.AssertionFailedf("unexpected key kind %s", keyKind.String()))
	}
	// If same user key, then the current key is obsolete if any of the
	// following is true:
	// C1 The prev key was obsolete.
	// C2 The prev key was not a MERGE. When the previous key is a MERGE we must
	//    preserve SET* and MERGE since their values will be merged into the
	//    previous key. We also must preserve DEL* since there may be an older
	//    SET*/MERGE in a lower level that must not be merged with the MERGE --
	//    if we omit the DEL* that lower SET*/MERGE will become visible.
	//
	// Regardless of whether it is the same user key or not
	// C3 The current key is some kind of point delete, and we are writing to
	//    the lowest level, then it is also obsolete. The correctness of this
	//    relies on the same user key not spanning multiple sstables in a level.
	//
	// C1 ensures that for a user key there is at most one transition from
	// !obsolete to obsolete. Consider a user key k, for which the first n keys
	// are not obsolete. We consider the various value of n:
	//
	// n = 0: This happens due to forceObsolete being set by the caller, or due
	// to C3. forceObsolete must only be set due a RANGEDEL, and that RANGEDEL
	// must also delete all the lower seqnums for the same user key. C3 triggers
	// due to a point delete and that deletes all the lower seqnums for the same
	// user key.
	//
	// n = 1: This is the common case. It happens when the first key is not a
	// MERGE, or the current key is some kind of point delete.
	//
	// n > 1: This is due to a sequence of MERGE keys, potentially followed by a
	// single non-MERGE key.
	isObsoleteC1AndC2 := cmpUser == 0 &&
		(prevPointKeyInfo.isObsolete || prevKeyKind != InternalKeyKindMerge)
	isObsoleteC3 := w.writingToLowestLevel &&
		(keyKind == InternalKeyKindDelete || keyKind == InternalKeyKindSingleDelete ||
			keyKind == InternalKeyKindDeleteSized)
	isObsolete = isObsoleteC1AndC2 || isObsoleteC3
	// TODO(sumeer): storing isObsolete SET and SETWITHDEL in value blocks is
	// possible, but requires some care in documenting and checking invariants.
	// There is code that assumes nothing in value blocks because of single MVCC
	// version (those should be ok). We have to ensure setHasSamePrefix is
	// correctly initialized here etc.

	if !w.disableKeyOrderChecks &&
		(cmpUser > 0 || (cmpUser == 0 && prevPointKeyInfo.trailer <= key.Trailer)) {
		return false, false, false, errors.Errorf(
			"pebble: keys must be added in strictly increasing order: %s, %s",
			prevPointKey.Pretty(w.formatKey), key.Pretty(w.formatKey))
	}
	if !considerWriteToValueBlock {
		return false, false, isObsolete, nil
	}
	// NB: it is possible that cmpUser == 0, i.e., these two SETs have identical
	// user keys (because of an open snapshot). This should be the rare case.
	setHasSamePrefix = cmpPrefix == 0
	// Use of 0 here is somewhat arbitrary. Given the minimum 3 byte encoding of
	// valueHandle, this should be > 3. But tiny values are common in test and
	// unlikely in production, so we use 0 here for better test coverage.
	const tinyValueThreshold = 0
	// NB: setting WriterOptions.DisableValueBlocks does not disable the
	// setHasSamePrefix optimization.
	considerWriteToValueBlock = setHasSamePrefix && valueLen > tinyValueThreshold && w.valueBlockWriter != nil
	return setHasSamePrefix, considerWriteToValueBlock, isObsolete, nil
}

func (w *RawRowWriter) addPoint(key InternalKey, value []byte, forceObsolete bool) error {
	if w.isStrictObsolete && key.Kind() == InternalKeyKindMerge {
		return errors.Errorf("MERGE not supported in a strict-obsolete sstable")
	}
	var err error
	var setHasSameKeyPrefix, writeToValueBlock, addPrefixToValueStoredWithKey bool
	var isObsolete bool
	maxSharedKeyLen := len(key.UserKey)
	if w.tableFormat >= TableFormatPebblev3 {
		// maxSharedKeyLen is limited to the prefix of the preceding key. If the
		// preceding key was in a different block, then the blockWriter will
		// ignore this maxSharedKeyLen.
		maxSharedKeyLen = w.lastPointKeyInfo.prefixLen
		setHasSameKeyPrefix, writeToValueBlock, isObsolete, err =
			w.makeAddPointDecisionV3(key, len(value))
		addPrefixToValueStoredWithKey = key.Kind() == InternalKeyKindSet
	} else {
		err = w.makeAddPointDecisionV2(key)
	}
	if err != nil {
		return err
	}
	isObsolete = w.tableFormat >= TableFormatPebblev4 && (isObsolete || forceObsolete)
	w.lastPointKeyInfo.isObsolete = isObsolete
	var valueStoredWithKey []byte
	var prefix block.ValuePrefix
	var valueStoredWithKeyLen int
	if writeToValueBlock {
		vh, err := w.valueBlockWriter.AddValue(value)
		if err != nil {
			return err
		}
		n := valblk.EncodeHandle(w.blockBuf.tmp[:], vh)
		valueStoredWithKey = w.blockBuf.tmp[:n]
		valueStoredWithKeyLen = len(valueStoredWithKey) + 1
		var attribute base.ShortAttribute
		if w.shortAttributeExtractor != nil {
			// TODO(sumeer): for compactions, it is possible that the input sstable
			// already has this value in the value section and so we have already
			// extracted the ShortAttribute. Avoid extracting it again. This will
			// require changing the Writer.Add interface.
			if attribute, err = w.shortAttributeExtractor(
				key.UserKey, w.lastPointKeyInfo.prefixLen, value); err != nil {
				return err
			}
		}
		prefix = block.ValueBlockHandlePrefix(setHasSameKeyPrefix, attribute)
	} else {
		valueStoredWithKey = value
		valueStoredWithKeyLen = len(value)
		if addPrefixToValueStoredWithKey {
			valueStoredWithKeyLen++
		}
		prefix = block.InPlaceValuePrefix(setHasSameKeyPrefix)
	}

	if err := w.maybeFlush(key, valueStoredWithKeyLen); err != nil {
		return err
	}

	for i := range w.blockPropCollectors {
		v := value
		if addPrefixToValueStoredWithKey {
			// Values for SET are not required to be in-place, and in the future may
			// not even be read by the compaction, so pass nil values. Block
			// property collectors in such Pebble DB's must not look at the value.
			v = nil
		}
		if err := w.blockPropCollectors[i].AddPointKey(key, v); err != nil {
			w.err = err
			return err
		}
	}
	if w.tableFormat >= TableFormatPebblev4 {
		w.obsoleteCollector.AddPoint(isObsolete)
	}

	w.maybeAddToFilter(key.UserKey)
	if err := w.dataBlockBuf.dataBlock.AddWithOptionalValuePrefix(
		key, isObsolete, valueStoredWithKey, maxSharedKeyLen, addPrefixToValueStoredWithKey, prefix,
		setHasSameKeyPrefix); err != nil {
		return err
	}

	w.meta.updateSeqNum(key.SeqNum())

	if !w.meta.HasPointKeys {
		k := w.dataBlockBuf.dataBlock.CurKey()
		// NB: We need to ensure that SmallestPoint.UserKey is set, so we create
		// an InternalKey which is semantically identical to the key, but won't
		// have a nil UserKey. We do this, because key.UserKey could be nil, and
		// we don't want SmallestPoint.UserKey to be nil.
		//
		// todo(bananabrick): Determine if it's okay to have a nil SmallestPoint
		// .UserKey now that we don't rely on a nil UserKey to determine if the
		// key has been set or not.
		w.meta.SetSmallestPointKey(k.Clone())
	}

	w.props.NumEntries++
	switch key.Kind() {
	case InternalKeyKindDelete, InternalKeyKindSingleDelete:
		w.props.NumDeletions++
		w.dataBlockBuf.numDeletions++
		w.props.RawPointTombstoneKeySize += uint64(len(key.UserKey))
		w.dataBlockBuf.deletionSize += len(key.UserKey)
	case InternalKeyKindDeleteSized:
		var size uint64
		if len(value) > 0 {
			var n int
			size, n = binary.Uvarint(value)
			if n <= 0 {
				w.err = errors.Newf("%s key's value (%x) does not parse as uvarint",
					errors.Safe(key.Kind().String()), value)
				return w.err
			}
		}
		w.props.NumDeletions++
		w.props.NumSizedDeletions++
		w.dataBlockBuf.numDeletions++
		w.props.RawPointTombstoneKeySize += uint64(len(key.UserKey))
		w.dataBlockBuf.deletionSize += len(key.UserKey)
		w.props.RawPointTombstoneValueSize += size
	case InternalKeyKindMerge:
		w.props.NumMergeOperands++
	}
	w.props.RawKeySize += uint64(key.Size())
	w.props.RawValueSize += uint64(len(value))
	return nil
}

func (w *RawRowWriter) prettyTombstone(k InternalKey, value []byte) fmt.Formatter {
	return keyspan.Span{
		Start: k.UserKey,
		End:   value,
		Keys:  []keyspan.Key{{Trailer: k.Trailer}},
	}.Pretty(w.formatKey)
}

func (w *RawRowWriter) addTombstone(key InternalKey, value []byte) error {
	if !w.disableKeyOrderChecks && w.rangeDelBlock.EntryCount() > 0 {
		// Check that tombstones are being added in fragmented order. If the two
		// tombstones overlap, their start and end keys must be identical.
		prevKey := w.rangeDelBlock.CurKey()
		switch c := w.compare(prevKey.UserKey, key.UserKey); {
		case c > 0:
			w.err = errors.Errorf("pebble: keys must be added in order: %s, %s",
				prevKey.Pretty(w.formatKey), key.Pretty(w.formatKey))
			return w.err
		case c == 0:
			prevValue := w.rangeDelBlock.CurValue()
			if w.compare(prevValue, value) != 0 {
				w.err = errors.Errorf("pebble: overlapping tombstones must be fragmented: %s vs %s",
					w.prettyTombstone(prevKey, prevValue),
					w.prettyTombstone(key, value))
				return w.err
			}
			if prevKey.SeqNum() <= key.SeqNum() {
				w.err = errors.Errorf("pebble: keys must be added in strictly increasing order: %s, %s",
					prevKey.Pretty(w.formatKey), key.Pretty(w.formatKey))
				return w.err
			}
		default:
			prevValue := w.rangeDelBlock.CurValue()
			if w.compare(prevValue, key.UserKey) > 0 {
				w.err = errors.Errorf("pebble: overlapping tombstones must be fragmented: %s vs %s",
					w.prettyTombstone(prevKey, prevValue),
					w.prettyTombstone(key, value))
				return w.err
			}
		}
	}

	if key.Trailer == base.InternalKeyRangeDeleteSentinel {
		w.err = errors.Errorf("pebble: cannot add range delete sentinel: %s", key.Pretty(w.formatKey))
		return w.err
	}

	w.meta.updateSeqNum(key.SeqNum())

	// Range tombstones are fragmented in the v2 range deletion block format,
	// so the start key of the first range tombstone added will be the smallest
	// range tombstone key. The largest range tombstone key will be determined
	// in Writer.Close() as the end key of the last range tombstone added.
	if w.props.NumRangeDeletions == 0 {
		w.meta.SetSmallestRangeDelKey(key.Clone())
	}

	w.props.NumEntries++
	w.props.NumDeletions++
	w.props.NumRangeDeletions++
	w.props.RawKeySize += uint64(key.Size())
	w.props.RawValueSize += uint64(len(value))
	return w.rangeDelBlock.Add(key, value)
}

// addRangeKey adds a range key set, unset, or delete key/value pair to the
// table being written.
//
// Range keys must be supplied in strictly ascending order of start key (i.e.
// user key ascending, sequence number descending, and key type descending).
// Ranges added must also be supplied in fragmented span order - i.e. other than
// spans that are perfectly aligned (same start and end keys), spans may not
// overlap. Range keys may be added out of order relative to point keys and
// range deletions.
func (w *RawRowWriter) addRangeKey(key InternalKey, value []byte) error {
	if !w.disableKeyOrderChecks && w.rangeKeyBlock.EntryCount() > 0 {
		prevStartKey := w.rangeKeyBlock.CurKey()
		prevEndKey, _, err := rangekey.DecodeEndKey(prevStartKey.Kind(), w.rangeKeyBlock.CurValue())
		if err != nil {
			// We panic here as we should have previously decoded and validated this
			// key and value when it was first added to the range key block.
			panic(err)
		}

		curStartKey := key
		curEndKey, _, err := rangekey.DecodeEndKey(curStartKey.Kind(), value)
		if err != nil {
			w.err = err
			return w.err
		}

		// Start keys must be strictly increasing.
		if base.InternalCompare(w.compare, prevStartKey, curStartKey) >= 0 {
			w.err = errors.Errorf(
				"pebble: range keys starts must be added in increasing order: %s, %s",
				prevStartKey.Pretty(w.formatKey), key.Pretty(w.formatKey))
			return w.err
		}

		// Start keys are increasing. If the start user keys are equal, the
		// end keys must be equal (i.e. aligned spans).
		if w.compare(prevStartKey.UserKey, curStartKey.UserKey) == 0 {
			if w.compare(prevEndKey, curEndKey) != 0 {
				w.err = errors.Errorf("pebble: overlapping range keys must be fragmented: %s, %s",
					prevStartKey.Pretty(w.formatKey),
					curStartKey.Pretty(w.formatKey))
				return w.err
			}
		} else if w.compare(prevEndKey, curStartKey.UserKey) > 0 {
			// If the start user keys are NOT equal, the spans must be disjoint (i.e.
			// no overlap).
			// NOTE: the inequality excludes zero, as we allow the end key of the
			// lower span be the same as the start key of the upper span, because
			// the range end key is considered an exclusive bound.
			w.err = errors.Errorf("pebble: overlapping range keys must be fragmented: %s, %s",
				prevStartKey.Pretty(w.formatKey),
				curStartKey.Pretty(w.formatKey))
			return w.err
		}
	}

	// TODO(travers): Add an invariant-gated check to ensure that suffix-values
	// are sorted within coalesced spans.

	// Range-keys and point-keys are intended to live in "parallel" keyspaces.
	// However, we track a single seqnum in the table metadata that spans both of
	// these keyspaces.
	// TODO(travers): Consider tracking range key seqnums separately.
	w.meta.updateSeqNum(key.SeqNum())

	// Range tombstones are fragmented, so the start key of the first range key
	// added will be the smallest. The largest range key is determined in
	// Writer.Close() as the end key of the last range key added to the block.
	if w.props.NumRangeKeys() == 0 {
		w.meta.SetSmallestRangeKey(key.Clone())
	}

	// Update table properties.
	w.props.RawRangeKeyKeySize += uint64(key.Size())
	w.props.RawRangeKeyValueSize += uint64(len(value))
	switch key.Kind() {
	case base.InternalKeyKindRangeKeyDelete:
		w.props.NumRangeKeyDels++
	case base.InternalKeyKindRangeKeySet:
		w.props.NumRangeKeySets++
	case base.InternalKeyKindRangeKeyUnset:
		w.props.NumRangeKeyUnsets++
	default:
		panic(errors.Errorf("pebble: invalid range key type: %s", key.Kind()))
	}

	// Add the key to the block.
	return w.rangeKeyBlock.Add(key, value)
}

func (w *RawRowWriter) maybeAddToFilter(key []byte) {
	if w.filter != nil {
		prefix := key[:w.split(key)]
		w.filter.addKey(prefix)
	}
}

// maybeIncrementTombstoneDenseBlocks increments the number of tombstone dense
// blocks if the number of deletions in the data block exceeds a threshold or
// the deletion size exceeds a threshold. It should be called after the
// data block has been finished.
// Invariant: w.dataBlockBuf.uncompressed must already be populated.
func (w *RawRowWriter) maybeIncrementTombstoneDenseBlocks() {
	minSize := w.deletionSizeRatioThreshold * float32(len(w.dataBlockBuf.uncompressed))
	if w.dataBlockBuf.numDeletions > w.numDeletionsThreshold || float32(w.dataBlockBuf.deletionSize) > minSize {
		w.props.NumTombstoneDenseBlocks++
	}
	w.dataBlockBuf.numDeletions = 0
	w.dataBlockBuf.deletionSize = 0
}

func (w *RawRowWriter) flush(key InternalKey) error {
	// We're finishing a data block.
	err := w.finishDataBlockProps(w.dataBlockBuf)
	if err != nil {
		return err
	}
	w.dataBlockBuf.finish()
	w.maybeIncrementTombstoneDenseBlocks()
	w.dataBlockBuf.compressAndChecksum(w.compression)
	// Since dataBlockEstimates.addInflightDataBlock was never called, the
	// inflightSize is set to 0.
	w.coordination.sizeEstimate.dataBlockCompressed(w.dataBlockBuf.physical.LengthWithoutTrailer(), 0)

	// Determine if the index block should be flushed. Since we're accessing the
	// dataBlockBuf.dataBlock.curKey here, we have to make sure that once we start
	// to pool the dataBlockBufs, the curKey isn't used by the Writer once the
	// dataBlockBuf is added back to a sync.Pool. In this particular case, the
	// byte slice which supports "sep" will eventually be copied when "sep" is
	// added to the index block.
	prevKey := w.dataBlockBuf.dataBlock.CurKey()
	sep := w.indexEntrySep(prevKey, key, w.dataBlockBuf)
	// We determine that we should flush an index block from the Writer client
	// goroutine, but we actually finish the index block from the writeQueue.
	// When we determine that an index block should be flushed, we need to call
	// BlockPropertyCollector.FinishIndexBlock. But block property collector
	// calls must happen sequentially from the Writer client. Therefore, we need
	// to determine that we are going to flush the index block from the Writer
	// client.
	shouldFlushIndexBlock := supportsTwoLevelIndex(w.tableFormat) &&
		w.indexBlock.shouldFlush(sep, encodedBHPEstimatedSize, &w.indexFlush)

	var indexProps []byte
	var flushableIndexBlock *indexBlockBuf
	if shouldFlushIndexBlock {
		flushableIndexBlock = w.indexBlock
		w.indexBlock = newIndexBlockBuf()
		// Call BlockPropertyCollector.FinishIndexBlock, since we've decided to
		// flush the index block.
		indexProps, err = w.finishIndexBlockProps()
		if err != nil {
			return err
		}
	}

	// We've called BlockPropertyCollector.FinishDataBlock, and, if necessary,
	// BlockPropertyCollector.FinishIndexBlock. Since we've decided to finish
	// the data block, we can call
	// BlockPropertyCollector.AddPrevDataBlockToIndexBlock.
	w.addPrevDataBlockToIndexBlockProps()

	// Schedule a write.
	writeTask := writeTaskPool.Get().(*writeTask)
	// We're setting compressionDone to indicate that compression of this block
	// has already been completed.
	writeTask.compressionDone <- true
	writeTask.buf = w.dataBlockBuf
	writeTask.indexEntrySep = sep
	writeTask.currIndexBlock = w.indexBlock
	writeTask.indexInflightSize = sep.Size() + encodedBHPEstimatedSize
	writeTask.finishedIndexProps = indexProps
	writeTask.flushableIndexBlock = flushableIndexBlock

	// The writeTask corresponds to an unwritten index entry.
	w.indexBlock.addInflight(writeTask.indexInflightSize)

	w.dataBlockBuf = nil
	err = w.coordination.writeQueue.addSync(writeTask)
	w.dataBlockBuf = newDataBlockBuf(w.restartInterval, w.checksumType)

	return err
}

func (w *RawRowWriter) maybeFlush(key InternalKey, valueLen int) error {
	if !w.dataBlockBuf.shouldFlush(key, valueLen, &w.dataFlush) {
		return nil
	}

	err := w.flush(key)

	if err != nil {
		w.err = err
		return err
	}

	return nil
}

// dataBlockBuf.dataBlockProps set by this method must be encoded before any future use of the
// dataBlockBuf.blockPropsEncoder, since the properties slice will get reused by the
// blockPropsEncoder.
func (w *RawRowWriter) finishDataBlockProps(buf *dataBlockBuf) error {
	if len(w.blockPropCollectors) == 0 {
		return nil
	}
	var err error
	buf.blockPropsEncoder.resetProps()
	for i := range w.blockPropCollectors {
		scratch := buf.blockPropsEncoder.getScratchForProp()
		if scratch, err = w.blockPropCollectors[i].FinishDataBlock(scratch); err != nil {
			return err
		}
		buf.blockPropsEncoder.addProp(shortID(i), scratch)
	}

	buf.dataBlockProps = buf.blockPropsEncoder.unsafeProps()
	return nil
}

// The BlockHandleWithProperties returned by this method must be encoded before any future use of
// the Writer.blockPropsEncoder, since the properties slice will get reused by the blockPropsEncoder.
// maybeAddBlockPropertiesToBlockHandle should only be called if block is being written synchronously
// with the Writer client.
func (w *RawRowWriter) maybeAddBlockPropertiesToBlockHandle(
	bh block.Handle,
) (block.HandleWithProperties, error) {
	err := w.finishDataBlockProps(w.dataBlockBuf)
	if err != nil {
		return block.HandleWithProperties{}, err
	}
	return block.HandleWithProperties{Handle: bh, Props: w.dataBlockBuf.dataBlockProps}, nil
}

func (w *RawRowWriter) indexEntrySep(
	prevKey, key InternalKey, dataBlockBuf *dataBlockBuf,
) InternalKey {
	// Make a rough guess that we want key-sized scratch to compute the separator.
	if cap(dataBlockBuf.sepScratch) < key.Size() {
		dataBlockBuf.sepScratch = make([]byte, 0, key.Size()*2)
	}

	var sep InternalKey
	if key.UserKey == nil && key.Trailer == 0 {
		sep = prevKey.Successor(w.compare, w.successor, dataBlockBuf.sepScratch[:0])
	} else {
		sep = prevKey.Separator(w.compare, w.separator, dataBlockBuf.sepScratch[:0], key)
	}
	if invariants.Enabled && invariants.Sometimes(25) {
		if w.compare(prevKey.UserKey, sep.UserKey) > 0 {
			panic(errors.AssertionFailedf("prevKey.UserKey > sep.UserKey: %s > %s",
				prevKey.UserKey, sep.UserKey))
		}
	}
	return sep
}

// addIndexEntry adds an index entry for the specified key and block handle.
// addIndexEntry can be called from both the Writer client goroutine, and the
// writeQueue goroutine. If the flushIndexBuf != nil, then the indexProps, as
// they're used when the index block is finished.
//
// Invariant:
//  1. addIndexEntry must not store references to the sep InternalKey, the tmp
//     byte slice, bhp.Props. That is, these must be either deep copied or
//     encoded.
//  2. addIndexEntry must not hold references to the flushIndexBuf, and the writeTo
//     indexBlockBufs.
func (w *RawRowWriter) addIndexEntry(
	sep InternalKey,
	bhp block.HandleWithProperties,
	tmp []byte,
	flushIndexBuf *indexBlockBuf,
	writeTo *indexBlockBuf,
	inflightSize int,
	indexProps []byte,
) error {
	if bhp.Length == 0 {
		// A valid blockHandle must be non-zero.
		// In particular, it must have a non-zero length.
		return nil
	}
	if invariants.Enabled {
		if err := w.validateKey.Validate(sep.UserKey); err != nil {
			return err
		}
	}

	encoded := bhp.EncodeVarints(tmp)
	if flushIndexBuf != nil {
		if cap(w.indexPartitions) == 0 {
			w.indexPartitions = make([]bufferedIndexBlock, 0, 32)
		}
		// Enable two level indexes if there is more than one index block.
		w.twoLevelIndex = true
		if err := w.finishIndexBlock(flushIndexBuf, indexProps); err != nil {
			return err
		}
	}
	return writeTo.add(sep, encoded, inflightSize)
}

func (w *RawRowWriter) addPrevDataBlockToIndexBlockProps() {
	for i := range w.blockPropCollectors {
		w.blockPropCollectors[i].AddPrevDataBlockToIndexBlock()
	}
}

// addIndexEntrySync adds an index entry for the specified key and block handle.
// Writer.addIndexEntry is only called synchronously once Writer.Close is called.
// addIndexEntrySync should only be called if we're sure that index entries
// aren't being written asynchronously.
//
// Invariant:
//  1. addIndexEntrySync must not store references to the prevKey, key InternalKey's,
//     the tmp byte slice. That is, these must be either deep copied or encoded.
//
// TODO: Improve coverage of this method. e.g. tests passed without the line
// `w.twoLevelIndex = true` previously.
func (w *RawRowWriter) addIndexEntrySync(
	prevKey, key InternalKey, bhp block.HandleWithProperties, tmp []byte,
) error {
	return w.addIndexEntrySep(w.indexEntrySep(prevKey, key, w.dataBlockBuf), bhp, tmp)
}

func (w *RawRowWriter) addIndexEntrySep(
	sep InternalKey, bhp block.HandleWithProperties, tmp []byte,
) error {
	shouldFlush := supportsTwoLevelIndex(w.tableFormat) &&
		w.indexBlock.shouldFlush(sep, encodedBHPEstimatedSize, &w.indexFlush)
	var flushableIndexBlock *indexBlockBuf
	var props []byte
	var err error
	if shouldFlush {
		flushableIndexBlock = w.indexBlock
		w.indexBlock = newIndexBlockBuf()
		w.twoLevelIndex = true
		// Call BlockPropertyCollector.FinishIndexBlock, since we've decided to
		// flush the index block.
		props, err = w.finishIndexBlockProps()
		if err != nil {
			return err
		}
	}

	err = w.addIndexEntry(sep, bhp, tmp, flushableIndexBlock, w.indexBlock, 0, props)
	if flushableIndexBlock != nil {
		flushableIndexBlock.clear()
		indexBlockBufPool.Put(flushableIndexBlock)
	}
	w.addPrevDataBlockToIndexBlockProps()
	return err
}

func shouldFlush(
	keyLen, valueLen int,
	restartInterval, estimatedBlockSize, numEntries int,
	flushGovernor *block.FlushGovernor,
) bool {
	if numEntries == 0 {
		return false
	}
	if estimatedBlockSize < flushGovernor.LowWatermark() {
		// Fast path when the block is too small to flush.
		return false
	}

	// Estimate the new size. This could be an overestimation because we don't
	// know how much of the key will be shared.
	newSize := estimatedBlockSize + keyLen + valueLen
	if numEntries%restartInterval == 0 {
		newSize += 4
	}
	newSize += 4                            // varint for shared prefix length
	newSize += uvarintLen(uint32(keyLen))   // varint for unshared key bytes
	newSize += uvarintLen(uint32(valueLen)) // varint for value size

	return flushGovernor.ShouldFlush(estimatedBlockSize, newSize)
}

func cloneKeyWithBuf(k InternalKey, a bytealloc.A) (bytealloc.A, InternalKey) {
	if len(k.UserKey) == 0 {
		return a, k
	}
	a, keyCopy := a.Copy(k.UserKey)
	return a, InternalKey{UserKey: keyCopy, Trailer: k.Trailer}
}

// Invariants: The byte slice returned by finishIndexBlockProps is heap-allocated
//
//	and has its own lifetime, independent of the Writer and the blockPropsEncoder,
//
// and it is safe to:
//  1. Reuse w.blockPropsEncoder without first encoding the byte slice returned.
//  2. Store the byte slice in the Writer since it is a copy and not supported by
//     an underlying buffer.
func (w *RawRowWriter) finishIndexBlockProps() ([]byte, error) {
	w.blockPropsEncoder.resetProps()
	for i := range w.blockPropCollectors {
		scratch := w.blockPropsEncoder.getScratchForProp()
		var err error
		if scratch, err = w.blockPropCollectors[i].FinishIndexBlock(scratch); err != nil {
			return nil, err
		}
		w.blockPropsEncoder.addProp(shortID(i), scratch)
	}
	return w.blockPropsEncoder.props(), nil
}

// finishIndexBlock finishes the current index block and adds it to the top
// level index block. This is only used when two level indexes are enabled.
//
// Invariants:
//  1. The props slice passed into finishedIndexBlock must not be a
//     owned by any other struct, since it will be stored in the Writer.indexPartitions
//     slice.
//  2. None of the buffers owned by indexBuf will be shallow copied and stored elsewhere.
//     That is, it must be safe to reuse indexBuf after finishIndexBlock has been called.
func (w *RawRowWriter) finishIndexBlock(indexBuf *indexBlockBuf, props []byte) error {
	part := bufferedIndexBlock{
		nEntries: indexBuf.block.EntryCount(), properties: props,
	}
	w.indexSepAlloc, part.sep = cloneKeyWithBuf(
		indexBuf.block.CurKey(), w.indexSepAlloc,
	)
	bk := indexBuf.finish()
	if len(w.indexBlockAlloc) < len(bk) {
		// Allocate enough bytes for approximately 16 index blocks.
		w.indexBlockAlloc = make([]byte, len(bk)*16)
	}
	n := copy(w.indexBlockAlloc, bk)
	part.block = w.indexBlockAlloc[:n:n]
	w.indexBlockAlloc = w.indexBlockAlloc[n:]
	w.indexPartitions = append(w.indexPartitions, part)
	w.indexPartitionsSizeSum += uint64(len(bk))
	return nil
}

func (w *RawRowWriter) writeTwoLevelIndex() (block.Handle, error) {
	props, err := w.finishIndexBlockProps()
	if err != nil {
		return block.Handle{}, err
	}
	// Add the final unfinished index.
	if err = w.finishIndexBlock(w.indexBlock, props); err != nil {
		return block.Handle{}, err
	}

	for i := range w.indexPartitions {
		b := &w.indexPartitions[i]
		w.props.NumDataBlocks += uint64(b.nEntries)

		data := b.block
		w.props.IndexSize += uint64(len(data))
		bh, err := w.layout.WriteIndexBlock(data)
		if err != nil {
			return block.Handle{}, err
		}
		err = w.topLevelIndexBlock.Add(b.sep, block.HandleWithProperties{
			Handle: bh,
			Props:  b.properties,
		}.EncodeVarints(w.blockBuf.tmp[:]))
		if err != nil {
			return block.Handle{}, err
		}
	}

	// NB: RocksDB includes the block trailer length in the index size
	// property, though it doesn't include the trailer in the top level
	// index size property.
	w.props.IndexPartitions = uint64(len(w.indexPartitions))
	w.props.TopLevelIndexSize = uint64(w.topLevelIndexBlock.EstimatedSize())
	w.props.IndexSize += w.props.TopLevelIndexSize + block.TrailerLen
	return w.layout.WriteIndexBlock(w.topLevelIndexBlock.Finish())
}

// assertFormatCompatibility ensures that the features present on the table are
// compatible with the table format version.
func (w *RawRowWriter) assertFormatCompatibility() error {
	// PebbleDBv1: block properties.
	if len(w.blockPropCollectors) > 0 && w.tableFormat < TableFormatPebblev1 {
		return errors.Newf(
			"table format version %s is less than the minimum required version %s for block properties",
			w.tableFormat, TableFormatPebblev1,
		)
	}

	// PebbleDBv2: range keys.
	if w.props.NumRangeKeys() > 0 && w.tableFormat < TableFormatPebblev2 {
		return errors.Newf(
			"table format version %s is less than the minimum required version %s for range keys",
			w.tableFormat, TableFormatPebblev2,
		)
	}

	// PebbleDBv3: value blocks.
	if (w.props.NumValueBlocks > 0 || w.props.NumValuesInValueBlocks > 0 ||
		w.props.ValueBlocksSize > 0) && w.tableFormat < TableFormatPebblev3 {
		return errors.Newf(
			"table format version %s is less than the minimum required version %s for value blocks",
			w.tableFormat, TableFormatPebblev3)
	}

	// PebbleDBv4: DELSIZED tombstones.
	if w.props.NumSizedDeletions > 0 && w.tableFormat < TableFormatPebblev4 {
		return errors.Newf(
			"table format version %s is less than the minimum required version %s for sized deletion tombstones",
			w.tableFormat, TableFormatPebblev4)
	}
	return nil
}

// ComparePrev compares the provided user to the last point key written to the
// writer. The returned value is equivalent to Compare(key, prevKey) where
// prevKey is the last point key written to the writer.
//
// If no key has been written yet, ComparePrev returns +1.
//
// Must not be called after Writer is closed.
func (w *RawRowWriter) ComparePrev(k []byte) int {
	if w == nil || w.dataBlockBuf.dataBlock.EntryCount() == 0 {
		return +1
	}
	return w.compare(k, w.dataBlockBuf.dataBlock.CurUserKey())
}

// EncodeSpan encodes the keys in the given span. The span can contain either
// only RANGEDEL keys or only range keys.
//
// This is a low-level API that bypasses the fragmenter. The spans passed to
// this function must be fragmented and ordered.
func (w *RawRowWriter) EncodeSpan(span keyspan.Span) error {
	if span.Empty() {
		return nil
	}
	if span.Keys[0].Kind() == base.InternalKeyKindRangeDelete {
		return rangedel.Encode(span, w.addTombstone)
	}
	for i := range w.blockPropCollectors {
		if err := w.blockPropCollectors[i].AddRangeKeys(span); err != nil {
			return err
		}
	}
	return w.rangeKeyEncoder.Encode(span)
}

// Error returns the current accumulated error, if any.
func (w *RawRowWriter) Error() error {
	return w.err
}

// Close finishes writing the table and closes the underlying file that the
// table was written to.
func (w *RawRowWriter) Close() (err error) {
	defer func() {
		if w.valueBlockWriter != nil {
			w.valueBlockWriter.Release()
			// Defensive code in case Close gets called again. We don't want to put
			// the same object to a sync.Pool.
			w.valueBlockWriter = nil
		}
		w.layout.Abort()
		// Record any error in the writer (so we can exit early if Close is called
		// again).
		if err != nil {
			w.err = err
		}
	}()

	// finish must be called before we check for an error, because finish will
	// block until every single task added to the writeQueue has been processed,
	// and an error could be encountered while any of those tasks are processed.
	if err := w.coordination.writeQueue.finish(); err != nil {
		return err
	}
	if w.err != nil {
		return w.err
	}

	// The w.meta.LargestPointKey is only used once the Writer is closed, so it is safe to set it
	// when the Writer is closed.
	//
	// The following invariants ensure that setting the largest key at this point of a Writer close
	// is correct:
	// 1. Keys must only be added to the Writer in an increasing order.
	// 2. The current w.dataBlockBuf is guaranteed to have the latest key added to the Writer. This
	//    must be true, because a w.dataBlockBuf is only switched out when a dataBlock is flushed,
	//    however, if a dataBlock is flushed, then we add a key to the new w.dataBlockBuf in the
	//    addPoint function after the flush occurs.
	if w.dataBlockBuf.dataBlock.EntryCount() >= 1 {
		w.meta.SetLargestPointKey(w.dataBlockBuf.dataBlock.CurKey().Clone())
	}

	// Finish the last data block, or force an empty data block if there
	// aren't any data blocks at all.
	if w.dataBlockBuf.dataBlock.EntryCount() > 0 || w.indexBlock.block.EntryCount() == 0 {
		w.dataBlockBuf.finish()
		w.maybeIncrementTombstoneDenseBlocks()
		bh, err := w.layout.WriteDataBlock(w.dataBlockBuf.uncompressed, &w.dataBlockBuf.blockBuf)
		if err != nil {
			return err
		}
		bhp, err := w.maybeAddBlockPropertiesToBlockHandle(bh)
		if err != nil {
			return err
		}
		prevKey := w.dataBlockBuf.dataBlock.CurKey()
		// NB: prevKey.UserKey may be nil if there are no keys and we're forcing
		// the creation of an empty data block.
		if err := w.addIndexEntrySync(prevKey, InternalKey{}, bhp, w.dataBlockBuf.tmp[:]); err != nil {
			return err
		}
	}
	w.props.DataSize = w.layout.offset

	// Write the filter block.
	if w.filter != nil {
		bh, err := w.layout.WriteFilterBlock(w.filter)
		if err != nil {
			return err
		}
		w.props.FilterPolicyName = w.filter.policyName()
		w.props.FilterSize = bh.Length
	}

	if w.twoLevelIndex {
		w.props.IndexType = twoLevelIndex
		// Write the two level index block.
		if _, err = w.writeTwoLevelIndex(); err != nil {
			return err
		}
	} else {
		w.props.IndexType = binarySearchIndex
		// NB: RocksDB includes the block trailer length in the index size
		// property, though it doesn't include the trailer in the filter size
		// property.
		w.props.IndexSize = uint64(w.indexBlock.estimatedSize()) + block.TrailerLen
		w.props.NumDataBlocks = uint64(w.indexBlock.block.EntryCount())
		// Write the single level index block.
		if _, err = w.layout.WriteIndexBlock(w.indexBlock.finish()); err != nil {
			return err
		}
	}

	// Write the range-del block.
	if w.props.NumRangeDeletions > 0 {
		// Because the range tombstones are fragmented, the end key of the last
		// added range tombstone will be the largest range tombstone key. Note
		// that we need to make this into a range deletion sentinel because
		// sstable boundaries are inclusive while the end key of a range
		// deletion tombstone is exclusive. A Clone() is necessary as
		// rangeDelBlock.curValue is the same slice that will get passed into
		// w.writer, and some implementations of vfs.File mutate the slice
		// passed into Write(). Also, w.meta will often outlive the blockWriter,
		// and so cloning curValue allows the rangeDelBlock's internal buffer to
		// get gc'd.
		k := base.MakeRangeDeleteSentinelKey(w.rangeDelBlock.CurValue()).Clone()
		w.meta.SetLargestRangeDelKey(k)
		if _, err := w.layout.WriteRangeDeletionBlock(w.rangeDelBlock.Finish()); err != nil {
			return err
		}
	}

	if w.props.NumRangeKeys() > 0 {
		key := w.rangeKeyBlock.CurKey()
		kind := key.Kind()
		endKey, _, err := rangekey.DecodeEndKey(kind, w.rangeKeyBlock.CurValue())
		if err != nil {
			return err
		}
		k := base.MakeExclusiveSentinelKey(kind, endKey).Clone()
		w.meta.SetLargestRangeKey(k)
		if _, err := w.layout.WriteRangeKeyBlock(w.rangeKeyBlock.Finish()); err != nil {
			return err
		}
	}

	if w.valueBlockWriter != nil {
		_, vbStats, err := w.valueBlockWriter.Finish(&w.layout, w.layout.offset)
		if err != nil {
			return err
		}
		w.props.NumValueBlocks = vbStats.NumValueBlocks
		w.props.NumValuesInValueBlocks = vbStats.NumValuesInValueBlocks
		w.props.ValueBlocksSize = vbStats.ValueBlocksAndIndexSize
	}

	{
		// Finish and record the prop collectors if props are not yet recorded.
		// Pre-computed props might have been copied by specialized sst creators
		// like suffix replacer.
		if len(w.props.UserProperties) == 0 {
			userProps := make(map[string]string)
			for i := range w.blockPropCollectors {
				scratch := w.blockPropsEncoder.getScratchForProp()
				// Place the shortID in the first byte.
				scratch = append(scratch, byte(i))
				buf, err := w.blockPropCollectors[i].FinishTable(scratch)
				if err != nil {
					return err
				}
				var prop string
				if len(buf) > 0 {
					prop = string(buf)
				}
				// NB: The property is populated in the map even if it is the
				// empty string, since the presence in the map is what indicates
				// that the block property collector was used when writing.
				userProps[w.blockPropCollectors[i].Name()] = prop
			}
			if len(userProps) > 0 {
				w.props.UserProperties = userProps
			}
		}

		// Write the properties block.
		var raw rowblk.Writer
		// The restart interval is set to infinity because the properties block
		// is always read sequentially and cached in a heap located object. This
		// reduces table size without a significant impact on performance.
		raw.RestartInterval = propertiesBlockRestartInterval
		w.props.CompressionOptions = rocksDBCompressionOptions
		if err := w.props.saveToRowWriter(w.tableFormat, &raw); err != nil {
			return err
		}
		if _, err := w.layout.WritePropertiesBlock(raw.Finish()); err != nil {
			return err
		}
	}

	// Write the table footer.
	w.meta.Size, err = w.layout.Finish()
	if err != nil {
		return err
	}
	w.meta.Properties = w.props

	// Check that the features present in the table are compatible with the format
	// configured for the table.
	if err = w.assertFormatCompatibility(); err != nil {
		return err
	}

	w.dataBlockBuf.clear()
	dataBlockBufPool.Put(w.dataBlockBuf)
	w.dataBlockBuf = nil
	w.indexBlock.clear()
	indexBlockBufPool.Put(w.indexBlock)
	w.indexBlock = nil

	// Make any future calls to Set or Close return an error.
	w.err = errWriterClosed
	return nil
}

// EstimatedSize returns the estimated size of the sstable being written if a
// call to Finish() was made without adding additional keys.
func (w *RawRowWriter) EstimatedSize() uint64 {
	if w == nil {
		return 0
	}

	// Begin with the estimated size of the data blocks that have already been
	// flushed to the file.
	size := w.coordination.sizeEstimate.size()
	// Add the size of value blocks. If any value blocks have already been
	// finished, these blocks will contribute post-compression size. If there is
	// currently an unfinished value block, it will contribute its pre-compression
	// size.
	if w.valueBlockWriter != nil {
		size += w.valueBlockWriter.Size()
	}

	// Add the size of the completed but unflushed index partitions, the
	// unfinished data block, the unfinished index block, the unfinished range
	// deletion block, and the unfinished range key block.
	//
	// All of these sizes are uncompressed sizes. It's okay to be pessimistic
	// here and use the uncompressed size because all this memory is buffered
	// until the sstable is finished.  Including the uncompressed size bounds
	// the memory usage used by the writer to the physical size limit.
	size += w.indexPartitionsSizeSum
	size += uint64(w.dataBlockBuf.dataBlock.EstimatedSize())
	size += w.indexBlock.estimatedSize()
	size += uint64(w.rangeDelBlock.EstimatedSize())
	size += uint64(w.rangeKeyBlock.EstimatedSize())
	// TODO(jackson): Account for the size of the filter block.
	return size
}

// Metadata returns the metadata for the finished sstable. Only valid to call
// after the sstable has been finished.
func (w *RawRowWriter) Metadata() (*WriterMetadata, error) {
	if !w.layout.IsFinished() {
		return nil, errors.New("pebble: writer is not closed")
	}
	return &w.meta, nil
}

func newRowWriter(writable objstorage.Writable, o WriterOptions) *RawRowWriter {
	if o.TableFormat.BlockColumnar() {
		panic(errors.AssertionFailedf("newRowWriter cannot create sstables with %s format", o.TableFormat))
	}
	o = o.ensureDefaults()
	w := &RawRowWriter{
		layout: makeLayoutWriter(writable, o),
		meta: WriterMetadata{
			SmallestSeqNum: math.MaxUint64,
		},
		compare:                    o.Comparer.Compare,
		pointSuffixCmp:             o.Comparer.ComparePointSuffixes,
		split:                      o.Comparer.Split,
		formatKey:                  o.Comparer.FormatKey,
		compression:                o.Compression,
		separator:                  o.Comparer.Separator,
		successor:                  o.Comparer.Successor,
		validateKey:                o.Comparer.ValidateKey,
		tableFormat:                o.TableFormat,
		isStrictObsolete:           o.IsStrictObsolete,
		writingToLowestLevel:       o.WritingToLowestLevel,
		restartInterval:            o.BlockRestartInterval,
		checksumType:               o.Checksum,
		disableKeyOrderChecks:      o.internal.DisableKeyOrderChecks,
		indexBlock:                 newIndexBlockBuf(),
		rangeDelBlock:              rowblk.Writer{RestartInterval: 1},
		rangeKeyBlock:              rowblk.Writer{RestartInterval: 1},
		topLevelIndexBlock:         rowblk.Writer{RestartInterval: 1},
		allocatorSizeClasses:       o.AllocatorSizeClasses,
		numDeletionsThreshold:      o.NumDeletionsThreshold,
		deletionSizeRatioThreshold: o.DeletionSizeRatioThreshold,
	}
	w.dataFlush = block.MakeFlushGovernor(o.BlockSize, o.BlockSizeThreshold, o.SizeClassAwareThreshold, o.AllocatorSizeClasses)
	w.indexFlush = block.MakeFlushGovernor(o.IndexBlockSize, o.BlockSizeThreshold, o.SizeClassAwareThreshold, o.AllocatorSizeClasses)
	if w.tableFormat >= TableFormatPebblev3 {
		w.shortAttributeExtractor = o.ShortAttributeExtractor
		w.requiredInPlaceValueBound = o.RequiredInPlaceValueBound
		if !o.DisableValueBlocks {
			w.valueBlockWriter = valblk.NewWriter(
				block.MakeFlushGovernor(o.BlockSize, o.BlockSizeThreshold, o.SizeClassAwareThreshold, o.AllocatorSizeClasses),
				w.compression, w.checksumType, func(compressedSize int) {
					w.coordination.sizeEstimate.dataBlockCompressed(compressedSize, 0)
				},
			)
		}
	}

	w.dataBlockBuf = newDataBlockBuf(w.restartInterval, w.checksumType)

	w.blockBuf = blockBuf{
		checksummer: block.Checksummer{Type: o.Checksum},
	}

	w.coordination.init(w)
	defer func() {
		if r := recover(); r != nil {
			// Don't leak a goroutine if we hit a panic.
			_ = w.coordination.writeQueue.finish()
			panic(r)
		}
	}()

	if writable == nil {
		w.err = errors.New("pebble: nil writable")
		return w
	}

	if o.FilterPolicy != nil {
		switch o.FilterType {
		case TableFilter:
			w.filter = newTableFilterWriter(o.FilterPolicy)
		default:
			panic(fmt.Sprintf("unknown filter type: %v", o.FilterType))
		}
	}

	w.props.ComparerName = o.Comparer.Name
	w.props.CompressionName = o.Compression.String()
	w.props.MergerName = o.MergerName
	w.props.PropertyCollectorNames = "[]"

	numBlockPropertyCollectors := len(o.BlockPropertyCollectors)
	shouldAddObsoleteCollector := w.tableFormat >= TableFormatPebblev4 && !o.disableObsoleteCollector
	if shouldAddObsoleteCollector {
		numBlockPropertyCollectors++
	}

	if numBlockPropertyCollectors > 0 {
		if numBlockPropertyCollectors > maxPropertyCollectors {
			w.err = errors.New("pebble: too many block property collectors")
			return w
		}
		w.blockPropCollectors = make([]BlockPropertyCollector, 0, numBlockPropertyCollectors)
		for _, constructFn := range o.BlockPropertyCollectors {
			w.blockPropCollectors = append(w.blockPropCollectors, constructFn())
		}
		if shouldAddObsoleteCollector {
			w.blockPropCollectors = append(w.blockPropCollectors, &w.obsoleteCollector)
		}

		var buf bytes.Buffer
		buf.WriteString("[")
		for i := range w.blockPropCollectors {
			if i > 0 {
				buf.WriteString(",")
			}
			buf.WriteString(w.blockPropCollectors[i].Name())
		}
		buf.WriteString("]")
		w.props.PropertyCollectorNames = buf.String()
	}

	// Initialize the range key fragmenter and encoder.
	w.rangeKeyEncoder.Emit = w.addRangeKey
	return w
}

// rewriteSuffixes implements RawWriter.
func (w *RawRowWriter) rewriteSuffixes(
	r *Reader, sst []byte, wo WriterOptions, from, to []byte, concurrency int,
) error {
	for _, c := range w.blockPropCollectors {
		if !c.SupportsSuffixReplacement() {
			return errors.Errorf("block property collector %s does not support suffix replacement", c.Name())
		}
	}
	l, err := r.Layout()
	if err != nil {
		return errors.Wrap(err, "reading layout")
	}

	// Copy data blocks in parallel, rewriting suffixes as we go.
	blocks, err := rewriteDataBlocksInParallel(r, sst, wo, l.Data, from, to, concurrency, func() blockRewriter {
		return rowblk.NewRewriter(r.Comparer, wo.BlockRestartInterval)
	})
	if err != nil {
		return errors.Wrap(err, "rewriting data blocks")
	}

	// oldShortIDs maps the shortID for the block property collector in the old
	// blocks to the shortID in the new blocks. Initialized once for the sstable.
	oldShortIDs, n, err := getShortIDs(r, w.blockPropCollectors)
	if err != nil {
		return errors.Wrap(err, "getting short IDs")
	}
	oldProps := make([][]byte, len(w.blockPropCollectors))

	for i := range blocks {
		// Write the rewritten block to the file.
		bh, err := w.layout.WritePrecompressedDataBlock(blocks[i].physical)
		if err != nil {
			return err
		}

		// Load any previous values for our prop collectors into oldProps.
		for i := range oldProps {
			oldProps[i] = nil
		}
		decoder := makeBlockPropertiesDecoder(n, l.Data[i].Props)
		for !decoder.Done() {
			id, val, err := decoder.Next()
			if err != nil {
				return err
			}
			if oldShortIDs[id].IsValid() {
				oldProps[oldShortIDs[id]] = val
			}
		}
		for i, p := range w.blockPropCollectors {
			if err := p.AddCollectedWithSuffixReplacement(oldProps[i], from, to); err != nil {
				return err
			}
		}

		bhp, err := w.maybeAddBlockPropertiesToBlockHandle(bh)
		if err != nil {
			return err
		}
		var nextKey InternalKey
		if i+1 < len(blocks) {
			nextKey = blocks[i+1].start
		}
		if err = w.addIndexEntrySync(blocks[i].end, nextKey, bhp, w.dataBlockBuf.tmp[:]); err != nil {
			return err
		}
	}
	if len(blocks) > 0 {
		w.meta.Size = w.layout.offset
		w.meta.updateSeqNum(blocks[0].start.SeqNum())
		w.props.NumEntries = r.Properties.NumEntries
		w.props.RawKeySize = r.Properties.RawKeySize
		w.props.RawValueSize = r.Properties.RawValueSize
		w.meta.SetSmallestPointKey(blocks[0].start)
		w.meta.SetLargestPointKey(blocks[len(blocks)-1].end)
	}

	// Copy range key block, replacing suffixes if it exists.
	if err := rewriteRangeKeyBlockToWriter(r, w, from, to); err != nil {
		return errors.Wrap(err, "rewriting range key blocks")
	}
	// Copy over the filter block if it exists (rewriteDataBlocksToWriter will
	// already have ensured this is valid if it exists).
	if w.filter != nil {
		if filterBlockBH, ok := l.FilterByName(w.filter.metaName()); ok {
			filterBlock, _, err := readBlockBuf(sst, filterBlockBH, r.blockReader.ChecksumType(), nil)
			if err != nil {
				return errors.Wrap(err, "reading filter")
			}
			w.filter = copyFilterWriter{
				origPolicyName: w.filter.policyName(),
				origMetaName:   w.filter.metaName(),
				// Clone the filter block, because readBlockBuf allows the
				// returned byte slice to point directly into sst.
				data: slices.Clone(filterBlock),
			}
		}
	}
	return nil
}

// copyDataBlocks implements RawWriter.
func (w *RawRowWriter) copyDataBlocks(
	ctx context.Context, blocks []indexEntry, rh objstorage.ReadHandle,
) error {
	blockOffset := blocks[0].bh.Offset
	// The block lengths don't include their trailers, which just sit after the
	// block length, before the next offset; We get the ones between the blocks
	// we copy implicitly but need to explicitly add the last trailer to length.
	length := blocks[len(blocks)-1].bh.Offset + blocks[len(blocks)-1].bh.Length + block.TrailerLen - blockOffset
	if spanEnd := length + blockOffset; spanEnd < blockOffset {
		return base.AssertionFailedf("invalid intersecting span for CopySpan [%d, %d)", blockOffset, spanEnd)
	}
	if err := objstorage.Copy(ctx, rh, w.layout.writable, blockOffset, length); err != nil {
		return err
	}
	// Update w.meta.Size so subsequently flushed metadata has correct offsets.
	w.meta.Size += length
	for i := range blocks {
		blocks[i].bh.Offset = w.layout.offset
		// blocks[i].bh.Length remains unmodified.
		sepKey := base.MakeInternalKey(blocks[i].sep, base.SeqNumMax, base.InternalKeyKindSeparator)
		if err := w.addIndexEntrySep(sepKey, blocks[i].bh, w.dataBlockBuf.tmp[:]); err != nil {
			return err
		}
		w.layout.offset += uint64(blocks[i].bh.Length) + block.TrailerLen
	}
	return nil
}

// addDataBlock implements RawWriter.
func (w *RawRowWriter) addDataBlock(b, sep []byte, bhp block.HandleWithProperties) error {
	blockBuf := &w.dataBlockBuf.blockBuf
	pb := block.CompressAndChecksum(
		&blockBuf.dataBuf,
		b,
		w.layout.compression,
		&blockBuf.checksummer,
	)

	// layout.WriteDataBlock keeps layout.offset up-to-date for us.
	// Note that this can mangle the pb data.
	bh, err := w.layout.writePrecompressedBlock(pb)
	if err != nil {
		return err
	}
	bhp.Handle = bh

	sepKey := base.MakeInternalKey(sep, base.SeqNumMax, base.InternalKeyKindSeparator)
	if err := w.addIndexEntrySep(sepKey, bhp, w.dataBlockBuf.tmp[:]); err != nil {
		return err
	}
	w.meta.Size += uint64(bh.Length) + block.TrailerLen
	return nil
}

// copyProperties implements RawWriter.
func (w *RawRowWriter) copyProperties(props Properties) {
	w.props = props
	// Remove all user properties to disable block properties, which we do not
	// calculate.
	w.props.UserProperties = nil
	// Reset props that we'll re-derive as we build our own index.
	w.props.IndexPartitions = 0
	w.props.TopLevelIndexSize = 0
	w.props.IndexSize = 0
	w.props.IndexType = 0
}

// copyFilter implements RawWriter.
func (w *RawRowWriter) copyFilter(filter []byte, filterName string) error {
	if w.filter != nil && filterName != w.filter.policyName() {
		return errors.New("mismatched filters")
	}
	w.filter = copyFilterWriter{
		origPolicyName: w.filter.policyName(), origMetaName: w.filter.metaName(), data: filter,
	}
	return nil
}

// SetSnapshotPinnedProperties sets the properties for pinned keys. Should only
// be used internally by Pebble.
func (w *RawRowWriter) SetSnapshotPinnedProperties(
	pinnedKeyCount, pinnedKeySize, pinnedValueSize uint64,
) {
	w.props.SnapshotPinnedKeys = pinnedKeyCount
	w.props.SnapshotPinnedKeySize = pinnedKeySize
	w.props.SnapshotPinnedValueSize = pinnedValueSize
}
