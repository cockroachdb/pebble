// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/fifo"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/bytealloc"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/crc"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/sstableinternal"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider/objiotracing"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/rowblk"
)

var errReaderClosed = errors.New("pebble/table: reader is closed")

// decodeBlockHandle returns the block handle encoded at the start of src, as
// well as the number of bytes it occupies. It returns zero if given invalid
// input. A block handle for a data block or a first/lower level index block
// should not be decoded using decodeBlockHandle since the caller may validate
// that the number of bytes decoded is equal to the length of src, which will
// be false if the properties are not decoded. In those cases the caller
// should use decodeBlockHandleWithProperties.
func decodeBlockHandle(src []byte) (block.Handle, int) {
	offset, n := binary.Uvarint(src)
	length, m := binary.Uvarint(src[n:])
	if n == 0 || m == 0 {
		return block.Handle{}, 0
	}
	return block.Handle{Offset: offset, Length: length}, n + m
}

// decodeBlockHandleWithProperties returns the block handle and properties
// encoded in src. src needs to be exactly the length that was encoded. This
// method must be used for data block and first/lower level index blocks. The
// properties in the block handle point to the bytes in src.
func decodeBlockHandleWithProperties(src []byte) (BlockHandleWithProperties, error) {
	bh, n := decodeBlockHandle(src)
	if n == 0 {
		return BlockHandleWithProperties{}, errors.Errorf("invalid BlockHandle")
	}
	return BlockHandleWithProperties{
		Handle: bh,
		Props:  src[n:],
	}, nil
}

func encodeBlockHandle(dst []byte, b block.Handle) int {
	n := binary.PutUvarint(dst, b.Offset)
	m := binary.PutUvarint(dst[n:], b.Length)
	return n + m
}

func encodeBlockHandleWithProperties(dst []byte, b BlockHandleWithProperties) []byte {
	n := encodeBlockHandle(dst, b.Handle)
	dst = append(dst[:n], b.Props...)
	return dst
}

type loadBlockResult int8

const (
	loadBlockOK loadBlockResult = iota
	// Could be due to error or because no block left to load.
	loadBlockFailed
	loadBlockIrrelevant
)

type blockTransform func([]byte) ([]byte, error)

// Reader is a table reader.
type Reader struct {
	readable objstorage.Readable

	// The following fields are copied from the ReadOptions.
	cacheOpts            sstableinternal.CacheOptions
	loadBlockSema        *fifo.Semaphore
	deniedUserProperties map[string]struct{}
	filterMetricsTracker *FilterMetricsTracker
	logger               base.LoggerAndTracer

	Comparer  *base.Comparer
	Compare   Compare
	SuffixCmp CompareSuffixes
	Equal     Equal
	Split     Split

	tableFilter *tableFilterReader

	err error

	indexBH      block.Handle
	filterBH     block.Handle
	rangeDelBH   block.Handle
	rangeKeyBH   block.Handle
	valueBIH     valueBlocksIndexHandle
	propertiesBH block.Handle
	metaIndexBH  block.Handle
	footerBH     block.Handle

	Properties   Properties
	tableFormat  TableFormat
	checksumType block.ChecksumType

	// metaBufferPool is a buffer pool used exclusively when opening a table and
	// loading its meta blocks. metaBufferPoolAlloc is used to batch-allocate
	// the BufferPool.pool slice as a part of the Reader allocation. It's
	// capacity 3 to accommodate the meta block (1), and both the compressed
	// properties block (1) and decompressed properties block (1)
	// simultaneously.
	metaBufferPool      block.BufferPool
	metaBufferPoolAlloc [3]block.AllocedBuffer
}

var _ CommonReader = (*Reader)(nil)

// Close the reader and the underlying objstorage.Readable.
func (r *Reader) Close() error {
	r.cacheOpts.Cache.Unref()

	if r.readable != nil {
		r.err = firstError(r.err, r.readable.Close())
		r.readable = nil
	}

	if r.err != nil {
		return r.err
	}
	// Make any future calls to Get, NewIter or Close return an error.
	r.err = errReaderClosed
	return nil
}

// NewPointIter returns an iterator for the point keys in the table.
//
// If transform.HideObsoletePoints is set, the callee assumes that filterer
// already includes obsoleteKeyBlockPropertyFilter. The caller can satisfy this
// contract by first calling TryAddBlockPropertyFilterForHideObsoletePoints.
func (r *Reader) NewPointIter(
	ctx context.Context,
	transforms IterTransforms,
	lower, upper []byte,
	filterer *BlockPropertiesFilterer,
	filterBlockSizeLimit FilterBlockSizeLimit,
	stats *base.InternalIteratorStats,
	categoryAndQoS CategoryAndQoS,
	statsCollector *CategoryStatsCollector,
	rp ReaderProvider,
) (Iterator, error) {
	return r.newPointIter(
		ctx, transforms, lower, upper, filterer, filterBlockSizeLimit,
		stats, categoryAndQoS, statsCollector, rp, nil)
}

// TryAddBlockPropertyFilterForHideObsoletePoints is expected to be called
// before the call to NewPointIter, to get the value of hideObsoletePoints and
// potentially add a block property filter.
func (r *Reader) TryAddBlockPropertyFilterForHideObsoletePoints(
	snapshotForHideObsoletePoints base.SeqNum,
	fileLargestSeqNum base.SeqNum,
	pointKeyFilters []BlockPropertyFilter,
) (hideObsoletePoints bool, filters []BlockPropertyFilter) {
	hideObsoletePoints = r.tableFormat >= TableFormatPebblev4 &&
		snapshotForHideObsoletePoints > fileLargestSeqNum
	if hideObsoletePoints {
		pointKeyFilters = append(pointKeyFilters, obsoleteKeyBlockPropertyFilter{})
	}
	return hideObsoletePoints, pointKeyFilters
}

func (r *Reader) newPointIter(
	ctx context.Context,
	transforms IterTransforms,
	lower, upper []byte,
	filterer *BlockPropertiesFilterer,
	filterBlockSizeLimit FilterBlockSizeLimit,
	stats *base.InternalIteratorStats,
	categoryAndQoS CategoryAndQoS,
	statsCollector *CategoryStatsCollector,
	rp ReaderProvider,
	vState *virtualState,
) (Iterator, error) {
	// NB: pebble.tableCache wraps the returned iterator with one which performs
	// reference counting on the Reader, preventing the Reader from being closed
	// until the final iterator closes.
	var res Iterator
	var err error
	if r.Properties.IndexType == twoLevelIndex {
		res, err = newTwoLevelIterator(ctx, r, vState, transforms, lower, upper, filterer, filterBlockSizeLimit,
			stats, categoryAndQoS, statsCollector, rp, nil /* bufferPool */)
	} else {
		res, err = newSingleLevelIterator(
			ctx, r, vState, transforms, lower, upper, filterer, filterBlockSizeLimit,
			stats, categoryAndQoS, statsCollector, rp, nil /* bufferPool */)
	}
	if err != nil {
		// Note: we don't want to return res here - it will be a nil
		// single/twoLevelIterator, not a nil Iterator.
		return nil, err
	}
	return res, nil
}

// NewIter returns an iterator for the point keys in the table. It is a
// simplified version of NewPointIter and should only be used for tests and
// tooling.
//
// NewIter must only be used when the Reader is guaranteed to outlive any
// LazyValues returned from the iter.
func (r *Reader) NewIter(transforms IterTransforms, lower, upper []byte) (Iterator, error) {
	// TODO(radu): we should probably not use bloom filters in this case, as there
	// likely isn't a cache set up.
	return r.NewPointIter(
		context.TODO(), transforms, lower, upper, nil, AlwaysUseFilterBlock,
		nil /* stats */, CategoryAndQoS{}, nil /* statsCollector */, MakeTrivialReaderProvider(r))
}

// NewCompactionIter returns an iterator similar to NewIter but it also increments
// the number of bytes iterated. If an error occurs, NewCompactionIter cleans up
// after itself and returns a nil iterator.
func (r *Reader) NewCompactionIter(
	transforms IterTransforms,
	categoryAndQoS CategoryAndQoS,
	statsCollector *CategoryStatsCollector,
	rp ReaderProvider,
	bufferPool *block.BufferPool,
) (Iterator, error) {
	return r.newCompactionIter(transforms, categoryAndQoS, statsCollector, rp, nil, bufferPool)
}

func (r *Reader) newCompactionIter(
	transforms IterTransforms,
	categoryAndQoS CategoryAndQoS,
	statsCollector *CategoryStatsCollector,
	rp ReaderProvider,
	vState *virtualState,
	bufferPool *block.BufferPool,
) (Iterator, error) {
	if vState != nil && vState.isSharedIngested {
		transforms.HideObsoletePoints = true
	}
	if r.Properties.IndexType == twoLevelIndex {
		i, err := newTwoLevelIterator(
			context.Background(),
			r, vState, transforms, nil /* lower */, nil /* upper */, nil,
			NeverUseFilterBlock, nil /* stats */, categoryAndQoS, statsCollector, rp, bufferPool,
		)
		if err != nil {
			return nil, err
		}
		i.SetupForCompaction()
		return i, nil
	}
	i, err := newSingleLevelIterator(
		context.Background(), r, vState, transforms, nil /* lower */, nil, /* upper */
		nil, NeverUseFilterBlock, nil /* stats */, categoryAndQoS, statsCollector, rp, bufferPool,
	)
	if err != nil {
		return nil, err
	}
	i.SetupForCompaction()
	return i, nil
}

// NewRawRangeDelIter returns an internal iterator for the contents of the
// range-del block for the table. Returns nil if the table does not contain
// any range deletions.
func (r *Reader) NewRawRangeDelIter(
	ctx context.Context, transforms FragmentIterTransforms,
) (keyspan.FragmentIterator, error) {
	if r.rangeDelBH.Length == 0 {
		return nil, nil
	}
	h, err := r.readRangeDel(ctx, nil /* stats */, nil /* iterStats */)
	if err != nil {
		return nil, err
	}
	transforms.ElideSameSeqNum = true
	i, err := rowblk.NewFragmentIter(r.cacheOpts.FileNum, r.Compare, r.Comparer.CompareSuffixes, r.Split, h, transforms)
	if err != nil {
		return nil, err
	}
	return keyspan.MaybeAssert(i, r.Compare), nil
}

// NewRawRangeKeyIter returns an internal iterator for the contents of the
// range-key block for the table. Returns nil if the table does not contain any
// range keys.
func (r *Reader) NewRawRangeKeyIter(
	ctx context.Context, transforms FragmentIterTransforms,
) (keyspan.FragmentIterator, error) {
	if r.rangeKeyBH.Length == 0 {
		return nil, nil
	}
	h, err := r.readRangeKey(ctx, nil /* stats */, nil /* iterStats */)
	if err != nil {
		return nil, err
	}
	i, err := rowblk.NewFragmentIter(r.cacheOpts.FileNum, r.Compare, r.Comparer.CompareSuffixes, r.Split, h, transforms)
	if err != nil {
		return nil, err
	}
	return keyspan.MaybeAssert(i, r.Compare), nil
}

func (r *Reader) readIndex(
	ctx context.Context,
	readHandle objstorage.ReadHandle,
	stats *base.InternalIteratorStats,
	iterStats *iterStatsAccumulator,
) (block.BufferHandle, error) {
	ctx = objiotracing.WithBlockType(ctx, objiotracing.MetadataBlock)
	return r.readBlock(ctx, r.indexBH, nil, readHandle, stats, iterStats, nil /* buffer pool */)
}

func (r *Reader) readFilter(
	ctx context.Context,
	readHandle objstorage.ReadHandle,
	stats *base.InternalIteratorStats,
	iterStats *iterStatsAccumulator,
) (block.BufferHandle, error) {
	ctx = objiotracing.WithBlockType(ctx, objiotracing.FilterBlock)
	return r.readBlock(ctx, r.filterBH, nil /* transform */, readHandle, stats, iterStats, nil /* buffer pool */)
}

func (r *Reader) readRangeDel(
	ctx context.Context, stats *base.InternalIteratorStats, iterStats *iterStatsAccumulator,
) (block.BufferHandle, error) {
	ctx = objiotracing.WithBlockType(ctx, objiotracing.MetadataBlock)
	return r.readBlock(ctx, r.rangeDelBH, nil /* transform */, nil /* readHandle */, stats, iterStats, nil /* buffer pool */)
}

func (r *Reader) readRangeKey(
	ctx context.Context, stats *base.InternalIteratorStats, iterStats *iterStatsAccumulator,
) (block.BufferHandle, error) {
	ctx = objiotracing.WithBlockType(ctx, objiotracing.MetadataBlock)
	return r.readBlock(ctx, r.rangeKeyBH, nil /* transform */, nil /* readHandle */, stats, iterStats, nil /* buffer pool */)
}

func checkChecksum(
	checksumType block.ChecksumType, b []byte, bh block.Handle, fileNum base.DiskFileNum,
) error {
	expectedChecksum := binary.LittleEndian.Uint32(b[bh.Length+1:])
	var computedChecksum uint32
	switch checksumType {
	case block.ChecksumTypeCRC32c:
		computedChecksum = crc.New(b[:bh.Length+1]).Value()
	case block.ChecksumTypeXXHash64:
		computedChecksum = uint32(xxhash.Sum64(b[:bh.Length+1]))
	default:
		return errors.Errorf("unsupported checksum type: %d", checksumType)
	}

	if expectedChecksum != computedChecksum {
		return base.CorruptionErrorf(
			"pebble/table: invalid table %s (checksum mismatch at %d/%d)",
			fileNum, errors.Safe(bh.Offset), errors.Safe(bh.Length))
	}
	return nil
}

// DeterministicReadBlockDurationForTesting is for tests that want a
// deterministic value of the time to read a block (that is not in the cache).
// The return value is a function that must be called before the test exits.
func DeterministicReadBlockDurationForTesting() func() {
	drbdForTesting := deterministicReadBlockDurationForTesting
	deterministicReadBlockDurationForTesting = true
	return func() {
		deterministicReadBlockDurationForTesting = drbdForTesting
	}
}

var deterministicReadBlockDurationForTesting = false

func (r *Reader) readBlock(
	ctx context.Context,
	bh block.Handle,
	transform blockTransform,
	readHandle objstorage.ReadHandle,
	stats *base.InternalIteratorStats,
	iterStats *iterStatsAccumulator,
	bufferPool *block.BufferPool,
) (handle block.BufferHandle, _ error) {
	if h := r.cacheOpts.Cache.Get(r.cacheOpts.CacheID, r.cacheOpts.FileNum, bh.Offset); h.Get() != nil {
		// Cache hit.
		if readHandle != nil {
			readHandle.RecordCacheHit(ctx, int64(bh.Offset), int64(bh.Length+block.TrailerLen))
		}
		if stats != nil {
			stats.BlockBytes += bh.Length
			stats.BlockBytesInCache += bh.Length
		}
		if iterStats != nil {
			iterStats.reportStats(bh.Length, bh.Length, 0)
		}
		// This block is already in the cache; return a handle to existing vlaue
		// in the cache.
		return block.CacheBufferHandle(h), nil
	}

	// Cache miss.

	if sema := r.loadBlockSema; sema != nil {
		if err := sema.Acquire(ctx, 1); err != nil {
			// An error here can only come from the context.
			return block.BufferHandle{}, err
		}
		defer sema.Release(1)
	}

	compressed := block.Alloc(int(bh.Length+block.TrailerLen), bufferPool)
	readStopwatch := makeStopwatch()
	var err error
	if readHandle != nil {
		err = readHandle.ReadAt(ctx, compressed.Get(), int64(bh.Offset))
	} else {
		err = r.readable.ReadAt(ctx, compressed.Get(), int64(bh.Offset))
	}
	readDuration := readStopwatch.stop()
	// Call IsTracingEnabled to avoid the allocations of boxing integers into an
	// interface{}, unless necessary.
	if readDuration >= slowReadTracingThreshold && r.logger.IsTracingEnabled(ctx) {
		_, file1, line1, _ := runtime.Caller(1)
		_, file2, line2, _ := runtime.Caller(2)
		r.logger.Eventf(ctx, "reading block of %d bytes took %s (fileNum=%s; %s/%s:%d -> %s/%s:%d)",
			int(bh.Length+block.TrailerLen), readDuration.String(),
			r.cacheOpts.FileNum,
			filepath.Base(filepath.Dir(file2)), filepath.Base(file2), line2,
			filepath.Base(filepath.Dir(file1)), filepath.Base(file1), line1)
	}
	if stats != nil {
		stats.BlockBytes += bh.Length
		stats.BlockReadDuration += readDuration
	}
	if err != nil {
		compressed.Release()
		return block.BufferHandle{}, err
	}
	if err := checkChecksum(r.checksumType, compressed.Get(), bh, r.cacheOpts.FileNum); err != nil {
		compressed.Release()
		return block.BufferHandle{}, err
	}

	typ := block.CompressionIndicator(compressed.Get()[bh.Length])
	compressed.Truncate(int(bh.Length))

	var decompressed block.Value
	if typ == block.NoCompressionIndicator {
		decompressed = compressed
	} else {
		// Decode the length of the decompressed value.
		decodedLen, prefixLen, err := block.DecompressedLen(typ, compressed.Get())
		if err != nil {
			compressed.Release()
			return block.BufferHandle{}, err
		}

		decompressed = block.Alloc(decodedLen, bufferPool)
		if err := block.DecompressInto(typ, compressed.Get()[prefixLen:], decompressed.Get()); err != nil {
			compressed.Release()
			return block.BufferHandle{}, err
		}
		compressed.Release()
	}

	if transform != nil {
		// Transforming blocks is very rare, so the extra copy of the
		// transformed data is not problematic.
		tmpTransformed, err := transform(decompressed.Get())
		if err != nil {
			decompressed.Release()
			return block.BufferHandle{}, err
		}

		transformed := block.Alloc(len(tmpTransformed), bufferPool)
		copy(transformed.Get(), tmpTransformed)
		decompressed.Release()
		decompressed = transformed
	}

	if iterStats != nil {
		iterStats.reportStats(bh.Length, 0, readDuration)
	}
	h := decompressed.MakeHandle(r.cacheOpts.Cache, r.cacheOpts.CacheID, r.cacheOpts.FileNum, bh.Offset)
	return h, nil
}

func (r *Reader) readMetaindex(
	ctx context.Context,
	metaindexBH block.Handle,
	readHandle objstorage.ReadHandle,
	filters map[string]FilterPolicy,
) error {
	// We use a BufferPool when reading metaindex blocks in order to avoid
	// populating the block cache with these blocks. In heavy-write workloads,
	// especially with high compaction concurrency, new tables may be created
	// frequently. Populating the block cache with these metaindex blocks adds
	// additional contention on the block cache mutexes (see #1997).
	// Additionally, these blocks are exceedingly unlikely to be read again
	// while they're still in the block cache except in misconfigurations with
	// excessive sstables counts or a table cache that's far too small.
	r.metaBufferPool.InitPreallocated(r.metaBufferPoolAlloc[:0])
	// When we're finished, release the buffers we've allocated back to memory
	// allocator. We don't expect to use metaBufferPool again.
	defer r.metaBufferPool.Release()

	b, err := r.readBlock(
		ctx, metaindexBH, nil /* transform */, readHandle, nil, /* stats */
		nil /* iterStats */, &r.metaBufferPool)
	if err != nil {
		return err
	}
	data := b.Get()
	defer b.Release()

	if uint64(len(data)) != metaindexBH.Length {
		return base.CorruptionErrorf("pebble/table: unexpected metaindex block size: %d vs %d",
			errors.Safe(len(data)), errors.Safe(metaindexBH.Length))
	}

	i, err := rowblk.NewRawIter(bytes.Compare, data)
	if err != nil {
		return err
	}

	meta := map[string]block.Handle{}
	for valid := i.First(); valid; valid = i.Next() {
		value := i.Value()
		if bytes.Equal(i.Key().UserKey, []byte(metaValueIndexName)) {
			vbih, n, err := decodeValueBlocksIndexHandle(i.Value())
			if err != nil {
				return err
			}
			if n == 0 || n != len(value) {
				return base.CorruptionErrorf("pebble/table: invalid table (bad value blocks index handle)")
			}
			r.valueBIH = vbih
		} else {
			bh, n := decodeBlockHandle(value)
			if n == 0 || n != len(value) {
				return base.CorruptionErrorf("pebble/table: invalid table (bad block handle)")
			}
			meta[string(i.Key().UserKey)] = bh
		}
	}
	if err := i.Close(); err != nil {
		return err
	}

	if bh, ok := meta[metaPropertiesName]; ok {
		b, err = r.readBlock(
			ctx, bh, nil /* transform */, readHandle, nil, /* stats */
			nil /* iterStats */, nil /* buffer pool */)
		if err != nil {
			return err
		}
		r.propertiesBH = bh
		err := r.Properties.load(b.Get(), r.deniedUserProperties)
		b.Release()
		if err != nil {
			return err
		}
	}

	if bh, ok := meta[metaRangeDelV2Name]; ok {
		r.rangeDelBH = bh
	} else if _, ok := meta[metaRangeDelV1Name]; ok {
		// This version of Pebble requires a format major version at least as
		// high as FormatFlushableIngest (see pebble.FormatMinSupported). In
		// this format major verison, we have a guarantee that we've compacted
		// away all RocksDB sstables. It should not be possible to encounter an
		// sstable with a v1 range deletion block but not a v2 range deletion
		// block.
		err := errors.Newf("pebble/table: unexpected range-del block type: %s", metaRangeDelV1Name)
		return errors.Mark(err, base.ErrCorruption)
	}

	if bh, ok := meta[metaRangeKeyName]; ok {
		r.rangeKeyBH = bh
	}

	for name, fp := range filters {
		types := []struct {
			ftype  FilterType
			prefix string
		}{
			{TableFilter, "fullfilter."},
		}
		var done bool
		for _, t := range types {
			if bh, ok := meta[t.prefix+name]; ok {
				r.filterBH = bh

				switch t.ftype {
				case TableFilter:
					r.tableFilter = newTableFilterReader(fp, r.filterMetricsTracker)
				default:
					return base.CorruptionErrorf("unknown filter type: %v", errors.Safe(t.ftype))
				}

				done = true
				break
			}
		}
		if done {
			break
		}
	}
	return nil
}

// Layout returns the layout (block organization) for an sstable.
func (r *Reader) Layout() (*Layout, error) {
	if r.err != nil {
		return nil, r.err
	}

	l := &Layout{
		Data:       make([]BlockHandleWithProperties, 0, r.Properties.NumDataBlocks),
		Filter:     r.filterBH,
		RangeDel:   r.rangeDelBH,
		RangeKey:   r.rangeKeyBH,
		ValueIndex: r.valueBIH.h,
		Properties: r.propertiesBH,
		MetaIndex:  r.metaIndexBH,
		Footer:     r.footerBH,
		Format:     r.tableFormat,
	}

	indexH, err := r.readIndex(context.Background(), nil, nil, nil)
	if err != nil {
		return nil, err
	}
	defer indexH.Release()

	var alloc bytealloc.A

	if r.Properties.IndexPartitions == 0 {
		l.Index = append(l.Index, r.indexBH)
		iter, _ := rowblk.NewIter(r.Compare, r.Split, indexH.Get(), NoTransforms)
		for kv := iter.First(); kv != nil; kv = iter.Next() {
			dataBH, err := decodeBlockHandleWithProperties(kv.InPlaceValue())
			if err != nil {
				return nil, errCorruptIndexEntry(err)
			}
			if len(dataBH.Props) > 0 {
				alloc, dataBH.Props = alloc.Copy(dataBH.Props)
			}
			l.Data = append(l.Data, dataBH)
		}
	} else {
		l.TopIndex = r.indexBH
		topIter, _ := rowblk.NewIter(r.Compare, r.Split, indexH.Get(), NoTransforms)
		iter := &rowblk.Iter{}
		for kv := topIter.First(); kv != nil; kv = topIter.Next() {
			indexBH, err := decodeBlockHandleWithProperties(kv.InPlaceValue())
			if err != nil {
				return nil, errCorruptIndexEntry(err)
			}
			l.Index = append(l.Index, indexBH.Handle)

			subIndex, err := r.readBlock(context.Background(), indexBH.Handle,
				nil /* transform */, nil /* readHandle */, nil /* stats */, nil /* iterStats */, nil /* buffer pool */)
			if err != nil {
				return nil, err
			}
			// TODO(msbutler): figure out how to pass virtualState to layout call.
			if err := iter.Init(r.Compare, r.Split, subIndex.Get(), NoTransforms); err != nil {
				return nil, err
			}
			for kv := iter.First(); kv != nil; kv = iter.Next() {
				dataBH, err := decodeBlockHandleWithProperties(kv.InPlaceValue())
				if len(dataBH.Props) > 0 {
					alloc, dataBH.Props = alloc.Copy(dataBH.Props)
				}
				if err != nil {
					return nil, errCorruptIndexEntry(err)
				}
				l.Data = append(l.Data, dataBH)
			}
			subIndex.Release()
			*iter = iter.ResetForReuse()
		}
	}
	if r.valueBIH.h.Length != 0 {
		vbiH, err := r.readBlock(context.Background(), r.valueBIH.h, nil, nil, nil, nil, nil /* buffer pool */)
		if err != nil {
			return nil, err
		}
		defer vbiH.Release()
		vbiBlock := vbiH.Get()
		indexEntryLen := int(r.valueBIH.blockNumByteLength + r.valueBIH.blockOffsetByteLength +
			r.valueBIH.blockLengthByteLength)
		i := 0
		for len(vbiBlock) != 0 {
			if len(vbiBlock) < indexEntryLen {
				return nil, errors.Errorf(
					"remaining value index block %d does not contain a full entry of length %d",
					len(vbiBlock), indexEntryLen)
			}
			n := int(r.valueBIH.blockNumByteLength)
			bn := int(littleEndianGet(vbiBlock, n))
			if bn != i {
				return nil, errors.Errorf("unexpected block num %d, expected %d",
					bn, i)
			}
			i++
			vbiBlock = vbiBlock[n:]
			n = int(r.valueBIH.blockOffsetByteLength)
			blockOffset := littleEndianGet(vbiBlock, n)
			vbiBlock = vbiBlock[n:]
			n = int(r.valueBIH.blockLengthByteLength)
			blockLen := littleEndianGet(vbiBlock, n)
			vbiBlock = vbiBlock[n:]
			l.ValueBlock = append(l.ValueBlock, block.Handle{Offset: blockOffset, Length: blockLen})
		}
	}

	return l, nil
}

// ValidateBlockChecksums validates the checksums for each block in the SSTable.
func (r *Reader) ValidateBlockChecksums() error {
	// Pre-compute the BlockHandles for the underlying file.
	l, err := r.Layout()
	if err != nil {
		return err
	}

	// Construct the set of blocks to check. Note that the footer is not checked
	// as it is not a block with a checksum.
	blocks := make([]block.Handle, len(l.Data))
	for i := range l.Data {
		blocks[i] = l.Data[i].Handle
	}
	blocks = append(blocks, l.Index...)
	blocks = append(blocks, l.TopIndex, l.Filter, l.RangeDel, l.RangeKey, l.Properties, l.MetaIndex)

	// Sorting by offset ensures we are performing a sequential scan of the
	// file.
	slices.SortFunc(blocks, func(a, b block.Handle) int {
		return cmp.Compare(a.Offset, b.Offset)
	})

	// Check all blocks sequentially. Make use of read-ahead, given we are
	// scanning the entire file from start to end.
	rh := r.readable.NewReadHandle(objstorage.NoReadBefore)
	defer rh.Close()

	for _, bh := range blocks {
		// Certain blocks may not be present, in which case we skip them.
		if bh.Length == 0 {
			continue
		}

		// Read the block, which validates the checksum.
		h, err := r.readBlock(context.Background(), bh, nil, rh, nil, nil /* iterStats */, nil /* buffer pool */)
		if err != nil {
			return err
		}
		h.Release()
	}

	return nil
}

// CommonProperties implemented the CommonReader interface.
func (r *Reader) CommonProperties() *CommonProperties {
	return &r.Properties.CommonProperties
}

// EstimateDiskUsage returns the total size of data blocks overlapping the range
// `[start, end]`. Even if a data block partially overlaps, or we cannot
// determine overlap due to abbreviated index keys, the full data block size is
// included in the estimation.
//
// This function does not account for any metablock space usage. Assumes there
// is at least partial overlap, i.e., `[start, end]` falls neither completely
// before nor completely after the file's range.
//
// Only blocks containing point keys are considered. Range deletion and range
// key blocks are not considered.
//
// TODO(ajkr): account for metablock space usage. Perhaps look at the fraction of
// data blocks overlapped and add that same fraction of the metadata blocks to the
// estimate.
func (r *Reader) EstimateDiskUsage(start, end []byte) (uint64, error) {
	if r.err != nil {
		return 0, r.err
	}

	indexH, err := r.readIndex(context.Background(), nil, nil, nil)
	if err != nil {
		return 0, err
	}
	defer indexH.Release()

	// Iterators over the bottom-level index blocks containing start and end.
	// These may be different in case of partitioned index but will both point
	// to the same blockIter over the single index in the unpartitioned case.
	var startIdxIter, endIdxIter *rowblk.Iter
	if r.Properties.IndexPartitions == 0 {
		iter, err := rowblk.NewIter(r.Compare, r.Split, indexH.Get(), NoTransforms)
		if err != nil {
			return 0, err
		}
		startIdxIter = iter
		endIdxIter = iter
	} else {
		topIter, err := rowblk.NewIter(r.Compare, r.Split, indexH.Get(), NoTransforms)
		if err != nil {
			return 0, err
		}

		kv := topIter.SeekGE(start, base.SeekGEFlagsNone)
		if kv == nil {
			// The range falls completely after this file, or an error occurred.
			return 0, topIter.Error()
		}
		startIdxBH, err := decodeBlockHandleWithProperties(kv.InPlaceValue())
		if err != nil {
			return 0, errCorruptIndexEntry(err)
		}
		startIdxBlock, err := r.readBlock(context.Background(), startIdxBH.Handle,
			nil /* transform */, nil /* readHandle */, nil /* stats */, nil /* iterStats */, nil /* buffer pool */)
		if err != nil {
			return 0, err
		}
		defer startIdxBlock.Release()
		startIdxIter, err = rowblk.NewIter(r.Compare, r.Split, startIdxBlock.Get(), NoTransforms)
		if err != nil {
			return 0, err
		}

		kv = topIter.SeekGE(end, base.SeekGEFlagsNone)
		if kv == nil {
			if err := topIter.Error(); err != nil {
				return 0, err
			}
		} else {
			endIdxBH, err := decodeBlockHandleWithProperties(kv.InPlaceValue())
			if err != nil {
				return 0, errCorruptIndexEntry(err)
			}
			endIdxBlock, err := r.readBlock(context.Background(),
				endIdxBH.Handle, nil /* transform */, nil /* readHandle */, nil /* stats */, nil /* iterStats */, nil /* buffer pool */)
			if err != nil {
				return 0, err
			}
			defer endIdxBlock.Release()
			endIdxIter, err = rowblk.NewIter(r.Compare, r.Split, endIdxBlock.Get(), NoTransforms)
			if err != nil {
				return 0, err
			}
		}
	}
	// startIdxIter should not be nil at this point, while endIdxIter can be if the
	// range spans past the end of the file.

	kv := startIdxIter.SeekGE(start, base.SeekGEFlagsNone)
	if kv == nil {
		// The range falls completely after this file, or an error occurred.
		return 0, startIdxIter.Error()
	}
	startBH, err := decodeBlockHandleWithProperties(kv.InPlaceValue())
	if err != nil {
		return 0, errCorruptIndexEntry(err)
	}

	includeInterpolatedValueBlocksSize := func(dataBlockSize uint64) uint64 {
		// INVARIANT: r.Properties.DataSize > 0 since startIdxIter is not nil.
		// Linearly interpolate what is stored in value blocks.
		//
		// TODO(sumeer): if we need more accuracy, without loading any data blocks
		// (which contain the value handles, and which may also be insufficient if
		// the values are in separate files), we will need to accumulate the
		// logical size of the key-value pairs and store the cumulative value for
		// each data block in the index block entry. This increases the size of
		// the BlockHandle, so wait until this becomes necessary.
		return dataBlockSize +
			uint64((float64(dataBlockSize)/float64(r.Properties.DataSize))*
				float64(r.Properties.ValueBlocksSize))
	}
	if endIdxIter == nil {
		// The range spans beyond this file. Include data blocks through the last.
		return includeInterpolatedValueBlocksSize(r.Properties.DataSize - startBH.Offset), nil
	}
	kv = endIdxIter.SeekGE(end, base.SeekGEFlagsNone)
	if kv == nil {
		if err := endIdxIter.Error(); err != nil {
			return 0, err
		}
		// The range spans beyond this file. Include data blocks through the last.
		return includeInterpolatedValueBlocksSize(r.Properties.DataSize - startBH.Offset), nil
	}
	endBH, err := decodeBlockHandleWithProperties(kv.InPlaceValue())
	if err != nil {
		return 0, errCorruptIndexEntry(err)
	}
	return includeInterpolatedValueBlocksSize(
		endBH.Offset + endBH.Length + block.TrailerLen - startBH.Offset), nil
}

// TableFormat returns the format version for the table.
func (r *Reader) TableFormat() (TableFormat, error) {
	if r.err != nil {
		return TableFormatUnspecified, r.err
	}
	return r.tableFormat, nil
}

// NewReader returns a new table reader for the file. Closing the reader will
// close the file.
//
// The context is used for tracing any operations performed by NewReader; it is
// NOT stored for future use.
func NewReader(ctx context.Context, f objstorage.Readable, o ReaderOptions) (*Reader, error) {
	if f == nil {
		return nil, errors.New("pebble/table: nil file")
	}
	o = o.ensureDefaults()
	r := &Reader{
		readable:             f,
		cacheOpts:            o.internal.CacheOpts,
		loadBlockSema:        o.LoadBlockSema,
		deniedUserProperties: o.DeniedUserProperties,
		filterMetricsTracker: o.FilterMetricsTracker,
		logger:               o.LoggerAndTracer,
	}
	if r.cacheOpts.Cache == nil {
		r.cacheOpts.Cache = cache.New(0)
	} else {
		r.cacheOpts.Cache.Ref()
	}
	if r.cacheOpts.CacheID == 0 {
		r.cacheOpts.CacheID = r.cacheOpts.Cache.NewID()
	}

	var preallocRH objstorageprovider.PreallocatedReadHandle
	rh := objstorageprovider.UsePreallocatedReadHandle(
		r.readable, objstorage.ReadBeforeForNewReader, &preallocRH)
	defer rh.Close()

	footer, err := readFooter(ctx, f, rh, r.logger, r.cacheOpts.FileNum)
	if err != nil {
		r.err = err
		return nil, r.Close()
	}
	r.checksumType = footer.checksum
	r.tableFormat = footer.format
	// Read the metaindex and properties blocks.
	if err := r.readMetaindex(ctx, footer.metaindexBH, rh, o.Filters); err != nil {
		r.err = err
		return nil, r.Close()
	}
	r.indexBH = footer.indexBH
	r.metaIndexBH = footer.metaindexBH
	r.footerBH = footer.footerBH

	if r.Properties.ComparerName == "" || o.Comparer.Name == r.Properties.ComparerName {
		r.Comparer = o.Comparer
		r.Compare = o.Comparer.Compare
		r.SuffixCmp = o.Comparer.CompareSuffixes
		r.Equal = o.Comparer.Equal
		r.Split = o.Comparer.Split
	} else if comparer, ok := o.Comparers[r.Properties.ComparerName]; ok {
		r.Comparer = o.Comparer
		r.Compare = comparer.Compare
		r.SuffixCmp = comparer.CompareSuffixes
		r.Equal = comparer.Equal
		r.Split = comparer.Split
	} else {
		r.err = errors.Errorf("pebble/table: %d: unknown comparer %s",
			errors.Safe(r.cacheOpts.FileNum), errors.Safe(r.Properties.ComparerName))
	}

	if mergerName := r.Properties.MergerName; mergerName != "" && mergerName != "nullptr" {
		if o.Merger != nil && o.Merger.Name == mergerName {
			// opts.Merger matches.
		} else if _, ok := o.Mergers[mergerName]; ok {
			// Known merger.
		} else {
			r.err = errors.Errorf("pebble/table: %d: unknown merger %s",
				errors.Safe(r.cacheOpts.FileNum), errors.Safe(r.Properties.MergerName))
		}
	}

	if r.err != nil {
		return nil, r.Close()
	}

	return r, nil
}

// ReadableFile describes the smallest subset of vfs.File that is required for
// reading SSTs.
type ReadableFile interface {
	io.ReaderAt
	io.Closer
	Stat() (os.FileInfo, error)
}

// NewSimpleReadable wraps a ReadableFile in a objstorage.Readable
// implementation (which does not support read-ahead)
func NewSimpleReadable(r ReadableFile) (objstorage.Readable, error) {
	info, err := r.Stat()
	if err != nil {
		return nil, err
	}
	res := &simpleReadable{
		f:    r,
		size: info.Size(),
	}
	res.rh = objstorage.MakeNoopReadHandle(res)
	return res, nil
}

// simpleReadable wraps a ReadableFile to implement objstorage.Readable.
type simpleReadable struct {
	f    ReadableFile
	size int64
	rh   objstorage.NoopReadHandle
}

var _ objstorage.Readable = (*simpleReadable)(nil)

// ReadAt is part of the objstorage.Readable interface.
func (s *simpleReadable) ReadAt(_ context.Context, p []byte, off int64) error {
	n, err := s.f.ReadAt(p, off)
	if invariants.Enabled && err == nil && n != len(p) {
		panic("short read")
	}
	return err
}

// Close is part of the objstorage.Readable interface.
func (s *simpleReadable) Close() error {
	return s.f.Close()
}

// Size is part of the objstorage.Readable interface.
func (s *simpleReadable) Size() int64 {
	return s.size
}

// NewReaddHandle is part of the objstorage.Readable interface.
func (s *simpleReadable) NewReadHandle(
	readBeforeSize objstorage.ReadBeforeSize,
) objstorage.ReadHandle {
	return &s.rh
}

func errCorruptIndexEntry(err error) error {
	err = base.CorruptionErrorf("pebble/table: corrupt index entry: %v", err)
	if invariants.Enabled {
		panic(err)
	}
	return err
}

type deterministicStopwatchForTesting struct {
	startTime time.Time
}

func makeStopwatch() deterministicStopwatchForTesting {
	return deterministicStopwatchForTesting{startTime: time.Now()}
}

func (w deterministicStopwatchForTesting) stop() time.Duration {
	dur := time.Since(w.startTime)
	if deterministicReadBlockDurationForTesting {
		dur = slowReadTracingThreshold
	}
	return dur
}
