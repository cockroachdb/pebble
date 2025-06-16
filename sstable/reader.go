// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/bytealloc"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/rangekey"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/block/blockkind"
	"github.com/cockroachdb/pebble/sstable/colblk"
	"github.com/cockroachdb/pebble/sstable/rowblk"
	"github.com/cockroachdb/pebble/sstable/valblk"
	"github.com/cockroachdb/pebble/sstable/virtual"
	"github.com/cockroachdb/pebble/vfs"
)

var errReaderClosed = errors.New("pebble/table: reader is closed")

type loadBlockResult int8

const (
	loadBlockOK loadBlockResult = iota
	// Could be due to error or because no block left to load.
	loadBlockFailed
	loadBlockIrrelevant
)

// Reader is a table reader.
// If you update this struct, make sure you also update the magic number in
// StringForTests() in metrics.go.
type Reader struct {
	blockReader block.Reader

	// The following fields are copied from the ReadOptions.
	keySchema            *colblk.KeySchema
	filterMetricsTracker *FilterMetricsTracker
	Comparer             *base.Comparer

	tableFilter *tableFilterReader

	err error

	indexBH        block.Handle
	filterBH       block.Handle
	rangeDelBH     block.Handle
	rangeKeyBH     block.Handle
	valueBIH       valblk.IndexHandle
	propertiesBH   block.Handle
	metaindexBH    block.Handle
	footerBH       block.Handle
	blobRefIndexBH block.Handle

	tableFormat    TableFormat
	Attributes     Attributes
	UserProperties map[string]string
}

type ReadEnv struct {
	Virtual *virtual.VirtualReaderParams
	// IsSharedIngested is true if this is a shared table that was ingested. Can
	// only be set when Virtual is non-nil.
	IsSharedIngested bool
	Block            block.ReadEnv
}

var NoReadEnv = ReadEnv{}

// Close the reader and the underlying objstorage.Readable.
func (r *Reader) Close() error {
	r.err = firstError(r.err, r.blockReader.Close())
	if r.err != nil {
		return r.err
	}
	// Make any future calls to Get, NewIter or Close return an error.
	r.err = errReaderClosed
	return nil
}

// IterOptions defines options for configuring a sstable pointer iterator.
type IterOptions struct {
	Lower, Upper         []byte
	Transforms           IterTransforms
	Filterer             *BlockPropertiesFilterer
	FilterBlockSizeLimit FilterBlockSizeLimit
	Env                  ReadEnv
	ReaderProvider       valblk.ReaderProvider
	BlobContext          TableBlobContext
}

// NewPointIter returns an iterator for the point keys in the table.
//
// If transform.HideObsoletePoints is set, the callee assumes that filterer
// already includes obsoleteKeyBlockPropertyFilter. The caller can satisfy this
// contract by first calling TryAddBlockPropertyFilterForHideObsoletePoints.
func (r *Reader) NewPointIter(ctx context.Context, opts IterOptions) (Iterator, error) {
	return r.newPointIter(ctx, opts)
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

func (r *Reader) newPointIter(ctx context.Context, opts IterOptions) (Iterator, error) {
	// NB: pebble.fileCache wraps the returned iterator with one which performs
	// reference counting on the Reader, preventing the Reader from being closed
	// until the final iterator closes.
	var res Iterator
	var err error
	if r.Attributes.Has(AttributeTwoLevelIndex) {
		if r.tableFormat.BlockColumnar() {
			res, err = newColumnBlockTwoLevelIterator(
				ctx, r, opts)
		} else {
			res, err = newRowBlockTwoLevelIterator(
				ctx, r, opts)
		}
	} else {
		if r.tableFormat.BlockColumnar() {
			res, err = newColumnBlockSingleLevelIterator(
				ctx, r, opts)
		} else {
			res, err = newRowBlockSingleLevelIterator(
				ctx, r, opts)
		}
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
func (r *Reader) NewIter(
	transforms IterTransforms, lower, upper []byte, blobContext TableBlobContext,
) (Iterator, error) {
	// TODO(radu): we should probably not use bloom filters in this case, as there
	// likely isn't a cache set up.
	opts := IterOptions{
		Lower:                lower,
		Upper:                upper,
		Transforms:           transforms,
		Filterer:             nil,
		FilterBlockSizeLimit: AlwaysUseFilterBlock,
		Env:                  NoReadEnv,
		ReaderProvider:       MakeTrivialReaderProvider(r),
		BlobContext:          blobContext,
	}
	return r.NewPointIter(context.TODO(), opts)
}

// NewCompactionIter returns an iterator similar to NewIter but it also increments
// the number of bytes iterated. If an error occurs, NewCompactionIter cleans up
// after itself and returns a nil iterator.
func (r *Reader) NewCompactionIter(
	transforms IterTransforms, env ReadEnv, rp valblk.ReaderProvider, blobContext TableBlobContext,
) (Iterator, error) {
	return r.newCompactionIter(transforms, env, rp, blobContext)
}

func (r *Reader) newCompactionIter(
	transforms IterTransforms, env ReadEnv, rp valblk.ReaderProvider, blobContext TableBlobContext,
) (Iterator, error) {
	if env.IsSharedIngested {
		transforms.HideObsoletePoints = true
	}
	ctx := context.Background()
	opts := IterOptions{
		Transforms:           transforms,
		Filterer:             nil,
		FilterBlockSizeLimit: NeverUseFilterBlock,
		Env:                  env,
		ReaderProvider:       rp,
		BlobContext:          blobContext,
	}

	if r.Attributes.Has(AttributeTwoLevelIndex) {
		if !r.tableFormat.BlockColumnar() {
			i, err := newRowBlockTwoLevelIterator(ctx, r, opts)
			if err != nil {
				return nil, err
			}
			i.SetupForCompaction()
			return i, nil
		}
		i, err := newColumnBlockTwoLevelIterator(ctx, r, opts)
		if err != nil {
			return nil, err
		}
		i.SetupForCompaction()
		return i, nil
	}
	if !r.tableFormat.BlockColumnar() {
		i, err := newRowBlockSingleLevelIterator(ctx, r, opts)
		if err != nil {
			return nil, err
		}
		i.SetupForCompaction()
		return i, nil
	}
	i, err := newColumnBlockSingleLevelIterator(ctx, r, opts)
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
	ctx context.Context, transforms FragmentIterTransforms, env ReadEnv,
) (iter keyspan.FragmentIterator, err error) {
	if r.rangeDelBH.Length == 0 {
		return nil, nil
	}
	h, err := r.readRangeDelBlock(ctx, env.Block, noReadHandle, r.rangeDelBH)
	if err != nil {
		return nil, err
	}
	if r.tableFormat.BlockColumnar() {
		iter = colblk.NewKeyspanIter(r.Comparer.Compare, h, transforms)
	} else {
		iter, err = rowblk.NewFragmentIter(r.blockReader.FileNum(), r.Comparer, h, transforms)
		if err != nil {
			return nil, err
		}
	}

	i := keyspan.MaybeAssert(iter, r.Comparer.Compare)
	if env.Virtual != nil {
		i = keyspan.Truncate(
			r.Comparer.Compare, i,
			base.UserKeyBoundsFromInternal(env.Virtual.Lower, env.Virtual.Upper),
		)
	}
	return i, nil
}

// NewRawRangeKeyIter returns an internal iterator for the contents of the
// range-key block for the table. Returns nil if the table does not contain any
// range keys.
func (r *Reader) NewRawRangeKeyIter(
	ctx context.Context, transforms FragmentIterTransforms, env ReadEnv,
) (iter keyspan.FragmentIterator, err error) {
	syntheticSeqNum := transforms.SyntheticSeqNum
	if env.IsSharedIngested {
		// Don't pass a synthetic sequence number for shared ingested sstables. We
		// need to know the materialized sequence numbers, and we will set up the
		// appropriate sequence number substitution below.
		transforms.SyntheticSeqNum = 0
	}

	if r.rangeKeyBH.Length == 0 {
		return nil, nil
	}
	h, err := r.readRangeKeyBlock(ctx, env.Block, noReadHandle, r.rangeKeyBH)
	if err != nil {
		return nil, err
	}
	if r.tableFormat.BlockColumnar() {
		iter = colblk.NewKeyspanIter(r.Comparer.Compare, h, transforms)
	} else {
		iter, err = rowblk.NewFragmentIter(r.blockReader.FileNum(), r.Comparer, h, transforms)
		if err != nil {
			return nil, err
		}
	}
	i := keyspan.MaybeAssert(iter, r.Comparer.Compare)

	if env.Virtual != nil {
		// We need to coalesce range keys within each sstable, and then apply the
		// synthetic sequence number. For this, we use ForeignSSTTransformer.
		//
		// TODO(bilal): Avoid these allocations by hoisting the transformer and
		// transform iter up.
		if env.IsSharedIngested {
			transform := &rangekey.ForeignSSTTransformer{
				Equal:  r.Comparer.Equal,
				SeqNum: base.SeqNum(syntheticSeqNum),
			}
			transformIter := &keyspan.TransformerIter{
				FragmentIterator: i,
				Transformer:      transform,
				SuffixCmp:        r.Comparer.CompareRangeSuffixes,
			}
			i = transformIter
		}
		i = keyspan.Truncate(
			r.Comparer.Compare, i,
			base.UserKeyBoundsFromInternal(env.Virtual.Lower, env.Virtual.Upper),
		)
	}
	return i, nil
}

// noReadHandle is used when we don't want to pass a ReadHandle to one of the
// read block methods.
var noReadHandle objstorage.ReadHandle = nil

var noInitBlockMetadataFn = func(*block.Metadata, []byte) error { return nil }

// readMetaindexBlock reads the metaindex block.
func (r *Reader) readMetaindexBlock(
	ctx context.Context, env block.ReadEnv, readHandle objstorage.ReadHandle,
) (block.BufferHandle, error) {
	return r.blockReader.Read(ctx, env, readHandle, r.metaindexBH, blockkind.Metadata, noInitBlockMetadataFn)
}

// readTopLevelIndexBlock reads the top-level index block.
func (r *Reader) readTopLevelIndexBlock(
	ctx context.Context, env block.ReadEnv, readHandle objstorage.ReadHandle,
) (block.BufferHandle, error) {
	return r.readIndexBlock(ctx, env, readHandle, r.indexBH)
}

// readIndexBlock reads a top-level or second-level index block.
func (r *Reader) readIndexBlock(
	ctx context.Context, env block.ReadEnv, readHandle objstorage.ReadHandle, bh block.Handle,
) (block.BufferHandle, error) {
	return r.blockReader.Read(ctx, env, readHandle, bh, blockkind.Index, r.initIndexBlockMetadata)
}

// initIndexBlockMetadata initializes the Metadata for a data block. This will
// later be used (and reused) when reading from the block.
func (r *Reader) initIndexBlockMetadata(metadata *block.Metadata, data []byte) error {
	if r.tableFormat.BlockColumnar() {
		return colblk.InitIndexBlockMetadata(metadata, data)
	}
	return nil
}

func (r *Reader) readDataBlock(
	ctx context.Context, env block.ReadEnv, readHandle objstorage.ReadHandle, bh block.Handle,
) (block.BufferHandle, error) {
	return r.blockReader.Read(ctx, env, readHandle, bh, blockkind.SSTableData, r.initDataBlockMetadata)
}

// initDataBlockMetadata initializes the Metadata for a data block. This will
// later be used (and reused) when reading from the block.
func (r *Reader) initDataBlockMetadata(metadata *block.Metadata, data []byte) error {
	if r.tableFormat.BlockColumnar() {
		return colblk.InitDataBlockMetadata(r.keySchema, metadata, data)
	}
	return nil
}

func (r *Reader) readFilterBlock(
	ctx context.Context, env block.ReadEnv, readHandle objstorage.ReadHandle, bh block.Handle,
) (block.BufferHandle, error) {
	return r.blockReader.Read(ctx, env, readHandle, bh, blockkind.Filter, noInitBlockMetadataFn)
}

func (r *Reader) readRangeDelBlock(
	ctx context.Context, env block.ReadEnv, readHandle objstorage.ReadHandle, bh block.Handle,
) (block.BufferHandle, error) {
	return r.blockReader.Read(ctx, env, readHandle, bh, blockkind.RangeDel, r.initKeyspanBlockMetadata)
}

func (r *Reader) readRangeKeyBlock(
	ctx context.Context, env block.ReadEnv, readHandle objstorage.ReadHandle, bh block.Handle,
) (block.BufferHandle, error) {
	return r.blockReader.Read(ctx, env, readHandle, bh, blockkind.RangeKey, r.initKeyspanBlockMetadata)
}

// initKeyspanBlockMetadata initializes the Metadata for a rangedel or range key
// block. This will later be used (and reused) when reading from the block.
func (r *Reader) initKeyspanBlockMetadata(metadata *block.Metadata, data []byte) error {
	if r.tableFormat.BlockColumnar() {
		return colblk.InitKeyspanBlockMetadata(metadata, data)
	}
	return nil
}

// ReadValueBlockExternal implements valblk.ExternalBlockReader, allowing a
// base.LazyValue to read a value block.
func (r *Reader) ReadValueBlockExternal(
	ctx context.Context, bh block.Handle,
) (block.BufferHandle, error) {
	return r.readValueBlock(ctx, block.NoReadEnv, noReadHandle, bh)
}

func (r *Reader) readValueBlock(
	ctx context.Context, env block.ReadEnv, readHandle objstorage.ReadHandle, bh block.Handle,
) (block.BufferHandle, error) {
	return r.blockReader.Read(ctx, env, readHandle, bh, blockkind.SSTableValue, noInitBlockMetadataFn)
}

// metaBufferPools is a sync pool of BufferPools used exclusively when opening a
// table and loading its meta blocks.
var metaBufferPools = sync.Pool{
	New: func() any {
		bp := new(block.BufferPool)
		// New pools are initialized with a capacity of 3 to accommodate the
		// meta block (1), and both the compressed properties block (1) and
		// decompressed properties block (1) simultaneously.
		bp.Init(3)
		return bp
	},
}

func (r *Reader) readAndDecodeMetaindex(
	ctx context.Context, bufferPool *block.BufferPool, readHandle objstorage.ReadHandle,
) (map[string]block.Handle, valblk.IndexHandle, error) {
	metaEnv := block.ReadEnv{BufferPool: bufferPool}
	b, err := r.readMetaindexBlock(ctx, metaEnv, readHandle)
	if err != nil {
		return nil, valblk.IndexHandle{}, err
	}
	data := b.BlockData()
	defer b.Release()

	if uint64(len(data)) != r.metaindexBH.Length {
		return nil, valblk.IndexHandle{}, base.CorruptionErrorf("pebble/table: unexpected metaindex block size: %d vs %d",
			errors.Safe(len(data)), errors.Safe(r.metaindexBH.Length))
	}

	var meta map[string]block.Handle
	var valueBIH valblk.IndexHandle
	if r.tableFormat >= TableFormatPebblev6 {
		meta, valueBIH, err = decodeColumnarMetaIndex(data)
	} else {
		meta, valueBIH, err = decodeMetaindex(data)
	}
	return meta, valueBIH, err
}

func (r *Reader) initMetaindexBlocks(
	ctx context.Context,
	bufferPool *block.BufferPool,
	readHandle objstorage.ReadHandle,
	filters map[string]FilterPolicy,
) error {
	var meta map[string]block.Handle
	var err error
	meta, r.valueBIH, err = r.readAndDecodeMetaindex(ctx, bufferPool, readHandle)
	if err != nil {
		return err
	}

	if bh, ok := meta[metaPropertiesName]; ok {
		r.propertiesBH = bh
	} else {
		return errors.New("did not read any value for the properties block in the meta index")
	}

	if bh, ok := meta[metaBlobRefIndexName]; ok {
		r.blobRefIndexBH = bh
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
		if bh, ok := meta["fullfilter."+name]; ok {
			r.filterBH = bh
			r.tableFilter = newTableFilterReader(fp, r.filterMetricsTracker)
			break
		}
	}
	return nil
}

// decodePropertiesBlock decodes the (uncompressed) properties block.
func decodePropertiesBlock(tableFormat TableFormat, blockData []byte) (Properties, error) {
	var props Properties
	if tableFormat >= TableFormatPebblev7 {
		var decoder colblk.KeyValueBlockDecoder
		decoder.Init(blockData)
		if err := props.load(decoder.All()); err != nil {
			return Properties{}, err
		}
	} else {
		i, err := rowblk.NewRawIter(bytes.Compare, blockData)
		if err != nil {
			return Properties{}, err
		}
		if err := props.load(i.All()); err != nil {
			return Properties{}, err
		}
	}
	return props, nil
}

var propertiesBlockBufPools = sync.Pool{
	New: func() any {
		bp := new(block.BufferPool)
		// New pools are initialized with a capacity of 2 to accommodate
		// both the compressed properties block (1) and decompressed
		// properties block (1).
		bp.Init(2)
		return bp
	},
}

// ReadPropertiesBlock reads the properties block
// from the table. We always read the properties block into a buffer pool
// instead of the block cache.
func (r *Reader) ReadPropertiesBlock(
	ctx context.Context, bufferPool *block.BufferPool,
) (Properties, error) {
	return r.readPropertiesBlockInternal(ctx, bufferPool, noReadHandle)
}

func (r *Reader) readPropertiesBlockInternal(
	ctx context.Context, bufferPool *block.BufferPool, readHandle objstorage.ReadHandle,
) (Properties, error) {
	if bufferPool == nil {
		// We always use a buffer pool when reading the properties block as
		// we don't want it in the block cache.
		bufferPool = propertiesBlockBufPools.Get().(*block.BufferPool)
		defer propertiesBlockBufPools.Put(bufferPool)
		defer bufferPool.Release()
	}
	env := block.ReadEnv{BufferPool: bufferPool}
	b, err := r.blockReader.Read(ctx, env, readHandle, r.propertiesBH, blockkind.Metadata, noInitBlockMetadataFn)
	if err != nil {
		return Properties{}, err
	}
	defer b.Release()
	return decodePropertiesBlock(r.tableFormat, b.BlockData())
}

// Layout returns the layout (block organization) for an sstable.
func (r *Reader) Layout() (*Layout, error) {
	if r.err != nil {
		return nil, r.err
	}

	l := &Layout{
		Data:               make([]block.HandleWithProperties, 0),
		RangeDel:           r.rangeDelBH,
		RangeKey:           r.rangeKeyBH,
		ValueIndex:         r.valueBIH.Handle,
		Properties:         r.propertiesBH,
		MetaIndex:          r.metaindexBH,
		Footer:             r.footerBH,
		Format:             r.tableFormat,
		BlobReferenceIndex: r.blobRefIndexBH,
	}

	bufferPool := metaBufferPools.Get().(*block.BufferPool)
	defer metaBufferPools.Put(bufferPool)
	defer bufferPool.Release()

	ctx := context.TODO()
	meta, _, err := r.readAndDecodeMetaindex(ctx, bufferPool, noReadHandle)
	if err != nil {
		return nil, err
	}
	for name, bh := range meta {
		if strings.HasPrefix(name, "fullfilter.") {
			l.Filter = append(l.Filter, NamedBlockHandle{Name: name, Handle: bh})
		}
	}

	indexH, err := r.readTopLevelIndexBlock(ctx, block.NoReadEnv, noReadHandle)
	if err != nil {
		return nil, err
	}
	defer indexH.Release()

	var alloc bytealloc.A

	if !r.Attributes.Has(AttributeTwoLevelIndex) {
		l.Index = append(l.Index, r.indexBH)
		iter := r.tableFormat.newIndexIter()
		err := iter.Init(r.Comparer, indexH.BlockData(), NoTransforms)
		if err != nil {
			return nil, errors.Wrap(err, "reading index block")
		}
		for valid := iter.First(); valid; valid = iter.Next() {
			dataBH, err := iter.BlockHandleWithProperties()
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
		topIter := r.tableFormat.newIndexIter()
		err := topIter.Init(r.Comparer, indexH.BlockData(), NoTransforms)
		if err != nil {
			return nil, errors.Wrap(err, "reading index block")
		}
		iter := r.tableFormat.newIndexIter()
		for valid := topIter.First(); valid; valid = topIter.Next() {
			indexBH, err := topIter.BlockHandleWithProperties()
			if err != nil {
				return nil, errCorruptIndexEntry(err)
			}
			l.Index = append(l.Index, indexBH.Handle)

			subIndex, err := r.readIndexBlock(ctx, block.NoReadEnv, noReadHandle, indexBH.Handle)
			if err != nil {
				return nil, err
			}
			err = func() error {
				defer subIndex.Release()
				// TODO(msbutler): figure out how to pass virtualState to layout call.
				if err := iter.Init(r.Comparer, subIndex.BlockData(), NoTransforms); err != nil {
					return err
				}
				for valid := iter.First(); valid; valid = iter.Next() {
					dataBH, err := iter.BlockHandleWithProperties()
					if err != nil {
						return errCorruptIndexEntry(err)
					}
					if len(dataBH.Props) > 0 {
						alloc, dataBH.Props = alloc.Copy(dataBH.Props)
					}
					l.Data = append(l.Data, dataBH)
				}
				return nil
			}()
			if err != nil {
				return nil, err
			}
		}
	}
	if r.valueBIH.Handle.Length != 0 {
		vbiH, err := r.readValueBlock(context.Background(), block.NoReadEnv, noReadHandle, r.valueBIH.Handle)
		if err != nil {
			return nil, err
		}
		defer vbiH.Release()
		l.ValueBlock, err = valblk.DecodeIndex(vbiH.BlockData(), r.valueBIH)
		if err != nil {
			return nil, err
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

	type blk struct {
		bh     block.Handle
		readFn func(context.Context, block.ReadEnv, objstorage.ReadHandle, block.Handle) (block.BufferHandle, error)
	}
	// Construct the set of blocks to check. Note that the footer is not checked
	// as it is not a block with a checksum.
	blocks := make([]blk, 0, len(l.Data)+6)
	for i := range l.Data {
		blocks = append(blocks, blk{
			bh:     l.Data[i].Handle,
			readFn: r.readDataBlock,
		})
	}
	for _, h := range l.Index {
		blocks = append(blocks, blk{
			bh:     h,
			readFn: r.readIndexBlock,
		})
	}
	blocks = append(blocks, blk{
		bh:     l.TopIndex,
		readFn: r.readIndexBlock,
	})
	for _, bh := range l.Filter {
		blocks = append(blocks, blk{
			bh:     bh.Handle,
			readFn: r.readFilterBlock,
		})
	}
	blocks = append(blocks, blk{
		bh:     l.RangeDel,
		readFn: r.readRangeDelBlock,
	})
	blocks = append(blocks, blk{
		bh:     l.RangeKey,
		readFn: r.readRangeKeyBlock,
	})
	readNoInit := func(ctx context.Context, env block.ReadEnv, rh objstorage.ReadHandle, bh block.Handle) (block.BufferHandle, error) {
		return r.blockReader.Read(ctx, env, rh, bh, blockkind.Metadata, noInitBlockMetadataFn)
	}
	blocks = append(blocks, blk{
		bh:     l.Properties,
		readFn: readNoInit,
	})
	blocks = append(blocks, blk{
		bh:     l.MetaIndex,
		readFn: readNoInit,
	})
	blocks = append(blocks, blk{
		bh:     l.BlobReferenceIndex,
		readFn: readNoInit,
	})

	// Sorting by offset ensures we are performing a sequential scan of the
	// file.
	slices.SortFunc(blocks, func(a, b blk) int {
		return cmp.Compare(a.bh.Offset, b.bh.Offset)
	})

	ctx := context.Background()
	for _, b := range blocks {
		// Certain blocks may not be present, in which case we skip them.
		if b.bh.Length == 0 {
			continue
		}
		h, err := b.readFn(ctx, block.NoReadEnv, noReadHandle, b.bh)
		if err != nil {
			return err
		}
		h.Release()
	}

	return nil
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
func (r *Reader) EstimateDiskUsage(start []byte, end []byte, env ReadEnv) (uint64, error) {
	if env.Virtual != nil {
		_, start, end = env.Virtual.ConstrainBounds(start, end, false, r.Comparer.Compare)
	}

	if !r.tableFormat.BlockColumnar() {
		return estimateDiskUsage[rowblk.IndexIter, *rowblk.IndexIter](r, start, end)
	}
	return estimateDiskUsage[colblk.IndexIter, *colblk.IndexIter](r, start, end)
}

func estimateDiskUsage[I any, PI indexBlockIterator[I]](
	r *Reader, start, end []byte,
) (uint64, error) {
	if r.err != nil {
		return 0, r.err
	}
	ctx := context.TODO()

	indexH, err := r.readTopLevelIndexBlock(ctx, block.NoReadEnv, noReadHandle)
	if err != nil {
		return 0, err
	}
	// We are using InitHandle below but we never Close those iterators, which
	// allows us to release the index handle ourselves.
	// TODO(radu): clean this up.
	defer indexH.Release()

	// Iterators over the bottom-level index blocks containing start and end.
	// These may be different in case of partitioned index but will both point
	// to the same blockIter over the single index in the unpartitioned case.
	var startIdxIter, endIdxIter PI
	if !r.Attributes.Has(AttributeTwoLevelIndex) {
		startIdxIter = new(I)
		if err := startIdxIter.InitHandle(r.Comparer, indexH, NoTransforms); err != nil {
			return 0, err
		}
		endIdxIter = startIdxIter
	} else {
		var topIter PI = new(I)
		if err := topIter.InitHandle(r.Comparer, indexH, NoTransforms); err != nil {
			return 0, err
		}
		if !topIter.SeekGE(start) {
			// The range falls completely after this file.
			return 0, nil
		}
		startIndexBH, err := topIter.BlockHandleWithProperties()
		if err != nil {
			return 0, errCorruptIndexEntry(err)
		}
		startIdxBlock, err := r.readIndexBlock(ctx, block.NoReadEnv, noReadHandle, startIndexBH.Handle)
		if err != nil {
			return 0, err
		}
		defer startIdxBlock.Release()
		startIdxIter = new(I)
		err = startIdxIter.InitHandle(r.Comparer, startIdxBlock, NoTransforms)
		if err != nil {
			return 0, err
		}

		if topIter.SeekGE(end) {
			endIndexBH, err := topIter.BlockHandleWithProperties()
			if err != nil {
				return 0, errCorruptIndexEntry(err)
			}
			endIdxBlock, err := r.readIndexBlock(ctx, block.NoReadEnv, noReadHandle, endIndexBH.Handle)
			if err != nil {
				return 0, err
			}
			defer endIdxBlock.Release()
			endIdxIter = new(I)
			err = endIdxIter.InitHandle(r.Comparer, endIdxBlock, NoTransforms)
			if err != nil {
				return 0, err
			}
		}
	}
	// startIdxIter should not be nil at this point, while endIdxIter can be if the
	// range spans past the end of the file.

	if !startIdxIter.SeekGE(start) {
		// The range falls completely after this file.
		return 0, nil
	}
	startBH, err := startIdxIter.BlockHandleWithProperties()
	if err != nil {
		return 0, errCorruptIndexEntry(err)
	}

	props, err := r.ReadPropertiesBlock(ctx, nil /* buffer pool */)
	if err != nil {
		return 0, err
	}

	includeInterpolatedValueBlocksSize := func(dataBlockSize uint64) uint64 {
		// INVARIANT: props.DataSize > 0 since startIdxIter is not nil.
		// Linearly interpolate what is stored in value blocks.
		//
		// TODO(sumeer): if we need more accuracy, without loading any data blocks
		// (which contain the value handles, and which may also be insufficient if
		// the values are in separate files), we will need to accumulate the
		// logical size of the key-value pairs and store the cumulative value for
		// each data block in the index block entry. This increases the size of
		// the BlockHandle, so wait until this becomes necessary.
		return dataBlockSize +
			uint64((float64(dataBlockSize)/float64(props.DataSize))*
				float64(props.ValueBlocksSize))
	}
	if endIdxIter == nil {
		// The range spans beyond this file. Include data blocks through the last.
		return includeInterpolatedValueBlocksSize(props.DataSize - startBH.Offset), nil
	}
	if !endIdxIter.SeekGE(end) {
		// The range spans beyond this file. Include data blocks through the last.
		return includeInterpolatedValueBlocksSize(props.DataSize - startBH.Offset), nil
	}
	endBH, err := endIdxIter.BlockHandleWithProperties()
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

// BlockReader returns the block.Reader that can be used to directly read
// blocks from the sstable.
func (r *Reader) BlockReader() *block.Reader {
	return &r.blockReader
}

// NewReader returns a new table reader for the file. Closing the reader will
// close the file.
//
// The context is used for tracing any operations performed by NewReader; it is
// NOT stored for future use.
//
// In error cases, the objstorage.Readable is still open. The caller remains
// responsible for closing it if necessary.
func NewReader(ctx context.Context, f objstorage.Readable, o ReaderOptions) (*Reader, error) {
	if f == nil {
		return nil, errors.New("pebble/table: nil file")
	}
	o = o.ensureDefaults()

	r := &Reader{
		filterMetricsTracker: o.FilterMetricsTracker,
	}

	var preallocRH objstorageprovider.PreallocatedReadHandle
	rh := objstorageprovider.UsePreallocatedReadHandle(
		f, objstorage.ReadBeforeForNewReader, &preallocRH)
	defer func() { _ = rh.Close() }()

	footer, err := readFooter(ctx, f, rh, o.LoggerAndTracer, o.CacheOpts.FileNum)
	if err != nil {
		return nil, err
	}
	r.blockReader.Init(f, o.ReaderOptions, footer.checksum)
	r.tableFormat = footer.format
	r.indexBH = footer.indexBH
	r.metaindexBH = footer.metaindexBH
	r.footerBH = footer.footerBH

	// Read the metaindex and properties blocks.
	// We use a BufferPool when reading metaindex blocks in order to avoid
	// populating the block cache with these blocks. In heavy-write workloads,
	// especially with high compaction concurrency, new tables may be created
	// frequently. Populating the block cache with these metaindex blocks adds
	// additional contention on the block cache mutexes (see #1997).
	// Additionally, these blocks are exceedingly unlikely to be read again
	// while they're still in the block cache except in misconfigurations with
	// excessive sstables counts or a file cache that's far too small.
	bufferPool := metaBufferPools.Get().(*block.BufferPool)
	defer metaBufferPools.Put(bufferPool)
	// When we're finished, release the buffers we've allocated back to memory
	// allocator.
	defer bufferPool.Release()

	if err := r.initMetaindexBlocks(ctx, bufferPool, rh, o.Filters); err != nil {
		r.err = err
		return nil, err
	}

	props, err := r.readPropertiesBlockInternal(ctx, bufferPool, rh)
	if err != nil {
		r.err = err
		return nil, err
	}
	r.UserProperties = props.UserProperties

	// Set which attributes are in use based on property values.
	r.Attributes = props.toAttributes()
	if footer.format >= TableFormatPebblev7 && footer.attributes != r.Attributes {
		// For now we just verify that our derived attributes from the properties match the bitset
		// on the footer.
		r.err = base.CorruptionErrorf("pebble/table: %d: attributes mismatch: %s (footer) vs %s (derived)",
			errors.Safe(r.blockReader.FileNum()), errors.Safe(footer.attributes), errors.Safe(r.Attributes))
	}

	if props.ComparerName == "" || o.Comparer.Name == props.ComparerName {
		r.Comparer = o.Comparer
	} else if comparer, ok := o.Comparers[props.ComparerName]; ok {
		r.Comparer = comparer
	} else {
		r.err = errors.Errorf("pebble/table: %d: unknown comparer %s",
			errors.Safe(r.blockReader.FileNum()), errors.Safe(props.ComparerName))
	}

	if mergerName := props.MergerName; mergerName != "" && mergerName != "nullptr" {
		if o.Merger != nil && o.Merger.Name == mergerName {
			// opts.Merger matches.
		} else if _, ok := o.Mergers[mergerName]; ok {
			// Known merger.
		} else {
			r.err = errors.Errorf("pebble/table: %d: unknown merger %s",
				errors.Safe(r.blockReader.FileNum()), errors.Safe(props.MergerName))
		}
	}

	if r.tableFormat.BlockColumnar() {
		if ks, ok := o.KeySchemas[props.KeySchemaName]; ok {
			r.keySchema = ks
		} else {
			var known []string
			for name := range o.KeySchemas {
				known = append(known, fmt.Sprintf("%q", name))
			}
			slices.Sort(known)

			r.err = errors.Newf("pebble/table: %d: unknown key schema %q; known key schemas: %s",
				errors.Safe(r.blockReader.FileNum()), errors.Safe(props.KeySchemaName), errors.Safe(known))
			panic(r.err)
		}
	}

	if r.err != nil {
		return nil, r.err
	}
	return r, nil
}

// ReadableFile describes the smallest subset of vfs.File that is required for
// reading SSTs.
type ReadableFile interface {
	io.ReaderAt
	io.Closer
	Stat() (vfs.FileInfo, error)
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

// NewReadHandle is part of the objstorage.Readable interface.
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

// MakeTrivialReaderProvider creates a valblk.ReaderProvider which always
// returns the given reader. It should be used when the Reader will outlive the
// iterator tree.
func MakeTrivialReaderProvider(r *Reader) valblk.ReaderProvider {
	return (*trivialReaderProvider)(r)
}

// trivialReaderProvider implements valblk.ReaderProvider for a Reader that will
// outlive the top-level iterator in the iterator tree.
//
// Defining the type in this manner (as opposed to a struct) avoids allocation.
type trivialReaderProvider Reader

var _ valblk.ReaderProvider = (*trivialReaderProvider)(nil)

// GetReader implements ReaderProvider.
func (trp *trivialReaderProvider) GetReader(
	ctx context.Context,
) (valblk.ExternalBlockReader, error) {
	return (*Reader)(trp), nil
}

// Close implements ReaderProvider.
func (trp *trivialReaderProvider) Close() {}
