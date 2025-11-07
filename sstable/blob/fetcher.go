// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package blob

import (
	"context"
	"slices"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable/block"
)

// A ValueReader is an interface defined over a file that can be used to read
// value blocks.
type ValueReader interface {
	// IndexHandle returns the handle for the file's index block.
	IndexHandle() block.Handle

	// InitReadHandle initializes a ReadHandle for the file, using the provided
	// preallocated read handle to avoid an allocation.
	InitReadHandle(rh *objstorageprovider.PreallocatedReadHandle) objstorage.ReadHandle

	// ReadValueBlock retrieves a value block described by the provided block
	// handle from the block cache, or reads it from the blob file if it's not
	// already cached.
	ReadValueBlock(context.Context, block.ReadEnv, objstorage.ReadHandle,
		block.Handle) (block.BufferHandle, error)

	// ReadIndexBlock retrieves the index block from the block cache, or reads
	// it from the blob file if it's not already cached.
	ReadIndexBlock(context.Context, block.ReadEnv, objstorage.ReadHandle) (block.BufferHandle, error)
}

// A ReaderProvider is an interface that can be used to retrieve a ValueReader
// for a given file number.
type ReaderProvider interface {
	// GetValueReader returns a ValueReader for the given object.
	GetValueReader(
		ctx context.Context, obj base.ObjectInfo, stats block.InitFileReadStats,
	) (r ValueReader, closeFunc func(), err error)
}

// SuggestedCachedReaders returns the suggested default number of cached readers
// to use for ValueFetcher.Init given the number of non-empty levels in the LSM.
func SuggestedCachedReaders(readAmp int) int {
	return 5 * max(1, readAmp)
}

// cachedReaderPool is a pool of cachedReaderSets. It's used to recycle
// []cachedReaders. Recycling is done explicitly via a pool, rather than
// retaining a recycled iterator's pool, because not all iterators need to
// retrieve separated values.
var cachedReaderPool sync.Pool = sync.Pool{
	New: func() interface{} {
		return &cachedReaderSet{}
	},
}

// minAllocedReaders is the minimum number of cached readers to allocate when
// allocating a new cachedReaderSet. Some ValueFetchers only use a small
// quantity of cached readers, and setting a minimum helps reduce allocations
// assuming typical read-amp of ~7.
const minAllocedReaders = 35

type cachedReaderSet struct {
	readers []cachedReader
}

// A ValueFetcher retrieves values stored out-of-band in separate blob files.
// The ValueFetcher caches accessed file readers to avoid redundant file cache
// and block cache lookups when performing consecutive value retrievals.
//
// A single ValueFetcher can be used to fetch values from multiple files, and it
// will internally cache readers for each file.
//
// When finished with a ValueFetcher, one must call Close to release all cached
// readers and block buffers.
type ValueFetcher struct {
	fileMapping      base.BlobFileMapping
	readerProvider   ReaderProvider
	env              block.ReadEnv
	fetchCount       int
	bufMangler       invariants.BufMangler
	maxCachedReaders int
	// cached is nil until the first call to retrieve.
	cached *cachedReaderSet
}

// TODO(jackson): Support setting up a read handle for compaction when relevant.

// Assert that ValueFetcher implements the ValueFetcher interface.
var _ base.ValueFetcher = (*ValueFetcher)(nil)

// Init initializes the ValueFetcher.
func (r *ValueFetcher) Init(
	fm base.BlobFileMapping, rp ReaderProvider, env block.ReadEnv, maxCachedReaders int,
) {
	r.fileMapping = fm
	r.readerProvider = rp
	r.env = env
	if r.readerProvider == nil {
		panic("readerProvider is nil")
	}
	r.maxCachedReaders = maxCachedReaders
}

// FetchHandle returns the value, given the handle. FetchHandle must not be
// called after Close.
func (r *ValueFetcher) FetchHandle(
	ctx context.Context, handle []byte, blobFileID base.BlobFileID, valLen uint32, buf []byte,
) (val []byte, callerOwned bool, err error) {
	handleSuffix := DecodeHandleSuffix(handle)
	vh := Handle{
		BlobFileID: blobFileID,
		ValueLen:   valLen,
		BlockID:    handleSuffix.BlockID,
		ValueID:    handleSuffix.ValueID,
	}
	v, err := r.retrieve(ctx, vh)
	if err == nil && len(v) != int(vh.ValueLen) {
		return nil, false,
			errors.AssertionFailedf("value length mismatch: %d != %d", len(v), vh.ValueLen)
	}
	if invariants.Enabled {
		v = r.bufMangler.MaybeMangleLater(v)
	}
	return v, false, err
}

// Fetch is like FetchHandle, but it constructs handle and does not
// validate the value length. Fetch must not be called after Close.
func (r *ValueFetcher) Fetch(
	ctx context.Context, blobFileID base.BlobFileID, blockID BlockID, valueID BlockValueID,
) (val []byte, callerOwned bool, err error) {
	vh := Handle{
		BlobFileID: blobFileID,
		BlockID:    blockID,
		ValueID:    valueID,
	}
	v, err := r.retrieve(ctx, vh)
	if invariants.Enabled {
		v = r.bufMangler.MaybeMangleLater(v)
	}
	return v, false, err
}

func (r *ValueFetcher) retrieve(ctx context.Context, vh Handle) (val []byte, err error) {
	if r.cached == nil {
		r.cached = cachedReaderPool.Get().(*cachedReaderSet)
		allocSize := max(minAllocedReaders, r.maxCachedReaders)
		r.cached.readers = slices.Grow(r.cached.readers[:0], allocSize)[:r.maxCachedReaders]
	}

	// Look for a cached reader for the file. Also, find the least-recently used
	// reader. If we don't find a cached reader, we'll replace the
	// least-recently used reader with the new one for the file indicated by
	// vh.FileNum.
	var cr *cachedReader
	var oldestFetchIndex int
	// TODO(jackson): Reconsider this O(len(readers)) scan.
	for i := range r.cached.readers {
		if r.cached.readers[i].blobFileID == vh.BlobFileID && r.cached.readers[i].r != nil {
			cr = &r.cached.readers[i]
			break
		} else if r.cached.readers[i].lastFetchCount < r.cached.readers[oldestFetchIndex].lastFetchCount {
			oldestFetchIndex = i
		}
	}

	if cr == nil {
		// No cached reader found for the file. Get one from the file cache.
		cr = &r.cached.readers[oldestFetchIndex]
		// Release the previous reader, if any.
		if cr.r != nil {
			if err = cr.Close(); err != nil {
				return nil, err
			}
		}
		obj, ok := r.fileMapping.Lookup(vh.BlobFileID)
		if !ok {
			return nil, errors.AssertionFailedf("blob file %s not found", vh.BlobFileID)
		}
		if cr.r, cr.closeFunc, err = r.readerProvider.GetValueReader(ctx, obj, block.InitFileReadStats{
			Stats:     r.env.Stats,
			IterStats: r.env.IterStats,
		}); err != nil {
			return nil, err
		}
		cr.blobFileID = vh.BlobFileID
		_, cr.diskFileNum = obj.FileInfo()
		cr.rh = cr.r.InitReadHandle(&cr.preallocRH)
		if r.env.Stats != nil {
			r.env.Stats.SeparatedPointValue.ReaderCacheMisses++
		}
	}

	if r.env.Stats != nil {
		r.env.Stats.SeparatedPointValue.CountFetched++
		r.env.Stats.SeparatedPointValue.ValueBytesFetched += uint64(vh.ValueLen)
	}

	r.fetchCount++
	cr.lastFetchCount = r.fetchCount
	val, err = cr.GetUnsafeValue(ctx, vh, r.env)
	return val, err
}

// Close closes the ValueFetcher and releases all cached readers. Once Close is
// called, the ValueFetcher is no longer usable.
func (r *ValueFetcher) Close() error {
	var err error
	if r.cached != nil {
		for i := range r.cached.readers {
			if r.cached.readers[i].r != nil {
				err = errors.CombineErrors(err, r.cached.readers[i].Close())
			}
		}
		cachedReaderPool.Put(r.cached)
		r.cached = nil
	}
	r.fileMapping = nil
	r.readerProvider = nil
	r.env = block.ReadEnv{}
	r.fetchCount = 0
	r.bufMangler = invariants.BufMangler{}
	r.maxCachedReaders = 0
	return err
}

// cachedReader holds a Reader into an open file, and possibly blocks retrieved
// from the block cache.
type cachedReader struct {
	blobFileID     base.BlobFileID
	diskFileNum    base.DiskFileNum
	r              ValueReader
	closeFunc      func()
	rh             objstorage.ReadHandle
	lastFetchCount int
	// indexBlock holds the index block for the file, lazily loaded on the first
	// call to GetUnsafeValue.
	indexBlock struct {
		// loaded indicates whether buf and dec are valid.
		loaded bool
		buf    block.BufferHandle
		dec    *indexBlockDecoder
	}
	// currentValueBlock holds the currently loaded blob value block, if any.
	currentValueBlock struct {
		// loaded indicates whether a block is currently loaded.
		loaded bool
		// virtualID is the virtual block ID used to retrieve the block. If the
		// blob file has not been rewritten, this equals the physicalIndex.
		virtualID BlockID
		// valueIDOffset is the offset that should be added to the value ID to
		// get the index of the value within the physical block for any blob
		// handles encoding a block ID of virtualID.
		valueIDOffset BlockValueID
		// physicalIndex is the physical index of the current value block.
		// physicalIndex is in the range [0, indexBlock.dec.BlockCount()).
		physicalIndex int
		buf           block.BufferHandle
		dec           *blobValueBlockDecoder
	}
	preallocRH objstorageprovider.PreallocatedReadHandle
}

// GetUnsafeValue retrieves the value for the given handle. The value is
// returned as a byte slice pointing directly into the block cache's data. The
// value is only guaranteed to be stable until the next call to GetUnsafeValue
// or until the cachedReader is closed.
func (cr *cachedReader) GetUnsafeValue(
	ctx context.Context, vh Handle, env block.ReadEnv,
) ([]byte, error) {
	valueID := vh.ValueID

	// Determine which block contains the value.
	//
	// If we already have a block loaded (eg, we're scanning retrieving multiple
	// values), the current block might contain the value.
	if !cr.currentValueBlock.loaded || cr.currentValueBlock.virtualID != vh.BlockID {
		if !cr.indexBlock.loaded {
			// Read the index block.
			var err error
			cr.indexBlock.buf, err = cr.r.ReadIndexBlock(ctx, env, cr.rh)
			if err != nil {
				return nil, err
			}
			cr.indexBlock.dec = block.CastMetadata[indexBlockDecoder](cr.indexBlock.buf.BlockMetadata())
			cr.indexBlock.loaded = true
		}

		// Determine which physical block contains the value. If this blob file
		// has never been rewritten, the BlockID is the physical index of the
		// block containing the value. If the blob file has been rewritten, we
		// need to remap the 'virtual' BlockID to the physical block index using
		// the virtualBlocks column. We also retrieve a 'value ID offset' which
		// should be added to the value handle's value ID to get the index of
		// the value within the physical block.
		var physicalBlockIndex int = int(vh.BlockID)
		var valueIDOffset BlockValueID
		if cr.indexBlock.dec.virtualBlockCount > 0 {
			physicalBlockIndex, valueIDOffset = cr.indexBlock.dec.RemapVirtualBlockID(vh.BlockID)
			if valueIDOffset == virtualBlockIndexMask {
				return nil, errors.AssertionFailedf("blob file indicates virtual block ID %d in %s should be unreferenced",
					vh.BlockID, vh.BlobFileID)
			}
		}
		invariants.CheckBounds(physicalBlockIndex, cr.indexBlock.dec.BlockCount())

		// Retrieve the block's handle, and read the blob value block into
		// memory.
		//
		// TODO(jackson): If the blob file has been rewritten, it's possible
		// that we already have the physical block in-memory because we
		// previously were accessing it under a different BlockID. We expect
		// this case to be rare, and this is a hot path for the more common case
		// of non-rewritten blob files, so we defer optimizing for now.
		h := cr.indexBlock.dec.BlockHandle(physicalBlockIndex)
		// Nil out the decoder before releasing the buffers to ensure the Go GC
		// doesn't misinterpret the freed memory backing the decoders.
		cr.currentValueBlock.dec = nil
		cr.currentValueBlock.buf.Release()
		cr.currentValueBlock.loaded = false
		var err error
		cr.currentValueBlock.buf, err = cr.r.ReadValueBlock(ctx, env, cr.rh, h)
		if err != nil {
			return nil, err
		}
		cr.currentValueBlock.dec = block.CastMetadata[blobValueBlockDecoder](cr.currentValueBlock.buf.BlockMetadata())
		cr.currentValueBlock.physicalIndex = physicalBlockIndex
		cr.currentValueBlock.virtualID = vh.BlockID
		cr.currentValueBlock.valueIDOffset = valueIDOffset
		cr.currentValueBlock.loaded = true
	}

	// If the ReadEnv is configured with a value-retrieval profile, record the
	// value retrieval to it. This is used to allow runtime profiling of value
	// retrievals, providing observability into which codepaths are responsible
	// for the comparatively expensive value retrievals.
	if env.ValueRetrievalProfile != nil {
		env.ValueRetrievalProfile.Record(int64(vh.ValueLen))
	}

	// Convert the ValueID to an index into the block's values. When a blob file
	// is first constructed, the ValueID == the index. However when a blob file
	// is rewritten, multiple blocks from the original blob file may be combined
	// into the same physical block. To translate the ValueID to the
	// apppropriate index, we need to add the 'virtual block' valueIDOffset.
	valueIndex := int(valueID) + int(cr.currentValueBlock.valueIDOffset)
	invariants.CheckBounds(valueIndex, cr.currentValueBlock.dec.bd.Rows())
	v := cr.currentValueBlock.dec.values.Slice(cr.currentValueBlock.dec.values.Offsets(valueIndex))
	return v, nil
}

// Close releases resources associated with the reader.
func (cfr *cachedReader) Close() (err error) {
	if cfr.rh != nil {
		err = cfr.rh.Close()
	}
	// Nil out the decoders before releasing the buffers to ensure the Go GC
	// doesn't misinterpret the freed memory backing the decoders.
	cfr.indexBlock.dec = nil
	cfr.currentValueBlock.dec = nil
	cfr.indexBlock.buf.Release()
	cfr.currentValueBlock.buf.Release()
	// Release the cfg.Reader. closeFunc is provided by the file cache and
	// decrements the refcount on the open file reader.
	cfr.closeFunc()
	*cfr = cachedReader{}
	return err
}
