// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/keyspan/keyspanimpl"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/sstableinternal"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider/objiotracing"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/valblk"
)

var emptyIter = &errorIter{err: nil}
var emptyKeyspanIter = &errorKeyspanIter{err: nil}

// tableNewIters creates new iterators (point, range deletion and/or range key)
// for the given file metadata. Which of the various iterator kinds the user is
// requesting is specified with the iterKinds bitmap.
//
// On success, the requested subset of iters.{point,rangeDel,rangeKey} are
// populated with iterators.
//
// If a point iterator is requested and the operation was successful,
// iters.point is guaranteed to be non-nil and must be closed when the caller is
// finished.
//
// If a range deletion or range key iterator is requested, the corresponding
// iterator may be nil if the table does not contain any keys of the
// corresponding kind. The returned iterSet type provides RangeDeletion() and
// RangeKey() convenience methods that return non-nil empty iterators that may
// be used if the caller requires a non-nil iterator.
//
// On error, all iterators are nil.
//
// The only (non-test) implementation of tableNewIters is
// fileCacheContainer.newIters().
type tableNewIters func(
	ctx context.Context,
	file *manifest.FileMetadata,
	opts *IterOptions,
	internalOpts internalIterOpts,
	kinds iterKinds,
) (iterSet, error)

// tableNewRangeDelIter takes a tableNewIters and returns a TableNewSpanIter
// for the rangedel iterator returned by tableNewIters.
func tableNewRangeDelIter(newIters tableNewIters) keyspanimpl.TableNewSpanIter {
	return func(ctx context.Context, file *manifest.FileMetadata, iterOptions keyspan.SpanIterOptions) (keyspan.FragmentIterator, error) {
		iters, err := newIters(ctx, file, nil, internalIterOpts{}, iterRangeDeletions)
		if err != nil {
			return nil, err
		}
		return iters.RangeDeletion(), nil
	}
}

// tableNewRangeKeyIter takes a tableNewIters and returns a TableNewSpanIter
// for the range key iterator returned by tableNewIters.
func tableNewRangeKeyIter(newIters tableNewIters) keyspanimpl.TableNewSpanIter {
	return func(ctx context.Context, file *manifest.FileMetadata, iterOptions keyspan.SpanIterOptions) (keyspan.FragmentIterator, error) {
		iters, err := newIters(ctx, file, nil, internalIterOpts{}, iterRangeKeys)
		if err != nil {
			return nil, err
		}
		return iters.RangeKey(), nil
	}
}

// fileCacheOpts contains the db specific fields of a file cache. This is stored
// in the fileCacheContainer along with the file cache.
//
// NB: It is important to make sure that the fields in this struct are
// read-only. Since the fields here are shared by every single fileCacheShard,
// if non read-only fields are updated, we could have unnecessary evictions of
// those fields, and the surrounding fields from the CPU caches.
type fileCacheOpts struct {
	// iterCount keeps track of how many iterators are open. It is used to keep
	// track of leaked iterators on a per-db level.
	iterCount *atomic.Int32

	loggerAndTracer   LoggerAndTracer
	cache             *cache.Cache
	cacheID           cache.ID
	objProvider       objstorage.Provider
	readerOpts        sstable.ReaderOptions
	sstStatsCollector *block.CategoryStatsCollector
}

// fileCacheContainer contains the file cache and fields which are unique to the
// DB.
type fileCacheContainer struct {
	fileCache *FileCache

	// dbOpts contains fields relevant to the file cache which are unique to
	// each DB.
	dbOpts fileCacheOpts
}

// newFileCacheContainer will panic if the underlying block cache in the file
// cache doesn't match Options.Cache.
func newFileCacheContainer(
	fc *FileCache,
	cacheID cache.ID,
	objProvider objstorage.Provider,
	opts *Options,
	size int,
	sstStatsCollector *block.CategoryStatsCollector,
) *fileCacheContainer {
	// We will release a ref to the file cache acquired here when
	// fileCacheContainer.close is called.
	if fc != nil {
		if fc.cache != opts.Cache {
			panic("pebble: underlying cache for the file cache and db are different")
		}
		fc.Ref()
	} else {
		// NewFileCache should create a ref to fc which the container should
		// drop whenever it is closed.
		fc = NewFileCache(opts.Cache, opts.Experimental.FileCacheShards, size)
	}

	t := &fileCacheContainer{}
	t.fileCache = fc
	t.dbOpts.loggerAndTracer = opts.LoggerAndTracer
	t.dbOpts.cache = opts.Cache
	t.dbOpts.cacheID = cacheID
	t.dbOpts.objProvider = objProvider
	t.dbOpts.readerOpts = opts.MakeReaderOptions()
	t.dbOpts.readerOpts.FilterMetricsTracker = &sstable.FilterMetricsTracker{}
	t.dbOpts.iterCount = new(atomic.Int32)
	t.dbOpts.sstStatsCollector = sstStatsCollector
	return t
}

// Before calling close, make sure that there will be no further need
// to access any of the files associated with the store.
func (c *fileCacheContainer) close() error {
	// We want to do some cleanup work here. Check for leaked iterators
	// by the DB using this container. Note that we'll still perform cleanup
	// below in the case that there are leaked iterators.
	var err error
	if v := c.dbOpts.iterCount.Load(); v > 0 {
		err = errors.Errorf("leaked iterators: %d", errors.Safe(v))
	}

	// Release nodes here.
	for _, shard := range c.fileCache.shards {
		if shard != nil {
			shard.removeDB(&c.dbOpts)
		}
	}
	return firstError(err, c.fileCache.Unref())
}

func (c *fileCacheContainer) newIters(
	ctx context.Context,
	file *manifest.FileMetadata,
	opts *IterOptions,
	internalOpts internalIterOpts,
	kinds iterKinds,
) (iterSet, error) {
	return c.fileCache.getShard(file.FileBacking.DiskFileNum).newIters(ctx, file, opts, internalOpts, &c.dbOpts, kinds)
}

// getTableProperties returns the properties associated with the backing physical
// table if the input metadata belongs to a virtual sstable.
func (c *fileCacheContainer) getTableProperties(file *fileMetadata) (*sstable.Properties, error) {
	return c.fileCache.getShard(file.FileBacking.DiskFileNum).getTableProperties(file, &c.dbOpts)
}

func (c *fileCacheContainer) evict(fileNum base.DiskFileNum) {
	c.fileCache.getShard(fileNum).evict(fileNum, &c.dbOpts, false)
}

func (c *fileCacheContainer) metrics() (CacheMetrics, FilterMetrics) {
	var m CacheMetrics
	for i := range c.fileCache.shards {
		s := c.fileCache.shards[i]
		s.mu.RLock()
		m.Count += int64(len(s.mu.nodes))
		s.mu.RUnlock()
		m.Hits += s.hits.Load()
		m.Misses += s.misses.Load()
	}
	m.Size = m.Count * int64(unsafe.Sizeof(fileCacheNode{})+unsafe.Sizeof(fileCacheValue{})+unsafe.Sizeof(sstable.Reader{}))
	f := c.dbOpts.readerOpts.FilterMetricsTracker.Load()
	return m, f
}

func (c *fileCacheContainer) estimateSize(
	meta *fileMetadata, lower, upper []byte,
) (size uint64, err error) {
	c.withCommonReader(meta, func(cr sstable.CommonReader) error {
		size, err = cr.EstimateDiskUsage(lower, upper)
		return err
	})
	return size, err
}

// createCommonReader creates a Reader for this file.
func createCommonReader(v *fileCacheValue, file *fileMetadata) sstable.CommonReader {
	// TODO(bananabrick): We suffer an allocation if file is a virtual sstable.
	r := v.mustSSTableReader()
	var cr sstable.CommonReader = r
	if file.Virtual {
		virtualReader := sstable.MakeVirtualReader(
			r, file.VirtualMeta().VirtualReaderParams(v.isShared),
		)
		cr = &virtualReader
	}
	return cr
}

func (c *fileCacheContainer) withCommonReader(
	meta *fileMetadata, fn func(sstable.CommonReader) error,
) error {
	s := c.fileCache.getShard(meta.FileBacking.DiskFileNum)
	v := s.findNode(context.TODO(), meta.FileBacking, &c.dbOpts)
	defer s.unrefValue(v)
	if v.err != nil {
		return v.err
	}
	return fn(createCommonReader(v, meta))
}

func (c *fileCacheContainer) withReader(meta physicalMeta, fn func(*sstable.Reader) error) error {
	s := c.fileCache.getShard(meta.FileBacking.DiskFileNum)
	v := s.findNode(context.TODO(), meta.FileBacking, &c.dbOpts)
	defer s.unrefValue(v)
	if v.err != nil {
		return v.err
	}
	return fn(v.reader.(*sstable.Reader))
}

// withVirtualReader fetches a VirtualReader associated with a virtual sstable.
func (c *fileCacheContainer) withVirtualReader(
	meta virtualMeta, fn func(sstable.VirtualReader) error,
) error {
	s := c.fileCache.getShard(meta.FileBacking.DiskFileNum)
	v := s.findNode(context.TODO(), meta.FileBacking, &c.dbOpts)
	defer s.unrefValue(v)
	if v.err != nil {
		return v.err
	}
	provider := c.dbOpts.objProvider
	objMeta, err := provider.Lookup(fileTypeTable, meta.FileBacking.DiskFileNum)
	if err != nil {
		return err
	}
	return fn(sstable.MakeVirtualReader(v.mustSSTableReader(), meta.VirtualReaderParams(objMeta.IsShared())))
}

func (c *fileCacheContainer) iterCount() int64 {
	return int64(c.dbOpts.iterCount.Load())
}

// FileCache is a shareable cache for open files. Open files are exclusively
// sstable files today.
type FileCache struct {
	refs atomic.Int64

	cache  *Cache
	shards []*fileCacheShard
}

// Ref adds a reference to the file cache. Once a file cache is constructed, the
// cache only remains valid if there is at least one reference to it.
func (c *FileCache) Ref() {
	v := c.refs.Add(1)
	// We don't want the reference count to ever go from 0 -> 1,
	// cause a reference count of 0 implies that we've closed the cache.
	if v <= 1 {
		panic(fmt.Sprintf("pebble: inconsistent reference count: %d", v))
	}
}

// Unref removes a reference to the file cache.
func (c *FileCache) Unref() error {
	v := c.refs.Add(-1)
	switch {
	case v < 0:
		panic(fmt.Sprintf("pebble: inconsistent reference count: %d", v))
	case v == 0:
		var err error
		for i := range c.shards {
			// The cache shard is not allocated yet, nothing to close.
			if c.shards[i] == nil {
				continue
			}
			err = firstError(err, c.shards[i].Close())
		}

		// Unref the cache which we create a reference to when the file cache is
		// first instantiated.
		c.cache.Unref()
		return err
	}
	return nil
}

// NewFileCache will create a new file cache with one outstanding reference. It
// is the callers responsibility to call Unref if they will no longer hold a
// reference to the file cache.
func NewFileCache(cache *Cache, numShards int, size int) *FileCache {
	if size == 0 {
		panic("pebble: cannot create a file cache of size 0")
	} else if numShards == 0 {
		panic("pebble: cannot create a file cache with 0 shards")
	}

	c := &FileCache{}
	c.cache = cache
	c.cache.Ref()

	c.shards = make([]*fileCacheShard, numShards)
	for i := range c.shards {
		c.shards[i] = &fileCacheShard{}
		c.shards[i].init(size / len(c.shards))
	}

	// Hold a ref to the cache here.
	c.refs.Store(1)

	return c
}

func (c *FileCache) getShard(fileNum base.DiskFileNum) *fileCacheShard {
	return c.shards[uint64(fileNum)%uint64(len(c.shards))]
}

type fileCacheKey struct {
	cacheID cache.ID
	fileNum base.DiskFileNum
}

type fileCacheShard struct {
	hits      atomic.Int64
	misses    atomic.Int64
	iterCount atomic.Int32

	size int

	mu struct {
		sync.RWMutex
		nodes map[fileCacheKey]*fileCacheNode
		// The iters map is only created and populated in race builds.
		iters map[io.Closer][]byte

		handHot  *fileCacheNode
		handCold *fileCacheNode
		handTest *fileCacheNode

		coldTarget int
		sizeHot    int
		sizeCold   int
		sizeTest   int
	}
	releasing       sync.WaitGroup
	releasingCh     chan *fileCacheValue
	releaseLoopExit sync.WaitGroup
}

func (c *fileCacheShard) init(size int) {
	c.size = size

	c.mu.nodes = make(map[fileCacheKey]*fileCacheNode)
	c.mu.coldTarget = size
	c.releasingCh = make(chan *fileCacheValue, 100)
	c.releaseLoopExit.Add(1)
	go c.releaseLoop()

	if invariants.RaceEnabled {
		c.mu.iters = make(map[io.Closer][]byte)
	}
}

func (c *fileCacheShard) releaseLoop() {
	defer c.releaseLoopExit.Done()
	for v := range c.releasingCh {
		v.release(c)
	}
}

// checkAndIntersectFilters checks the specific table and block property filters
// for intersection with any available table and block-level properties. Returns
// true for ok if this table should be read by this iterator.
func checkAndIntersectFilters(
	r *sstable.Reader,
	blockPropertyFilters []BlockPropertyFilter,
	boundLimitedFilter sstable.BoundLimitedBlockPropertyFilter,
	syntheticSuffix sstable.SyntheticSuffix,
) (ok bool, filterer *sstable.BlockPropertiesFilterer, err error) {
	if boundLimitedFilter != nil || len(blockPropertyFilters) > 0 {
		filterer, err = sstable.IntersectsTable(
			blockPropertyFilters,
			boundLimitedFilter,
			r.Properties.UserProperties,
			syntheticSuffix,
		)
		// NB: IntersectsTable will return a nil filterer if the table-level
		// properties indicate there's no intersection with the provided filters.
		if filterer == nil || err != nil {
			return false, nil, err
		}
	}
	return true, filterer, nil
}

func (c *fileCacheShard) newIters(
	ctx context.Context,
	file *manifest.FileMetadata,
	opts *IterOptions,
	internalOpts internalIterOpts,
	dbOpts *fileCacheOpts,
	kinds iterKinds,
) (iterSet, error) {
	// TODO(sumeer): constructing the Reader should also use a plumbed context,
	// since parts of the sstable are read during the construction. The Reader
	// should not remember that context since the Reader can be long-lived.

	// Calling findNode gives us the responsibility of decrementing v's
	// refCount. If opening the underlying table resulted in error, then we
	// decrement this straight away. Otherwise, we pass that responsibility to
	// the sstable iterator, which decrements when it is closed.
	v := c.findNode(ctx, file.FileBacking, dbOpts)
	if v.err != nil {
		defer c.unrefValue(v)
		return iterSet{}, v.err
	}

	r := v.mustSSTableReader()
	// Note: This suffers an allocation for virtual sstables.
	cr := createCommonReader(v, file)
	var iters iterSet
	var err error
	if kinds.RangeKey() && file.HasRangeKeys {
		iters.rangeKey, err = newRangeKeyIter(ctx, r, file, cr, opts.SpanIterOptions(), internalOpts)
	}
	if kinds.RangeDeletion() && file.HasPointKeys && err == nil {
		iters.rangeDeletion, err = newRangeDelIter(ctx, file, cr, dbOpts, internalOpts)
	}
	if kinds.Point() && err == nil {
		iters.point, err = c.newPointIter(ctx, v, file, cr, opts, internalOpts, dbOpts)
	}
	if err != nil {
		// NB: There's a subtlety here: Because the point iterator is the last
		// iterator we attempt to create, it's not possible for:
		//   err != nil && iters.point != nil
		// If it were possible, we'd need to account for it to avoid double
		// unref-ing here, once during CloseAll and once during `unrefValue`.
		iters.CloseAll()
		c.unrefValue(v)
		return iterSet{}, err
	}
	// Only point iterators ever require the reader stay pinned in the cache. If
	// we're not returning a point iterator to the caller, we need to unref v.
	if iters.point == nil {
		c.unrefValue(v)
	}
	return iters, nil
}

// For flushable ingests, we decide whether to use the bloom filter base on
// size.
const filterBlockSizeLimitForFlushableIngests = 64 * 1024

// newPointIter is an internal helper that constructs a point iterator over a
// sstable. This function is for internal use only, and callers should use
// newIters instead.
func (c *fileCacheShard) newPointIter(
	ctx context.Context,
	v *fileCacheValue,
	file *manifest.FileMetadata,
	cr sstable.CommonReader,
	opts *IterOptions,
	internalOpts internalIterOpts,
	dbOpts *fileCacheOpts,
) (internalIterator, error) {
	var (
		hideObsoletePoints bool = false
		pointKeyFilters    []BlockPropertyFilter
		filterer           *sstable.BlockPropertiesFilterer
	)
	r := v.mustSSTableReader()
	if opts != nil {
		// This code is appending (at most one filter) in-place to
		// opts.PointKeyFilters even though the slice is shared for iterators in
		// the same iterator tree. This is acceptable since all the following
		// properties are true:
		// - The iterator tree is single threaded, so the shared backing for the
		//   slice is being mutated in a single threaded manner.
		// - Each shallow copy of the slice has its own notion of length.
		// - The appended element is always the obsoleteKeyBlockPropertyFilter
		//   struct, which is stateless, so overwriting that struct when creating
		//   one sstable iterator is harmless to other sstable iterators that are
		//   relying on that struct.
		//
		// An alternative would be to have different slices for different sstable
		// iterators, but that requires more work to avoid allocations.
		//
		// TODO(bilal): for compaction reads of foreign sstables, we do hide
		// obsolete points (see sstable.Reader.newCompactionIter) but we don't
		// apply the obsolete block property filter. We could optimize this by
		// applying the filter.
		hideObsoletePoints, pointKeyFilters =
			r.TryAddBlockPropertyFilterForHideObsoletePoints(
				opts.snapshotForHideObsoletePoints, file.LargestSeqNum, opts.PointKeyFilters)

		var ok bool
		var err error
		ok, filterer, err = checkAndIntersectFilters(r, pointKeyFilters,
			internalOpts.boundLimitedFilter, file.SyntheticPrefixAndSuffix.Suffix())
		if err != nil {
			return nil, err
		} else if !ok {
			// No point keys within the table match the filters.
			return nil, nil
		}
	}

	var iter sstable.Iterator
	filterBlockSizeLimit := sstable.AlwaysUseFilterBlock
	if opts != nil {
		// By default, we don't use block filters for L6 and restrict the size for
		// flushable ingests, as these blocks can be very big.
		if !opts.UseL6Filters {
			if opts.layer == manifest.Level(6) {
				filterBlockSizeLimit = sstable.NeverUseFilterBlock
			} else if opts.layer.IsFlushableIngests() {
				filterBlockSizeLimit = filterBlockSizeLimitForFlushableIngests
			}
		}
		if opts.layer.IsSet() && !opts.layer.IsFlushableIngests() {
			ctx = objiotracing.WithLevel(ctx, opts.layer.Level())
		}
	}

	if v.isShared && file.SyntheticSeqNum() != 0 {
		// The table is shared and ingested.
		hideObsoletePoints = true
	}
	transforms := file.IterTransforms()
	transforms.HideObsoletePoints = hideObsoletePoints
	iterStatsAccum := internalOpts.iterStatsAccumulator
	if iterStatsAccum == nil && opts != nil && dbOpts.sstStatsCollector != nil {
		iterStatsAccum = dbOpts.sstStatsCollector.Accumulator(
			uint64(uintptr(unsafe.Pointer(r))), opts.Category)
	}
	var err error
	if internalOpts.compaction {
		iter, err = cr.NewCompactionIter(transforms, block.ReadEnv{IterStats: iterStatsAccum, BufferPool: internalOpts.bufferPool}, &v.readerProvider)
	} else {
		iter, err = cr.NewPointIter(
			ctx, transforms, opts.GetLowerBound(), opts.GetUpperBound(), filterer, filterBlockSizeLimit,
			block.ReadEnv{Stats: internalOpts.stats, IterStats: iterStatsAccum}, &v.readerProvider)
	}
	if err != nil {
		return nil, err
	}
	// NB: v.closeHook takes responsibility for calling unrefValue(v) here. Take
	// care to avoid introducing an allocation here by adding a closure.
	iter.SetCloseHook(v.closeHook)
	c.iterCount.Add(1)
	dbOpts.iterCount.Add(1)
	if invariants.RaceEnabled {
		c.mu.Lock()
		c.mu.iters[iter] = debug.Stack()
		c.mu.Unlock()
	}
	return iter, nil
}

// newRangeDelIter is an internal helper that constructs an iterator over a
// sstable's range deletions. This function is for table-cache internal use
// only, and callers should use newIters instead.
func newRangeDelIter(
	ctx context.Context,
	file *manifest.FileMetadata,
	cr sstable.CommonReader,
	dbOpts *fileCacheOpts,
	internalOpts internalIterOpts,
) (keyspan.FragmentIterator, error) {
	// NB: range-del iterator does not maintain a reference to the table, nor
	// does it need to read from it after creation.
	readBlockEnv := block.ReadEnv{
		Stats:      internalOpts.stats,
		IterStats:  internalOpts.iterStatsAccumulator,
		BufferPool: nil,
	}
	rangeDelIter, err := cr.NewRawRangeDelIter(ctx, file.FragmentIterTransforms(), readBlockEnv)
	if err != nil {
		return nil, err
	}
	// Assert expected bounds in tests.
	if invariants.Sometimes(50) && rangeDelIter != nil {
		cmp := base.DefaultComparer.Compare
		if dbOpts.readerOpts.Comparer != nil {
			cmp = dbOpts.readerOpts.Comparer.Compare
		}
		rangeDelIter = keyspan.AssertBounds(
			rangeDelIter, file.SmallestPointKey, file.LargestPointKey.UserKey, cmp,
		)
	}
	return rangeDelIter, nil
}

// newRangeKeyIter is an internal helper that constructs an iterator over a
// sstable's range keys. This function is for table-cache internal use only, and
// callers should use newIters instead.
func newRangeKeyIter(
	ctx context.Context,
	r *sstable.Reader,
	file *fileMetadata,
	cr sstable.CommonReader,
	opts keyspan.SpanIterOptions,
	internalOpts internalIterOpts,
) (keyspan.FragmentIterator, error) {
	transforms := file.FragmentIterTransforms()
	// Don't filter a table's range keys if the file contains RANGEKEYDELs.
	// The RANGEKEYDELs may delete range keys in other levels. Skipping the
	// file's range key blocks may surface deleted range keys below. This is
	// done here, rather than deferring to the block-property collector in order
	// to maintain parity with point keys and the treatment of RANGEDELs.
	if r.Properties.NumRangeKeyDels == 0 && len(opts.RangeKeyFilters) > 0 {
		ok, _, err := checkAndIntersectFilters(r, opts.RangeKeyFilters, nil, transforms.SyntheticSuffix())
		if err != nil {
			return nil, err
		} else if !ok {
			return nil, nil
		}
	}
	// TODO(radu): wrap in an AssertBounds.
	readBlockEnv := block.ReadEnv{
		Stats:      internalOpts.stats,
		IterStats:  internalOpts.iterStatsAccumulator,
		BufferPool: nil,
	}
	return cr.NewRawRangeKeyIter(ctx, transforms, readBlockEnv)
}

// tableCacheShardReaderProvider implements sstable.ReaderProvider for a
// specific table.
type tableCacheShardReaderProvider struct {
	c              *fileCacheShard
	dbOpts         *fileCacheOpts
	backingFileNum base.DiskFileNum

	mu struct {
		sync.Mutex
		// v is the result of findNode. Whenever it is not null, we hold a refcount
		// on the fileCacheValue.
		v *fileCacheValue
		// refCount is the number of GetReader() calls that have not received a
		// corresponding Close().
		refCount int
	}
}

var _ valblk.ReaderProvider = &tableCacheShardReaderProvider{}

func (rp *tableCacheShardReaderProvider) init(
	c *fileCacheShard, dbOpts *fileCacheOpts, backingFileNum base.DiskFileNum,
) {
	rp.c = c
	rp.dbOpts = dbOpts
	rp.backingFileNum = backingFileNum
	rp.mu.v = nil
	rp.mu.refCount = 0
}

// GetReader implements sstable.ReaderProvider. Note that it is not the
// responsibility of tableCacheShardReaderProvider to ensure that the file
// continues to exist. The ReaderProvider is used in iterators where the
// top-level iterator is pinning the read state and preventing the files from
// being deleted.
//
// The caller must call tableCacheShardReaderProvider.Close.
//
// Note that currently the Reader returned here is only used to read value
// blocks. This reader shouldn't be used for other purposes like reading keys
// outside of virtual sstable bounds.
//
// TODO(bananabrick): We could return a wrapper over the Reader to ensure
// that the reader isn't used for other purposes.
func (rp *tableCacheShardReaderProvider) GetReader(
	ctx context.Context,
) (valblk.ExternalBlockReader, error) {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	if rp.mu.v != nil {
		rp.mu.refCount++
		return rp.mu.v.mustSSTableReader(), nil
	}

	// Calling findNodeInternal gives us the responsibility of decrementing v's
	// refCount. Note that if the table is no longer in the cache,
	// findNodeInternal will need to do IO to initialize a new Reader. We hold
	// rp.mu during this time so that concurrent GetReader calls block until the
	// Reader is created.
	v := rp.c.findNodeInternal(ctx, rp.backingFileNum, rp.dbOpts)
	if v.err != nil {
		defer rp.c.unrefValue(v)
		return nil, v.err
	}
	rp.mu.v = v
	rp.mu.refCount = 1
	return v.mustSSTableReader(), nil
}

// Close implements sstable.ReaderProvider.
func (rp *tableCacheShardReaderProvider) Close() {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	rp.mu.refCount--
	if rp.mu.refCount <= 0 {
		if rp.mu.refCount < 0 {
			panic("pebble: sstable.ReaderProvider misuse")
		}
		rp.c.unrefValue(rp.mu.v)
		rp.mu.v = nil
	}
}

// getTableProperties return sst table properties for target file.
func (c *fileCacheShard) getTableProperties(
	file *fileMetadata, dbOpts *fileCacheOpts,
) (*sstable.Properties, error) {
	// Calling findNode gives us the responsibility of decrementing v's refCount here
	v := c.findNode(context.TODO(), file.FileBacking, dbOpts)
	defer c.unrefValue(v)

	if v.err != nil {
		return nil, v.err
	}
	r := v.mustSSTableReader()
	return &r.Properties, nil
}

// releaseNode releases a node from the fileCacheShard.
//
// c.mu must be held when calling this.
func (c *fileCacheShard) releaseNode(n *fileCacheNode) {
	c.unlinkNode(n)
	c.clearNode(n)
}

// unlinkNode removes a node from the fileCacheShard, leaving the shard
// reference in place.
//
// c.mu must be held when calling this.
func (c *fileCacheShard) unlinkNode(n *fileCacheNode) {
	key := fileCacheKey{n.cacheID, n.fileNum}
	delete(c.mu.nodes, key)

	switch n.ptype {
	case fileCacheNodeHot:
		c.mu.sizeHot--
	case fileCacheNodeCold:
		c.mu.sizeCold--
	case fileCacheNodeTest:
		c.mu.sizeTest--
	}

	if n == c.mu.handHot {
		c.mu.handHot = c.mu.handHot.prev()
	}
	if n == c.mu.handCold {
		c.mu.handCold = c.mu.handCold.prev()
	}
	if n == c.mu.handTest {
		c.mu.handTest = c.mu.handTest.prev()
	}

	if n.unlink() == n {
		// This was the last entry in the cache.
		c.mu.handHot = nil
		c.mu.handCold = nil
		c.mu.handTest = nil
	}

	n.links.prev = nil
	n.links.next = nil
}

func (c *fileCacheShard) clearNode(n *fileCacheNode) {
	if v := n.value; v != nil {
		n.value = nil
		c.unrefValue(v)
	}
}

// unrefValue decrements the reference count for the specified value, releasing
// it if the reference count fell to 0. Note that the value has a reference if
// it is present in fileCacheShard.mu.nodes, so a reference count of 0 means the
// node has already been removed from that map.
func (c *fileCacheShard) unrefValue(v *fileCacheValue) {
	if v.refCount.Add(-1) == 0 {
		c.releasing.Add(1)
		c.releasingCh <- v
	}
}

// findNode returns the node for the table with the given file number, creating
// that node if it didn't already exist. The caller is responsible for
// decrementing the returned node's refCount.
func (c *fileCacheShard) findNode(
	ctx context.Context, b *fileBacking, dbOpts *fileCacheOpts,
) *fileCacheValue {
	// The backing must have a positive refcount (otherwise it could be deleted
	// at any time).
	b.MustHaveRefs()
	// Caution! Here b can be a physical or virtual sstable, or a blob file.
	// File cache sstable readers are associated with the physical backings. All
	// virtual tables with the same backing will use the same reader from the
	// cache; so no information that can differ among these virtual tables can
	// be passed to findNodeInternal.
	backingFileNum := b.DiskFileNum

	return c.findNodeInternal(ctx, backingFileNum, dbOpts)
}

func (c *fileCacheShard) findNodeInternal(
	ctx context.Context, backingFileNum base.DiskFileNum, dbOpts *fileCacheOpts,
) *fileCacheValue {
	// Fast-path for a hit in the cache.
	c.mu.RLock()
	key := fileCacheKey{dbOpts.cacheID, backingFileNum}
	if n := c.mu.nodes[key]; n != nil && n.value != nil {
		// Fast-path hit.
		//
		// The caller is responsible for decrementing the refCount.
		v := n.value
		v.refCount.Add(1)
		c.mu.RUnlock()
		n.referenced.Store(true)
		c.hits.Add(1)
		<-v.loaded
		return v
	}
	c.mu.RUnlock()

	c.mu.Lock()

	n := c.mu.nodes[key]
	switch {
	case n == nil:
		// Slow-path miss of a non-existent node.
		n = &fileCacheNode{
			fileNum: backingFileNum,
			ptype:   fileCacheNodeCold,
		}
		c.addNode(n, dbOpts)
		c.mu.sizeCold++

	case n.value != nil:
		// Slow-path hit of a hot or cold node.
		//
		// The caller is responsible for decrementing the refCount.
		v := n.value
		v.refCount.Add(1)
		n.referenced.Store(true)
		c.hits.Add(1)
		c.mu.Unlock()
		<-v.loaded
		return v

	default:
		// Slow-path miss of a test node.
		c.unlinkNode(n)
		c.mu.coldTarget++
		if c.mu.coldTarget > c.size {
			c.mu.coldTarget = c.size
		}

		n.referenced.Store(false)
		n.ptype = fileCacheNodeHot
		c.addNode(n, dbOpts)
		c.mu.sizeHot++
	}

	c.misses.Add(1)

	v := &fileCacheValue{
		loaded: make(chan struct{}),
	}
	v.readerProvider.init(c, dbOpts, backingFileNum)
	v.refCount.Store(2)
	// Cache the closure invoked when an iterator is closed. This avoids an
	// allocation on every call to newIters.
	v.closeHook = func(i sstable.Iterator) error {
		if invariants.RaceEnabled {
			c.mu.Lock()
			delete(c.mu.iters, i)
			c.mu.Unlock()
		}
		c.unrefValue(v)
		c.iterCount.Add(-1)
		dbOpts.iterCount.Add(-1)
		return nil
	}
	n.value = v

	c.mu.Unlock()

	// Note adding to the cache lists must complete before we begin loading the
	// table as a failure during load will result in the node being unlinked.
	v.load(ctx, backingFileNum, c, dbOpts)
	return v
}

func (c *fileCacheShard) addNode(n *fileCacheNode, dbOpts *fileCacheOpts) {
	c.evictNodes()
	n.cacheID = dbOpts.cacheID
	key := fileCacheKey{n.cacheID, n.fileNum}
	c.mu.nodes[key] = n

	n.links.next = n
	n.links.prev = n
	if c.mu.handHot == nil {
		// First element.
		c.mu.handHot = n
		c.mu.handCold = n
		c.mu.handTest = n
	} else {
		c.mu.handHot.link(n)
	}

	if c.mu.handCold == c.mu.handHot {
		c.mu.handCold = c.mu.handCold.prev()
	}
}

func (c *fileCacheShard) evictNodes() {
	for c.size <= c.mu.sizeHot+c.mu.sizeCold && c.mu.handCold != nil {
		c.runHandCold()
	}
}

func (c *fileCacheShard) runHandCold() {
	n := c.mu.handCold
	if n.ptype == fileCacheNodeCold {
		if n.referenced.Load() {
			n.referenced.Store(false)
			n.ptype = fileCacheNodeHot
			c.mu.sizeCold--
			c.mu.sizeHot++
		} else {
			c.clearNode(n)
			n.ptype = fileCacheNodeTest
			c.mu.sizeCold--
			c.mu.sizeTest++
			for c.size < c.mu.sizeTest && c.mu.handTest != nil {
				c.runHandTest()
			}
		}
	}

	c.mu.handCold = c.mu.handCold.next()

	for c.size-c.mu.coldTarget <= c.mu.sizeHot && c.mu.handHot != nil {
		c.runHandHot()
	}
}

func (c *fileCacheShard) runHandHot() {
	if c.mu.handHot == c.mu.handTest && c.mu.handTest != nil {
		c.runHandTest()
		if c.mu.handHot == nil {
			return
		}
	}

	n := c.mu.handHot
	if n.ptype == fileCacheNodeHot {
		if n.referenced.Load() {
			n.referenced.Store(false)
		} else {
			n.ptype = fileCacheNodeCold
			c.mu.sizeHot--
			c.mu.sizeCold++
		}
	}

	c.mu.handHot = c.mu.handHot.next()
}

func (c *fileCacheShard) runHandTest() {
	if c.mu.sizeCold > 0 && c.mu.handTest == c.mu.handCold && c.mu.handCold != nil {
		c.runHandCold()
		if c.mu.handTest == nil {
			return
		}
	}

	n := c.mu.handTest
	if n.ptype == fileCacheNodeTest {
		c.mu.coldTarget--
		if c.mu.coldTarget < 0 {
			c.mu.coldTarget = 0
		}
		c.unlinkNode(n)
		c.clearNode(n)
	}

	c.mu.handTest = c.mu.handTest.next()
}

func (c *fileCacheShard) evict(fileNum base.DiskFileNum, dbOpts *fileCacheOpts, allowLeak bool) {
	c.mu.Lock()
	key := fileCacheKey{dbOpts.cacheID, fileNum}
	n := c.mu.nodes[key]
	var v *fileCacheValue
	if n != nil {
		// NB: This is equivalent to fileCacheShard.releaseNode(), but we
		// perform the fileCacheShard.release() call synchronously below to
		// ensure the sstable file descriptor is closed before returning. Note
		// that fileCacheShard.releasing needs to be incremented while holding
		// fileCacheShard.mu in order to avoid a race with Close()
		c.unlinkNode(n)
		v = n.value
		if v != nil {
			if !allowLeak {
				if t := v.refCount.Add(-1); t != 0 {
					dbOpts.loggerAndTracer.Fatalf("sstable %s: refcount is not zero: %d\n%s", fileNum, t, debug.Stack())
				}
			}
			c.releasing.Add(1)
		}
	}

	c.mu.Unlock()

	if v != nil {
		v.release(c)
	}

	dbOpts.cache.EvictFile(dbOpts.cacheID, fileNum)
}

// removeDB evicts any nodes which have a reference to the DB
// associated with dbOpts.cacheID. Make sure that there will
// be no more accesses to the files associated with the DB.
func (c *fileCacheShard) removeDB(dbOpts *fileCacheOpts) {
	var fileNums []base.DiskFileNum

	c.mu.RLock()
	// Collect the fileNums which need to be cleaned.
	var firstNode *fileCacheNode
	node := c.mu.handHot
	for node != firstNode {
		if firstNode == nil {
			firstNode = node
		}

		if node.cacheID == dbOpts.cacheID {
			fileNums = append(fileNums, node.fileNum)
		}
		node = node.next()
	}
	c.mu.RUnlock()

	// Evict all the nodes associated with the DB.
	// This should synchronously close all the files
	// associated with the DB.
	for _, fileNum := range fileNums {
		c.evict(fileNum, dbOpts, true)
	}
}

func (c *fileCacheShard) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check for leaked iterators. Note that we'll still perform cleanup below in
	// the case that there are leaked iterators.
	var err error
	if v := c.iterCount.Load(); v > 0 {
		if !invariants.RaceEnabled {
			err = errors.Errorf("leaked iterators: %d", errors.Safe(v))
		} else {
			var buf bytes.Buffer
			for _, stack := range c.mu.iters {
				fmt.Fprintf(&buf, "%s\n", stack)
			}
			err = errors.Errorf("leaked iterators: %d\n%s", errors.Safe(v), buf.String())
		}
	}

	for c.mu.handHot != nil {
		n := c.mu.handHot
		if n.value != nil {
			if n.value.refCount.Add(-1) == 0 {
				c.releasing.Add(1)
				c.releasingCh <- n.value
			}
		}
		c.unlinkNode(n)
	}
	c.mu.nodes = nil
	c.mu.handHot = nil
	c.mu.handCold = nil
	c.mu.handTest = nil

	// Only shutdown the releasing goroutine if there were no leaked
	// iterators. If there were leaked iterators, we leave the goroutine running
	// and the releasingCh open so that a subsequent iterator close can
	// complete. This behavior is used by iterator leak tests. Leaking the
	// goroutine for these tests is less bad not closing the iterator which
	// triggers other warnings about block cache handles not being released.
	if err != nil {
		c.releasing.Wait()
		return err
	}

	close(c.releasingCh)
	c.releasing.Wait()
	c.releaseLoopExit.Wait()
	return err
}

type fileCacheValue struct {
	closeHook func(i sstable.Iterator) error
	reader    io.Closer // *sstable.Reader
	err       error
	loaded    chan struct{}
	// Reference count for the value. The reader is closed when the reference
	// count drops to zero.
	refCount atomic.Int32
	isShared bool

	// readerProvider is embedded here so that we only allocate it once as long as
	// the table stays in the cache. Its state is not always logically tied to
	// this specific fileCacheShard - if a table goes out of the cache and then
	// comes back in, the readerProvider in a now-defunct fileCacheValue can
	// still be used and will internally refer to the new fileCacheValue.
	readerProvider tableCacheShardReaderProvider
}

// mustSSTable retrieves the value's *sstable.Reader. It panics if the cached
// file is not a sstable (i.e., it is a blob file).
func (v *fileCacheValue) mustSSTableReader() *sstable.Reader {
	return v.reader.(*sstable.Reader)
}

func (v *fileCacheValue) load(
	ctx context.Context, backingFileNum base.DiskFileNum, c *fileCacheShard, dbOpts *fileCacheOpts,
) {
	// Try opening the file first.
	var f objstorage.Readable
	var r *sstable.Reader
	var err error
	f, err = dbOpts.objProvider.OpenForReading(
		ctx, fileTypeTable, backingFileNum, objstorage.OpenOptions{MustExist: true},
	)
	if err == nil {
		o := dbOpts.readerOpts
		o.CacheOpts = sstableinternal.CacheOptions{
			Cache:   dbOpts.cache,
			CacheID: dbOpts.cacheID,
			FileNum: backingFileNum,
		}
		r, err = sstable.NewReader(ctx, f, o)
	}
	if err == nil {
		v.reader = r
		var objMeta objstorage.ObjectMetadata
		objMeta, err = dbOpts.objProvider.Lookup(fileTypeTable, backingFileNum)
		v.isShared = objMeta.IsShared()
	}
	if err != nil {
		v.err = errors.Wrapf(
			err, "pebble: backing file %s error", backingFileNum)
	}
	if v.err != nil {
		c.mu.Lock()
		defer c.mu.Unlock()
		// Lookup the node in the cache again as it might have already been
		// removed.
		key := fileCacheKey{dbOpts.cacheID, backingFileNum}
		n := c.mu.nodes[key]
		if n != nil && n.value == v {
			c.releaseNode(n)
		}
	}
	close(v.loaded)
}

func (v *fileCacheValue) release(c *fileCacheShard) {
	<-v.loaded
	// Nothing to be done about an error at this point. Close the reader if it is
	// open.
	if v.reader != nil {
		_ = v.reader.Close()
	}
	c.releasing.Done()
}

type fileCacheNodeType int8

const (
	fileCacheNodeTest fileCacheNodeType = iota
	fileCacheNodeCold
	fileCacheNodeHot
)

func (p fileCacheNodeType) String() string {
	switch p {
	case fileCacheNodeTest:
		return "test"
	case fileCacheNodeCold:
		return "cold"
	case fileCacheNodeHot:
		return "hot"
	}
	return "unknown"
}

type fileCacheNode struct {
	fileNum base.DiskFileNum
	value   *fileCacheValue

	links struct {
		next *fileCacheNode
		prev *fileCacheNode
	}
	ptype fileCacheNodeType
	// referenced is atomically set to indicate that this entry has been accessed
	// since the last time one of the clock hands swept it.
	referenced atomic.Bool

	// Storing the cache id associated with the DB instance here
	// avoids the need to thread the dbOpts struct through many functions.
	cacheID cache.ID
}

func (n *fileCacheNode) next() *fileCacheNode {
	if n == nil {
		return nil
	}
	return n.links.next
}

func (n *fileCacheNode) prev() *fileCacheNode {
	if n == nil {
		return nil
	}
	return n.links.prev
}

func (n *fileCacheNode) link(s *fileCacheNode) {
	s.links.prev = n.links.prev
	s.links.prev.links.next = s
	s.links.next = n
	s.links.next.links.prev = s
}

func (n *fileCacheNode) unlink() *fileCacheNode {
	next := n.links.next
	n.links.prev.links.next = n.links.next
	n.links.next.links.prev = n.links.prev
	n.links.prev = n
	n.links.next = n
	return next
}

// iterSet holds a set of iterators of various key kinds, all constructed over
// the same data structure (eg, an sstable). A subset of the fields may be
// populated depending on the `iterKinds` passed to newIters.
type iterSet struct {
	point         internalIterator
	rangeDeletion keyspan.FragmentIterator
	rangeKey      keyspan.FragmentIterator
}

// TODO(jackson): Consider adding methods for fast paths that check whether an
// iterator of a particular kind is nil, so that these call sites don't need to
// reach into the struct's fields directly.

// Point returns the contained point iterator. If there is no point iterator,
// Point returns a non-nil empty point iterator.
func (s *iterSet) Point() internalIterator {
	if s.point == nil {
		return emptyIter
	}
	return s.point
}

// RangeDeletion returns the contained range deletion iterator. If there is no
// range deletion iterator, RangeDeletion returns a non-nil empty keyspan
// iterator.
func (s *iterSet) RangeDeletion() keyspan.FragmentIterator {
	if s.rangeDeletion == nil {
		return emptyKeyspanIter
	}
	return s.rangeDeletion
}

// RangeKey returns the contained range key iterator. If there is no range key
// iterator, RangeKey returns a non-nil empty keyspan iterator.
func (s *iterSet) RangeKey() keyspan.FragmentIterator {
	if s.rangeKey == nil {
		return emptyKeyspanIter
	}
	return s.rangeKey
}

// CloseAll closes all of the held iterators. If CloseAll is called, then Close
// must be not be called on the constituent iterators.
func (s *iterSet) CloseAll() error {
	var err error
	if s.point != nil {
		err = s.point.Close()
		s.point = nil
	}
	if s.rangeDeletion != nil {
		s.rangeDeletion.Close()
		s.rangeDeletion = nil
	}
	if s.rangeKey != nil {
		s.rangeKey.Close()
		s.rangeKey = nil
	}
	return err
}

// iterKinds is a bitmap indicating a set of kinds of iterators. Callers may
// bitwise-OR iterPointKeys, iterRangeDeletions and/or iterRangeKeys together to
// represent a set of desired iterator kinds.
type iterKinds uint8

func (t iterKinds) Point() bool         { return (t & iterPointKeys) != 0 }
func (t iterKinds) RangeDeletion() bool { return (t & iterRangeDeletions) != 0 }
func (t iterKinds) RangeKey() bool      { return (t & iterRangeKeys) != 0 }

const (
	iterPointKeys iterKinds = 1 << iota
	iterRangeDeletions
	iterRangeKeys
)
