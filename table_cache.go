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
	"runtime/pprof"
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
// tableCacheContainer.newIters().
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

var tableCacheLabels = pprof.Labels("pebble", "table-cache")

// tableCacheOpts contains the db specific fields
// of a table cache. This is stored in the tableCacheContainer
// along with the table cache.
// NB: It is important to make sure that the fields in this
// struct are read-only. Since the fields here are shared
// by every single tableCacheShard, if non read-only fields
// are updated, we could have unnecessary evictions of those
// fields, and the surrounding fields from the CPU caches.
type tableCacheOpts struct {
	// iterCount keeps track of how many iterators are open. It is used to keep
	// track of leaked iterators on a per-db level.
	iterCount *atomic.Int32

	loggerAndTracer   LoggerAndTracer
	cache             *cache.Cache
	cacheID           cache.ID
	objProvider       objstorage.Provider
	readerOpts        sstable.ReaderOptions
	sstStatsCollector *sstable.CategoryStatsCollector
}

// tableCacheContainer contains the table cache and
// fields which are unique to the DB.
type tableCacheContainer struct {
	tableCache *TableCache

	// dbOpts contains fields relevant to the table cache
	// which are unique to each DB.
	dbOpts tableCacheOpts
}

// newTableCacheContainer will panic if the underlying cache in the table cache
// doesn't match Options.Cache.
func newTableCacheContainer(
	tc *TableCache,
	cacheID cache.ID,
	objProvider objstorage.Provider,
	opts *Options,
	size int,
	sstStatsCollector *sstable.CategoryStatsCollector,
) *tableCacheContainer {
	// We will release a ref to table cache acquired here when tableCacheContainer.close is called.
	if tc != nil {
		if tc.cache != opts.Cache {
			panic("pebble: underlying cache for the table cache and db are different")
		}
		tc.Ref()
	} else {
		// NewTableCache should create a ref to tc which the container should
		// drop whenever it is closed.
		tc = NewTableCache(opts.Cache, opts.Experimental.TableCacheShards, size)
	}

	t := &tableCacheContainer{}
	t.tableCache = tc
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
func (c *tableCacheContainer) close() error {
	// We want to do some cleanup work here. Check for leaked iterators
	// by the DB using this container. Note that we'll still perform cleanup
	// below in the case that there are leaked iterators.
	var err error
	if v := c.dbOpts.iterCount.Load(); v > 0 {
		err = errors.Errorf("leaked iterators: %d", errors.Safe(v))
	}

	// Release nodes here.
	for _, shard := range c.tableCache.shards {
		if shard != nil {
			shard.removeDB(&c.dbOpts)
		}
	}
	return firstError(err, c.tableCache.Unref())
}

func (c *tableCacheContainer) newIters(
	ctx context.Context,
	file *manifest.FileMetadata,
	opts *IterOptions,
	internalOpts internalIterOpts,
	kinds iterKinds,
) (iterSet, error) {
	return c.tableCache.getShard(file.FileBacking.DiskFileNum).newIters(ctx, file, opts, internalOpts, &c.dbOpts, kinds)
}

// getTableProperties returns the properties associated with the backing physical
// table if the input metadata belongs to a virtual sstable.
func (c *tableCacheContainer) getTableProperties(file *fileMetadata) (*sstable.Properties, error) {
	return c.tableCache.getShard(file.FileBacking.DiskFileNum).getTableProperties(file, &c.dbOpts)
}

func (c *tableCacheContainer) evict(fileNum base.DiskFileNum) {
	c.tableCache.getShard(fileNum).evict(fileNum, &c.dbOpts, false)
}

func (c *tableCacheContainer) metrics() (CacheMetrics, FilterMetrics) {
	var m CacheMetrics
	for i := range c.tableCache.shards {
		s := c.tableCache.shards[i]
		s.mu.RLock()
		m.Count += int64(len(s.mu.nodes))
		s.mu.RUnlock()
		m.Hits += s.hits.Load()
		m.Misses += s.misses.Load()
	}
	m.Size = m.Count * int64(unsafe.Sizeof(sstable.Reader{}))
	f := c.dbOpts.readerOpts.FilterMetricsTracker.Load()
	return m, f
}

func (c *tableCacheContainer) estimateSize(
	meta *fileMetadata, lower, upper []byte,
) (size uint64, err error) {
	c.withCommonReader(meta, func(cr sstable.CommonReader) error {
		size, err = cr.EstimateDiskUsage(lower, upper)
		return err
	})
	return size, err
}

// createCommonReader creates a Reader for this file.
func createCommonReader(v *tableCacheValue, file *fileMetadata) sstable.CommonReader {
	// TODO(bananabrick): We suffer an allocation if file is a virtual sstable.
	var cr sstable.CommonReader = v.reader
	if file.Virtual {
		virtualReader := sstable.MakeVirtualReader(
			v.reader, file.VirtualMeta().VirtualReaderParams(v.isShared),
		)
		cr = &virtualReader
	}
	return cr
}

func (c *tableCacheContainer) withCommonReader(
	meta *fileMetadata, fn func(sstable.CommonReader) error,
) error {
	s := c.tableCache.getShard(meta.FileBacking.DiskFileNum)
	v := s.findNode(context.TODO(), meta.FileBacking, &c.dbOpts)
	defer s.unrefValue(v)
	if v.err != nil {
		return v.err
	}
	return fn(createCommonReader(v, meta))
}

func (c *tableCacheContainer) withReader(meta physicalMeta, fn func(*sstable.Reader) error) error {
	s := c.tableCache.getShard(meta.FileBacking.DiskFileNum)
	v := s.findNode(context.TODO(), meta.FileBacking, &c.dbOpts)
	defer s.unrefValue(v)
	if v.err != nil {
		return v.err
	}
	return fn(v.reader)
}

// withVirtualReader fetches a VirtualReader associated with a virtual sstable.
func (c *tableCacheContainer) withVirtualReader(
	meta virtualMeta, fn func(sstable.VirtualReader) error,
) error {
	s := c.tableCache.getShard(meta.FileBacking.DiskFileNum)
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
	return fn(sstable.MakeVirtualReader(v.reader, meta.VirtualReaderParams(objMeta.IsShared())))
}

func (c *tableCacheContainer) iterCount() int64 {
	return int64(c.dbOpts.iterCount.Load())
}

// TableCache is a shareable cache for open sstables.
type TableCache struct {
	refs atomic.Int64

	cache  *Cache
	shards []*tableCacheShard
}

// Ref adds a reference to the table cache. Once tableCache.init returns,
// the table cache only remains valid if there is at least one reference
// to it.
func (c *TableCache) Ref() {
	v := c.refs.Add(1)
	// We don't want the reference count to ever go from 0 -> 1,
	// cause a reference count of 0 implies that we've closed the cache.
	if v <= 1 {
		panic(fmt.Sprintf("pebble: inconsistent reference count: %d", v))
	}
}

// Unref removes a reference to the table cache.
func (c *TableCache) Unref() error {
	v := c.refs.Add(-1)
	switch {
	case v < 0:
		panic(fmt.Sprintf("pebble: inconsistent reference count: %d", v))
	case v == 0:
		var err error
		for i := range c.shards {
			// The cache shard is not allocated yet, nothing to close
			if c.shards[i] == nil {
				continue
			}
			err = firstError(err, c.shards[i].Close())
		}

		// Unref the cache which we create a reference to when the tableCache
		// is first instantiated.
		c.cache.Unref()
		return err
	}
	return nil
}

// NewTableCache will create a reference to the table cache. It is the callers responsibility
// to call tableCache.Unref if they will no longer hold a reference to the table cache.
func NewTableCache(cache *Cache, numShards int, size int) *TableCache {
	if size == 0 {
		panic("pebble: cannot create a table cache of size 0")
	} else if numShards == 0 {
		panic("pebble: cannot create a table cache with 0 shards")
	}

	c := &TableCache{}
	c.cache = cache
	c.cache.Ref()

	c.shards = make([]*tableCacheShard, numShards)
	for i := range c.shards {
		c.shards[i] = &tableCacheShard{}
		c.shards[i].init(size / len(c.shards))
	}

	// Hold a ref to the cache here.
	c.refs.Store(1)

	return c
}

func (c *TableCache) getShard(fileNum base.DiskFileNum) *tableCacheShard {
	return c.shards[uint64(fileNum)%uint64(len(c.shards))]
}

type tableCacheKey struct {
	cacheID cache.ID
	fileNum base.DiskFileNum
}

type tableCacheShard struct {
	hits      atomic.Int64
	misses    atomic.Int64
	iterCount atomic.Int32

	size int

	mu struct {
		sync.RWMutex
		nodes map[tableCacheKey]*tableCacheNode
		// The iters map is only created and populated in race builds.
		iters map[io.Closer][]byte

		handHot  *tableCacheNode
		handCold *tableCacheNode
		handTest *tableCacheNode

		coldTarget int
		sizeHot    int
		sizeCold   int
		sizeTest   int
	}
	releasing       sync.WaitGroup
	releasingCh     chan *tableCacheValue
	releaseLoopExit sync.WaitGroup
}

func (c *tableCacheShard) init(size int) {
	c.size = size

	c.mu.nodes = make(map[tableCacheKey]*tableCacheNode)
	c.mu.coldTarget = size
	c.releasingCh = make(chan *tableCacheValue, 100)
	c.releaseLoopExit.Add(1)
	go c.releaseLoop()

	if invariants.RaceEnabled {
		c.mu.iters = make(map[io.Closer][]byte)
	}
}

func (c *tableCacheShard) releaseLoop() {
	pprof.Do(context.Background(), tableCacheLabels, func(context.Context) {
		defer c.releaseLoopExit.Done()
		for v := range c.releasingCh {
			v.release(c)
		}
	})
}

// checkAndIntersectFilters checks the specific table and block property filters
// for intersection with any available table and block-level properties. Returns
// true for ok if this table should be read by this iterator.
func (c *tableCacheShard) checkAndIntersectFilters(
	v *tableCacheValue,
	blockPropertyFilters []BlockPropertyFilter,
	boundLimitedFilter sstable.BoundLimitedBlockPropertyFilter,
	syntheticSuffix sstable.SyntheticSuffix,
) (ok bool, filterer *sstable.BlockPropertiesFilterer, err error) {
	if boundLimitedFilter != nil || len(blockPropertyFilters) > 0 {
		filterer, err = sstable.IntersectsTable(
			blockPropertyFilters,
			boundLimitedFilter,
			v.reader.Properties.UserProperties,
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

func (c *tableCacheShard) newIters(
	ctx context.Context,
	file *manifest.FileMetadata,
	opts *IterOptions,
	internalOpts internalIterOpts,
	dbOpts *tableCacheOpts,
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

	// Note: This suffers an allocation for virtual sstables.
	cr := createCommonReader(v, file)
	var iters iterSet
	var err error
	if kinds.RangeKey() && file.HasRangeKeys {
		iters.rangeKey, err = c.newRangeKeyIter(ctx, v, file, cr, opts.SpanIterOptions())
	}
	if kinds.RangeDeletion() && file.HasPointKeys && err == nil {
		iters.rangeDeletion, err = c.newRangeDelIter(ctx, file, cr, dbOpts)
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
func (c *tableCacheShard) newPointIter(
	ctx context.Context,
	v *tableCacheValue,
	file *manifest.FileMetadata,
	cr sstable.CommonReader,
	opts *IterOptions,
	internalOpts internalIterOpts,
	dbOpts *tableCacheOpts,
) (internalIterator, error) {
	var (
		hideObsoletePoints bool = false
		pointKeyFilters    []BlockPropertyFilter
		filterer           *sstable.BlockPropertiesFilterer
	)
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
			v.reader.TryAddBlockPropertyFilterForHideObsoletePoints(
				opts.snapshotForHideObsoletePoints, file.LargestSeqNum, opts.PointKeyFilters)

		var ok bool
		var err error
		ok, filterer, err = c.checkAndIntersectFilters(v, pointKeyFilters,
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
	tableFormat, err := v.reader.TableFormat()
	if err != nil {
		return nil, err
	}
	var rp sstable.ReaderProvider
	if tableFormat >= sstable.TableFormatPebblev3 && v.reader.Properties.NumValueBlocks > 0 {
		rp = &tableCacheShardReaderProvider{c: c, file: file, dbOpts: dbOpts}
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
			uint64(uintptr(unsafe.Pointer(v.reader))), opts.CategoryAndQoS)
	}
	if internalOpts.compaction {
		iter, err = cr.NewCompactionIter(transforms, iterStatsAccum, rp, internalOpts.bufferPool)
	} else {
		iter, err = cr.NewPointIter(
			ctx, transforms, opts.GetLowerBound(), opts.GetUpperBound(), filterer, filterBlockSizeLimit,
			internalOpts.stats, iterStatsAccum, rp)
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
func (c *tableCacheShard) newRangeDelIter(
	ctx context.Context, file *manifest.FileMetadata, cr sstable.CommonReader, dbOpts *tableCacheOpts,
) (keyspan.FragmentIterator, error) {
	// NB: range-del iterator does not maintain a reference to the table, nor
	// does it need to read from it after creation.
	rangeDelIter, err := cr.NewRawRangeDelIter(ctx, file.FragmentIterTransforms())
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
func (c *tableCacheShard) newRangeKeyIter(
	ctx context.Context,
	v *tableCacheValue,
	file *fileMetadata,
	cr sstable.CommonReader,
	opts keyspan.SpanIterOptions,
) (keyspan.FragmentIterator, error) {
	transforms := file.FragmentIterTransforms()
	// Don't filter a table's range keys if the file contains RANGEKEYDELs.
	// The RANGEKEYDELs may delete range keys in other levels. Skipping the
	// file's range key blocks may surface deleted range keys below. This is
	// done here, rather than deferring to the block-property collector in order
	// to maintain parity with point keys and the treatment of RANGEDELs.
	if v.reader.Properties.NumRangeKeyDels == 0 && len(opts.RangeKeyFilters) > 0 {
		ok, _, err := c.checkAndIntersectFilters(v, opts.RangeKeyFilters, nil, transforms.SyntheticSuffix())
		if err != nil {
			return nil, err
		} else if !ok {
			return nil, nil
		}
	}
	// TODO(radu): wrap in an AssertBounds.
	return cr.NewRawRangeKeyIter(ctx, transforms)
}

type tableCacheShardReaderProvider struct {
	c      *tableCacheShard
	file   *manifest.FileMetadata
	dbOpts *tableCacheOpts
	v      *tableCacheValue
}

var _ sstable.ReaderProvider = &tableCacheShardReaderProvider{}

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
func (rp *tableCacheShardReaderProvider) GetReader(ctx context.Context) (*sstable.Reader, error) {
	// Calling findNode gives us the responsibility of decrementing v's
	// refCount.
	v := rp.c.findNode(ctx, rp.file.FileBacking, rp.dbOpts)
	if v.err != nil {
		defer rp.c.unrefValue(v)
		return nil, v.err
	}
	rp.v = v
	return v.reader, nil
}

// Close implements sstable.ReaderProvider.
func (rp *tableCacheShardReaderProvider) Close() {
	rp.c.unrefValue(rp.v)
	rp.v = nil
}

// getTableProperties return sst table properties for target file
func (c *tableCacheShard) getTableProperties(
	file *fileMetadata, dbOpts *tableCacheOpts,
) (*sstable.Properties, error) {
	// Calling findNode gives us the responsibility of decrementing v's refCount here
	v := c.findNode(context.TODO(), file.FileBacking, dbOpts)
	defer c.unrefValue(v)

	if v.err != nil {
		return nil, v.err
	}
	return &v.reader.Properties, nil
}

// releaseNode releases a node from the tableCacheShard.
//
// c.mu must be held when calling this.
func (c *tableCacheShard) releaseNode(n *tableCacheNode) {
	c.unlinkNode(n)
	c.clearNode(n)
}

// unlinkNode removes a node from the tableCacheShard, leaving the shard
// reference in place.
//
// c.mu must be held when calling this.
func (c *tableCacheShard) unlinkNode(n *tableCacheNode) {
	key := tableCacheKey{n.cacheID, n.fileNum}
	delete(c.mu.nodes, key)

	switch n.ptype {
	case tableCacheNodeHot:
		c.mu.sizeHot--
	case tableCacheNodeCold:
		c.mu.sizeCold--
	case tableCacheNodeTest:
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

func (c *tableCacheShard) clearNode(n *tableCacheNode) {
	if v := n.value; v != nil {
		n.value = nil
		c.unrefValue(v)
	}
}

// unrefValue decrements the reference count for the specified value, releasing
// it if the reference count fell to 0. Note that the value has a reference if
// it is present in tableCacheShard.mu.nodes, so a reference count of 0 means
// the node has already been removed from that map.
func (c *tableCacheShard) unrefValue(v *tableCacheValue) {
	if v.refCount.Add(-1) == 0 {
		c.releasing.Add(1)
		c.releasingCh <- v
	}
}

// findNode returns the node for the table with the given file number, creating
// that node if it didn't already exist. The caller is responsible for
// decrementing the returned node's refCount.
func (c *tableCacheShard) findNode(
	ctx context.Context, b *fileBacking, dbOpts *tableCacheOpts,
) *tableCacheValue {
	// The backing must have a positive refcount (otherwise it could be deleted at any time).
	b.MustHaveRefs()
	// Caution! Here fileMetadata can be a physical or virtual table. Table cache
	// readers are associated with the physical backings. All virtual tables with
	// the same backing will use the same reader from the cache; so no information
	// that can differ among these virtual tables can be plumbed into loadInfo.
	info := loadInfo{
		backingFileNum: b.DiskFileNum,
	}

	return c.findNodeInternal(ctx, info, dbOpts)
}

func (c *tableCacheShard) findNodeInternal(
	ctx context.Context, loadInfo loadInfo, dbOpts *tableCacheOpts,
) *tableCacheValue {
	// Fast-path for a hit in the cache.
	c.mu.RLock()
	key := tableCacheKey{dbOpts.cacheID, loadInfo.backingFileNum}
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
		n = &tableCacheNode{
			fileNum: loadInfo.backingFileNum,
			ptype:   tableCacheNodeCold,
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
		n.ptype = tableCacheNodeHot
		c.addNode(n, dbOpts)
		c.mu.sizeHot++
	}

	c.misses.Add(1)

	v := &tableCacheValue{
		loaded: make(chan struct{}),
	}
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
	pprof.Do(context.Background(), tableCacheLabels, func(context.Context) {
		v.load(ctx, loadInfo, c, dbOpts)
	})
	return v
}

func (c *tableCacheShard) addNode(n *tableCacheNode, dbOpts *tableCacheOpts) {
	c.evictNodes()
	n.cacheID = dbOpts.cacheID
	key := tableCacheKey{n.cacheID, n.fileNum}
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

func (c *tableCacheShard) evictNodes() {
	for c.size <= c.mu.sizeHot+c.mu.sizeCold && c.mu.handCold != nil {
		c.runHandCold()
	}
}

func (c *tableCacheShard) runHandCold() {
	n := c.mu.handCold
	if n.ptype == tableCacheNodeCold {
		if n.referenced.Load() {
			n.referenced.Store(false)
			n.ptype = tableCacheNodeHot
			c.mu.sizeCold--
			c.mu.sizeHot++
		} else {
			c.clearNode(n)
			n.ptype = tableCacheNodeTest
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

func (c *tableCacheShard) runHandHot() {
	if c.mu.handHot == c.mu.handTest && c.mu.handTest != nil {
		c.runHandTest()
		if c.mu.handHot == nil {
			return
		}
	}

	n := c.mu.handHot
	if n.ptype == tableCacheNodeHot {
		if n.referenced.Load() {
			n.referenced.Store(false)
		} else {
			n.ptype = tableCacheNodeCold
			c.mu.sizeHot--
			c.mu.sizeCold++
		}
	}

	c.mu.handHot = c.mu.handHot.next()
}

func (c *tableCacheShard) runHandTest() {
	if c.mu.sizeCold > 0 && c.mu.handTest == c.mu.handCold && c.mu.handCold != nil {
		c.runHandCold()
		if c.mu.handTest == nil {
			return
		}
	}

	n := c.mu.handTest
	if n.ptype == tableCacheNodeTest {
		c.mu.coldTarget--
		if c.mu.coldTarget < 0 {
			c.mu.coldTarget = 0
		}
		c.unlinkNode(n)
		c.clearNode(n)
	}

	c.mu.handTest = c.mu.handTest.next()
}

func (c *tableCacheShard) evict(fileNum base.DiskFileNum, dbOpts *tableCacheOpts, allowLeak bool) {
	c.mu.Lock()
	key := tableCacheKey{dbOpts.cacheID, fileNum}
	n := c.mu.nodes[key]
	var v *tableCacheValue
	if n != nil {
		// NB: This is equivalent to tableCacheShard.releaseNode(), but we perform
		// the tableCacheNode.release() call synchronously below to ensure the
		// sstable file descriptor is closed before returning. Note that
		// tableCacheShard.releasing needs to be incremented while holding
		// tableCacheShard.mu in order to avoid a race with Close()
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
func (c *tableCacheShard) removeDB(dbOpts *tableCacheOpts) {
	var fileNums []base.DiskFileNum

	c.mu.RLock()
	// Collect the fileNums which need to be cleaned.
	var firstNode *tableCacheNode
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

func (c *tableCacheShard) Close() error {
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

type tableCacheValue struct {
	closeHook func(i sstable.Iterator) error
	reader    *sstable.Reader
	err       error
	isShared  bool
	loaded    chan struct{}
	// Reference count for the value. The reader is closed when the reference
	// count drops to zero.
	refCount atomic.Int32
}

// loadInfo contains the information needed to populate a new cache entry.
type loadInfo struct {
	backingFileNum base.DiskFileNum
}

func (v *tableCacheValue) load(
	ctx context.Context, loadInfo loadInfo, c *tableCacheShard, dbOpts *tableCacheOpts,
) {
	// Try opening the file first.
	var f objstorage.Readable
	var err error
	f, err = dbOpts.objProvider.OpenForReading(
		ctx, fileTypeTable, loadInfo.backingFileNum, objstorage.OpenOptions{MustExist: true},
	)
	if err == nil {
		o := dbOpts.readerOpts
		o.SetInternalCacheOpts(sstableinternal.CacheOptions{
			Cache:   dbOpts.cache,
			CacheID: dbOpts.cacheID,
			FileNum: loadInfo.backingFileNum,
		})
		v.reader, err = sstable.NewReader(ctx, f, o)
	}
	if err == nil {
		var objMeta objstorage.ObjectMetadata
		objMeta, err = dbOpts.objProvider.Lookup(fileTypeTable, loadInfo.backingFileNum)
		v.isShared = objMeta.IsShared()
	}
	if err != nil {
		v.err = errors.Wrapf(
			err, "pebble: backing file %s error", loadInfo.backingFileNum)
	}
	if v.err != nil {
		c.mu.Lock()
		defer c.mu.Unlock()
		// Lookup the node in the cache again as it might have already been
		// removed.
		key := tableCacheKey{dbOpts.cacheID, loadInfo.backingFileNum}
		n := c.mu.nodes[key]
		if n != nil && n.value == v {
			c.releaseNode(n)
		}
	}
	close(v.loaded)
}

func (v *tableCacheValue) release(c *tableCacheShard) {
	<-v.loaded
	// Nothing to be done about an error at this point. Close the reader if it is
	// open.
	if v.reader != nil {
		_ = v.reader.Close()
	}
	c.releasing.Done()
}

type tableCacheNodeType int8

const (
	tableCacheNodeTest tableCacheNodeType = iota
	tableCacheNodeCold
	tableCacheNodeHot
)

func (p tableCacheNodeType) String() string {
	switch p {
	case tableCacheNodeTest:
		return "test"
	case tableCacheNodeCold:
		return "cold"
	case tableCacheNodeHot:
		return "hot"
	}
	return "unknown"
}

type tableCacheNode struct {
	fileNum base.DiskFileNum
	value   *tableCacheValue

	links struct {
		next *tableCacheNode
		prev *tableCacheNode
	}
	ptype tableCacheNodeType
	// referenced is atomically set to indicate that this entry has been accessed
	// since the last time one of the clock hands swept it.
	referenced atomic.Bool

	// Storing the cache id associated with the DB instance here
	// avoids the need to thread the dbOpts struct through many functions.
	cacheID cache.ID
}

func (n *tableCacheNode) next() *tableCacheNode {
	if n == nil {
		return nil
	}
	return n.links.next
}

func (n *tableCacheNode) prev() *tableCacheNode {
	if n == nil {
		return nil
	}
	return n.links.prev
}

func (n *tableCacheNode) link(s *tableCacheNode) {
	s.links.prev = n.links.prev
	s.links.prev.links.next = s
	s.links.next = n
	s.links.next.links.prev = s
}

func (n *tableCacheNode) unlink() *tableCacheNode {
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
