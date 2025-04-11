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
	"github.com/cockroachdb/pebble/internal/genericcache"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/keyspan/keyspanimpl"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/sstableinternal"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider/objiotracing"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/blob"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/valblk"
	"github.com/cockroachdb/redact"
)

var emptyIter = &errorIter{err: nil}
var emptyKeyspanIter = &errorKeyspanIter{err: nil}

// tableNewIters creates new iterators (point, range deletion and/or range key)
// for the given table metadata. Which of the various iterator kinds the user is
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
// fileCacheHandle.newIters().
type tableNewIters func(
	ctx context.Context,
	file *manifest.TableMetadata,
	opts *IterOptions,
	internalOpts internalIterOpts,
	kinds iterKinds,
) (iterSet, error)

// tableNewRangeDelIter takes a tableNewIters and returns a TableNewSpanIter
// for the rangedel iterator returned by tableNewIters.
func tableNewRangeDelIter(newIters tableNewIters) keyspanimpl.TableNewSpanIter {
	return func(ctx context.Context, file *manifest.TableMetadata, iterOptions keyspan.SpanIterOptions) (keyspan.FragmentIterator, error) {
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
	return func(ctx context.Context, file *manifest.TableMetadata, iterOptions keyspan.SpanIterOptions) (keyspan.FragmentIterator, error) {
		iters, err := newIters(ctx, file, nil, internalIterOpts{}, iterRangeKeys)
		if err != nil {
			return nil, err
		}
		return iters.RangeKey(), nil
	}
}

// fileCacheHandle is used to access the file cache. Each DB has its own handle.
type fileCacheHandle struct {
	fileCache *FileCache

	// The handle contains fields which are unique to each DB. Note that these get
	// accessed from all shards, so keep read-only fields separate for read-write
	// fields.
	loggerAndTracer  LoggerAndTracer
	blockCacheHandle *cache.Handle
	objProvider      objstorage.Provider
	readerOpts       sstable.ReaderOptions

	// iterCount keeps track of how many iterators are open. It is used to keep
	// track of leaked iterators on a per-db level.
	iterCount         atomic.Int32
	sstStatsCollector block.CategoryStatsCollector

	// reportCorruptionFn is used for block.ReadEnv.ReportCorruptionFn. It expects
	// the first argument to be a `*TableMetadata`. It returns an error that
	// contains more details.
	reportCorruptionFn func(any, error) error

	// This struct is only populated in race builds.
	raceMu struct {
		sync.Mutex
		// nextRefID is the next ID to allocate for a new reference.
		nextRefID uint64
		// openRefs maps reference IDs to the stack trace recorded at creation
		// time. It's used to track which call paths leaked open references to
		// files.
		openRefs map[uint64][]byte
	}
}

// Assert that *fileCacheHandle implements blob.ReaderProvider.
var _ blob.ReaderProvider = (*fileCacheHandle)(nil)

// newHandle creates a handle for the FileCache which has its own options. Each
// handle has its own set of files in the cache, separate from those of other
// handles.
func (c *FileCache) newHandle(
	cacheHandle *cache.Handle,
	objProvider objstorage.Provider,
	loggerAndTracer LoggerAndTracer,
	readerOpts sstable.ReaderOptions,
	reportCorruptionFn func(any, error) error,
) *fileCacheHandle {
	c.Ref()

	t := &fileCacheHandle{
		fileCache:        c,
		loggerAndTracer:  loggerAndTracer,
		blockCacheHandle: cacheHandle,
		objProvider:      objProvider,
	}
	t.readerOpts = readerOpts
	t.readerOpts.FilterMetricsTracker = &sstable.FilterMetricsTracker{}
	t.reportCorruptionFn = reportCorruptionFn
	if invariants.RaceEnabled {
		t.raceMu.openRefs = make(map[uint64][]byte)
	}
	return t
}

// Close the handle, make sure that there will be no further need
// to access any of the files associated with the store.
func (h *fileCacheHandle) Close() error {
	// We want to do some cleanup work here. Check for leaked iterators
	// by the DB using this container. Note that we'll still perform cleanup
	// below in the case that there are leaked iterators.
	var err error
	if v := h.iterCount.Load(); v > 0 {
		if !invariants.RaceEnabled {
			err = errors.Errorf("leaked iterators: %d", errors.Safe(v))
		} else {
			var buf bytes.Buffer
			for _, stack := range h.raceMu.openRefs {
				fmt.Fprintf(&buf, "%s\n", stack)
			}
			err = errors.Errorf("leaked iterators: %d\n%s", errors.Safe(v), buf.String())
		}
	}

	// EvictAll would panic if there are still outstanding references.
	if err == nil {
		keys := h.fileCache.c.EvictAll(func(key fileCacheKey) bool {
			return key.handle == h
		})
		// Evict any associated blocks in the cache.
		for i := range keys {
			h.blockCacheHandle.EvictFile(keys[i].fileNum)
		}
	}

	h.fileCache.Unref()
	// TODO(radu): we have to tolerate metrics() calls after close (see
	// https://github.com/cockroachdb/cockroach/issues/140454).
	// *h = fileCacheHandle{}
	return err
}

// openFile is called when we insert a new entry in the file cache.
func (h *fileCacheHandle) openFile(
	ctx context.Context, fileNum base.DiskFileNum, fileType base.FileType,
) (io.Closer, objstorage.ObjectMetadata, error) {
	f, err := h.objProvider.OpenForReading(
		ctx, fileType, fileNum, objstorage.OpenOptions{MustExist: true},
	)
	if err != nil {
		return nil, objstorage.ObjectMetadata{}, err
	}
	objMeta, err := h.objProvider.Lookup(fileType, fileNum)
	if err != nil {
		return nil, objstorage.ObjectMetadata{}, err
	}

	o := h.readerOpts
	o.CacheOpts = sstableinternal.CacheOptions{
		CacheHandle: h.blockCacheHandle,
		FileNum:     fileNum,
	}
	switch fileType {
	case base.FileTypeTable:
		r, err := sstable.NewReader(ctx, f, o)
		if err != nil {
			return nil, objMeta, err
		}
		return r, objMeta, nil
	case base.FileTypeBlob:
		r, err := blob.NewFileReader(ctx, f, blob.FileReaderOptions{
			ReaderOptions: o.ReaderOptions,
		})
		if err != nil {
			return nil, objMeta, err
		}
		return r, objMeta, nil
	default:
		panic(errors.AssertionFailedf("pebble: unexpected file cache file type: %s", fileType))
	}
}

// findOrCreateTable retrieves an existing sstable reader or creates a new one
// for the backing file of the given table. If a corruption error is
// encountered, reportCorruptionFn() is called.
func (h *fileCacheHandle) findOrCreateTable(
	ctx context.Context, meta *manifest.TableMetadata,
) (genericcache.ValueRef[fileCacheKey, fileCacheValue], error) {
	key := fileCacheKey{
		handle:   h,
		fileNum:  meta.FileBacking.DiskFileNum,
		fileType: base.FileTypeTable,
	}
	valRef, err := h.fileCache.c.FindOrCreate(ctx, key)
	if err != nil && IsCorruptionError(err) {
		err = h.reportCorruptionFn(meta, err)
	}
	return valRef, err
}

// findOrCreateBlob retrieves an existing blob reader or creates a new one for
// the given blob file. If a corruption error is encountered,
// reportCorruptionFn() is called.
func (h *fileCacheHandle) findOrCreateBlob(
	ctx context.Context, fileNum base.DiskFileNum,
) (genericcache.ValueRef[fileCacheKey, fileCacheValue], error) {
	key := fileCacheKey{
		handle:   h,
		fileNum:  fileNum,
		fileType: base.FileTypeBlob,
	}
	valRef, err := h.fileCache.c.FindOrCreate(ctx, key)
	// TODO(jackson): Propagate a blob metadata object here.
	if err != nil && IsCorruptionError(err) {
		err = h.reportCorruptionFn(nil, err)
	}
	return valRef, err
}

// Evict the given file from the file cache and the block cache.
func (h *fileCacheHandle) Evict(fileNum base.DiskFileNum, fileType base.FileType) {
	h.fileCache.c.Evict(fileCacheKey{handle: h, fileNum: fileNum, fileType: fileType})
	h.blockCacheHandle.EvictFile(fileNum)
}

func (h *fileCacheHandle) SSTStatsCollector() *block.CategoryStatsCollector {
	return &h.sstStatsCollector
}

// Metrics returns metrics for the file cache. Note that the CacheMetrics track
// the global cache which is shared between multiple handles (stores). The
// FilterMetrics are per-handle.
func (h *fileCacheHandle) Metrics() (CacheMetrics, FilterMetrics) {
	m := h.fileCache.c.Metrics()

	// The generic cache maintains a count of entries, but it doesn't know which
	// entries are sstables and which are blob files, which affects the memory
	// footprint of the table cache. So the FileCache maintains its own counts,
	// incremented when initializing a new value and decremented by the
	// releasing func.
	countSSTables := h.fileCache.counts.sstables.Load()
	countBlobFiles := h.fileCache.counts.blobFiles.Load()

	cm := CacheMetrics{
		Hits:   m.Hits,
		Misses: m.Misses,
		Count:  countSSTables + countBlobFiles,
		Size: m.Size + countSSTables*int64(unsafe.Sizeof(sstable.Reader{})) +
			countBlobFiles*int64(unsafe.Sizeof(blob.FileReader{})),
	}
	fm := h.readerOpts.FilterMetricsTracker.Load()
	return cm, fm
}

func (h *fileCacheHandle) estimateSize(
	meta *tableMetadata, lower, upper []byte,
) (size uint64, err error) {
	err = h.withReader(context.TODO(), block.NoReadEnv, meta, func(r *sstable.Reader, env sstable.ReadEnv) error {
		size, err = r.EstimateDiskUsage(lower, upper, env)
		return err
	})
	return size, err
}

func createReader(v *fileCacheValue, file *tableMetadata) (*sstable.Reader, sstable.ReadEnv) {
	r := v.mustSSTableReader()
	env := sstable.ReadEnv{}
	if file.Virtual.IsVirtual {
		env.Virtual = file.InitVirtual(v.isShared)
	}
	return r, env
}

func (h *fileCacheHandle) withReader(
	ctx context.Context,
	blockEnv block.ReadEnv,
	meta *tableMetadata,
	fn func(*sstable.Reader, sstable.ReadEnv) error,
) error {
	ref, err := h.findOrCreateTable(ctx, meta)
	if err != nil {
		return err
	}
	defer ref.Unref()
	v := ref.Value()
	blockEnv.ReportCorruptionFn = h.reportCorruptionFn
	blockEnv.ReportCorruptionArg = meta
	env := sstable.ReadEnv{Block: blockEnv}

	r := v.mustSSTableReader()
	if meta.Virtual.IsVirtual {
		env.Virtual = meta.InitVirtual(v.isShared)
	}

	return fn(r, env)

}

func (h *fileCacheHandle) IterCount() int64 {
	return int64(h.iterCount.Load())
}

// GetValueReader returns a blob.ValueReader for blob file identified by fileNum.
func (h *fileCacheHandle) GetValueReader(
	ctx context.Context, fileNum base.DiskFileNum,
) (r blob.ValueReader, closeFunc func(), err error) {
	ref, err := h.findOrCreateBlob(ctx, fileNum)
	if err != nil {
		return nil, nil, err
	}
	v := ref.Value()
	r = v.mustBlob()
	// NB: The call to findOrCreateBlob incremented the value's reference count.
	// The closeHook (v.closeHook) takes responsibility for unreferencing the
	// value. Take care to avoid introducing an allocation here by adding a
	// closure.
	closeHook := h.addReference(v)
	return r, closeHook, nil
}

// FileCache is a shareable cache for open files. Open files are exclusively
// sstable files today.
type FileCache struct {
	refs   atomic.Int64
	counts struct {
		sstables  atomic.Int64
		blobFiles atomic.Int64
	}

	c genericcache.Cache[fileCacheKey, fileCacheValue]
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
func (c *FileCache) Unref() {
	v := c.refs.Add(-1)
	switch {
	case v < 0:
		panic(fmt.Sprintf("pebble: inconsistent reference count: %d", v))
	case v == 0:
		c.c.Close()
		c.c = genericcache.Cache[fileCacheKey, fileCacheValue]{}
	}
}

// NewFileCache will create a new file cache with one outstanding reference. It
// is the callers responsibility to call Unref if they will no longer hold a
// reference to the file cache.
func NewFileCache(numShards int, size int) *FileCache {
	if size == 0 {
		panic("pebble: cannot create a file cache of size 0")
	} else if numShards == 0 {
		panic("pebble: cannot create a file cache with 0 shards")
	}

	c := &FileCache{}

	// initFn is used whenever a new entry is added to the file cache.
	initFn := func(ctx context.Context, key fileCacheKey, vRef genericcache.ValueRef[fileCacheKey, fileCacheValue]) error {
		v := vRef.Value()
		handle := key.handle
		v.readerProvider.init(c, key)
		v.closeHook = func() {
			// closeHook is called when an iterator is closed; the initialization of
			// an iterator with this value will happen after a FindOrCreate() call
			// with returns the same vRef.
			vRef.Unref()
			handle.iterCount.Add(-1)
		}
		reader, objMeta, err := handle.openFile(ctx, key.fileNum, key.fileType)
		if err != nil {
			return errors.Wrapf(err, "pebble: backing file %s error", redact.Safe(key.fileNum))
		}
		v.reader = reader
		v.isShared = objMeta.IsShared()
		switch key.fileType {
		case base.FileTypeTable:
			c.counts.sstables.Add(1)
		case base.FileTypeBlob:
			c.counts.blobFiles.Add(1)
		default:
			panic("unexpected file type")
		}
		return nil
	}

	releaseFn := func(v *fileCacheValue) {
		if v.reader != nil {
			switch v.reader.(type) {
			case *sstable.Reader:
				c.counts.sstables.Add(-1)
			case *blob.FileReader:
				c.counts.blobFiles.Add(-1)
			}
			_ = v.reader.Close()
			v.reader = nil
		}
	}

	c.c.Init(size, numShards, initFn, releaseFn)

	// Hold a ref to the cache here.
	c.refs.Store(1)

	return c
}

type fileCacheKey struct {
	handle  *fileCacheHandle
	fileNum base.DiskFileNum
	// fileType describes the type of file being cached (blob or sstable). A
	// file number alone uniquely identifies every file within a DB, but we need
	// to propagate the type so the file cache looks for the correct file in
	// object storage / the filesystem.
	fileType base.FileType
}

// Shard implements the genericcache.Key interface.
func (k fileCacheKey) Shard(numShards int) int {
	// TODO(radu): maybe incorporate a handle ID.
	return int(uint64(k.fileNum) % uint64(numShards))
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

func (h *fileCacheHandle) newIters(
	ctx context.Context,
	file *manifest.TableMetadata,
	opts *IterOptions,
	internalOpts internalIterOpts,
	kinds iterKinds,
) (iterSet, error) {
	// Calling findOrCreate gives us the responsibility of Unref()ing vRef.
	vRef, err := h.findOrCreateTable(ctx, file)
	if err != nil {
		return iterSet{}, err
	}

	internalOpts.readEnv.Block.ReportCorruptionFn = h.reportCorruptionFn
	internalOpts.readEnv.Block.ReportCorruptionArg = file

	v := vRef.Value()
	r, env := createReader(v, file)
	internalOpts.readEnv.Virtual = env.Virtual

	var iters iterSet
	if kinds.RangeKey() && file.HasRangeKeys {
		iters.rangeKey, err = newRangeKeyIter(ctx, file, r, opts.SpanIterOptions(), internalOpts)
	}
	if kinds.RangeDeletion() && file.HasPointKeys && err == nil {
		iters.rangeDeletion, err = newRangeDelIter(ctx, file, r, h, internalOpts)
	}
	if kinds.Point() && err == nil {
		iters.point, err = h.newPointIter(ctx, v, file, r, opts, internalOpts, h)
	}
	if err != nil {
		// NB: There's a subtlety here: Because the point iterator is the last
		// iterator we attempt to create, it's not possible for:
		//   err != nil && iters.point != nil
		// If it were possible, we'd need to account for it to avoid double
		// unref-ing here, once during CloseAll and once during `unrefValue`.
		_ = iters.CloseAll()
		vRef.Unref()
		return iterSet{}, err
	}
	// Only point iterators ever require the reader stay pinned in the cache. If
	// we're not returning a point iterator to the caller, we need to unref v.
	//
	// For point iterators, v.closeHook will be called which will release the ref.
	if iters.point == nil {
		vRef.Unref()
	}
	return iters, nil
}

// For flushable ingests, we decide whether to use the bloom filter base on
// size.
const filterBlockSizeLimitForFlushableIngests = 64 * 1024

// newPointIter is an internal helper that constructs a point iterator over a
// sstable. This function is for internal use only, and callers should use
// newIters instead.
func (h *fileCacheHandle) newPointIter(
	ctx context.Context,
	v *fileCacheValue,
	file *manifest.TableMetadata,
	reader *sstable.Reader,
	opts *IterOptions,
	internalOpts internalIterOpts,
	handle *fileCacheHandle,
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
	tableFormat, err := r.TableFormat()
	if err != nil {
		return nil, err
	}

	if v.isShared && file.SyntheticSeqNum() != 0 {
		if tableFormat < sstable.TableFormatPebblev4 {
			return nil, errors.New("pebble: shared ingested sstable has a lower table format than expected")
		}
		// The table is shared and ingested.
		hideObsoletePoints = true
	}
	transforms := file.IterTransforms()
	transforms.HideObsoletePoints = hideObsoletePoints
	if internalOpts.readEnv.Block.IterStats == nil && opts != nil {
		internalOpts.readEnv.Block.IterStats = handle.SSTStatsCollector().Accumulator(uint64(uintptr(unsafe.Pointer(r))), opts.Category)
	}
	if internalOpts.compaction {
		iter, err = reader.NewCompactionIter(transforms, internalOpts.readEnv,
			&v.readerProvider, sstable.TableBlobContext{
				ValueFetcher: internalOpts.blobValueFetcher,
				References:   file.BlobReferences,
			})
	} else {
		iter, err = reader.NewPointIter(ctx, sstable.IterOptions{
			Lower:                opts.GetLowerBound(),
			Upper:                opts.GetUpperBound(),
			Transforms:           transforms,
			FilterBlockSizeLimit: filterBlockSizeLimit,
			Filterer:             filterer,
			Env:                  internalOpts.readEnv,
			ReaderProvider:       &v.readerProvider,
			BlobContext: sstable.TableBlobContext{
				ValueFetcher: internalOpts.blobValueFetcher,
				References:   file.BlobReferences,
			},
		})
	}
	if err != nil {
		return nil, err
	}
	// NB: closeHook (v.closeHook) takes responsibility for calling
	// unrefValue(v) here. Take care to avoid introducing an allocation here by
	// adding a closure.
	closeHook := h.addReference(v)
	iter.SetCloseHook(closeHook)
	return iter, nil
}

func (h *fileCacheHandle) addReference(v *fileCacheValue) (closeHook func()) {
	h.iterCount.Add(1)
	closeHook = v.closeHook
	if invariants.RaceEnabled {
		stack := debug.Stack()
		h.raceMu.Lock()
		refID := h.raceMu.nextRefID
		h.raceMu.openRefs[refID] = stack
		h.raceMu.nextRefID++
		h.raceMu.Unlock()
		// In race builds, this closeHook closure will force an allocation.
		// Race builds are already unperformant (and allocate a stack trace), so
		// we don't care.
		closeHook = func() {
			v.closeHook()
			h.raceMu.Lock()
			defer h.raceMu.Unlock()
			delete(h.raceMu.openRefs, refID)
		}
	}
	return closeHook
}

// newRangeDelIter is an internal helper that constructs an iterator over a
// sstable's range deletions. This function is for table-cache internal use
// only, and callers should use newIters instead.
func newRangeDelIter(
	ctx context.Context,
	file *manifest.TableMetadata,
	r *sstable.Reader,
	handle *fileCacheHandle,
	internalOpts internalIterOpts,
) (keyspan.FragmentIterator, error) {
	// NB: range-del iterator does not maintain a reference to the table, nor
	// does it need to read from it after creation.
	rangeDelIter, err := r.NewRawRangeDelIter(ctx, file.FragmentIterTransforms(), internalOpts.readEnv)
	if err != nil {
		return nil, err
	}
	// Assert expected bounds in tests.
	if invariants.Sometimes(50) && rangeDelIter != nil {
		cmp := base.DefaultComparer.Compare
		if handle.readerOpts.Comparer != nil {
			cmp = handle.readerOpts.Comparer.Compare
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
	file *tableMetadata,
	r *sstable.Reader,
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
	return r.NewRawRangeKeyIter(ctx, transforms, internalOpts.readEnv)
}

// tableCacheShardReaderProvider implements sstable.ReaderProvider for a
// specific table.
type tableCacheShardReaderProvider struct {
	c   *genericcache.Cache[fileCacheKey, fileCacheValue]
	key fileCacheKey

	mu struct {
		sync.Mutex
		// r is the result of c.FindOrCreate(), only set iff refCount > 0.
		r genericcache.ValueRef[fileCacheKey, fileCacheValue]
		// refCount is the number of GetReader() calls that have not received a
		// corresponding Close().
		refCount int
	}
}

var _ valblk.ReaderProvider = &tableCacheShardReaderProvider{}

func (rp *tableCacheShardReaderProvider) init(fc *FileCache, key fileCacheKey) {
	rp.c = &fc.c
	rp.key = key
	rp.mu.r = genericcache.ValueRef[fileCacheKey, fileCacheValue]{}
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

	if rp.mu.refCount > 0 {
		// We already have a value.
		rp.mu.refCount++
		return rp.mu.r.Value().mustSSTableReader(), nil
	}

	// Calling FindOrCreate gives us the responsibility of Unref()ing r, which
	// will happen when rp.mu.refCount reaches 0. Note that if the table is no
	// longer in the cache, FindOrCreate will need to do IO (through initFn in
	// NewFileCache) to initialize a new Reader. We hold rp.mu during this time so
	// that concurrent GetReader calls block until the Reader is created.
	r, err := rp.c.FindOrCreate(ctx, rp.key)
	if err != nil {
		return nil, err
	}
	rp.mu.r = r
	rp.mu.refCount = 1
	return r.Value().mustSSTableReader(), nil
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
		rp.mu.r.Unref()
		rp.mu.r = genericcache.ValueRef[fileCacheKey, fileCacheValue]{}
	}
}

// getTableProperties returns sst table properties for the backing file.
//
// WARNING! If file is a virtual table, we return the properties of the physical
// table.
func (h *fileCacheHandle) getTableProperties(file *tableMetadata) (*sstable.Properties, error) {
	// Calling findOrCreateTable gives us the responsibility of decrementing v's
	// refCount here
	v, err := h.findOrCreateTable(context.TODO(), file)
	if err != nil {
		return nil, err
	}
	defer v.Unref()

	r := v.Value().mustSSTableReader()
	return &r.Properties, nil
}

type fileCacheValue struct {
	closeHook func()
	reader    io.Closer // *sstable.Reader or *blob.FileReader
	isShared  bool

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

// mustBlob retrieves the value's *blob.FileReader. It panics if the cached file
// is not a blob file.
func (v *fileCacheValue) mustBlob() *blob.FileReader {
	return v.reader.(*blob.FileReader)
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
