// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package pebble provides an ordered key/value store.
package pebble // import "github.com/cockroachdb/pebble"

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble/internal/arenaskl"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/record"
	"github.com/cockroachdb/pebble/vfs"
)

const (
	// minTableCacheSize is the minimum size of the table cache.
	minTableCacheSize = 64

	// numNonTableCacheFiles is an approximation for the number of MaxOpenFiles
	// that we don't use for table caches.
	numNonTableCacheFiles = 10
)

var (
	// ErrNotFound is returned when a get operation does not find the requested
	// key.
	ErrNotFound = base.ErrNotFound
	// ErrClosed is returned when an operation is performed on a closed snapshot
	// or DB.
	ErrClosed = errors.New("pebble: closed")
	// ErrReadOnly is returned when a write operation is performed on a read-only
	// database.
	ErrReadOnly = errors.New("pebble: read-only")
)

type flushable interface {
	newIter(o *IterOptions) internalIterator
	newFlushIter(o *IterOptions, bytesFlushed *uint64) internalIterator
	newRangeDelIter(o *IterOptions) internalIterator
	inuseBytes() uint64
	totalBytes() uint64
	readerRef()
	readerUnref()
	flushed() chan struct{}
	manualFlush() bool
	readyForFlush() bool
	logInfo() (logNum, size uint64)
}

// Reader is a readable key/value store.
//
// It is safe to call Get and NewIter from concurrent goroutines.
type Reader interface {
	// Get gets the value for the given key. It returns ErrNotFound if the DB
	// does not contain the key.
	//
	// The caller should not modify the contents of the returned slice, but
	// it is safe to modify the contents of the argument after Get returns.
	Get(key []byte) (value []byte, err error)

	// NewIter returns an iterator that is unpositioned (Iterator.Valid() will
	// return false). The iterator can be positioned via a call to SeekGE,
	// SeekLT, First or Last.
	NewIter(o *IterOptions) *Iterator

	// Close closes the Reader. It may or may not close any underlying io.Reader
	// or io.Writer, depending on how the DB was created.
	//
	// It is not safe to close a DB until all outstanding iterators are closed.
	// It is valid to call Close multiple times. Other methods should not be
	// called after the DB has been closed.
	Close() error
}

// Writer is a writable key/value store.
//
// Goroutine safety is dependent on the specific implementation.
type Writer interface {
	// Apply the operations contained in the batch to the DB.
	//
	// It is safe to modify the contents of the arguments after Apply returns.
	Apply(batch *Batch, o *WriteOptions) error

	// Delete deletes the value for the given key. Deletes are blind all will
	// succeed even if the given key does not exist.
	//
	// It is safe to modify the contents of the arguments after Delete returns.
	Delete(key []byte, o *WriteOptions) error

	// SingleDelete is similar to Delete in that it deletes the value for the given key. Like Delete,
	// it is a blind operation that will succeed even if the given key does not exist.
	//
	// WARNING: Undefined (non-deterministic) behavior will result if a key is overwritten and
	// then deleted using SingleDelete. The record may appear deleted immediately, but be
	// resurrected at a later time after compactions have been performed. Or the record may
	// be deleted permanently. A Delete operation lays down a "tombstone" which shadows all
	// previous versions of a key. The SingleDelete operation is akin to "anti-matter" and will
	// only delete the most recently written version for a key. These different semantics allow
	// the DB to avoid propagating a SingleDelete operation during a compaction as soon as the
	// corresponding Set operation is encountered. These semantics require extreme care to handle
	// properly. Only use if you have a workload where the performance gain is critical and you
	// can guarantee that a record is written once and then deleted once.
	//
	// SingleDelete is internally transformed into a Delete if the most recent record for a key is either
	// a Merge or Delete record.
	//
	// It is safe to modify the contents of the arguments after SingleDelete returns.
	SingleDelete(key []byte, o *WriteOptions) error

	// DeleteRange deletes all of the keys (and values) in the range [start,end)
	// (inclusive on start, exclusive on end).
	//
	// It is safe to modify the contents of the arguments after Delete returns.
	DeleteRange(start, end []byte, o *WriteOptions) error

	// LogData adds the specified to the batch. The data will be written to the
	// WAL, but not added to memtables or sstables. Log data is never indexed,
	// which makes it useful for testing WAL performance.
	//
	// It is safe to modify the contents of the argument after LogData returns.
	LogData(data []byte, opts *WriteOptions) error

	// Merge merges the value for the given key. The details of the merge are
	// dependent upon the configured merge operation.
	//
	// It is safe to modify the contents of the arguments after Merge returns.
	Merge(key, value []byte, o *WriteOptions) error

	// Set sets the value for the given key. It overwrites any previous value
	// for that key; a DB is not a multi-map.
	//
	// It is safe to modify the contents of the arguments after Set returns.
	Set(key, value []byte, o *WriteOptions) error
}

// DB provides a concurrent, persistent ordered key/value store.
//
// A DB's basic operations (Get, Set, Delete) should be self-explanatory. Get
// and Delete will return ErrNotFound if the requested key is not in the store.
// Callers are free to ignore this error.
//
// A DB also allows for iterating over the key/value pairs in key order. If d
// is a DB, the code below prints all key/value pairs whose keys are 'greater
// than or equal to' k:
//
//	iter := d.NewIter(readOptions)
//	for iter.SeekGE(k); iter.Valid(); iter.Next() {
//		fmt.Printf("key=%q value=%q\n", iter.Key(), iter.Value())
//	}
//	return iter.Close()
//
// The Options struct holds the optional parameters for the DB, including a
// Comparer to define a 'less than' relationship over keys. It is always valid
// to pass a nil *Options, which means to use the default parameter values. Any
// zero field of a non-nil *Options also means to use the default value for
// that parameter. Thus, the code below uses a custom Comparer, but the default
// values for every other parameter:
//
//	db := pebble.Open(&Options{
//		Comparer: myComparer,
//	})
type DB struct {
	cacheID        uint64
	dirname        string
	walDirname     string
	opts           *Options
	cmp            Compare
	equal          Equal
	merge          Merge
	split          Split
	abbreviatedKey AbbreviatedKey

	dataDir vfs.File
	walDir  vfs.File

	tableCache tableCache
	newIters   tableNewIters

	commit   *commitPipeline
	fileLock io.Closer

	largeBatchThreshold int
	optionsFileNum      uint64

	// readState provides access to the state needed for reading without needing
	// to acquire DB.mu.
	readState struct {
		sync.RWMutex
		val *readState
	}

	logRecycler logRecycler

	closed int32 // updated atomically

	memTableReserved int64 // number of bytes reserved in the cache for memtables

	compactionLimiter limiter

	// bytesFlushed is the number of bytes flushed in the current flush. This
	// must be read/written atomically since it is accessed by both the flush
	// and compaction routines.
	bytesFlushed uint64
	// bytesCompacted is the number of bytes compacted in the current compaction.
	// This is used as a dummy variable to increment during compaction, and the
	// value is not used anywhere.
	bytesCompacted uint64

	flushLimiter limiter

	// TODO(peter): describe exactly what this mutex protects. So far: every
	// field in the struct.
	mu struct {
		sync.Mutex

		nextJobID int

		versions versionSet

		log struct {
			queue   []uint64
			size    uint64
			bytesIn uint64
			// The LogWriter is protected by commitPipeline.mu. This allows log
			// writes to be performed without holding DB.mu, but requires both
			// commitPipeline.mu and DB.mu to be held when rotating the WAL/memtable
			// (i.e. makeRoomForWrite).
			*record.LogWriter
		}

		mem struct {
			cond sync.Cond
			// The current mutable memTable.
			mutable *memTable
			// Queue of flushables (the mutable memtable is at end). Elements are
			// added to the end of the slice and removed from the beginning. Once an
			// index is set it is never modified making a fixed slice immutable and
			// safe for concurrent reads.
			queue []flushable
			// True when the memtable is actively been switched. Both mem.mutable and
			// log.LogWriter are invalid while switching is true.
			switching bool
			// nextSize is the size of the next memtable. The memtable size starts at
			// min(256KB,Options.MemTableSize) and doubles each time a new memtable
			// is allocated up to Options.MemTableSize. This reduces the memory
			// footprint of memtables when lots of DB instances are used concurrently
			// in test environments.
			nextSize int
		}

		compact struct {
			cond           sync.Cond
			flushing       bool
			compacting     bool
			pendingOutputs map[uint64]struct{}
			manual         []*manualCompaction
		}

		cleaner struct {
			cond     sync.Cond
			cleaning bool
			disabled int
		}

		// The list of active snapshots.
		snapshots snapshotList
	}
}

var _ Reader = (*DB)(nil)
var _ Writer = (*DB)(nil)

// Get gets the value for the given key. It returns ErrNotFound if the DB does
// not contain the key.
//
// The caller should not modify the contents of the returned slice, but it is
// safe to modify the contents of the argument after Get returns.
func (d *DB) Get(key []byte) ([]byte, error) {
	return d.getInternal(key, nil /* batch */, nil /* snapshot */)
}

func (d *DB) getInternal(key []byte, b *Batch, s *Snapshot) ([]byte, error) {
	if atomic.LoadInt32(&d.closed) != 0 {
		panic(ErrClosed)
	}

	// Grab and reference the current readState. This prevents the underlying
	// files in the associated version from being deleted if there is a current
	// compaction. The readState is unref'd by Iterator.Close().
	readState := d.loadReadState()

	// Determine the seqnum to read at after grabbing the read state (current and
	// memtables) above.
	var seqNum uint64
	if s != nil {
		seqNum = s.seqNum
	} else {
		seqNum = atomic.LoadUint64(&d.mu.versions.visibleSeqNum)
	}

	var buf struct {
		dbi Iterator
		get getIter
	}

	get := &buf.get
	get.cmp = d.cmp
	get.equal = d.equal
	get.newIters = d.newIters
	get.snapshot = seqNum
	get.key = key
	get.batch = b
	get.mem = readState.memtables
	get.l0 = readState.current.Files[0]
	get.version = readState.current

	i := &buf.dbi
	i.cmp = d.cmp
	i.equal = d.equal
	i.merge = d.merge
	i.split = d.split
	i.iter = get
	i.readState = readState

	defer i.Close()
	if !i.First() {
		err := i.Error()
		if err != nil {
			return nil, err
		}
		return nil, ErrNotFound
	}
	return i.Value(), nil
}

// Set sets the value for the given key. It overwrites any previous value
// for that key; a DB is not a multi-map.
//
// It is safe to modify the contents of the arguments after Set returns.
func (d *DB) Set(key, value []byte, opts *WriteOptions) error {
	b := newBatch(d)
	_ = b.Set(key, value, opts)
	if err := d.Apply(b, opts); err != nil {
		return err
	}
	// Only release the batch on success.
	b.release()
	return nil
}

// Delete deletes the value for the given key. Deletes are blind all will
// succeed even if the given key does not exist.
//
// It is safe to modify the contents of the arguments after Delete returns.
func (d *DB) Delete(key []byte, opts *WriteOptions) error {
	b := newBatch(d)
	_ = b.Delete(key, opts)
	if err := d.Apply(b, opts); err != nil {
		return err
	}
	// Only release the batch on success.
	b.release()
	return nil
}

// SingleDelete adds an action to the batch that single deletes the entry for key.
// See Writer.SingleDelete for more details on the semantics of SingleDelete.
//
// It is safe to modify the contents of the arguments after SingleDelete returns.
func (d *DB) SingleDelete(key []byte, opts *WriteOptions) error {
	b := newBatch(d)
	_ = b.SingleDelete(key, opts)
	if err := d.Apply(b, opts); err != nil {
		return err
	}
	// Only release the batch on success.
	b.release()
	return nil
}

// DeleteRange deletes all of the keys (and values) in the range [start,end)
// (inclusive on start, exclusive on end).
//
// It is safe to modify the contents of the arguments after DeleteRange
// returns.
func (d *DB) DeleteRange(start, end []byte, opts *WriteOptions) error {
	b := newBatch(d)
	_ = b.DeleteRange(start, end, opts)
	if err := d.Apply(b, opts); err != nil {
		return err
	}
	// Only release the batch on success.
	b.release()
	return nil
}

// Merge adds an action to the DB that merges the value at key with the new
// value. The details of the merge are dependent upon the configured merge
// operator.
//
// It is safe to modify the contents of the arguments after Merge returns.
func (d *DB) Merge(key, value []byte, opts *WriteOptions) error {
	b := newBatch(d)
	_ = b.Merge(key, value, opts)
	if err := d.Apply(b, opts); err != nil {
		return err
	}
	// Only release the batch on success.
	b.release()
	return nil
}

// LogData adds the specified to the batch. The data will be written to the
// WAL, but not added to memtables or sstables. Log data is never indexed,
// which makes it useful for testing WAL performance.
//
// It is safe to modify the contents of the argument after LogData returns.
func (d *DB) LogData(data []byte, opts *WriteOptions) error {
	b := newBatch(d)
	_ = b.LogData(data, opts)
	if err := d.Apply(b, opts); err != nil {
		return err
	}
	// Only release the batch on success.
	b.release()
	return nil
}

// Apply the operations contained in the batch to the DB. If the batch is large
// the contents of the batch may be retained by the database. If that occurs
// the batch contents will be cleared preventing the caller from attempting to
// reuse them.
//
// It is safe to modify the contents of the arguments after Apply returns.
func (d *DB) Apply(batch *Batch, opts *WriteOptions) error {
	if atomic.LoadInt32(&d.closed) != 0 {
		panic(ErrClosed)
	}
	if d.opts.ReadOnly {
		return ErrReadOnly
	}
	if batch.db != nil && batch.db != d {
		panic(fmt.Sprintf("pebble: batch db mismatch: %p != %p", batch.db, d))
	}

	sync := opts.GetSync()
	if sync && d.opts.DisableWAL {
		return errors.New("pebble: WAL disabled")
	}

	if batch.db == nil {
		batch.refreshMemTableSize()
	}
	if int(batch.memTableSize) >= d.largeBatchThreshold {
		batch.flushable = newFlushableBatch(batch, d.opts.Comparer)
	}
	if err := d.commit.Commit(batch, sync); err != nil {
		// There isn't much we can do on an error here. The commit pipeline will be
		// horked at this point.
		d.opts.Logger.Fatalf("%v", err)
	}
	// If this is a large batch, we need to clear the batch contents as the
	// flushable batch may still be present in the flushables queue.
	//
	// TODO(peter): Currently large batches are written to the WAL. We could
	// skip the WAL write and instead wait for the large batch to be flushed to
	// an sstable. For a 100 MB batch, this might actually be faster. For a 1
	// GB batch this is almost certainly faster.
	if batch.flushable != nil {
		batch.data = nil
	}
	return nil
}

func (d *DB) commitApply(b *Batch, mem *memTable) error {
	if b.flushable != nil {
		// This is a large batch which was already added to the immutable queue.
		return nil
	}
	err := mem.apply(b, b.SeqNum())
	if err != nil {
		return err
	}
	if mem.writerUnref() {
		d.mu.Lock()
		d.maybeScheduleFlush()
		d.mu.Unlock()
	}
	return nil
}

func (d *DB) commitWrite(b *Batch, syncWG *sync.WaitGroup, syncErr *error) (*memTable, error) {
	var size int64
	repr := b.Repr()

	if b.flushable != nil {
		// We have a large batch. Such batches are special in that they don't get
		// added to the memtable, and are instead inserted into the queue of
		// memtables. The call to makeRoomForWrite with this batch will force the
		// current memtable to be flushed. We want the large batch to be part of
		// the same log, so we add it to the WAL here, rather than after the call
		// to makeRoomForWrite().
		//
		// Set the sequence number since it was not set to the correct value earlier
		// (see comment in newFlushableBatch()).
		b.flushable.setSeqNum(b.SeqNum())
		if !d.opts.DisableWAL {
			var err error
			size, err = d.mu.log.SyncRecord(repr, syncWG, syncErr)
			if err != nil {
				panic(err)
			}
		}
	}

	d.mu.Lock()

	// Switch out the memtable if there was not enough room to store the batch.
	err := d.makeRoomForWrite(b)

	if err == nil && !d.opts.DisableWAL {
		d.mu.log.bytesIn += uint64(len(repr))
	}

	// Grab a reference to the memtable while holding DB.mu. Note that for
	// non-flushable batches (b.flushable == nil) makeRoomForWrite() added a
	// reference to the memtable which will prevent it from being flushed until
	// we unreference it. This reference is dropped in DB.commitApply().
	mem := d.mu.mem.mutable

	d.mu.Unlock()
	if err != nil {
		return nil, err
	}

	if d.opts.DisableWAL {
		return mem, nil
	}

	if b.flushable == nil {
		size, err = d.mu.log.SyncRecord(repr, syncWG, syncErr)
		if err != nil {
			panic(err)
		}
	}

	atomic.StoreUint64(&d.mu.log.size, uint64(size))
	return mem, err
}

type iterAlloc struct {
	dbi     Iterator
	merging mergingIter
	mlevels [3 + numLevels]mergingIterLevel
	levels  [numLevels]levelIter
}

var iterAllocPool = sync.Pool{
	New: func() interface{} {
		return &iterAlloc{}
	},
}

// newIterInternal constructs a new iterator, merging in batchIter as an extra
// level.
func (d *DB) newIterInternal(
	batchIter internalIterator, batchRangeDelIter internalIterator, s *Snapshot, o *IterOptions,
) *Iterator {
	if atomic.LoadInt32(&d.closed) != 0 {
		panic(ErrClosed)
	}

	// Grab and reference the current readState. This prevents the underlying
	// files in the associated version from being deleted if there is a current
	// compaction. The readState is unref'd by Iterator.Close().
	readState := d.loadReadState()

	// Determine the seqnum to read at after grabbing the read state (current and
	// memtables) above.
	var seqNum uint64
	if s != nil {
		seqNum = s.seqNum
	} else {
		seqNum = atomic.LoadUint64(&d.mu.versions.visibleSeqNum)
	}

	// Bundle various structures under a single umbrella in order to allocate
	// them together.
	buf := iterAllocPool.Get().(*iterAlloc)
	dbi := &buf.dbi
	dbi.alloc = buf
	dbi.cmp = d.cmp
	dbi.equal = d.equal
	dbi.merge = d.merge
	dbi.split = d.split
	dbi.readState = readState
	if o != nil {
		dbi.opts = *o
	}

	mlevels := buf.mlevels[:0]
	if batchIter != nil {
		mlevels = append(mlevels, mergingIterLevel{
			iter:         batchIter,
			rangeDelIter: batchRangeDelIter,
		})
	}

	// TODO(peter): We only need to add memtables which contain sequence numbers
	// older than seqNum. Unfortunately, memtables don't track their oldest
	// sequence number currently.
	memtables := readState.memtables
	for i := len(memtables) - 1; i >= 0; i-- {
		mem := memtables[i]
		mlevels = append(mlevels, mergingIterLevel{
			iter:         mem.newIter(&dbi.opts),
			rangeDelIter: mem.newRangeDelIter(&dbi.opts),
		})
	}

	// The level 0 files need to be added from newest to oldest.
	//
	// Note that level 0 files do not contain untruncated tombstones, even in the presence of
	// L0=>L0 compactions since such compactions output a single file. Therefore, we do not
	// need to wrap level 0 files individually in level iterators.
	current := readState.current
	for i := len(current.Files[0]) - 1; i >= 0; i-- {
		f := &current.Files[0][i]
		iter, rangeDelIter, err := d.newIters(f, &dbi.opts, nil)
		if err != nil {
			dbi.err = err
			return dbi
		}
		mlevels = append(mlevels, mergingIterLevel{
			iter:         iter,
			rangeDelIter: rangeDelIter,
		})
	}

	// Determine the final size for mlevels so that we can avoid any more
	// reallocations. This is important because each levelIter will hold a
	// reference to elements in mlevels.
	start := len(mlevels)
	for level := 1; level < len(current.Files); level++ {
		if len(current.Files[level]) == 0 {
			continue
		}
		mlevels = append(mlevels, mergingIterLevel{})
	}
	finalMLevels := mlevels
	mlevels = mlevels[start:]

	// Add level iterators for the remaining files.
	levels := buf.levels[:]
	for level := 1; level < len(current.Files); level++ {
		if len(current.Files[level]) == 0 {
			continue
		}

		var li *levelIter
		if len(levels) > 0 {
			li = &levels[0]
			levels = levels[1:]
		} else {
			li = &levelIter{}
		}

		li.init(&dbi.opts, d.cmp, d.newIters, current.Files[level], nil)
		li.initRangeDel(&mlevels[0].rangeDelIter)
		li.initSmallestLargestUserKey(&mlevels[0].smallestUserKey, &mlevels[0].largestUserKey,
			&mlevels[0].isLargestUserKeyRangeDelSentinel)
		mlevels[0].iter = li
		mlevels = mlevels[1:]
	}

	buf.merging.init(d.cmp, finalMLevels...)
	buf.merging.snapshot = seqNum
	dbi.iter = &buf.merging
	return dbi
}

// NewBatch returns a new empty write-only batch. Any reads on the batch will
// return an error. If the batch is committed it will be applied to the DB.
func (d *DB) NewBatch() *Batch {
	return newBatch(d)
}

// NewIndexedBatch returns a new empty read-write batch. Any reads on the batch
// will read from both the batch and the DB. If the batch is committed it will
// be applied to the DB. An indexed batch is slower that a non-indexed batch
// for insert operations. If you do not need to perform reads on the batch, use
// NewBatch instead.
func (d *DB) NewIndexedBatch() *Batch {
	return newIndexedBatch(d, d.opts.Comparer)
}

// NewIter returns an iterator that is unpositioned (Iterator.Valid() will
// return false). The iterator can be positioned via a call to SeekGE, SeekLT,
// First or Last. The iterator provides a point-in-time view of the current DB
// state. This view is maintained by preventing file deletions and preventing
// memtables referenced by the iterator from being deleted. Using an iterator
// to maintain a long-lived point-in-time view of the DB state can lead to an
// apparent memory and disk usage leak. Use snapshots (see NewSnapshot) for
// point-in-time snapshots which avoids these problems.
func (d *DB) NewIter(o *IterOptions) *Iterator {
	return d.newIterInternal(nil, /* batchIter */
		nil /* batchRangeDelIter */, nil /* snapshot */, o)
}

// NewSnapshot returns a point-in-time view of the current DB state. Iterators
// created with this handle will all observe a stable snapshot of the current
// DB state. The caller must call Snapshot.Close() when the snapshot is no
// longer needed. Snapshots are not persisted across DB restarts (close ->
// open). Unlike the implicit snapshot maintained by an iterator, a snapshot
// will not prevent memtables from being released or sstables from being
// deleted. Instead, a snapshot prevents deletion of sequence numbers
// referenced by the snapshot.
func (d *DB) NewSnapshot() *Snapshot {
	if atomic.LoadInt32(&d.closed) != 0 {
		panic(ErrClosed)
	}

	s := &Snapshot{
		db:     d,
		seqNum: atomic.LoadUint64(&d.mu.versions.visibleSeqNum),
	}
	d.mu.Lock()
	d.mu.snapshots.pushBack(s)
	d.mu.Unlock()
	return s
}

// Close closes the DB.
//
// It is not safe to close a DB until all outstanding iterators are closed.
// It is valid to call Close multiple times. Other methods should not be
// called after the DB has been closed.
func (d *DB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if atomic.LoadInt32(&d.closed) != 0 {
		panic(ErrClosed)
	}
	atomic.StoreInt32(&d.closed, 1)
	for d.mu.compact.compacting || d.mu.compact.flushing {
		d.mu.compact.cond.Wait()
	}
	err := d.tableCache.Close()
	if !d.opts.ReadOnly {
		err = firstError(err, d.mu.log.Close())
	} else if d.mu.log.LogWriter != nil {
		panic("pebble: log-writer should be nil in read-only mode")
	}
	err = firstError(err, d.fileLock.Close())

	// Note that versionSet.close() only closes the MANIFEST. The versions list
	// is still valid for the checks below.
	err = firstError(err, d.mu.versions.close())

	err = firstError(err, d.dataDir.Close())
	if d.dataDir != d.walDir {
		err = firstError(err, d.walDir.Close())
	}

	if err == nil {
		d.readState.val.unrefLocked()

		current := d.mu.versions.currentVersion()
		for v := d.mu.versions.versions.Front(); true; v = v.Next() {
			refs := v.Refs()
			if v == current {
				if refs != 1 {
					return fmt.Errorf("leaked iterators: current\n%s", v)
				}
				break
			}
			if refs != 0 {
				return fmt.Errorf("leaked iterators:\n%s", v)
			}
		}

		for _, mem := range d.mu.mem.queue {
			mem.readerUnref()
		}
		if reserved := atomic.LoadInt64(&d.memTableReserved); reserved != 0 {
			return fmt.Errorf("leaked memtable reservation: %d", reserved)
		}
	}
	return err
}

// Compact the specified range of keys in the database.
func (d *DB) Compact(
	start, end []byte, /* CompactionOptions */
) error {
	if atomic.LoadInt32(&d.closed) != 0 {
		panic(ErrClosed)
	}
	if d.opts.ReadOnly {
		return ErrReadOnly
	}

	iStart := base.MakeInternalKey(start, InternalKeySeqNumMax, InternalKeyKindMax)
	iEnd := base.MakeInternalKey(end, 0, 0)
	meta := []*fileMetadata{&fileMetadata{Smallest: iStart, Largest: iEnd}}

	d.mu.Lock()
	maxLevelWithFiles := 1
	cur := d.mu.versions.currentVersion()
	for level := 0; level < numLevels; level++ {
		if len(cur.Overlaps(level, d.cmp, start, end)) > 0 {
			maxLevelWithFiles = level + 1
		}
	}

	// Determine if any memtable overlaps with the compaction range. We wait for
	// any such overlap to flush (initiating a flush if necessary).
	mem, err := func() (flushable, error) {
		if ingestMemtableOverlaps(d.cmp, d.mu.mem.mutable, meta) {
			mem := d.mu.mem.mutable

			// We have to hold both commitPipeline.mu and DB.mu when calling
			// makeRoomForWrite(). Lock order requirements elsewhere force us to
			// unlock DB.mu in order to grab commitPipeline.mu first.
			d.mu.Unlock()
			d.commit.mu.Lock()
			d.mu.Lock()
			defer d.commit.mu.Unlock()
			if mem == d.mu.mem.mutable {
				// Only flush if the active memtable is unchanged.
				return mem, d.makeRoomForWrite(nil)
			}
			return mem, nil
		}
		// Check to see if any files overlap with any of the immutable
		// memtables. The queue is ordered from oldest to newest. We want to wait
		// for the newest table that overlaps.
		for i := len(d.mu.mem.queue) - 1; i >= 0; i-- {
			mem := d.mu.mem.queue[i]
			if ingestMemtableOverlaps(d.cmp, mem, meta) {
				return mem, nil
			}
		}
		return nil, nil
	}()

	d.mu.Unlock()

	if err != nil {
		return err
	}
	if mem != nil {
		<-mem.flushed()
	}

	for level := 0; level < maxLevelWithFiles; {
		manual := &manualCompaction{
			done:  make(chan error, 1),
			level: level,
			start: iStart,
			end:   iEnd,
		}
		if err := d.manualCompact(manual); err != nil {
			return err
		}
		level = manual.outputLevel
		if level == numLevels-1 {
			// A manual compaction of the bottommost level occured. There is no next
			// level to try and compact.
			break
		}
	}
	return nil
}

func (d *DB) manualCompact(manual *manualCompaction) error {
	d.mu.Lock()
	d.mu.compact.manual = append(d.mu.compact.manual, manual)
	d.maybeScheduleCompaction()
	d.mu.Unlock()
	return <-manual.done
}

// Flush the memtable to stable storage.
func (d *DB) Flush() error {
	flushDone, err := d.AsyncFlush()
	if err != nil {
		return err
	}
	<-flushDone
	return nil
}

// AsyncFlush asynchronously flushes the memtable to stable storage.
//
// If no error is returned, the caller can receive from the returned channel in
// order to wait for the flush to complete.
func (d *DB) AsyncFlush() (<-chan struct{}, error) {
	if atomic.LoadInt32(&d.closed) != 0 {
		panic(ErrClosed)
	}
	if d.opts.ReadOnly {
		return nil, ErrReadOnly
	}

	d.commit.mu.Lock()
	d.mu.Lock()
	mem := d.mu.mem.mutable
	err := d.makeRoomForWrite(nil)
	d.mu.Unlock()
	d.commit.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return mem.flushed(), nil
}

// Metrics returns metrics about the database.
func (d *DB) Metrics() *Metrics {
	metrics := &Metrics{}
	recycledLogs := d.logRecycler.count()

	d.mu.Lock()
	*metrics = d.mu.versions.metrics
	metrics.Compact.EstimatedDebt = d.mu.versions.picker.estimatedCompactionDebt(0)
	for _, m := range d.mu.mem.queue {
		metrics.MemTable.Size += m.totalBytes()
	}
	metrics.MemTable.Count = int64(len(d.mu.mem.queue))
	metrics.WAL.ObsoleteFiles = int64(recycledLogs)
	metrics.WAL.Size = atomic.LoadUint64(&d.mu.log.size)
	metrics.WAL.BytesIn = d.mu.log.bytesIn // protected by d.mu
	for i, n := 0, len(d.mu.mem.queue)-1; i < n; i++ {
		_, size := d.mu.mem.queue[i].logInfo()
		metrics.WAL.Size += size
	}
	metrics.WAL.BytesWritten = metrics.Levels[0].BytesIn + metrics.WAL.Size
	metrics.Levels[0].Score = float64(metrics.Levels[0].NumFiles) / float64(d.opts.L0CompactionThreshold)
	if p := d.mu.versions.picker; p != nil {
		for level := 1; level < numLevels; level++ {
			metrics.Levels[level].Score = float64(metrics.Levels[level].Size) / float64(p.levelMaxBytes[level])
		}
	}
	d.mu.Unlock()

	metrics.BlockCache = d.opts.Cache.Metrics()
	metrics.TableCache, metrics.Filter = d.tableCache.metrics()
	metrics.TableIters = int64(d.tableCache.iterCount())
	return metrics
}

// SSTables retrieves the current sstables. The returned slice is indexed by
// level and each level is indexed by the position of the sstable within the
// level. Note that this information may be out of date due to concurrent
// flushes and compactions.
func (d *DB) SSTables() [][]TableInfo {
	// Grab and reference the current readState.
	readState := d.loadReadState()
	defer readState.unref()

	// TODO(peter): This is somewhat expensive, especially on a large
	// database. It might be worthwhile to unify TableInfo and FileMetadata and
	// then we could simply return current.Files. Note that RocksDB is doing
	// something similar to the current code, so perhaps it isn't too bad.
	srcLevels := readState.current.Files
	var totalTables int
	for i := range srcLevels {
		totalTables += len(srcLevels[i])
	}

	destTables := make([]TableInfo, totalTables)
	destLevels := make([][]TableInfo, len(srcLevels))
	for i := range destLevels {
		srcLevel := srcLevels[i]
		destLevel := destTables[:len(srcLevel):len(srcLevel)]
		destTables = destTables[len(srcLevel):]
		for j := range destLevel {
			destLevel[j] = srcLevel[j].TableInfo()
		}
		destLevels[i] = destLevel
	}
	return destLevels
}

// EstimateDiskUsage returns the estimated filesystem space used in bytes for
// storing the range `[start, end]`. The estimation is computed as follows:
//
// - For sstables fully contained in the range the whole file size is included.
// - For sstables partially contained in the range the overlapping data block sizes
//   are included. Even if a data block partially overlaps, or we cannot determine
//   overlap due to abbreviated index keys, the full data block size is included in
//   the estimation. Note that unlike fully contained sstables, none of the
//   meta-block space is counted for partially overlapped files.
// - There may also exist WAL entries for unflushed keys in this range. This
//   estimation currently excludes space used for the range in the WAL.
func (d *DB) EstimateDiskUsage(start, end []byte) (uint64, error) {
	if atomic.LoadInt32(&d.closed) != 0 {
		panic(ErrClosed)
	}
	if d.opts.Comparer.Compare(start, end) > 0 {
		return 0, errors.New("invalid key-range specified (start > end)")
	}

	// Grab and reference the current readState. This prevents the underlying
	// files in the associated version from being deleted if there is a concurrent
	// compaction.
	readState := d.loadReadState()
	defer readState.unref()

	var totalSize uint64
	for level, files := range readState.current.Files {
		if level > 0 {
			// We can only use `Overlaps` to restrict `files` at L1+ since at L0 it
			// expands the range iteratively until it has found a set of files that
			// do not overlap any other L0 files outside that set.
			files = readState.current.Overlaps(level, d.opts.Comparer.Compare, start, end)
		}
		for fileIdx, file := range files {
			if level > 0 && fileIdx > 0 && fileIdx < len(files)-1 {
				// The files to the left and the right at least partially overlap
				// with `file`, which means `file` is fully contained within the
				// range specified by `[start, end]`.
				totalSize += file.Size
			} else if d.opts.Comparer.Compare(start, file.Smallest.UserKey) <= 0 &&
				d.opts.Comparer.Compare(file.Largest.UserKey, end) <= 0 {
				// The range fully contains the file, so skip looking it up in
				// table cache/looking at its indexes, and add the full file size.
				totalSize += file.Size
			} else if d.opts.Comparer.Compare(file.Smallest.UserKey, end) <= 0 &&
				d.opts.Comparer.Compare(start, file.Largest.UserKey) <= 0 {
				size, err := d.tableCache.estimateDiskUsage(&file, start, end)
				if err != nil {
					return 0, err
				}
				totalSize += size
			}
		}
	}
	return totalSize, nil
}

func (d *DB) walPreallocateSize() int {
	// Set the WAL preallocate size to 110% of the memtable size. Note that there
	// is a bit of apples and oranges in units here as the memtabls size
	// corresponds to the memory usage of the memtable while the WAL size is the
	// size of the batches (plus overhead) stored in the WAL.
	//
	// TODO(peter): 110% of the memtable size is quite hefty for a block
	// size. This logic is taken from GetWalPreallocateBlockSize in
	// RocksDB. Could a smaller preallocation block size be used?
	size := d.opts.MemTableSize
	size = (size / 10) + size
	return size
}

func (d *DB) newMemTable() *memTable {
	size := d.mu.mem.nextSize
	if d.mu.mem.nextSize < d.opts.MemTableSize {
		d.mu.mem.nextSize *= 2
		if d.mu.mem.nextSize > d.opts.MemTableSize {
			d.mu.mem.nextSize = d.opts.MemTableSize
		}
	}
	return newMemTable(d.opts, size, &d.memTableReserved)
}

// makeRoomForWrite ensures that the memtable has room to hold the contents of
// Batch. It reserves the space in the memtable and adds a reference to the
// memtable. The caller must later ensure that the memtable is unreferenced. If
// the memtable is full, or a nil Batch is provided, the current memtable is
// rotated (marked as immutable) and a new mutable memtable is allocated. This
// memtable rotation also causes a log rotation.
//
// Both DB.mu and commitPipeline.mu must be held by the caller. Note that DB.mu
// may be released and reacquired.
func (d *DB) makeRoomForWrite(b *Batch) error {
	force := b == nil || b.flushable != nil
	stalled := false
	for {
		if d.mu.mem.switching {
			d.mu.mem.cond.Wait()
			continue
		}
		if b != nil && b.flushable == nil {
			err := d.mu.mem.mutable.prepare(b)
			if err != arenaskl.ErrArenaFull {
				if stalled {
					stalled = false
					d.opts.EventListener.WriteStallEnd()
				}
				return err
			}
		} else if !force {
			if stalled {
				stalled = false
				d.opts.EventListener.WriteStallEnd()
			}
			return nil
		}
		// force || err == ErrArenaFull, so we need to rotate the current memtable.
		{
			var size uint64
			for i := range d.mu.mem.queue {
				size += d.mu.mem.queue[i].totalBytes()
			}
			if size >= uint64(d.opts.MemTableStopWritesThreshold)*uint64(d.opts.MemTableSize) {
				// We have filled up the current memtable, but already queued memtables
				// are still flushing, so we wait.
				if !stalled {
					stalled = true
					d.opts.EventListener.WriteStallBegin(WriteStallBeginInfo{
						Reason: "memtable count limit reached",
					})
				}
				d.mu.compact.cond.Wait()
				continue
			}
		}
		if len(d.mu.versions.currentVersion().Files[0]) >= d.opts.L0StopWritesThreshold {
			// There are too many level-0 files, so we wait.
			if !stalled {
				stalled = true
				d.opts.EventListener.WriteStallBegin(WriteStallBeginInfo{
					Reason: "L0 file count limit exceeded",
				})
			}
			d.mu.compact.cond.Wait()
			continue
		}

		var newLogNum uint64
		var newLogFile vfs.File
		var prevLogSize uint64
		var err error

		if !d.opts.DisableWAL {
			jobID := d.mu.nextJobID
			d.mu.nextJobID++
			newLogNum = d.mu.versions.getNextFileNum()
			d.mu.mem.switching = true
			d.mu.Unlock()

			newLogName := base.MakeFilename(d.opts.FS, d.walDirname, fileTypeLog, newLogNum)

			// Try to use a recycled log file. Recycling log files is an important
			// performance optimization as it is faster to sync a file that has
			// already been written, than one which is being written for the first
			// time. This is due to the need to sync file metadata when a file is
			// being written for the first time. Note this is true even if file
			// preallocation is performed (e.g. fallocate).
			recycleLogNum := d.logRecycler.peek()
			if recycleLogNum > 0 {
				recycleLogName := base.MakeFilename(d.opts.FS, d.walDirname, fileTypeLog, recycleLogNum)
				newLogFile, err = d.opts.FS.ReuseForWrite(recycleLogName, newLogName)
			} else {
				newLogFile, err = d.opts.FS.Create(newLogName)
			}

			if err == nil {
				// TODO(peter): RocksDB delays sync of the parent directory until the
				// first time the log is synced. Is that worthwhile?
				err = d.walDir.Sync()
			}

			if err == nil {
				prevLogSize = uint64(d.mu.log.Size())
				err = d.mu.log.Close()
				if err != nil {
					newLogFile.Close()
				} else {
					newLogFile = vfs.NewSyncingFile(newLogFile, vfs.SyncingFileOptions{
						BytesPerSync:    d.opts.BytesPerSync,
						PreallocateSize: d.walPreallocateSize(),
					})
				}
			}

			if recycleLogNum > 0 {
				err = firstError(err, d.logRecycler.pop(recycleLogNum))
			}

			d.opts.EventListener.WALCreated(WALCreateInfo{
				JobID:           jobID,
				Path:            newLogName,
				FileNum:         newLogNum,
				RecycledFileNum: recycleLogNum,
				Err:             err,
			})

			d.mu.Lock()
			d.mu.mem.switching = false
			d.mu.mem.cond.Broadcast()

			d.mu.versions.metrics.WAL.Files++
		}

		if err != nil {
			// TODO(peter): avoid chewing through file numbers in a tight loop if there
			// is an error here.
			//
			// What to do here? Stumbling on doesn't seem worthwhile. If we failed to
			// close the previous log it is possible we lost a write.
			panic(err)
		}

		if !d.opts.DisableWAL {
			d.mu.log.queue = append(d.mu.log.queue, newLogNum)
			d.mu.log.LogWriter = record.NewLogWriter(newLogFile, newLogNum)
			d.mu.log.LogWriter.SetMinSyncInterval(d.opts.WALMinSyncInterval)
		}

		imm := d.mu.mem.mutable
		imm.logSize = prevLogSize
		imm.flushManual = b == nil

		// If we are manually flushing and we used less than half of the bytes in
		// the memtable, don't increase the size for the next memtable. This
		// reduces memtable memory pressure when an application is frequently
		// manually flushing.
		if imm.flushManual && uint64(imm.availBytes()) > imm.totalBytes()/2 {
			d.mu.mem.nextSize = int(imm.totalBytes())
		}

		if b != nil && b.flushable != nil {
			// The batch is too large to fit in the memtable so add it directly to
			// the immutable queue. The flushable batch is associated with the same
			// log as the immutable memtable, but logically occurs after it in
			// seqnum space. So give the flushable batch the logNum and clear it from
			// the immutable log. This is done as a defensive measure to prevent the
			// WAL containing the large batch from being deleted prematurely if the
			// corresponding memtable is flushed without flushing the large batch.
			//
			// See DB.commitWrite for the special handling of log writes for large
			// batches. In particular, the large batch has already written to
			// imm.logNum.
			b.flushable.logNum, imm.logNum = imm.logNum, 0
			d.mu.mem.queue = append(d.mu.mem.queue, b.flushable)
		}

		// Create a new memtable, scheduling the previous one for flushing. We do
		// this even if the previous memtable was empty because the DB.Flush
		// mechanism is dependent on being able to wait for the empty memtable to
		// flush. We can't just mark the empty memtable as flushed here because we
		// also have to wait for all previous immutable tables to
		// flush. Additionally, the memtable is tied to particular WAL file and we
		// want to go through the flush path in order to recycle that WAL file.
		d.mu.mem.mutable = d.newMemTable()
		// NB: newLogNum corresponds to the WAL that contains mutations that are
		// present in the new memtable. When immutable memtables are flushed to
		// disk, a VersionEdit will be created telling the manifest the minimum
		// unflushed log number (which will be the next one in d.mu.mem.mutable
		// that was not flushed).
		d.mu.mem.mutable.logNum = newLogNum
		d.mu.mem.queue = append(d.mu.mem.queue, d.mu.mem.mutable)
		d.updateReadStateLocked()
		if imm.writerUnref() {
			d.maybeScheduleFlush()
		}
		force = false
	}
}

// firstError returns the first non-nil error of err0 and err1, or nil if both
// are nil.
func firstError(err0, err1 error) error {
	if err0 != nil {
		return err0
	}
	return err1
}
