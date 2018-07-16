// Copyright 2012 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package pebble provides an ordered key/value store.
//
// TODO(peter):
//
// - Benchmarking
//   - synctest
//   - mvccScan
//   - mvccPut
//
// - MemTable
//   - Allow arbitrary sized batches. If a batch is too large to fit in a
//     single memTable, then allocate a custom-sized memTable to hold it.
//
// - Miscellaneous
//   - Implement {block,table}Iter.SeekLT
//   - Merge operator
//   - Debug/event logging
//   - Faster block cache (sharded?)
//     - Reserve block cache memory for memtables
//     - Lock-free cache: http://myui.github.io/publications/ICDE10_conf_full_409.pdf
//     - Tiny-LFU: https://github.com/dgryski/go-tinylfu
//     - Take inspiration from Caffeine: https://github.com/ben-manes/caffeine
//
// - Commit pipeline
//   - Rate limiting user writes
//   - Controlled delay
//     - Check if the queue has been drained within the last N seconds, if not
//       set queue timeout to 10-30ms
//
// - DeleteRange
//   - Store range tombstones in the memtable
//   - Write range tombstones to sstables in range-del block
//   - Read range tombstones from sstable
//   - Process range tombstones during iteration
//
// - Compactions
//   - Concurrent compactions
//   - Manual compaction
//   - Manual flush
//   - Dynamic level bytes
//   - Rate limiting (disk and CPU)
//
// - Options
//   - Options should specify sizing and tunable knobs, not enable or disable
//     significant bits of functionality
//   - Parse RocksDB OPTIONS file
//
// - Sstables
//   - SSTable ingestion
//   - Whole file bloom filter
//   - Prefix extractor and prefix bloom filter
//   - Read/write RocksDB table properties
//
// - Iterators
//   - Prefix same as start
//   - Iterate upper bound
//   - Iterate lower bound
//   - Debug checks that iterators are always closed
//
// - Testing
//   - Datadriven test infrastructure
//   - DB.NewIter
//   - Batch.NewIter
//   - Expand dbIter tests (with {merging,memTable,batch,level}Iter)
//   - LogWriter (test all error paths)
//   - commitPipeline (test all error paths)
//   - Benchmark cache under concurrency
//
// - Optimizations
//   - In-order insertion into memtable/indexed-batches
//   - Add InlineKey to mergingIter
//     - Speed up Next/Prev with fast-path comparison check
//   - Fractional cascading on lookups
//     https://github.com/facebook/rocksdb/wiki/Indexing-SST-Files-for-Better-Lookup-Performance
//   - IO scheduler ala Scylla to reduce IO latency
//   - Dynamically adjust compaction bandwidth based on user requests
package pebble // import "github.com/petermattis/pebble"

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/petermattis/pebble/arenaskl"
	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/rate"
	"github.com/petermattis/pebble/record"
	"github.com/petermattis/pebble/sstable"
	"github.com/petermattis/pebble/storage"
)

const (
	// minTableCacheSize is the minimum size of the table cache.
	minTableCacheSize = 64

	// numNonTableCacheFiles is an approximation for the number of MaxOpenFiles
	// that we don't use for table caches.
	numNonTableCacheFiles = 10
)

// Reader is a readable key/value store.
//
// It is safe to call Get and NewIter from concurrent goroutines.
type Reader interface {
	// Get gets the value for the given key. It returns ErrNotFound if the DB
	// does not contain the key.
	//
	// The caller should not modify the contents of the returned slice, but
	// it is safe to modify the contents of the argument after Get returns.
	Get(key []byte, o *db.ReadOptions) (value []byte, err error)

	// NewIter returns an iterator that is unpositioned (Iterator.Valid() will
	// return false). The iterator can be positioned via a call to SeekGE,
	// SeekLT, First or Last.
	NewIter(o *db.ReadOptions) db.Iterator

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
	Apply(batch *Batch, o *db.WriteOptions) error

	// Delete deletes the value for the given key. Deletes are blind all will
	// succeed even if the given key does not exist.
	//
	// It is safe to modify the contents of the arguments after Delete returns.
	Delete(key []byte, o *db.WriteOptions) error

	// DeleteRange deletes all of the keys (and values) in the range [start,end)
	// (inclusive on start, exclusive on end).
	//
	// It is safe to modify the contents of the arguments after Delete returns.
	DeleteRange(start, end []byte, o *db.WriteOptions) error

	// Merge merges the value for the given key. The details of the merge are
	// dependent upon the configured merge operation.
	//
	// It is safe to modify the contents of the arguments after Merge returns.
	Merge(key, value []byte, o *db.WriteOptions) error

	// Set sets the value for the given key. It overwrites any previous value
	// for that key; a DB is not a multi-map.
	//
	// It is safe to modify the contents of the arguments after Set returns.
	Set(key, value []byte, o *db.WriteOptions) error
}

// TODO(peter): document DB.
type DB struct {
	dirname   string
	opts      *db.Options
	cmp       db.Compare
	inlineKey db.InlineKey

	tableCache tableCache
	newIter    tableNewIter

	commit   *commitPipeline
	fileLock io.Closer

	// Rate limiter for how much bandwidth to allow for commits, compactions, and
	// flushes.
	//
	// TODO(peter): Add a controller module that balances the limits so that
	// commits cannot happen faster than flushes and the backlog of compaction
	// work does not grow too large.
	commitController  *controller
	compactController *controller
	flushController   *controller

	// TODO(peter): describe exactly what this mutex protects. So far: every
	// field in the struct.
	mu struct {
		sync.Mutex

		closed bool

		versions versionSet

		log struct {
			number uint64
			*record.LogWriter
		}

		mem struct {
			cond sync.Cond
			// The current mutable memTable.
			mutable *memTable
			// Queue of memtables (mutable is at end). Elements are added to the end
			// of the slice and removed from the beginning. Once an index is set it
			// is never modified making a fixed slice immutable and safe for
			// concurrent reads.
			queue []*memTable
			// True when the memtable is actively been switched.
			switching bool
		}

		compact struct {
			cond           sync.Cond
			active         bool
			pendingOutputs map[uint64]struct{}
		}
	}
}

var _ Reader = (*DB)(nil)
var _ Writer = (*DB)(nil)

// Get gets the value for the given key. It returns ErrNotFound if the DB
// does not contain the key.
//
// The caller should not modify the contents of the returned slice, but
// it is safe to modify the contents of the argument after Get returns.
func (d *DB) Get(key []byte, opts *db.ReadOptions) ([]byte, error) {
	d.mu.Lock()
	snapshot := atomic.LoadUint64(&d.mu.versions.visibleSeqNum)
	// Grab and reference the current version to prevent its underlying files
	// from being deleted if we have a concurrent compaction. Note that
	// version.unref() can be called without holding DB.mu.
	current := d.mu.versions.currentVersion()
	current.ref()
	defer current.unref()
	memtables := d.mu.mem.queue
	d.mu.Unlock()

	ikey := db.MakeInternalKey(key, snapshot, db.InternalKeyKindMax)

	// Look in the memtables before going to the on-disk current version.
	for i := len(memtables) - 1; i >= 0; i-- {
		mem := memtables[i]
		iter := mem.NewIter(opts)
		iter.SeekGE(key)
		value, conclusive, err := internalGet(iter, d.cmp, ikey)
		if conclusive {
			return value, err
		}
	}

	// TODO(peter): update stats, maybe schedule compaction.

	return current.get(ikey, d.newIter, d.cmp, opts)
}

// Set sets the value for the given key. It overwrites any previous value
// for that key; a DB is not a multi-map.
//
// It is safe to modify the contents of the arguments after Set returns.
func (d *DB) Set(key, value []byte, opts *db.WriteOptions) error {
	b := newBatch(d)
	defer b.release()
	b.Set(key, value, opts)
	return d.Apply(b, opts)
}

// Delete deletes the value for the given key. Deletes are blind all will
// succeed even if the given key does not exist.
//
// It is safe to modify the contents of the arguments after Delete returns.
func (d *DB) Delete(key []byte, opts *db.WriteOptions) error {
	b := newBatch(d)
	defer b.release()
	b.Delete(key, opts)
	return d.Apply(b, opts)
}

// DeleteRange deletes all of the keys (and values) in the range [start,end)
// (inclusive on start, exclusive on end).
//
// It is safe to modify the contents of the arguments after DeleteRange
// returns.
func (d *DB) DeleteRange(start, end []byte, opts *db.WriteOptions) error {
	b := newBatch(d)
	defer b.release()
	b.DeleteRange(start, end, opts)
	return d.Apply(b, opts)
}

// Merge adds an action to the DB that merges the value at key with the new
// value. The details of the merge are dependent upon the configured merge
// operator.
//
// It is safe to modify the contents of the arguments after Merge returns.
func (d *DB) Merge(key, value []byte, opts *db.WriteOptions) error {
	b := newBatch(d)
	defer b.release()
	b.Merge(key, value, opts)
	return d.Apply(b, opts)
}

// Apply the operations contained in the batch to the DB.
//
// It is safe to modify the contents of the arguments after Apply returns.
func (d *DB) Apply(batch *Batch, opts *db.WriteOptions) error {
	return d.commit.Commit(batch, opts.GetSync())
}

func (d *DB) commitApply(b *Batch, mem *memTable) error {
	err := mem.apply(b, b.seqNum())
	if err != nil {
		return err
	}
	if mem.unref() {
		d.mu.Lock()
		d.maybeScheduleCompaction()
		d.mu.Unlock()
	}
	return nil
}

func (d *DB) commitSync() error {
	d.mu.Lock()
	log := d.mu.log.LogWriter
	d.mu.Unlock()
	// NB: The log might have been closed after we unlock d.mu. That's ok because
	// it will have been synced and all we're guaranteeing is that the log that
	// was open at the start of this call was synced by the end of it.
	return log.Sync()
}

func (d *DB) commitWrite(b *Batch) (*memTable, error) {
	// NB: commitWrite is called with d.mu locked.

	// Throttle writes if there are too many L0 tables.
	d.throttleWrite()

	// Switch out the memtable if there was not enough room to store the
	// batch.
	if err := d.makeRoomForWrite(b); err != nil {
		return nil, err
	}

	_, err := d.mu.log.WriteRecord(b.data)
	return d.mu.mem.mutable, err
}

// newIterInternal constructs a new iterator, merging in batchIter as an extra
// level.
func (d *DB) newIterInternal(batchIter db.InternalIterator, o *db.ReadOptions) db.Iterator {
	d.mu.Lock()
	seqNum := atomic.LoadUint64(&d.mu.versions.visibleSeqNum)
	// TODO(peter): The sstables in current are guaranteed to have sequence
	// numbers less than d.mu.versions.logSeqNum, so why does dbIter need to check
	// sequence numbers for every iter? Perhaps the sequence number filtering
	// should be folded into mergingIter (or InternalIterator).
	//
	// Grab and reference the current version to prevent its underlying files
	// from being deleted if we have a concurrent compaction. Note that
	// version.unref() can be called without holding DB.mu.
	current := d.mu.versions.currentVersion()
	current.ref()
	memtables := d.mu.mem.queue
	d.mu.Unlock()

	var buf struct {
		dbi    dbIter
		iters  [3 + numLevels]db.InternalIterator
		levels [numLevels]levelIter
	}

	dbi := &buf.dbi
	dbi.version = current

	iters := buf.iters[:0]
	if batchIter != nil {
		iters = append(iters, batchIter)
	}

	for i := len(memtables) - 1; i >= 0; i-- {
		mem := memtables[i]
		iters = append(iters, mem.NewIter(o))
	}

	// The level 0 files need to be added from newest to oldest.
	for i := len(current.files[0]) - 1; i >= 0; i-- {
		f := current.files[0][i]
		iter, err := d.newIter(f.fileNum)
		if err != nil {
			dbi.err = err
			return dbi
		}
		iters = append(iters, iter)
	}

	// Add level iterators for the remaining files.
	levels := buf.levels[:]
	for level := 1; level < len(current.files); level++ {
		n := len(current.files[level])
		if n == 0 {
			continue
		}

		var li *levelIter
		if len(levels) > 0 {
			li = &levels[0]
			levels = levels[1:]
		} else {
			li = &levelIter{}
		}

		li.init(d.cmp, d.newIter, current.files[level])
		iters = append(iters, li)
	}

	dbi.iter = newMergingIter(d.cmp, iters...)
	dbi.seqNum = seqNum
	return dbi
}

// NewIter returns an iterator that is unpositioned (Iterator.Valid() will
// return false). The iterator can be positioned via a call to SeekGE,
// SeekLT, First or Last.
func (d *DB) NewIter(o *db.ReadOptions) db.Iterator {
	return d.newIterInternal(nil, o)
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

// Close closes the DB.
//
// It is not safe to close a DB until all outstanding iterators are closed.
// It is valid to call Close multiple times. Other methods should not be
// called after the DB has been closed.
func (d *DB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.mu.closed {
		return nil
	}
	for d.mu.compact.active {
		d.mu.compact.cond.Wait()
	}
	err := d.tableCache.Close()
	err = firstError(err, d.mu.log.Close())
	err = firstError(err, d.fileLock.Close())
	d.commit.Close()
	d.mu.closed = true
	return err
}

// Compact TODO(peter)
func (d *DB) Compact(start, end []byte /* CompactionOptions */) error {
	panic("pebble.DB: Compact unimplemented")
}

// Flush the memtable to stable storage.
//
// TODO(peter): untested
func (d *DB) Flush() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	// TODO(peter): Wait for the memtable to be flushed.
	return d.makeRoomForWrite(nil)
}

// Ingest TODO(peter)
func (d *DB) Ingest(paths []string) error {
	panic("pebble.DB: Ingest unimplemented")
}

type fileNumAndName struct {
	num  uint64
	name string
}

type fileNumAndNameSlice []fileNumAndName

func (p fileNumAndNameSlice) Len() int           { return len(p) }
func (p fileNumAndNameSlice) Less(i, j int) bool { return p[i].num < p[j].num }
func (p fileNumAndNameSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func createDB(dirname string, opts *db.Options) (retErr error) {
	const manifestFileNum = 1
	ve := versionEdit{
		comparatorName: opts.Comparer.Name,
		nextFileNumber: manifestFileNum + 1,
	}
	manifestFilename := dbFilename(dirname, fileTypeManifest, manifestFileNum)
	f, err := opts.Storage.Create(manifestFilename)
	if err != nil {
		return fmt.Errorf("pebble: could not create %q: %v", manifestFilename, err)
	}
	defer func() {
		if retErr != nil {
			opts.Storage.Remove(manifestFilename)
		}
	}()
	defer f.Close()

	recWriter := record.NewWriter(f)
	w, err := recWriter.Next()
	if err != nil {
		return err
	}
	err = ve.encode(w)
	if err != nil {
		return err
	}
	err = recWriter.Close()
	if err != nil {
		return err
	}
	return setCurrentFile(dirname, opts.Storage, manifestFileNum)
}

// Open opens a LevelDB whose files live in the given directory.
func Open(dirname string, opts *db.Options) (*DB, error) {
	const defaultRateLimit = rate.Limit(50 << 20) // 50 MB/sec
	const defaultBurst = 1 << 20                  // 1 MB

	opts = opts.EnsureDefaults()
	d := &DB{
		dirname:           dirname,
		opts:              opts,
		cmp:               opts.Comparer.Compare,
		inlineKey:         opts.Comparer.InlineKey,
		commitController:  newController(rate.NewLimiter(defaultRateLimit, defaultBurst)),
		compactController: newController(rate.NewLimiter(defaultRateLimit, defaultBurst)),
		flushController:   newController(rate.NewLimiter(rate.Inf, defaultBurst)),
	}
	tableCacheSize := opts.MaxOpenFiles - numNonTableCacheFiles
	if tableCacheSize < minTableCacheSize {
		tableCacheSize = minTableCacheSize
	}
	d.tableCache.init(dirname, opts.Storage, d.opts, tableCacheSize)
	d.newIter = d.tableCache.newIter
	d.commit = newCommitPipeline(commitEnv{
		mu:            &d.mu.Mutex,
		logSeqNum:     &d.mu.versions.logSeqNum,
		visibleSeqNum: &d.mu.versions.visibleSeqNum,
		controller:    d.commitController,
		apply:         d.commitApply,
		sync:          d.commitSync,
		write:         d.commitWrite,
	})
	d.mu.mem.cond.L = &d.mu.Mutex
	d.mu.mem.mutable = newMemTable(d.opts)
	d.mu.mem.queue = append(d.mu.mem.queue, d.mu.mem.mutable)
	d.mu.compact.cond.L = &d.mu.Mutex
	d.mu.compact.pendingOutputs = make(map[uint64]struct{})
	// TODO(peter): This initialization is funky.
	d.mu.versions.versions.mu = &d.mu.Mutex

	d.mu.Lock()
	defer d.mu.Unlock()

	// Lock the database directory.
	fs := opts.Storage
	err := fs.MkdirAll(dirname, 0755)
	if err != nil {
		return nil, err
	}
	fileLock, err := fs.Lock(dbFilename(dirname, fileTypeLock, 0))
	if err != nil {
		return nil, err
	}
	defer func() {
		if fileLock != nil {
			fileLock.Close()
		}
	}()

	if _, err := fs.Stat(dbFilename(dirname, fileTypeCurrent, 0)); os.IsNotExist(err) {
		// Create the DB if it did not already exist.
		if err := createDB(dirname, opts); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, fmt.Errorf("pebble: database %q: %v", dirname, err)
	} else if opts.ErrorIfDBExists {
		return nil, fmt.Errorf("pebble: database %q already exists", dirname)
	}

	// Load the version set.
	err = d.mu.versions.load(dirname, opts)
	if err != nil {
		return nil, err
	}

	// Replay any newer log files than the ones named in the manifest.
	var ve versionEdit
	ls, err := fs.List(dirname)
	if err != nil {
		return nil, err
	}
	var logFiles fileNumAndNameSlice
	for _, filename := range ls {
		ft, fn, ok := parseDBFilename(filename)
		if ok && ft == fileTypeLog && (fn >= d.mu.versions.logNumber || fn == d.mu.versions.prevLogNumber) {
			logFiles = append(logFiles, fileNumAndName{fn, filename})
		}
	}
	sort.Sort(logFiles)
	for _, lf := range logFiles {
		maxSeqNum, err := d.replayWAL(&ve, fs, filepath.Join(dirname, lf.name))
		if err != nil {
			return nil, err
		}
		d.mu.versions.markFileNumUsed(lf.num)
		if d.mu.versions.logSeqNum < maxSeqNum {
			d.mu.versions.logSeqNum = maxSeqNum
		}
	}
	d.mu.versions.visibleSeqNum = d.mu.versions.logSeqNum

	// Create an empty .log file.
	ve.logNumber = d.mu.versions.nextFileNum()
	d.mu.log.number = ve.logNumber
	logFile, err := fs.Create(dbFilename(dirname, fileTypeLog, ve.logNumber))
	if err != nil {
		return nil, err
	}
	d.mu.log.LogWriter = record.NewLogWriter(logFile)

	// Write a new manifest to disk.
	if err := d.mu.versions.logAndApply(d.opts, dirname, &ve); err != nil {
		return nil, err
	}

	d.deleteObsoleteFiles()
	d.maybeScheduleCompaction()

	d.fileLock, fileLock = fileLock, nil
	return d, nil
}

// replayWAL replays the edits in the specified log file.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) replayWAL(
	ve *versionEdit,
	fs storage.Storage,
	filename string,
) (maxSeqNum uint64, err error) {
	file, err := fs.Open(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var (
		b   Batch
		buf bytes.Buffer
		mem *memTable
		rr  = record.NewReader(file)
	)
	for {
		r, err := rr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
		_, err = io.Copy(&buf, r)
		if err != nil {
			return 0, err
		}

		if buf.Len() < batchHeaderLen {
			return 0, fmt.Errorf("pebble: corrupt log file %q", filename)
		}
		b = Batch{}
		b.data = buf.Bytes()
		b.refreshMemTableSize()
		seqNum := b.seqNum()
		maxSeqNum = seqNum + uint64(b.count())

		if mem == nil {
			mem = newMemTable(d.opts)
		}

		for {
			err := mem.prepare(&b)
			if err == arenaskl.ErrArenaFull {
				// TODO(peter): write the memtable to disk.
				panic(err)
			}
			if err != nil {
				return 0, err
			}
			break
		}

		if err := mem.apply(&b, seqNum); err != nil {
			return 0, err
		}
		if mem.unref() {
			d.maybeScheduleCompaction()
		}

		buf.Reset()
	}

	if mem != nil && !mem.Empty() {
		meta, err := d.writeLevel0Table(fs, mem.NewIter(nil))
		if err != nil {
			return 0, err
		}
		ve.newFiles = append(ve.newFiles, newFileEntry{level: 0, meta: meta})
		// Strictly speaking, it's too early to delete meta.fileNum from d.pendingOutputs,
		// but we are replaying the log file, which happens before Open returns, so there
		// is no possibility of deleteObsoleteFiles being called concurrently here.
		delete(d.mu.compact.pendingOutputs, meta.fileNum)
	}

	return maxSeqNum, nil
}

// firstError returns the first non-nil error of err0 and err1, or nil if both
// are nil.
func firstError(err0, err1 error) error {
	if err0 != nil {
		return err0
	}
	return err1
}

// writeLevel0Table writes a memtable to a level-0 on-disk table.
//
// If no error is returned, it adds the file number of that on-disk table to
// d.pendingOutputs. It is the caller's responsibility to remove that fileNum
// from that set when it has been applied to d.mu.versions.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) writeLevel0Table(
	fs storage.Storage, iter db.InternalIterator,
) (meta fileMetadata, err error) {
	meta.fileNum = d.mu.versions.nextFileNum()
	filename := dbFilename(d.dirname, fileTypeTable, meta.fileNum)
	d.mu.compact.pendingOutputs[meta.fileNum] = struct{}{}
	defer func(fileNum uint64) {
		if err != nil {
			delete(d.mu.compact.pendingOutputs, fileNum)
		}
	}(meta.fileNum)

	// Release the d.mu lock while doing I/O.
	// Note the unusual order: Unlock and then Lock.
	d.mu.Unlock()
	defer d.mu.Lock()

	var (
		file storage.File
		tw   *sstable.Writer
	)
	defer func() {
		if iter != nil {
			err = firstError(err, iter.Close())
		}
		if tw != nil {
			err = firstError(err, tw.Close())
		}
		if err != nil {
			fs.Remove(filename)
			meta = fileMetadata{}
		}
	}()

	iter.First()
	if !iter.Valid() {
		return fileMetadata{}, fmt.Errorf("pebble: memtable empty")
	}

	file, err = fs.Create(filename)
	if err != nil {
		return fileMetadata{}, err
	}
	file = newRateLimitedFile(file, d.flushController)
	tw = sstable.NewWriter(file, d.opts, d.opts.Level(0))

	meta.smallest = iter.Key().Clone()
	for {
		meta.largest = iter.Key()
		if err1 := tw.Add(meta.largest, iter.Value()); err1 != nil {
			return fileMetadata{}, err1
		}
		if !iter.Next() {
			break
		}
	}
	meta.largest = meta.largest.Clone()

	if err1 := iter.Close(); err1 != nil {
		iter = nil
		return fileMetadata{}, err1
	}
	iter = nil

	if err1 := tw.Close(); err1 != nil {
		tw = nil
		return fileMetadata{}, err1
	}

	stat, err := tw.Stat()
	if err != nil {
		return fileMetadata{}, err
	}
	size := stat.Size()
	if size < 0 {
		return fileMetadata{}, fmt.Errorf("pebble: table file %q has negative size %d", filename, size)
	}
	meta.size = uint64(size)
	tw = nil

	// TODO(peter): After a flush we set the commit rate to 110% of the flush
	// rate. The rationale behind the 110% is to account for slack. Investigate a
	// more principled way of setting this.
	// d.commitController.limiter.SetLimit(rate.Limit(d.flushController.sensor.Rate()))
	// if false {
	// 	fmt.Printf("flush: %.1f MB/s\n", d.flushController.sensor.Rate()/float64(1<<20))
	// }

	// TODO(peter): compaction stats.

	return meta, nil
}

func (d *DB) throttleWrite() {
	if len(d.mu.versions.currentVersion().files[0]) <= d.opts.L0SlowdownWritesThreshold {
		return
	}
	fmt.Printf("L0 slowdown writes threshold\n")
	// We are getting close to hitting a hard limit on the number of L0
	// files. Rather than delaying a single write by several seconds when we hit
	// the hard limit, start delaying each individual write by 1ms to reduce
	// latency variance.
	//
	// TODO(peter): Use more sophisticated rate limiting.
	d.mu.Unlock()
	time.Sleep(1 * time.Millisecond)
	d.mu.Lock()
}

func (d *DB) makeRoomForWrite(b *Batch) error {
	for {
		if b != nil {
			err := d.mu.mem.mutable.prepare(b)
			if err == nil {
				return nil
			}
			if err != arenaskl.ErrArenaFull {
				return err
			}
		}
		if d.mu.mem.switching {
			d.mu.mem.cond.Wait()
			continue
		}
		if len(d.mu.mem.queue) >= d.opts.MemTableStopWritesThreshold {
			// We have filled up the current memtable, but the previous one is still
			// being compacted, so we wait.
			fmt.Printf("memtable stop writes threshold\n")
			d.mu.compact.cond.Wait()
			continue
		}
		if len(d.mu.versions.currentVersion().files[0]) > d.opts.L0StopWritesThreshold {
			// There are too many level-0 files, so we wait.
			fmt.Printf("L0 stop writes threshold\n")
			d.mu.compact.cond.Wait()
			continue
		}

		newLogNumber := d.mu.versions.nextFileNum()
		d.mu.mem.switching = true
		d.mu.Unlock()

		newLogFile, err := d.opts.Storage.Create(dbFilename(d.dirname, fileTypeLog, newLogNumber))
		if err == nil {
			err = d.mu.log.Close()
			if err != nil {
				newLogFile.Close()
			}
		}

		d.mu.Lock()
		d.mu.mem.switching = false
		d.mu.mem.cond.Broadcast()

		if err != nil {
			// TODO(peter): avoid chewing through file numbers in a tight loop if there
			// is an error here.
			//
			// What to do here? Stumbling on doesn't seem worthwhile. If we failed to
			// close the previous log it is possible we lost a write.
			panic(err)
		}

		// NB: When the immutable memtable is flushed to disk it will apply a
		// versionEdit to the manifest telling it that log files < d.mu.log.number
		// have been applied.
		d.mu.log.number = newLogNumber
		d.mu.log.LogWriter = record.NewLogWriter(newLogFile)
		imm := d.mu.mem.mutable
		d.mu.mem.mutable = newMemTable(d.opts)
		d.mu.mem.queue = append(d.mu.mem.queue, d.mu.mem.mutable)
		if imm.unref() {
			d.maybeScheduleCompaction()
		}
	}
}

// deleteObsoleteFiles deletes those files that are no longer needed.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) deleteObsoleteFiles() {
	liveFileNums := map[uint64]struct{}{}
	for fileNum := range d.mu.compact.pendingOutputs {
		liveFileNums[fileNum] = struct{}{}
	}
	d.mu.versions.addLiveFileNums(liveFileNums)
	logNumber := d.mu.versions.logNumber
	manifestFileNumber := d.mu.versions.manifestFileNumber

	// Release the d.mu lock while doing I/O.
	// Note the unusual order: Unlock and then Lock.
	d.mu.Unlock()
	defer d.mu.Lock()

	fs := d.opts.Storage
	list, err := fs.List(d.dirname)
	if err != nil {
		// Ignore any filesystem errors.
		return
	}
	for _, filename := range list {
		fileType, fileNum, ok := parseDBFilename(filename)
		if !ok {
			return
		}
		keep := true
		switch fileType {
		case fileTypeLog:
			// TODO(peter): also look at prevLogNumber?
			keep = fileNum >= logNumber
		case fileTypeManifest:
			keep = fileNum >= manifestFileNumber
		case fileTypeTable:
			_, keep = liveFileNums[fileNum]
		}
		if keep {
			continue
		}
		if fileType == fileTypeTable {
			d.tableCache.evict(fileNum)
		}
		// Ignore any file system errors.
		fs.Remove(filepath.Join(d.dirname, filename))
	}
}
