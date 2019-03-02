// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package pebble provides an ordered key/value store.
package pebble // import "github.com/petermattis/pebble"

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/internal/arenaskl"
	"github.com/petermattis/pebble/internal/record"
	"github.com/petermattis/pebble/storage"
)

const (
	// minTableCacheSize is the minimum size of the table cache.
	minTableCacheSize = 64

	// numNonTableCacheFiles is an approximation for the number of MaxOpenFiles
	// that we don't use for table caches.
	numNonTableCacheFiles = 10
)

type flushable interface {
	newIter(o *db.IterOptions) internalIterator
	newRangeDelIter(o *db.IterOptions) internalIterator
	flushed() chan struct{}
	readyForFlush() bool
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
	NewIter(o *db.IterOptions) *Iterator

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
// The Options struct in the db package holds the optional parameters for the
// DB, including a Comparer to define a 'less than' relationship over keys. It
// is always valid to pass a nil *Options, which means to use the default
// parameter values. Any zero field of a non-nil *Options also means to use the
// default value for that parameter. Thus, the code below uses a custom
// Comparer, but the default values for every other parameter:
//
//	db := pebble.Open(&db.Options{
//		Comparer: myComparer,
//	})
type DB struct {
	dirname        string
	opts           *db.Options
	cmp            *db.Comparer
	merge          db.Merge

	tableCache tableCache
	newIters   tableNewIters

	commit   *commitPipeline
	fileLock io.Closer

	largeBatchThreshold int
	optionsFileNum      uint64

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

		closed    bool
		nextJobID int

		versions versionSet

		log struct {
			number uint64
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
		}

		compact struct {
			cond           sync.Cond
			flushing       bool
			compacting     bool
			pendingOutputs map[uint64]struct{}
			manual         []*manualCompaction
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
	var seqNum uint64
	d.mu.Lock()
	if s != nil {
		seqNum = s.seqNum
	} else {
		seqNum = atomic.LoadUint64(&d.mu.versions.visibleSeqNum)
	}
	// Grab and reference the current version to prevent its underlying files
	// from being deleted if we have a concurrent compaction. Note that
	// version.unref() can be called without holding DB.mu.
	current := d.mu.versions.currentVersion()
	current.ref()
	memtables := d.mu.mem.queue
	d.mu.Unlock()

	var buf struct {
		dbi Iterator
		get getIter
	}

	get := &buf.get
	get.cmp = d.cmp.Compare
	get.equal = d.cmp.Equal
	get.newIters = d.newIters
	get.snapshot = seqNum
	get.key = key
	get.batch = b
	get.mem = memtables
	get.l0 = current.files[0]
	get.version = current

	i := &buf.dbi
	i.cmp = d.cmp.Compare
	i.equal = d.cmp.Equal
	i.merge = d.merge
	i.iter = get
	i.version = current

	defer i.Close()
	if !i.Next() {
		err := i.Error()
		if err != nil {
			return nil, err
		}
		return nil, db.ErrNotFound
	}
	return i.Value(), nil
}

// Set sets the value for the given key. It overwrites any previous value
// for that key; a DB is not a multi-map.
//
// It is safe to modify the contents of the arguments after Set returns.
func (d *DB) Set(key, value []byte, opts *db.WriteOptions) error {
	b := newBatch(d)
	defer b.release()
	_ = b.Set(key, value, opts)
	return d.Apply(b, opts)
}

// Delete deletes the value for the given key. Deletes are blind all will
// succeed even if the given key does not exist.
//
// It is safe to modify the contents of the arguments after Delete returns.
func (d *DB) Delete(key []byte, opts *db.WriteOptions) error {
	b := newBatch(d)
	defer b.release()
	_ = b.Delete(key, opts)
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
	_ = b.DeleteRange(start, end, opts)
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
	_ = b.Merge(key, value, opts)
	return d.Apply(b, opts)
}

// Apply the operations contained in the batch to the DB. If the batch is large
// the contents of the batch may be retained by the database. If that occurs
// the batch contents will be cleared preventing the caller from attempting to
// reuse them.
//
// It is safe to modify the contents of the arguments after Apply returns.
func (d *DB) Apply(batch *Batch, opts *db.WriteOptions) error {
	if int(batch.memTableSize) >= d.largeBatchThreshold {
		batch.flushable = newFlushableBatch(batch, d.opts.Comparer)
	}
	err := d.commit.Commit(batch, opts.GetSync())
	if err == nil {
		// If this is a large batch, we need to clear the batch contents as the
		// flushable batch may still be present in the flushables queue.
		if batch.flushable != nil {
			batch.data = nil
		}
	}
	return err
}

func (d *DB) commitApply(b *Batch, mem *memTable) error {
	if b.flushable != nil {
		// This is a large batch which was already added to the immutable queue.
		return nil
	}
	err := mem.apply(b, b.seqNum())
	if err != nil {
		return err
	}
	if mem.unref() {
		d.mu.Lock()
		d.maybeScheduleFlush()
		d.mu.Unlock()
	}
	return nil
}

func (d *DB) commitSync() error {
	if d.opts.DisableWAL {
		return errors.New("pebble: WAL disabled")
	}

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

	if b.flushable != nil {
		b.flushable.seqNum = b.seqNum()
	}

	// Switch out the memtable if there was not enough room to store the
	// batch.
	if err := d.makeRoomForWrite(b); err != nil {
		return nil, err
	}

	if d.opts.DisableWAL {
		return d.mu.mem.mutable, nil
	}

	_, err := d.mu.log.WriteRecord(b.data)
	if err != nil {
		panic(err)
	}
	return d.mu.mem.mutable, err
}

// newIterInternal constructs a new iterator, merging in batchIter as an extra
// level.
func (d *DB) newIterInternal(
	batchIter internalIterator,
	batchRangeDelIter internalIterator,
	s *Snapshot,
	o *db.IterOptions,
) *Iterator {
	var seqNum uint64
	d.mu.Lock()
	if s != nil {
		seqNum = s.seqNum
	} else {
		seqNum = atomic.LoadUint64(&d.mu.versions.visibleSeqNum)
	}
	// Grab and reference the current version to prevent its underlying files
	// from being deleted if we have a concurrent compaction. Note that
	// version.unref() can be called without holding DB.mu.
	current := d.mu.versions.currentVersion()
	current.ref()
	memtables := d.mu.mem.queue
	d.mu.Unlock()

	// Bundle various structures under a single umbrella in order to allocate
	// them together.
	var buf struct {
		dbi           Iterator
		merging       mergingIter
		iters         [3 + numLevels]internalIterator
		rangeDelIters [3 + numLevels]internalIterator
		levels        [numLevels]levelIter
	}

	dbi := &buf.dbi
	dbi.opts = o
	dbi.cmp = d.cmp.Compare
	dbi.equal = d.cmp.Equal
	dbi.merge = d.merge
	dbi.version = current

	iters := buf.iters[:0]
	rangeDelIters := buf.rangeDelIters[:0]
	if batchIter != nil {
		iters = append(iters, batchIter)
		rangeDelIters = append(rangeDelIters, batchRangeDelIter)
	}

	// TODO(peter): We only need to add memtables which contain sequence numbers
	// older than seqNum. Unfortunately, memtables don't track their oldest
	// sequence number currently.
	for i := len(memtables) - 1; i >= 0; i-- {
		mem := memtables[i]
		iters = append(iters, mem.newIter(o))
		rangeDelIters = append(rangeDelIters, mem.newRangeDelIter(o))
	}

	// The level 0 files need to be added from newest to oldest.
	for i := len(current.files[0]) - 1; i >= 0; i-- {
		f := &current.files[0][i]
		iter, rangeDelIter, err := d.newIters(f)
		if err != nil {
			dbi.err = err
			return dbi
		}
		iters = append(iters, iter)
		rangeDelIters = append(rangeDelIters, rangeDelIter)
	}

	start := len(rangeDelIters)
	for level := 1; level < len(current.files); level++ {
		if len(current.files[level]) == 0 {
			continue
		}
		rangeDelIters = append(rangeDelIters, nil)
	}
	buf.merging.rangeDelIters = rangeDelIters
	rangeDelIters = rangeDelIters[start:]

	// Add level iterators for the remaining files.
	levels := buf.levels[:]
	for level := 1; level < len(current.files); level++ {
		if len(current.files[level]) == 0 {
			continue
		}

		var li *levelIter
		if len(levels) > 0 {
			li = &levels[0]
			levels = levels[1:]
		} else {
			li = &levelIter{}
		}

		li.init(o, d.cmp.Compare, d.newIters, current.files[level])
		li.initRangeDel(&rangeDelIters[0])
		iters = append(iters, li)
		rangeDelIters = rangeDelIters[1:]
	}

	buf.merging.init(d.cmp, iters...)
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
func (d *DB) NewIter(o *db.IterOptions) *Iterator {
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
	s := &Snapshot{db: d}
	d.mu.Lock()
	s.seqNum = atomic.LoadUint64(&d.mu.versions.visibleSeqNum)
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
	if d.mu.closed {
		return nil
	}
	for d.mu.compact.compacting || d.mu.compact.flushing {
		d.mu.compact.cond.Wait()
	}
	err := d.tableCache.Close()
	err = firstError(err, d.mu.log.Close())
	err = firstError(err, d.fileLock.Close())
	d.commit.Close()
	d.mu.closed = true

	if err == nil {
		current := d.mu.versions.currentVersion()
		for v := d.mu.versions.versions.front(); true; v = v.next {
			refs := atomic.LoadInt32(&v.refs)
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
	}
	return err
}

// Compact the specified range of keys in the database.
func (d *DB) Compact(start, end []byte /* CompactionOptions */) error {
	iStart := db.MakeInternalKey(start, db.InternalKeySeqNumMax, db.InternalKeyKindMax)
	iEnd := db.MakeInternalKey(end, 0, 0)
	meta := []*fileMetadata{&fileMetadata{smallest: iStart, largest: iEnd}}

	d.mu.Lock()
	maxLevelWithFiles := 1
	cur := d.mu.versions.currentVersion()
	for level := 0; level < numLevels; level++ {
		if len(cur.overlaps(level, d.cmp.Compare, start, end)) > 0 {
			maxLevelWithFiles = level + 1
		}
	}

	// Determine if any memtable overlaps with the compaction range. We wait for
	// any such overlap to flush (initiating a flush if necessary).
	mem, err := func() (flushable, error) {
		if ingestMemtableOverlaps(d.cmp.Compare, d.mu.mem.mutable, meta) {
			mem := d.mu.mem.mutable
			return mem, d.makeRoomForWrite(nil)
		}
		// Check to see if any files overlap with any of the immutable
		// memtables. The queue is ordered from oldest to newest. We want to wait
		// for the newest table that overlaps.
		for i := len(d.mu.mem.queue) - 1; i >= 0; i-- {
			mem := d.mu.mem.queue[i]
			if ingestMemtableOverlaps(d.cmp.Compare, mem, meta) {
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

	for level := 0; level < maxLevelWithFiles; level++ {
		manual := &manualCompaction{
			done:  make(chan error, 1),
			level: level,
			start: iStart,
			end:   iEnd,
		}
		if err := d.manualCompact(manual); err != nil {
			return err
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
	d.mu.Lock()
	mem := d.mu.mem.mutable
	err := d.makeRoomForWrite(nil)
	d.mu.Unlock()
	if err != nil {
		return err
	}
	<-mem.flushed()
	return nil
}

// AsyncFlush asynchronously flushes the memtable to stable storage.
//
// TODO(peter): untested
func (d *DB) AsyncFlush() error {
	d.mu.Lock()
	err := d.makeRoomForWrite(nil)
	d.mu.Unlock()
	return err
}

func (d *DB) throttleWrite() {
	if len(d.mu.versions.currentVersion().files[0]) <= d.opts.L0SlowdownWritesThreshold {
		return
	}
	// fmt.Printf("L0 slowdown writes threshold\n")
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
	force := b == nil || b.flushable != nil
	for {
		if d.mu.mem.switching {
			d.mu.mem.cond.Wait()
			continue
		}
		if b != nil && b.flushable == nil {
			err := d.mu.mem.mutable.prepare(b)
			if err == nil {
				return nil
			}
			if err != arenaskl.ErrArenaFull {
				return err
			}
		} else if !force {
			return nil
		}
		if len(d.mu.mem.queue) >= d.opts.MemTableStopWritesThreshold {
			// We have filled up the current memtable, but the previous one is still
			// being compacted, so we wait.
			// fmt.Printf("memtable stop writes threshold\n")
			d.mu.compact.cond.Wait()
			continue
		}
		if len(d.mu.versions.currentVersion().files[0]) > d.opts.L0StopWritesThreshold {
			// There are too many level-0 files, so we wait.
			// fmt.Printf("L0 stop writes threshold\n")
			d.mu.compact.cond.Wait()
			continue
		}

		var newLogNumber uint64
		var newLogFile storage.File
		var err error

		if !d.opts.DisableWAL {
			newLogNumber = d.mu.versions.nextFileNum()
			d.mu.mem.switching = true
			d.mu.Unlock()

			newLogFile, err = d.opts.Storage.Create(dbFilename(d.dirname, fileTypeLog, newLogNumber))
			if err == nil {
				err = d.mu.log.Close()
				if err != nil {
					newLogFile.Close()
				}
			}

			d.mu.Lock()
			d.mu.mem.switching = false
			d.mu.mem.cond.Broadcast()
		}

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
		if !d.opts.DisableWAL {
			d.mu.log.number = newLogNumber
			d.mu.log.LogWriter = record.NewLogWriter(newLogFile)
		}
		imm := d.mu.mem.mutable
		if imm.empty() {
			// If the mutable memtable is empty, then remove it from the queue. We'll
			// reuse the memtable by leaving d.mu.mem.mutable non nil.
			d.mu.mem.queue = d.mu.mem.queue[:len(d.mu.mem.queue)-1]
			imm = nil
		} else {
			d.mu.mem.mutable = nil
		}
		var scheduleFlush bool
		if b != nil && b.flushable != nil {
			// The batch is too large to fit in the memtable so add it directly to
			// the immutable queue.
			d.mu.mem.queue = append(d.mu.mem.queue, b.flushable)
			scheduleFlush = true
		}
		if d.mu.mem.mutable == nil {
			// Create a new memtable if we are flushing the previous mutable
			// memtable.
			d.mu.mem.mutable = newMemTable(d.opts)
		}
		d.mu.mem.queue = append(d.mu.mem.queue, d.mu.mem.mutable)
		if (imm != nil && imm.unref()) || scheduleFlush {
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
