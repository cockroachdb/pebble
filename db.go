// Copyright 2012 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package pebble provides an ordered key/value store.
//
// TODO(peter):
//
// - MemTable
//   - Allow arbitrary sized batches. RocksDB allows this by allowing
//     additional arena blocks to be added as necessary to hold the contents of
//     a batch. With the memTable.prepare() functionality, we could allocate a
//     custom sized arena to hold a batch that is larger than the normal
//     arena. Need to be cognizant of what this would mean for
//     throughput. Should memTable internally have a stack of skiplists?
//
// - Miscellaneous
//   - Implement {block,table}Iter.SeekLT
//   - Add support for referencing a version and preventing file deletion for a
//     referenced version. Use in DB.Get and DB.NewIter.
//   - Merge operator
//   - Debug/event logging
//   - Faster block cache (sharded?)
//     - Reserve block cache memory for memtables
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
//   - Add options for hardcoded settings (e.g. l0CompactionTrigger)
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
//
// - Optimizations
//   - In-order insertion into memtable/indexed-batches
//   - Add InlineKey to mergingIter
//     - Speed up Next/Prev with fast-path comparison check
//   - Fractional cascading on lookups
//     https://github.com/facebook/rocksdb/wiki/Indexing-SST-Files-for-Better-Lookup-Performance
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

	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/record"
	"github.com/petermattis/pebble/sstable"
	"github.com/petermattis/pebble/storage"
)

const (
	// l0CompactionTrigger is the number of files at which level-0 compaction
	// starts.
	l0CompactionTrigger = 4

	// l0SlowdownWritesTrigger is the soft limit on number of level-0 files.
	// We slow down writes at this point.
	l0SlowdownWritesTrigger = 8

	// l0StopWritesTrigger is the maximum number of level-0 files. We stop
	// writes at this point.
	l0StopWritesTrigger = 12

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
			mutable *memTable     // the current mutable memTable
			queue   memTableQueue // queue of memtables (mutable is at head)
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
	// TODO(peter): do we need to ref-count the current version, so that we don't
	// delete its underlying files if we have a concurrent compaction?
	current := d.mu.versions.currentVersion()
	memtables := d.mu.mem.queue.iter()
	d.mu.Unlock()

	ikey := db.MakeInternalKey(key, snapshot, db.InternalKeyKindMax)

	// Look in the memtables before going to the on-disk current version.
	for ; !memtables.done(); memtables.next() {
		mem := memtables.table()
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
	if err := d.makeRoomForWrite(false); err != nil {
		return err
	}
	return d.commit.commit(batch, opts.GetSync())
}

func (d *DB) commitApply(b *Batch, mem *memTable) error {
	return mem.apply(b, b.seqNum())
}

func (d *DB) commitSync(log *record.LogWriter, pos, n int64) error {
	return log.Sync( /* pos, n */ )
}

func (d *DB) commitWrite(b *Batch) (*memTable, *record.LogWriter, int64, error) {
	// NB: commitWrite is called with d.mu locked.

	// TODO(peter): If the memtable is full, allocate a new one. Mark the full
	// memtable as ready to be flushed as soon as the reference count drops to 0.
	err := d.mu.mem.mutable.prepare(b)
	if err != nil {
		return nil, nil, 0, err
	}
	pos, err := d.mu.log.WriteRecord(b.data)
	return d.mu.mem.mutable, d.mu.log.LogWriter, pos, err
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
	// TODO(peter): do we need to ref-count the current version, so that we don't
	// delete its underlying files if we have a concurrent compaction?
	current := d.mu.versions.currentVersion()
	memtables := d.mu.mem.queue.iter()
	d.mu.Unlock()

	var buf struct {
		dbi    dbIter
		iters  [3 + numLevels]db.InternalIterator
		levels [numLevels]levelIter
	}

	iters := buf.iters[:0]
	if batchIter != nil {
		iters = append(iters, batchIter)
	}

	for ; !memtables.done(); memtables.next() {
		mem := memtables.table()
		iters = append(iters, mem.NewIter(o))
	}

	// The level 0 files need to be added from newest to oldest.
	for i := len(current.files[0]) - 1; i >= 0; i-- {
		f := current.files[0][i]
		iter, err := d.newIter(f.fileNum)
		if err != nil {
			return &dbIter{err: err}
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

	dbi := &buf.dbi
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
	return newIndexedBatch(d, d.opts.GetComparer())
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
	d.mu.closed = true
	return err
}

// Compact TODO(peter)
func (d *DB) Compact(start, end []byte /* CompactionOptions */) error {
	panic("pebble.DB: Compact unimplemented")
}

// Flush the memtable to stable storage. TODO(peter)
func (d *DB) Flush() error {
	panic("pebble.DB: Flush unimplemented")
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
		comparatorName: opts.GetComparer().Name,
		nextFileNumber: manifestFileNum + 1,
	}
	manifestFilename := dbFilename(dirname, fileTypeManifest, manifestFileNum)
	f, err := opts.GetStorage().Create(manifestFilename)
	if err != nil {
		return fmt.Errorf("pebble: could not create %q: %v", manifestFilename, err)
	}
	defer func() {
		if retErr != nil {
			opts.GetStorage().Remove(manifestFilename)
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
	return setCurrentFile(dirname, opts.GetStorage(), manifestFileNum)
}

// Open opens a LevelDB whose files live in the given directory.
func Open(dirname string, opts *db.Options) (*DB, error) {
	d := &DB{
		dirname:   dirname,
		opts:      opts,
		cmp:       opts.GetComparer().Compare,
		inlineKey: opts.GetComparer().InlineKey,
	}
	tableCacheSize := opts.GetMaxOpenFiles() - numNonTableCacheFiles
	if tableCacheSize < minTableCacheSize {
		tableCacheSize = minTableCacheSize
	}
	d.tableCache.init(dirname, opts.GetStorage(), d.opts, tableCacheSize)
	d.newIter = d.tableCache.newIter
	d.commit = newCommitPipeline(commitEnv{
		mu:            &d.mu.Mutex,
		logSeqNum:     &d.mu.versions.logSeqNum,
		visibleSeqNum: &d.mu.versions.visibleSeqNum,
		apply:         d.commitApply,
		sync:          d.commitSync,
		write:         d.commitWrite,
	})
	d.mu.mem.mutable = newMemTable(d.opts)
	d.mu.mem.queue.init()
	d.mu.mem.queue.pushLocked(d.mu.mem.mutable)
	d.mu.compact.cond = sync.Cond{L: &d.mu}
	d.mu.compact.pendingOutputs = make(map[uint64]struct{})
	fs := opts.GetStorage()

	d.mu.Lock()
	defer d.mu.Unlock()

	// Lock the database directory.
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
	} else if opts.GetErrorIfDBExists() {
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
		maxSeqNum, err := d.replayLogFile(&ve, fs, filepath.Join(dirname, lf.name))
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
	defer func() {
		if logFile != nil {
			logFile.Close()
		}
	}()
	d.mu.log.LogWriter = record.NewLogWriter(logFile)

	// Write a new manifest to disk.
	if err := d.mu.versions.logAndApply(dirname, &ve); err != nil {
		return nil, err
	}

	d.deleteObsoleteFiles()
	d.maybeScheduleCompaction()

	d.fileLock, fileLock = fileLock, nil
	return d, nil
}

// replayLogFile replays the edits in the named log file.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) replayLogFile(
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
		mem      *memTable
		batchBuf = new(bytes.Buffer)
		rr       = record.NewReader(file)
	)
	for {
		r, err := rr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
		_, err = io.Copy(batchBuf, r)
		if err != nil {
			return 0, err
		}

		if batchBuf.Len() < batchHeaderLen {
			return 0, fmt.Errorf("pebble: corrupt log file %q", filename)
		}
		var b Batch
		b.data = batchBuf.Bytes()
		seqNum := b.seqNum()
		seqNum1 := seqNum + uint64(b.count())
		if maxSeqNum < seqNum1 {
			maxSeqNum = seqNum1
		}

		if mem == nil {
			mem = newMemTable(d.opts)
		}

		t := b.iter()
		for ; seqNum != seqNum1; seqNum++ {
			kind, ukey, value, ok := t.next()
			if !ok {
				return 0, fmt.Errorf("pebble: corrupt log file %q", filename)
			}
			mem.set(db.MakeInternalKey(ukey, seqNum, kind), value)
		}
		if len(t) != 0 {
			return 0, fmt.Errorf("pebble: corrupt log file %q", filename)
		}

		// TODO(peter): if mem is large enough, write it to a level-0 table and set
		// mem = nil.

		batchBuf.Reset()
	}

	if mem != nil && !mem.Empty() {
		meta, err := d.writeLevel0Table(fs, mem)
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
func (d *DB) writeLevel0Table(fs storage.Storage, mem *memTable) (meta fileMetadata, err error) {
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
		iter db.InternalIterator
	)
	defer func() {
		if iter != nil {
			err = firstError(err, iter.Close())
		}
		if tw != nil {
			err = firstError(err, tw.Close())
		}
		if file != nil {
			err = firstError(err, file.Close())
		}
		if err != nil {
			fs.Remove(filename)
			meta = fileMetadata{}
		}
	}()

	file, err = fs.Create(filename)
	if err != nil {
		return fileMetadata{}, err
	}
	tw = sstable.NewWriter(file, d.opts)

	iter = mem.NewIter(nil)
	iter.Next()
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
	tw = nil

	// TODO(peter): currently, closing a table.Writer closes its underlying file.
	// We have to re-open the file to Sync or Stat it, which seems stupid.
	file, err = fs.Open(filename)
	if err != nil {
		return fileMetadata{}, err
	}

	if err1 := file.Sync(); err1 != nil {
		return fileMetadata{}, err1
	}

	if stat, err1 := file.Stat(); err1 != nil {
		return fileMetadata{}, err1
	} else {
		size := stat.Size()
		if size < 0 {
			return fileMetadata{}, fmt.Errorf("pebble: table file %q has negative size %d", filename, size)
		}
		meta.size = uint64(size)
	}

	// TODO(peter): compaction stats.

	return meta, nil
}

// makeRoomForWrite ensures that there is room in d.mu.mem for the next write.
func (d *DB) makeRoomForWrite(force bool) error {
	// TODO(peter): Fix makeRoomForWrite to not require DB.mu to be locked.
	d.mu.Lock()
	defer d.mu.Unlock()

	allowDelay := !force
	for {
		// TODO(peter): check any previous sticky error, if the paranoid option is
		// set.

		if allowDelay && len(d.mu.versions.currentVersion().files[0]) > l0SlowdownWritesTrigger {
			// We are getting close to hitting a hard limit on the number of
			// L0 files. Rather than delaying a single write by several
			// seconds when we hit the hard limit, start delaying each
			// individual write by 1ms to reduce latency variance.
			d.mu.Unlock()
			time.Sleep(1 * time.Millisecond)
			d.mu.Lock()
			allowDelay = false
			// TODO(peter): how do we ensure we are still 'at the front of the writer
			// queue'?
			continue
		}

		if !force && d.mu.mem.mutable.ApproximateMemoryUsage() <= d.opts.GetWriteBufferSize() {
			// There is room in the current memtable.
			break
		}

		if d.mu.mem.queue.len() >= 2 {
			// We have filled up the current memtable, but the previous
			// one is still being compacted, so we wait.
			d.mu.compact.cond.Wait()
			continue
		}

		if len(d.mu.versions.currentVersion().files[0]) > l0StopWritesTrigger {
			// There are too many level-0 files.
			d.mu.compact.cond.Wait()
			continue
		}

		// Attempt to switch to a new memtable and trigger compaction of old
		// TODO(peter): drop and re-acquire d.mu around the I/O.
		newLogNumber := d.mu.versions.nextFileNum()
		newLogFile, err := d.opts.GetStorage().Create(dbFilename(d.dirname, fileTypeLog, newLogNumber))
		if err != nil {
			// TODO(peter): avoid chewing through file numbers in a tight loop if
			// there is an error here.
			return err
		}
		newLog := record.NewLogWriter(newLogFile)
		if err := d.mu.log.Close(); err != nil {
			newLog.Close()
			return err
		}
		// NB: When the immutable memtable is flushed to disk it will apply a
		// versionEdit to the manifest telling it that log files < d.mu.log.number
		// have been applied.
		d.mu.log.number, d.mu.log.LogWriter = newLogNumber, newLog
		d.mu.mem.mutable = newMemTable(d.opts)
		d.mu.mem.queue.pushLocked(d.mu.mem.mutable)
		force = false
		d.maybeScheduleCompaction()
	}
	return nil
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

	fs := d.opts.GetStorage()
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
