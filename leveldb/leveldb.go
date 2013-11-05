// Copyright 2012 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package leveldb provides an ordered key/value store.
//
// BUG: This package is incomplete.
package leveldb

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"code.google.com/p/leveldb-go/leveldb/db"
	"code.google.com/p/leveldb-go/leveldb/memdb"
	"code.google.com/p/leveldb-go/leveldb/record"
	"code.google.com/p/leveldb-go/leveldb/table"
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

// TODO: document DB.
type DB struct {
	dirname string
	opts    *db.Options
	icmp    internalKeyComparer
	// icmpOpts is a copy of opts that overrides the Comparer to be icmp.
	icmpOpts db.Options

	tableCache tableCache

	// TODO: describe exactly what this mutex protects. So far: every field
	// below.
	mu sync.Mutex

	fileLock  io.Closer
	logNumber uint64
	logFile   db.File
	log       *record.Writer

	versions versionSet

	// mem is non-nil and the MemDB pointed to is mutable. imm is possibly
	// nil, but if non-nil, the MemDB pointed to is immutable and will be
	// copied out as an on-disk table. mem's sequence numbers are all
	// higher than imm's, and imm's sequence numbers are all higher than
	// those on-disk.
	mem, imm *memdb.MemDB

	compactionCond sync.Cond
	compacting     bool

	pendingOutputs map[uint64]struct{}
}

var _ db.DB = (*DB)(nil)

func (d *DB) Get(key []byte, opts *db.ReadOptions) ([]byte, error) {
	d.mu.Lock()
	// TODO: add an opts.LastSequence field, or a DB.Snapshot method?
	snapshot := d.versions.lastSequence
	current := d.versions.currentVersion()
	// TODO: do we need to ref-count the current version, so that we don't
	// delete its underlying files if we have a concurrent compaction?
	memtables := [2]*memdb.MemDB{d.mem, d.imm}
	d.mu.Unlock()

	ikey := makeInternalKey(nil, key, internalKeyKindMax, snapshot)

	// Look in the memtables before going to the on-disk current version.
	for _, mem := range memtables {
		if mem == nil {
			continue
		}
		value, conclusive, err := internalGet(mem.Find(ikey, opts), d.icmp.userCmp, key)
		if conclusive {
			return value, err
		}
	}

	// TODO: update stats, maybe schedule compaction.

	return current.get(ikey, &d.tableCache, d.icmp.userCmp, opts)
}

func (d *DB) Set(key, value []byte, opts *db.WriteOptions) error {
	var batch Batch
	batch.Set(key, value)
	return d.Apply(batch, opts)
}

func (d *DB) Delete(key []byte, opts *db.WriteOptions) error {
	var batch Batch
	batch.Delete(key)
	return d.Apply(batch, opts)
}

func (d *DB) Apply(batch Batch, opts *db.WriteOptions) error {
	if len(batch.data) == 0 {
		return nil
	}
	n := batch.count()
	if n == invalidBatchCount {
		return errors.New("leveldb: invalid batch")
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if err := d.makeRoomForWrite(false); err != nil {
		return err
	}

	seqNum := d.versions.lastSequence + 1
	batch.setSeqNum(seqNum)
	d.versions.lastSequence += uint64(n)

	// Write the batch to the log.
	// TODO: drop and re-acquire d.mu around the I/O.
	w, err := d.log.Next()
	if err != nil {
		return fmt.Errorf("leveldb: could not create log entry: %v", err)
	}
	if _, err = w.Write(batch.data); err != nil {
		return fmt.Errorf("leveldb: could not write log entry: %v", err)
	}
	if opts.GetSync() {
		if err = d.log.Flush(); err != nil {
			return fmt.Errorf("leveldb: could not flush log entry: %v", err)
		}
		if err = d.logFile.Sync(); err != nil {
			return fmt.Errorf("leveldb: could not sync log entry: %v", err)
		}
	}

	// Apply the batch to the memtable.
	for iter, ikey := batch.iter(), internalKey(nil); ; seqNum++ {
		kind, ukey, value, ok := iter.next()
		if !ok {
			break
		}
		ikey = makeInternalKey(ikey, ukey, kind, seqNum)
		d.mem.Set(ikey, value, nil)
	}

	if seqNum != d.versions.lastSequence+1 {
		panic("leveldb: inconsistent batch count")
	}
	return nil
}

func (d *DB) Find(key []byte, opts *db.ReadOptions) db.Iterator {
	panic("unimplemented")
}

func (d *DB) Close() error {
	err := d.tableCache.Close()
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.fileLock == nil {
		return err
	}
	err = firstError(err, d.fileLock.Close())
	d.fileLock = nil
	return err
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
		comparatorName: opts.GetComparer().Name(),
		nextFileNumber: manifestFileNum + 1,
	}
	manifestFilename := dbFilename(dirname, fileTypeManifest, manifestFileNum)
	f, err := opts.GetFileSystem().Create(manifestFilename)
	if err != nil {
		return fmt.Errorf("leveldb: could not create %q: %v", manifestFilename, err)
	}
	defer func() {
		if retErr != nil {
			opts.GetFileSystem().Remove(manifestFilename)
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
	return setCurrentFile(dirname, opts.GetFileSystem(), manifestFileNum)
}

// Open opens a LevelDB whose files live in the given directory.
func Open(dirname string, opts *db.Options) (*DB, error) {
	d := &DB{
		dirname:        dirname,
		opts:           opts,
		icmp:           internalKeyComparer{opts.GetComparer()},
		pendingOutputs: make(map[uint64]struct{}),
	}
	if opts != nil {
		d.icmpOpts = *opts
	}
	d.icmpOpts.Comparer = d.icmp
	tableCacheSize := opts.GetMaxOpenFiles() - numNonTableCacheFiles
	if tableCacheSize < minTableCacheSize {
		tableCacheSize = minTableCacheSize
	}
	d.tableCache.init(dirname, opts.GetFileSystem(), &d.icmpOpts, tableCacheSize)
	d.mem = memdb.New(&d.icmpOpts)
	d.compactionCond = sync.Cond{L: &d.mu}
	fs := opts.GetFileSystem()

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
		return nil, fmt.Errorf("leveldb: database %q: %v", dirname, err)
	} else if opts.GetErrorIfDBExists() {
		return nil, fmt.Errorf("leveldb: database %q already exists", dirname)
	}

	// Load the version set.
	err = d.versions.load(dirname, opts)
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
		if ok && ft == fileTypeLog && (fn >= d.versions.logNumber || fn == d.versions.prevLogNumber) {
			logFiles = append(logFiles, fileNumAndName{fn, filename})
		}
	}
	sort.Sort(logFiles)
	for _, lf := range logFiles {
		maxSeqNum, err := d.replayLogFile(&ve, fs, filepath.Join(dirname, lf.name))
		if err != nil {
			return nil, err
		}
		d.versions.markFileNumUsed(lf.num)
		if d.versions.lastSequence < maxSeqNum {
			d.versions.lastSequence = maxSeqNum
		}
	}

	// Create an empty .log file.
	ve.logNumber = d.versions.nextFileNum()
	d.logNumber = ve.logNumber
	logFile, err := fs.Create(dbFilename(dirname, fileTypeLog, ve.logNumber))
	if err != nil {
		return nil, err
	}
	defer func() {
		if logFile != nil {
			logFile.Close()
		}
	}()
	d.log = record.NewWriter(logFile)

	// Write a new manifest to disk.
	if err := d.versions.logAndApply(dirname, &ve); err != nil {
		return nil, err
	}

	d.deleteObsoleteFiles()
	d.maybeScheduleCompaction()

	d.logFile, logFile = logFile, nil
	d.fileLock, fileLock = fileLock, nil
	return d, nil
}

// replayLogFile replays the edits in the named log file.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) replayLogFile(ve *versionEdit, fs db.FileSystem, filename string) (maxSeqNum uint64, err error) {
	file, err := fs.Open(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var (
		mem      *memdb.MemDB
		batchBuf = new(bytes.Buffer)
		ikey     = make(internalKey, 512)
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
			return 0, fmt.Errorf("leveldb: corrupt log file %q", filename)
		}
		b := Batch{batchBuf.Bytes()}
		seqNum := b.seqNum()
		seqNum1 := seqNum + uint64(b.count())
		if maxSeqNum < seqNum1 {
			maxSeqNum = seqNum1
		}

		if mem == nil {
			mem = memdb.New(&d.icmpOpts)
		}

		t := b.iter()
		for ; seqNum != seqNum1; seqNum++ {
			kind, ukey, value, ok := t.next()
			if !ok {
				return 0, fmt.Errorf("leveldb: corrupt log file %q", filename)
			}
			// Convert seqNum, kind and key into an internalKey, and add that ikey/value
			// pair to mem.
			//
			// TODO: instead of copying to an intermediate buffer (ikey), is it worth
			// adding a SetTwoPartKey(db.TwoPartKey{key0, key1}, value, opts) method to
			// memdb.MemDB? What effect does that have on the db.Comparer interface?
			//
			// The C++ LevelDB code does not need an intermediate copy because its memdb
			// implementation is a private implementation detail, and copies each internal
			// key component from the Batch format straight to the skiplist buffer.
			//
			// Go's LevelDB considers the memdb functionality to be useful in its own
			// right, and so leveldb/memdb is a separate package that is usable without
			// having to import the top-level leveldb package. That extra abstraction
			// means that we need to copy to an intermediate buffer here, to reconstruct
			// the complete internal key to pass to the memdb.
			ikey = makeInternalKey(ikey, ukey, kind, seqNum)
			mem.Set(ikey, value, nil)
		}
		if len(t) != 0 {
			return 0, fmt.Errorf("leveldb: corrupt log file %q", filename)
		}

		// TODO: if mem is large enough, write it to a level-0 table and set mem = nil.

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
		delete(d.pendingOutputs, meta.fileNum)
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
// from that set when it has been applied to d.versions.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) writeLevel0Table(fs db.FileSystem, mem *memdb.MemDB) (meta fileMetadata, err error) {
	meta.fileNum = d.versions.nextFileNum()
	filename := dbFilename(d.dirname, fileTypeTable, meta.fileNum)
	d.pendingOutputs[meta.fileNum] = struct{}{}
	defer func(fileNum uint64) {
		if err != nil {
			delete(d.pendingOutputs, fileNum)
		}
	}(meta.fileNum)

	// Release the d.mu lock while doing I/O.
	// Note the unusual order: Unlock and then Lock.
	d.mu.Unlock()
	defer d.mu.Lock()

	var (
		file db.File
		tw   *table.Writer
		iter db.Iterator
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
	tw = table.NewWriter(file, &db.Options{
		Comparer: d.icmp,
	})

	iter = mem.Find(nil, nil)
	iter.Next()
	meta.smallest = internalKey(iter.Key()).clone()
	for {
		meta.largest = iter.Key()
		if err1 := tw.Set(meta.largest, iter.Value(), nil); err1 != nil {
			return fileMetadata{}, err1
		}
		if !iter.Next() {
			break
		}
	}
	meta.largest = meta.largest.clone()

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

	// TODO: currently, closing a table.Writer closes its underlying file.
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
			return fileMetadata{}, fmt.Errorf("leveldb: table file %q has negative size %d", filename, size)
		}
		meta.size = uint64(size)
	}

	// TODO: compaction stats.

	return meta, nil
}

// makeRoomForWrite ensures that there is room in d.mem for the next write.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) makeRoomForWrite(force bool) error {
	allowDelay := !force
	for {
		// TODO: check any previous sticky error, if the paranoid option is set.

		if allowDelay && len(d.versions.currentVersion().files[0]) > l0SlowdownWritesTrigger {
			// We are getting close to hitting a hard limit on the number of
			// L0 files. Rather than delaying a single write by several
			// seconds when we hit the hard limit, start delaying each
			// individual write by 1ms to reduce latency variance.
			d.mu.Unlock()
			time.Sleep(1 * time.Millisecond)
			d.mu.Lock()
			allowDelay = false
			// TODO: how do we ensure we are still 'at the front of the writer queue'?
			continue
		}

		if !force && d.mem.ApproximateMemoryUsage() <= d.opts.GetWriteBufferSize() {
			// There is room in the current memtable.
			break
		}

		if d.imm != nil {
			// We have filled up the current memtable, but the previous
			// one is still being compacted, so we wait.
			d.compactionCond.Wait()
			continue
		}

		if len(d.versions.currentVersion().files[0]) > l0StopWritesTrigger {
			// There are too many level-0 files.
			d.compactionCond.Wait()
			continue
		}

		// Attempt to switch to a new memtable and trigger compaction of old
		// TODO: drop and re-acquire d.mu around the I/O.
		newLogNumber := d.versions.nextFileNum()
		newLogFile, err := d.opts.GetFileSystem().Create(dbFilename(d.dirname, fileTypeLog, newLogNumber))
		if err != nil {
			return err
		}
		newLog := record.NewWriter(newLogFile)
		if err := d.log.Close(); err != nil {
			newLogFile.Close()
			return err
		}
		if err := d.logFile.Close(); err != nil {
			newLog.Close()
			newLogFile.Close()
			return err
		}
		d.logNumber, d.logFile, d.log = newLogNumber, newLogFile, newLog
		d.imm, d.mem = d.mem, memdb.New(&d.icmpOpts)
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
	for fileNum := range d.pendingOutputs {
		liveFileNums[fileNum] = struct{}{}
	}
	d.versions.addLiveFileNums(liveFileNums)
	logNumber := d.versions.logNumber
	manifestFileNumber := d.versions.manifestFileNumber

	// Release the d.mu lock while doing I/O.
	// Note the unusual order: Unlock and then Lock.
	d.mu.Unlock()
	defer d.mu.Lock()

	fs := d.opts.GetFileSystem()
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
			// TODO: also look at prevLogNumber?
			keep = fileNum >= logNumber
		case fileTypeManifest:
			keep = fileNum >= manifestFileNumber
		case fileTypeTable, fileTypeOldFashionedTable:
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
