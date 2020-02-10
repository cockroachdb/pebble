// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/internal/arenaskl"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/rate"
	"github.com/cockroachdb/pebble/internal/record"
	"github.com/cockroachdb/pebble/vfs"
)

const initialMemTableSize = 256 << 10 // 256 KB

// Open opens a LevelDB whose files live in the given directory.
func Open(dirname string, opts *Options) (db *DB, _ error) {
	// Make a copy of the options so that we don't mutate the passed in options.
	opts = opts.Clone()
	opts = opts.EnsureDefaults()
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	if opts.Cache == nil {
		opts.Cache = cache.New(cacheDefaultSize)
	} else {
		opts.Cache.Ref()
	}

	d := &DB{
		cacheID:             opts.Cache.NewID(),
		dirname:             dirname,
		walDirname:          opts.WALDir,
		opts:                opts,
		cmp:                 opts.Comparer.Compare,
		equal:               opts.Comparer.Equal,
		merge:               opts.Merger.Merge,
		split:               opts.Comparer.Split,
		abbreviatedKey:      opts.Comparer.AbbreviatedKey,
		largeBatchThreshold: (opts.MemTableSize - int(memTableEmptySize)) / 2,
		logRecycler:         logRecycler{limit: opts.MemTableStopWritesThreshold + 1},
	}
	defer func() {
		if db == nil {
			// We're failing to return the DB for some reason. Release our references
			// to the Cache. Note that both the DB, and tableCache have a reference
			// and we need to release both.
			opts.Cache.Unref()
			opts.Cache.Unref()
		}
	}()

	if d.equal == nil {
		d.equal = bytes.Equal
	}
	tableCacheSize := opts.MaxOpenFiles - numNonTableCacheFiles
	if tableCacheSize < minTableCacheSize {
		tableCacheSize = minTableCacheSize
	}
	d.tableCache.init(d.cacheID, dirname, opts.FS, d.opts, tableCacheSize, defaultTableCacheHitBuffer)
	d.newIters = d.tableCache.newIters
	d.commit = newCommitPipeline(commitEnv{
		logSeqNum:     &d.mu.versions.logSeqNum,
		visibleSeqNum: &d.mu.versions.visibleSeqNum,
		apply:         d.commitApply,
		write:         d.commitWrite,
	})
	d.compactionLimiter = rate.NewLimiter(rate.Limit(d.opts.MinCompactionRate), d.opts.MinCompactionRate)
	d.flushLimiter = rate.NewLimiter(rate.Limit(d.opts.MinFlushRate), d.opts.MinFlushRate)
	d.mu.nextJobID = 1
	d.mu.mem.nextSize = opts.MemTableSize
	if d.mu.mem.nextSize > initialMemTableSize {
		d.mu.mem.nextSize = initialMemTableSize
	}
	d.mu.mem.cond.L = &d.mu.Mutex
	d.mu.cleaner.cond.L = &d.mu.Mutex
	d.mu.compact.cond.L = &d.mu.Mutex
	d.mu.compact.inProgress = make(map[*compaction]struct{})
	d.mu.snapshots.init()

	d.timeNow = time.Now

	d.mu.Lock()
	defer d.mu.Unlock()

	if !d.opts.ReadOnly {
		err := opts.FS.MkdirAll(dirname, 0755)
		if err != nil {
			return nil, err
		}
	}

	// Open the database and WAL directories first in order to check for their
	// existence.
	var err error
	d.dataDir, err = opts.FS.OpenDir(dirname)
	if err != nil {
		return nil, err
	}
	if d.walDirname == "" {
		d.walDirname = d.dirname
	}
	if d.walDirname == d.dirname {
		d.walDir = d.dataDir
	} else {
		if !d.opts.ReadOnly {
			err := opts.FS.MkdirAll(d.walDirname, 0755)
			if err != nil {
				return nil, err
			}
		}
		d.walDir, err = opts.FS.OpenDir(d.walDirname)
		if err != nil {
			return nil, err
		}
	}

	// Lock the database directory.
	fileLock, err := opts.FS.Lock(base.MakeFilename(opts.FS, dirname, fileTypeLock, 0))
	if err != nil {
		d.dataDir.Close()
		if d.dataDir != d.walDir {
			d.walDir.Close()
		}
		return nil, err
	}
	defer func() {
		if fileLock != nil {
			fileLock.Close()
		}
	}()

	jobID := d.mu.nextJobID
	d.mu.nextJobID++

	currentName := base.MakeFilename(opts.FS, dirname, fileTypeCurrent, 0)
	if _, err := opts.FS.Stat(currentName); os.IsNotExist(err) &&
		!d.opts.ReadOnly && !d.opts.ErrorIfNotExists {
		// Create the DB if it did not already exist.
		if err := d.mu.versions.create(jobID, dirname, d.dataDir, opts, &d.mu.Mutex); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, fmt.Errorf("pebble: database %q: %v", dirname, err)
	} else if opts.ErrorIfExists {
		return nil, fmt.Errorf("pebble: database %q already exists", dirname)
	} else {
		// Load the version set.
		if err := d.mu.versions.load(dirname, opts, &d.mu.Mutex); err != nil {
			return nil, err
		}
	}

	// In read-only mode, we replay directly into the mutable memtable but never
	// flush it. We need to delay creation of the memtable until we know the
	// sequence number of the first batch that will be inserted.
	if !d.opts.ReadOnly {
		var entry *flushableEntry
		d.mu.mem.mutable, entry = d.newMemTable(0 /* logNum */, d.mu.versions.logSeqNum)
		d.mu.mem.queue = append(d.mu.mem.queue, entry)
	}

	ls, err := opts.FS.List(d.walDirname)
	if err != nil {
		return nil, err
	}
	if d.dirname != d.walDirname {
		ls2, err := opts.FS.List(d.dirname)
		if err != nil {
			return nil, err
		}
		ls = append(ls, ls2...)
	}

	// Replay any newer log files than the ones named in the manifest.
	type fileNumAndName struct {
		num  uint64
		name string
	}
	var logFiles []fileNumAndName
	for _, filename := range ls {
		ft, fn, ok := base.ParseFilename(opts.FS, filename)
		if !ok {
			continue
		}
		switch ft {
		case fileTypeLog:
			if fn >= d.mu.versions.minUnflushedLogNum {
				logFiles = append(logFiles, fileNumAndName{fn, filename})
			}
		case fileTypeOptions:
			if err := checkOptions(opts, opts.FS.PathJoin(dirname, filename)); err != nil {
				return nil, err
			}
		}
	}
	sort.Slice(logFiles, func(i, j int) bool {
		return logFiles[i].num < logFiles[j].num
	})

	var ve versionEdit
	for _, lf := range logFiles {
		maxSeqNum, err := d.replayWAL(jobID, &ve, opts.FS, opts.FS.PathJoin(d.walDirname, lf.name), lf.num)
		if err != nil {
			return nil, err
		}
		d.mu.versions.markFileNumUsed(lf.num)
		if d.mu.versions.logSeqNum < maxSeqNum {
			d.mu.versions.logSeqNum = maxSeqNum
		}
	}
	d.mu.versions.visibleSeqNum = d.mu.versions.logSeqNum

	if !d.opts.ReadOnly {
		// Create an empty .log file.
		newLogNum := d.mu.versions.getNextFileNum()
		newLogName := base.MakeFilename(opts.FS, d.walDirname, fileTypeLog, newLogNum)
		d.mu.log.queue = append(d.mu.log.queue, newLogNum)
		logFile, err := opts.FS.Create(newLogName)
		if err != nil {
			return nil, err
		}
		if err := d.walDir.Sync(); err != nil {
			return nil, err
		}
		d.opts.EventListener.WALCreated(WALCreateInfo{
			JobID:   jobID,
			Path:    newLogName,
			FileNum: newLogNum,
		})
		// This isn't strictly necessary as we don't use the log number for
		// memtables being flushed, only for the next unflushed memtable.
		d.mu.mem.queue[len(d.mu.mem.queue)-1].logNum = newLogNum

		logFile = vfs.NewSyncingFile(logFile, vfs.SyncingFileOptions{
			BytesPerSync:    d.opts.BytesPerSync,
			PreallocateSize: d.walPreallocateSize(),
		})
		d.mu.log.LogWriter = record.NewLogWriter(logFile, newLogNum)
		d.mu.log.LogWriter.SetMinSyncInterval(d.opts.WALMinSyncInterval)
		d.mu.versions.metrics.WAL.Files++

		// This logic is slightly different than RocksDB's. Specifically, RocksDB
		// sets MinUnflushedLogNum to max-recovered-log-num + 1. We set it to the
		// newLogNum. There should be no difference in using either value.
		ve.MinUnflushedLogNum = newLogNum
		d.mu.versions.logLock()
		if err := d.mu.versions.logAndApply(jobID, &ve, nil, d.dataDir, func() []compactionInfo {
			return nil
		}); err != nil {
			return nil, err
		}
	}
	var checker func() error
	if d.opts.DebugCheck {
		checker = func() error { return d.CheckLevels(nil) }
	}
	d.updateReadStateLocked(checker)

	if !d.opts.ReadOnly {
		// Write the current options to disk.
		d.optionsFileNum = d.mu.versions.getNextFileNum()
		optionsFile, err := opts.FS.Create(
			base.MakeFilename(opts.FS, dirname, fileTypeOptions, d.optionsFileNum))
		if err != nil {
			return nil, err
		}
		if _, err := optionsFile.Write([]byte(opts.String())); err != nil {
			return nil, err
		}
		_ = optionsFile.Sync()
		_ = optionsFile.Close()
		if err := d.dataDir.Sync(); err != nil {
			return nil, err
		}
	}

	if !d.opts.ReadOnly {
		d.scanObsoleteFiles(ls)
		d.deleteObsoleteFiles(jobID)
	}
	d.maybeScheduleFlush()
	d.maybeScheduleCompaction()

	d.fileLock, fileLock = fileLock, nil
	return d, nil
}

// replayWAL replays the edits in the specified log file.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) replayWAL(
	jobID int, ve *versionEdit, fs vfs.FS, filename string, logNum uint64,
) (maxSeqNum uint64, err error) {
	file, err := fs.Open(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var (
		b               Batch
		buf             bytes.Buffer
		mem             *memTable
		entry           *flushableEntry
		toFlush         flushableList
		rr              = record.NewReader(file, logNum)
		offset          int64 // byte offset in rr
		lastFlushOffset int64
	)

	if d.opts.ReadOnly {
		// In read-only mode, we replay directly into the mutable memtable which will
		// never be flushed.
		mem = d.mu.mem.mutable
		if mem != nil {
			entry = d.mu.mem.queue[len(d.mu.mem.queue)-1]
		}
	}

	// Flushes the current memtable, if not nil.
	flushMem := func() {
		if mem == nil {
			return
		}
		var logSize uint64
		if offset >= lastFlushOffset {
			logSize = uint64(offset - lastFlushOffset)
		}
		// Else, this was the initial memtable in the read-only case which must have
		// been empty, but we need to flush it since we don't want to add to it later.
		lastFlushOffset = offset
		entry.logSize = logSize
		if !d.opts.ReadOnly {
			toFlush = append(toFlush, entry)
		}
		mem, entry = nil, nil
	}
	// Creates a new memtable if there is no current memtable.
	ensureMem := func(seqNum uint64) {
		if mem != nil {
			return
		}
		mem, entry = d.newMemTable(logNum, seqNum)
		if d.opts.ReadOnly {
			d.mu.mem.mutable = mem
			d.mu.mem.queue = append(d.mu.mem.queue, entry)
		}
	}
	for {
		offset = rr.Offset()
		r, err := rr.Next()
		if err == nil {
			_, err = io.Copy(&buf, r)
		}
		if err != nil {
			// It is common to encounter a zeroed or invalid chunk due to WAL
			// preallocation and WAL recycling. We need to distinguish these errors
			// from EOF in order to recognize that the record was truncated, but want
			// to otherwise treat them like EOF.
			if err == io.EOF || err == record.ErrZeroedChunk || err == record.ErrInvalidChunk {
				break
			}
			return 0, err
		}

		if buf.Len() < batchHeaderLen {
			return 0, fmt.Errorf("pebble: corrupt log file %q", filename)
		}

		// Specify Batch.db so that Batch.SetRepr will compute Batch.memTableSize
		// which is used below.
		b = Batch{db: d}
		b.SetRepr(buf.Bytes())
		seqNum := b.SeqNum()
		maxSeqNum = seqNum + uint64(b.Count())

		if b.memTableSize >= uint32(d.largeBatchThreshold) {
			flushMem()
			// Make a copy of the data slice since it is currently owned by buf and will
			// be reused in the next iteration.
			b.data = append([]byte(nil), b.data...)
			b.flushable = newFlushableBatch(&b, d.opts.Comparer)
			entry := d.newFlushableEntry(b.flushable, logNum, b.SeqNum())
			// Disable memory accounting by adding a reader ref that will never be
			// removed.
			entry.readerRefs++
			if d.opts.ReadOnly {
				d.mu.mem.queue = append(d.mu.mem.queue, entry)
			} else {
				toFlush = append(toFlush, entry)
			}
		} else {
			ensureMem(seqNum)
			if err = mem.prepare(&b); err != nil && err != arenaskl.ErrArenaFull {
				return 0, err
			}
			// We loop since DB.newMemTable() slowly grows the size of allocated memtables, so the
			// batch may not initially fit, but will eventually fit (since it is smaller than
			// largeBatchThreshold).
			for err == arenaskl.ErrArenaFull {
				flushMem()
				ensureMem(seqNum)
				err = mem.prepare(&b)
				if err != nil && err != arenaskl.ErrArenaFull {
					return 0, err
				}
			}
			if err = mem.apply(&b, seqNum); err != nil {
				return 0, err
			}
			mem.writerUnref()
		}
		buf.Reset()
	}
	flushMem()
	// mem is nil here.
	if d.opts.ReadOnly {
		// We need to restore the invariant that the last memtable in d.mu.mem.queue is the
		// mutable one for the next WAL file, since in read-only mode, each WAL file is replayed
		// into its own set of memtables. This is done so that the WAL metrics can be accurately
		// provided.
		ensureMem(atomic.LoadUint64(&d.mu.versions.logSeqNum))
		d.mu.versions.metrics.WAL.Files++
	} else {
		c := newFlush(d.opts, d.mu.versions.currentVersion(),
			1 /* base level */, toFlush, &d.bytesFlushed)
		newVE, _, err := d.runCompaction(jobID, c, nilPacer)
		if err != nil {
			return 0, err
		}
		ve.NewFiles = append(ve.NewFiles, newVE.NewFiles...)
		for i := range toFlush {
			toFlush[i].readerUnref()
		}
	}
	return maxSeqNum, nil
}

func checkOptions(opts *Options, path string) error {
	f, err := opts.FS.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	data, err := ioutil.ReadAll(f)
	if err != nil {
		return err
	}
	return opts.Check(string(data))
}
