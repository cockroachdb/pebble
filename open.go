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
	"sync"

	"github.com/cockroachdb/pebble/internal/arenaskl"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/rate"
	"github.com/cockroachdb/pebble/internal/record"
	"github.com/cockroachdb/pebble/vfs"
)

var dbNumAlloc = struct {
	sync.Mutex
	seq uint64
}{seq: 1}

func allocDBNum() uint64 {
	dbNumAlloc.Lock()
	num := dbNumAlloc.seq
	dbNumAlloc.seq++
	dbNumAlloc.Unlock()
	return num
}

// Open opens a LevelDB whose files live in the given directory.
func Open(dirname string, opts *Options) (*DB, error) {
	// Make a copy of the options so that we don't mutate the passed in options.
	opts = opts.Clone()
	opts = opts.EnsureDefaults()

	d := &DB{
		dbNum:          allocDBNum(),
		dirname:        dirname,
		walDirname:     opts.WALDir,
		opts:           opts,
		cmp:            opts.Comparer.Compare,
		equal:          opts.Comparer.Equal,
		merge:          opts.Merger.Merge,
		split:          opts.Comparer.Split,
		abbreviatedKey: opts.Comparer.AbbreviatedKey,
		logRecycler:    logRecycler{limit: opts.MemTableStopWritesThreshold + 1},
	}
	if d.equal == nil {
		d.equal = bytes.Equal
	}
	tableCacheSize := opts.MaxOpenFiles - numNonTableCacheFiles
	if tableCacheSize < minTableCacheSize {
		tableCacheSize = minTableCacheSize
	}
	d.tableCache.init(d.dbNum, dirname, opts.FS, d.opts, tableCacheSize, defaultTableCacheHitBuffer)
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
	d.mu.mem.cond.L = &d.mu.Mutex
	d.mu.mem.mutable = newMemTable(d.opts)
	d.mu.mem.queue = append(d.mu.mem.queue, d.mu.mem.mutable)
	d.mu.cleaner.cond.L = &d.mu.Mutex
	d.mu.compact.cond.L = &d.mu.Mutex
	d.mu.compact.pendingOutputs = make(map[uint64]struct{})
	d.mu.snapshots.init()
	d.largeBatchThreshold = (d.opts.MemTableSize - int(d.mu.mem.mutable.emptySize)) / 2

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
	if _, err := opts.FS.Stat(currentName); os.IsNotExist(err) && !d.opts.ReadOnly {
		// Create the DB if it did not already exist.
		if err := d.mu.versions.create(jobID, dirname, d.dataDir, opts, &d.mu.Mutex); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, fmt.Errorf("pebble: database %q: %v", dirname, err)
	} else if opts.ErrorIfDBExists {
		return nil, fmt.Errorf("pebble: database %q already exists", dirname)
	} else {
		// Load the version set.
		if err := d.mu.versions.load(dirname, opts, &d.mu.Mutex); err != nil {
			return nil, err
		}
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

		logFile = vfs.NewSyncingFile(logFile, vfs.SyncingFileOptions{
			BytesPerSync:    d.opts.BytesPerSync,
			PreallocateSize: d.walPreallocateSize(),
		})
		d.mu.log.LogWriter = record.NewLogWriter(logFile, newLogNum)
		d.mu.versions.metrics.WAL.Files++

		// This logic is slightly different than RocksDB's. Specifically, RocksDB
		// sets MinUnflushedLogNum to max-recovered-log-num + 1. We set it to the
		// newLogNum. There should be no difference in using either value.
		ve.MinUnflushedLogNum = newLogNum
		if err := d.mu.versions.logAndApply(jobID, &ve, nil, d.dataDir); err != nil {
			return nil, err
		}
	}
	d.updateReadStateLocked()

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
		optionsFile.Close()
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
	jobID int,
	ve *versionEdit,
	fs vfs.FS,
	filename string,
	logNum uint64,
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
		rr  = record.NewReader(file, logNum)
	)

	// In read-only mode, we replay directly into the mutable memtable which will
	// never be flushed.
	if d.opts.ReadOnly {
		mem = d.mu.mem.mutable
	}

	for {
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

		// TODO(peter): If the batch is too large to fit in the memtable, flush the
		// existing memtable and write the batch as a separate L0 table.
		b = Batch{}
		b.SetRepr(buf.Bytes())
		seqNum := b.SeqNum()
		maxSeqNum = seqNum + uint64(b.Count())

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
		mem.unref()

		buf.Reset()
	}

	if d.opts.ReadOnly {
		// In read-only mode, each WAL file is replayed into its own memtable. This
		// is done so that the WAL metrics can be accurately provided.
		mem.logSize = uint64(rr.Offset())
		d.mu.mem.mutable = newMemTable(d.opts)
		d.mu.mem.queue = append(d.mu.mem.queue, d.mu.mem.mutable)
		d.mu.versions.metrics.WAL.Files++
	} else if mem != nil && !mem.empty() {
		c := newFlush(d.opts, d.mu.versions.currentVersion(),
			1 /* base level */, []flushable{mem}, &d.bytesFlushed)
		newVE, pendingOutputs, err := d.runCompaction(jobID, c, nilPacer)
		if err != nil {
			return 0, err
		}
		ve.NewFiles = append(ve.NewFiles, newVE.NewFiles...)
		// Strictly speaking, it's too early to delete from d.pendingOutputs, but
		// we are replaying the log file, which happens before Open returns, so
		// there is no possibility of deleteObsoleteFiles being called concurrently
		// here.
		for _, fileNum := range pendingOutputs {
			delete(d.mu.compact.pendingOutputs, fileNum)
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
