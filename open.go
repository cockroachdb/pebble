// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/internal/arenaskl"
	"github.com/petermattis/pebble/internal/rate"
	"github.com/petermattis/pebble/internal/record"
	"github.com/petermattis/pebble/storage"
)

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
		cmp:               opts.Comparer,
		merge:             opts.Merger.Merge,
		commitController:  newController(rate.NewLimiter(defaultRateLimit, defaultBurst)),
		compactController: newController(rate.NewLimiter(defaultRateLimit, defaultBurst)),
		flushController:   newController(rate.NewLimiter(rate.Inf, defaultBurst)),
	}
	tableCacheSize := opts.MaxOpenFiles - numNonTableCacheFiles
	if tableCacheSize < minTableCacheSize {
		tableCacheSize = minTableCacheSize
	}
	d.tableCache.init(dirname, opts.Storage, d.opts, tableCacheSize)
	d.newIters = d.tableCache.newIters
	d.commit = newCommitPipeline(commitEnv{
		mu:            &d.mu.Mutex,
		logSeqNum:     &d.mu.versions.logSeqNum,
		visibleSeqNum: &d.mu.versions.visibleSeqNum,
		controller:    d.commitController,
		apply:         d.commitApply,
		sync:          d.commitSync,
		write:         d.commitWrite,
	})
	d.mu.nextJobID = 1
	d.mu.mem.cond.L = &d.mu.Mutex
	d.mu.mem.mutable = newMemTable(d.opts)
	d.mu.mem.queue = append(d.mu.mem.queue, d.mu.mem.mutable)
	d.mu.compact.cond.L = &d.mu.Mutex
	d.mu.compact.pendingOutputs = make(map[uint64]struct{})
	d.mu.snapshots.init()
	d.largeBatchThreshold = (d.opts.MemTableSize - int(d.mu.mem.mutable.emptySize)) / 2

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
	err = d.mu.versions.load(dirname, opts, &d.mu.Mutex)
	if err != nil {
		return nil, err
	}

	// Replay any newer log files than the ones named in the manifest.
	var ve versionEdit
	ls, err := fs.List(dirname)
	if err != nil {
		return nil, err
	}

	type fileNumAndName struct {
		num  uint64
		name string
	}
	var logFiles []fileNumAndName
	for _, filename := range ls {
		ft, fn, ok := parseDBFilename(filename)
		if ok && ft == fileTypeLog && (fn >= d.mu.versions.logNumber || fn == d.mu.versions.prevLogNumber) {
			logFiles = append(logFiles, fileNumAndName{fn, filename})
		}
	}
	sort.Slice(logFiles, func(i, j int) bool {
		return logFiles[i].num < logFiles[j].num
	})
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
	if err := d.mu.versions.logAndApply(&ve); err != nil {
		return nil, err
	}

	// Write the current options to disk.
	d.optionsFileNum = d.mu.versions.nextFileNum()
	optionsFile, err := fs.Create(dbFilename(dirname, fileTypeOptions, d.optionsFileNum))
	if err != nil {
		return nil, err
	}
	if _, err := optionsFile.Write([]byte(opts.String())); err != nil {
		return nil, err
	}
	optionsFile.Close()

	jobID := d.mu.nextJobID
	d.mu.nextJobID++
	d.deleteObsoleteFiles(jobID)
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

		// TODO(peter): If the batch is too large to fit in the memtable, flush the
		// existing memtable and write the batch as a separate L0 table.
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
		mem.unref()

		buf.Reset()
	}

	if mem != nil && !mem.empty() {
		meta, err := d.writeLevel0Table(fs, mem.newIter(nil),
			true /* allowRangeTombstoneElision */)
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
