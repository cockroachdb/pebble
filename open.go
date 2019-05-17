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
	"path/filepath"
	"sort"

	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/internal/arenaskl"
	"github.com/petermattis/pebble/internal/record"
	"github.com/petermattis/pebble/vfs"
)

func createDB(dirname string, opts *db.Options) (retErr error) {
	const manifestFileNum = 1
	ve := versionEdit{
		comparatorName: opts.Comparer.Name,
		nextFileNumber: manifestFileNum + 1,
	}
	manifestFilename := dbFilename(dirname, fileTypeManifest, manifestFileNum)
	f, err := opts.FS.Create(manifestFilename)
	if err != nil {
		return fmt.Errorf("pebble: could not create %q: %v", manifestFilename, err)
	}
	defer func() {
		if retErr != nil {
			opts.FS.Remove(manifestFilename)
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
	return setCurrentFile(dirname, opts.FS, manifestFileNum)
}

// Open opens a LevelDB whose files live in the given directory.
func Open(dirname string, opts *db.Options) (*DB, error) {
	opts = opts.EnsureDefaults()
	d := &DB{
		dirname:        dirname,
		walDirname:     opts.WALDir,
		opts:           opts,
		cmp:            opts.Comparer.Compare,
		equal:          opts.Comparer.Equal,
		merge:          opts.Merger.Merge,
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
	d.tableCache.init(dirname, opts.FS, d.opts, tableCacheSize)
	d.newIters = d.tableCache.newIters
	d.commit = newCommitPipeline(commitEnv{
		logSeqNum:     &d.mu.versions.logSeqNum,
		visibleSeqNum: &d.mu.versions.visibleSeqNum,
		apply:         d.commitApply,
		write:         d.commitWrite,
	})
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

	// Lock the database directory.
	err := opts.FS.MkdirAll(dirname, 0755)
	if err != nil {
		return nil, err
	}
	fileLock, err := opts.FS.Lock(dbFilename(dirname, fileTypeLock, 0))
	if err != nil {
		return nil, err
	}
	defer func() {
		if fileLock != nil {
			fileLock.Close()
		}
	}()

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
		err := opts.FS.MkdirAll(d.walDirname, 0755)
		if err != nil {
			return nil, err
		}
		d.walDir, err = opts.FS.OpenDir(d.walDirname)
	}

	if _, err := opts.FS.Stat(dbFilename(dirname, fileTypeCurrent, 0)); os.IsNotExist(err) {
		// Create the DB if it did not already exist.
		if err := createDB(dirname, opts); err != nil {
			return nil, err
		}
		if err := d.dataDir.Sync(); err != nil {
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

	ls, err := opts.FS.List(d.walDirname)
	if err != nil {
		return nil, err
	}

	// Replay any newer log files than the ones named in the manifest.
	type fileNumAndName struct {
		num  uint64
		name string
	}
	var logFiles []fileNumAndName
	for _, filename := range ls {
		ft, fn, ok := parseDBFilename(filename)
		if !ok {
			continue
		}
		switch ft {
		case fileTypeLog:
			if fn >= d.mu.versions.logNumber || fn == d.mu.versions.prevLogNumber {
				logFiles = append(logFiles, fileNumAndName{fn, filename})
			}
		case fileTypeOptions:
			if err := checkOptions(opts, filepath.Join(dirname, filename)); err != nil {
				return nil, err
			}
		}
	}
	sort.Slice(logFiles, func(i, j int) bool {
		return logFiles[i].num < logFiles[j].num
	})
	var ve versionEdit
	for _, lf := range logFiles {
		maxSeqNum, err := d.replayWAL(&ve, opts.FS, filepath.Join(d.walDirname, lf.name), lf.num)
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
	d.mu.log.queue = append(d.mu.log.queue, ve.logNumber)
	logFile, err := opts.FS.Create(dbFilename(d.walDirname, fileTypeLog, ve.logNumber))
	if err != nil {
		return nil, err
	}
	if err := d.walDir.Sync(); err != nil {
		return nil, err
	}
	logFile = vfs.NewSyncingFile(logFile, vfs.SyncingFileOptions{
		BytesPerSync:    d.opts.BytesPerSync,
		PreallocateSize: d.walPreallocateSize(),
	})
	d.mu.log.LogWriter = record.NewLogWriter(logFile, ve.logNumber)

	// Write a new manifest to disk.
	if err := d.mu.versions.logAndApply(0, &ve, d.dataDir); err != nil {
		return nil, err
	}
	d.updateReadStateLocked()

	// Write the current options to disk.
	d.optionsFileNum = d.mu.versions.nextFileNum()
	optionsFile, err := opts.FS.Create(dbFilename(dirname, fileTypeOptions, d.optionsFileNum))
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

	jobID := d.mu.nextJobID
	d.mu.nextJobID++
	d.scanObsoleteFiles()
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
		b.storage.data = buf.Bytes()
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
		c := newFlush(d.opts, d.mu.versions.currentVersion(),
			1 /* base level */, []flushable{mem})
		newVE, pendingOutputs, err := d.runCompaction(c)
		if err != nil {
			return 0, err
		}
		ve.newFiles = append(ve.newFiles, newVE.newFiles...)
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

func checkOptions(opts *db.Options, path string) error {
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
