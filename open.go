// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/internal/arenaskl"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/manual"
	"github.com/cockroachdb/pebble/internal/rate"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	initialMemTableSize = 256 << 10 // 256 KB

	// The max batch size is limited by the uint32 offsets stored in
	// internal/batchskl.node, DeferredBatchOp, and flushableBatchEntry.
	maxBatchSize = 4 << 30 // 4 GB

	// The max memtable size is limited by the uint32 offsets stored in
	// internal/arenaskl.node, DeferredBatchOp, and flushableBatchEntry.
	maxMemTableSize = 4 << 30 // 4 GB
)

// TableCacheSize can be used to determine the table
// cache size for a single db, given the maximum open
// files which can be used by a table cache which is
// only used by a single db.
func TableCacheSize(maxOpenFiles int) int {
	tableCacheSize := maxOpenFiles - numNonTableCacheFiles
	if tableCacheSize < minTableCacheSize {
		tableCacheSize = minTableCacheSize
	}
	return tableCacheSize
}

// Open opens a DB whose files live in the given directory.
func Open(dirname string, opts *Options) (db *DB, _ error) {
	// Make a copy of the options so that we don't mutate the passed in options.
	opts = opts.Clone()
	opts = opts.EnsureDefaults()
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	if opts.LoggerAndTracer == nil {
		opts.LoggerAndTracer = &base.LoggerWithNoopTracer{Logger: opts.Logger}
	} else {
		opts.Logger = opts.LoggerAndTracer
	}

	// In all error cases, we return db = nil; this is used by various
	// deferred cleanups.

	// Open the database and WAL directories first.
	walDirname, dataDir, walDir, err := prepareAndOpenDirs(dirname, opts)
	if err != nil {
		return nil, err
	}
	defer func() {
		if db == nil {
			if walDir != dataDir {
				walDir.Close()
			}
			dataDir.Close()
		}
	}()

	// Lock the database directory.
	fileLock, err := opts.FS.Lock(base.MakeFilepath(opts.FS, dirname, fileTypeLock, base.FileNum(0).DiskFileNum()))
	if err != nil {
		return nil, err
	}
	defer func() {
		if db == nil {
			fileLock.Close()
		}
	}()

	// Establish the format major version.
	formatVersion, formatVersionMarker, err := lookupFormatMajorVersion(opts.FS, dirname)
	if err != nil {
		return nil, err
	}
	defer func() {
		if db == nil {
			formatVersionMarker.Close()
		}
	}()

	// Find the currently active manifest, if there is one.
	manifestMarker, manifestFileNum, manifestExists, err := findCurrentManifest(formatVersion, opts.FS, dirname)
	if err != nil {
		return nil, errors.Wrapf(err, "pebble: database %q", dirname)
	}
	defer func() {
		if db == nil {
			manifestMarker.Close()
		}
	}()

	// Atomic markers may leave behind obsolete files if there's a crash
	// mid-update. Clean these up if we're not in read-only mode.
	if !opts.ReadOnly {
		if err := formatVersionMarker.RemoveObsolete(); err != nil {
			return nil, err
		}
		if err := manifestMarker.RemoveObsolete(); err != nil {
			return nil, err
		}
	}

	if opts.Cache == nil {
		opts.Cache = cache.New(cacheDefaultSize)
	} else {
		opts.Cache.Ref()
	}

	d := &DB{
		cacheID:             opts.Cache.NewID(),
		dirname:             dirname,
		walDirname:          walDirname,
		opts:                opts,
		cmp:                 opts.Comparer.Compare,
		equal:               opts.equal(),
		merge:               opts.Merger.Merge,
		split:               opts.Comparer.Split,
		abbreviatedKey:      opts.Comparer.AbbreviatedKey,
		largeBatchThreshold: (opts.MemTableSize - int(memTableEmptySize)) / 2,
		fileLock:            fileLock,
		dataDir:             dataDir,
		walDir:              walDir,
		logRecycler:         logRecycler{limit: opts.MemTableStopWritesThreshold + 1},
		closed:              new(atomic.Value),
		closedCh:            make(chan struct{}),
	}
	d.mu.versions = &versionSet{}
	d.diskAvailBytes.Store(math.MaxUint64)
	d.mu.versions.diskAvailBytes = d.getDiskAvailableBytesCached

	defer func() {
		// If an error or panic occurs during open, attempt to release the manually
		// allocated memory resources. Note that rather than look for an error, we
		// look for the return of a nil DB pointer.
		if r := recover(); db == nil {
			// Release our references to the Cache. Note that both the DB, and
			// tableCache have a reference. When we release the reference to
			// the tableCache, and if there are no other references to
			// the tableCache, then the tableCache will also release its
			// reference to the cache.
			opts.Cache.Unref()

			if d.tableCache != nil {
				_ = d.tableCache.close()
			}

			for _, mem := range d.mu.mem.queue {
				switch t := mem.flushable.(type) {
				case *memTable:
					manual.Free(t.arenaBuf)
					t.arenaBuf = nil
				}
			}
			if d.objProvider != nil {
				d.objProvider.Close()
			}
			if r != nil {
				panic(r)
			}
		}
	}()

	d.commit = newCommitPipeline(commitEnv{
		logSeqNum:     &d.mu.versions.logSeqNum,
		visibleSeqNum: &d.mu.versions.visibleSeqNum,
		apply:         d.commitApply,
		write:         d.commitWrite,
	})
	d.deletionLimiter = rate.NewLimiter(
		rate.Limit(d.opts.Experimental.MinDeletionRate),
		d.opts.Experimental.MinDeletionRate)
	d.mu.nextJobID = 1
	d.mu.mem.nextSize = opts.MemTableSize
	if d.mu.mem.nextSize > initialMemTableSize {
		d.mu.mem.nextSize = initialMemTableSize
	}
	d.mu.mem.cond.L = &d.mu.Mutex
	d.mu.cleaner.cond.L = &d.mu.Mutex
	d.mu.compact.cond.L = &d.mu.Mutex
	d.mu.compact.inProgress = make(map[*compaction]struct{})
	d.mu.compact.noOngoingFlushStartTime = time.Now()
	d.mu.snapshots.init()
	// logSeqNum is the next sequence number that will be assigned. Start
	// assigning sequence numbers from base.SeqNumStart to leave room for reserved
	// sequence numbers (see comments around SeqNumStart).
	d.mu.versions.logSeqNum.Store(base.SeqNumStart)
	d.mu.formatVers.vers = formatVersion
	d.mu.formatVers.marker = formatVersionMarker

	d.timeNow = time.Now

	d.mu.Lock()
	defer d.mu.Unlock()

	jobID := d.mu.nextJobID
	d.mu.nextJobID++

	setCurrent := setCurrentFunc(d.mu.formatVers.vers, manifestMarker, opts.FS, dirname, d.dataDir)

	if !manifestExists {
		// DB does not exist.
		if d.opts.ErrorIfNotExists || d.opts.ReadOnly {
			return nil, errors.Wrapf(ErrDBDoesNotExist, "dirname=%q", dirname)
		}

		// Create the DB.
		if err := d.mu.versions.create(jobID, dirname, opts, manifestMarker, setCurrent, &d.mu.Mutex); err != nil {
			return nil, err
		}
	} else {
		if opts.ErrorIfExists {
			return nil, errors.Wrapf(ErrDBAlreadyExists, "dirname=%q", dirname)
		}
		// Load the version set.
		if err := d.mu.versions.load(dirname, opts, manifestFileNum.FileNum(), manifestMarker, setCurrent, &d.mu.Mutex); err != nil {
			return nil, err
		}
		if opts.ErrorIfNotPristine {
			liveFileNums := make(map[base.DiskFileNum]struct{})
			d.mu.versions.addLiveFileNums(liveFileNums)
			if len(liveFileNums) != 0 {
				return nil, errors.Wrapf(ErrDBNotPristine, "dirname=%q", dirname)
			}
		}
	}

	// In read-only mode, we replay directly into the mutable memtable but never
	// flush it. We need to delay creation of the memtable until we know the
	// sequence number of the first batch that will be inserted.
	if !d.opts.ReadOnly {
		var entry *flushableEntry
		d.mu.mem.mutable, entry = d.newMemTable(0 /* logNum */, d.mu.versions.logSeqNum.Load())
		d.mu.mem.queue = append(d.mu.mem.queue, entry)
	}

	// List the objects
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
	providerSettings := objstorageprovider.Settings{
		Logger:              opts.Logger,
		FS:                  opts.FS,
		FSDirName:           dirname,
		FSDirInitialListing: ls,
		FSCleaner:           opts.Cleaner,
		NoSyncOnClose:       opts.NoSyncOnClose,
		BytesPerSync:        opts.BytesPerSync,
	}
	providerSettings.Shared.Storage = opts.Experimental.SharedStorage

	d.objProvider, err = objstorageprovider.Open(providerSettings)
	if err != nil {
		return nil, err
	}

	if manifestExists {
		curVersion := d.mu.versions.currentVersion()
		if err := checkConsistency(curVersion, dirname, d.objProvider); err != nil {
			return nil, err
		}
	}

	tableCacheSize := TableCacheSize(opts.MaxOpenFiles)
	d.tableCache = newTableCacheContainer(opts.TableCache, d.cacheID, d.objProvider, d.opts, tableCacheSize)
	d.newIters = d.tableCache.newIters
	d.tableNewRangeKeyIter = d.tableCache.newRangeKeyIter

	// Replay any newer log files than the ones named in the manifest.
	type fileNumAndName struct {
		num  FileNum
		name string
	}
	var logFiles []fileNumAndName
	var previousOptionsFileNum FileNum
	var previousOptionsFilename string
	for _, filename := range ls {
		ft, fn, ok := base.ParseFilename(opts.FS, filename)
		if !ok {
			continue
		}

		// Don't reuse any obsolete file numbers to avoid modifying an
		// ingested sstable's original external file.
		if d.mu.versions.nextFileNum <= fn.FileNum() {
			d.mu.versions.nextFileNum = fn.FileNum() + 1
		}

		switch ft {
		case fileTypeLog:
			if fn.FileNum() >= d.mu.versions.minUnflushedLogNum {
				logFiles = append(logFiles, fileNumAndName{fn.FileNum(), filename})
			}
			if d.logRecycler.minRecycleLogNum <= fn.FileNum() {
				d.logRecycler.minRecycleLogNum = fn.FileNum() + 1
			}
		case fileTypeOptions:
			if previousOptionsFileNum < fn.FileNum() {
				previousOptionsFileNum = fn.FileNum()
				previousOptionsFilename = filename
			}
		case fileTypeTemp, fileTypeOldTemp:
			if !d.opts.ReadOnly {
				// Some codepaths write to a temporary file and then
				// rename it to its final location when complete.  A
				// temp file is leftover if a process exits before the
				// rename.  Remove it.
				err := opts.FS.Remove(opts.FS.PathJoin(dirname, filename))
				if err != nil {
					return nil, err
				}
			}
		}
	}

	// Validate the most-recent OPTIONS file, if there is one.
	var strictWALTail bool
	if previousOptionsFilename != "" {
		path := opts.FS.PathJoin(dirname, previousOptionsFilename)
		strictWALTail, err = checkOptions(opts, path)
		if err != nil {
			return nil, err
		}
	}

	sort.Slice(logFiles, func(i, j int) bool {
		return logFiles[i].num < logFiles[j].num
	})

	var ve versionEdit
	var toFlush flushableList
	for i, lf := range logFiles {
		lastWAL := i == len(logFiles)-1
		flush, maxSeqNum, err := d.replayWAL(jobID, &ve, opts.FS,
			opts.FS.PathJoin(d.walDirname, lf.name), lf.num, strictWALTail && !lastWAL)
		if err != nil {
			return nil, err
		}
		toFlush = append(toFlush, flush...)
		d.mu.versions.markFileNumUsed(lf.num)
		if d.mu.versions.logSeqNum.Load() < maxSeqNum {
			d.mu.versions.logSeqNum.Store(maxSeqNum)
		}
	}
	d.mu.versions.visibleSeqNum.Store(d.mu.versions.logSeqNum.Load())

	if !d.opts.ReadOnly {
		// Create an empty .log file.
		newLogNum := d.mu.versions.getNextFileNum()

		// This logic is slightly different than RocksDB's. Specifically, RocksDB
		// sets MinUnflushedLogNum to max-recovered-log-num + 1. We set it to the
		// newLogNum. There should be no difference in using either value.
		ve.MinUnflushedLogNum = newLogNum

		// Create the manifest with the updated MinUnflushedLogNum before
		// creating the new log file. If we created the log file first, a
		// crash before the manifest is synced could leave two WALs with
		// unclean tails.
		d.mu.versions.logLock()
		if err := d.mu.versions.logAndApply(jobID, &ve, newFileMetrics(ve.NewFiles), false /* forceRotation */, func() []compactionInfo {
			return nil
		}); err != nil {
			return nil, err
		}

		for _, entry := range toFlush {
			entry.readerUnrefLocked(true)
		}

		newLogName := base.MakeFilepath(opts.FS, d.walDirname, fileTypeLog, newLogNum.DiskFileNum())
		d.mu.log.queue = append(d.mu.log.queue, fileInfo{fileNum: newLogNum.DiskFileNum(), fileSize: 0})
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
			NoSyncOnClose:   d.opts.NoSyncOnClose,
			BytesPerSync:    d.opts.WALBytesPerSync,
			PreallocateSize: d.walPreallocateSize(),
		})
		d.mu.log.metrics.fsyncLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
			Buckets: FsyncLatencyBuckets,
		})

		logWriterConfig := record.LogWriterConfig{
			WALMinSyncInterval: d.opts.WALMinSyncInterval,
			WALFsyncLatency:    d.mu.log.metrics.fsyncLatency,
			QueueSemChan:       d.commit.logSyncQSem,
		}
		d.mu.log.LogWriter = record.NewLogWriter(logFile, newLogNum, logWriterConfig)
		d.mu.versions.metrics.WAL.Files++
	}
	d.updateReadStateLocked(d.opts.DebugCheck)

	// If the Options specify a format major version higher than the
	// loaded database's, upgrade it. If this is a new database, this
	// code path also performs an initial upgrade from the starting
	// implicit MostCompatible version.
	//
	// We ratchet the version this far into Open so that migrations have a read
	// state available.
	if !d.opts.ReadOnly && opts.FormatMajorVersion > d.mu.formatVers.vers {
		if err := d.ratchetFormatMajorVersionLocked(opts.FormatMajorVersion); err != nil {
			return nil, err
		}
	}

	if !d.opts.ReadOnly {
		// Write the current options to disk.
		d.optionsFileNum = d.mu.versions.getNextFileNum().DiskFileNum()
		tmpPath := base.MakeFilepath(opts.FS, dirname, fileTypeTemp, d.optionsFileNum)
		optionsPath := base.MakeFilepath(opts.FS, dirname, fileTypeOptions, d.optionsFileNum)

		// Write them to a temporary file first, in case we crash before
		// we're done. A corrupt options file prevents opening the
		// database.
		optionsFile, err := opts.FS.Create(tmpPath)
		if err != nil {
			return nil, err
		}
		serializedOpts := []byte(opts.String())
		if _, err := optionsFile.Write(serializedOpts); err != nil {
			return nil, errors.CombineErrors(err, optionsFile.Close())
		}
		d.optionsFileSize = uint64(len(serializedOpts))
		if err := optionsFile.Sync(); err != nil {
			return nil, errors.CombineErrors(err, optionsFile.Close())
		}
		if err := optionsFile.Close(); err != nil {
			return nil, err
		}
		// Atomically rename to the OPTIONS-XXXXXX path. This rename is
		// guaranteed to be atomic because the destination path does not
		// exist.
		if err := opts.FS.Rename(tmpPath, optionsPath); err != nil {
			return nil, err
		}
		if err := d.dataDir.Sync(); err != nil {
			return nil, err
		}
	}

	if !d.opts.ReadOnly {
		d.scanObsoleteFiles(ls)
		d.deleteObsoleteFiles(jobID, true /* waitForOngoing */)
	} else {
		// All the log files are obsolete.
		d.mu.versions.metrics.WAL.Files = int64(len(logFiles))
	}
	d.mu.tableStats.cond.L = &d.mu.Mutex
	d.mu.tableValidation.cond.L = &d.mu.Mutex
	if !d.opts.ReadOnly && !d.opts.private.disableTableStats {
		d.maybeCollectTableStatsLocked()
	}
	d.calculateDiskAvailableBytes()

	d.maybeScheduleFlush()
	d.maybeScheduleCompaction()

	// Note: this is a no-op if invariants are disabled or race is enabled.
	//
	// Setting a finalizer on *DB causes *DB to never be reclaimed and the
	// finalizer to never be run. The problem is due to this limitation of
	// finalizers mention in the SetFinalizer docs:
	//
	//   If a cyclic structure includes a block with a finalizer, that cycle is
	//   not guaranteed to be garbage collected and the finalizer is not
	//   guaranteed to run, because there is no ordering that respects the
	//   dependencies.
	//
	// DB has cycles with several of its internal structures: readState,
	// newIters, tableCache, versions, etc. Each of this individually cause a
	// cycle and prevent the finalizer from being run. But we can workaround this
	// finializer limitation by setting a finalizer on another object that is
	// tied to the lifetime of DB: the DB.closed atomic.Value.
	dPtr := fmt.Sprintf("%p", d)
	invariants.SetFinalizer(d.closed, func(obj interface{}) {
		v := obj.(*atomic.Value)
		if err := v.Load(); err == nil {
			fmt.Fprintf(os.Stderr, "%s: unreferenced DB not closed\n", dPtr)
			os.Exit(1)
		}
	})

	return d, nil
}

// prepareAndOpenDirs opens the directories for the store (and creates them if
// necessary).
//
// Returns an error if ReadOnly is set and the directories don't exist.
func prepareAndOpenDirs(
	dirname string, opts *Options,
) (walDirname string, dataDir vfs.File, walDir vfs.File, err error) {
	walDirname = opts.WALDir
	if opts.WALDir == "" {
		walDirname = dirname
	}

	// Create directories if needed.
	if !opts.ReadOnly {
		if err := opts.FS.MkdirAll(dirname, 0755); err != nil {
			return "", nil, nil, err
		}
		if walDirname != dirname {
			if err := opts.FS.MkdirAll(walDirname, 0755); err != nil {
				return "", nil, nil, err
			}
		}
	}

	dataDir, err = opts.FS.OpenDir(dirname)
	if err != nil {
		if opts.ReadOnly && oserror.IsNotExist(err) {
			return "", nil, nil, errors.Errorf("pebble: database %q does not exist", dirname)
		}
		return "", nil, nil, err
	}

	if walDirname == dirname {
		walDir = dataDir
	} else {
		walDir, err = opts.FS.OpenDir(walDirname)
		if err != nil {
			dataDir.Close()
			return "", nil, nil, err
		}
	}
	return walDirname, dataDir, walDir, nil
}

// GetVersion returns the engine version string from the latest options
// file present in dir. Used to check what Pebble or RocksDB version was last
// used to write to the database stored in this directory. An empty string is
// returned if no valid OPTIONS file with a version key was found.
func GetVersion(dir string, fs vfs.FS) (string, error) {
	ls, err := fs.List(dir)
	if err != nil {
		return "", err
	}
	var version string
	lastOptionsSeen := FileNum(0)
	for _, filename := range ls {
		ft, fn, ok := base.ParseFilename(fs, filename)
		if !ok {
			continue
		}
		switch ft {
		case fileTypeOptions:
			// If this file has a higher number than the last options file
			// processed, reset version. This is because rocksdb often
			// writes multiple options files without deleting previous ones.
			// Otherwise, skip parsing this options file.
			if fn.FileNum() > lastOptionsSeen {
				version = ""
				lastOptionsSeen = fn.FileNum()
			} else {
				continue
			}
			f, err := fs.Open(fs.PathJoin(dir, filename))
			if err != nil {
				return "", err
			}
			data, err := io.ReadAll(f)
			f.Close()

			if err != nil {
				return "", err
			}
			err = parseOptions(string(data), func(section, key, value string) error {
				switch {
				case section == "Version":
					switch key {
					case "pebble_version":
						version = value
					case "rocksdb_version":
						version = fmt.Sprintf("rocksdb v%s", value)
					}
				}
				return nil
			})
			if err != nil {
				return "", err
			}
		}
	}
	return version, nil
}

// replayWAL replays the edits in the specified log file. If the DB is in
// read only mode, then the WALs are replayed into memtables and not flushed. If
// the DB is not in read only mode, then the contents of the WAL are guaranteed
// to be flushed.
//
// The toFlush return value is a list of flushables associated with the WAL
// being replayed which will be flushed. Once the version edit has been applied
// to the manifest, it is up to the caller of replayWAL to unreference the
// toFlush flushables returned by replayWAL.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) replayWAL(
	jobID int, ve *versionEdit, fs vfs.FS, filename string, logNum FileNum, strictWALTail bool,
) (toFlush flushableList, maxSeqNum uint64, err error) {
	file, err := fs.Open(filename)
	if err != nil {
		return nil, 0, err
	}
	defer file.Close()
	var (
		b               Batch
		buf             bytes.Buffer
		mem             *memTable
		entry           *flushableEntry
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

	// updateVE is used to update ve with information about new files created
	// during the flush of any flushable not of type ingestedFlushable. For the
	// flushable of type ingestedFlushable we use custom handling below.
	updateVE := func() error {
		// TODO(bananabrick): See if we can use the actual base level here,
		// instead of using 1.
		c := newFlush(d.opts, d.mu.versions.currentVersion(),
			1 /* base level */, toFlush)
		newVE, _, _, err := d.runCompaction(jobID, c)
		if err != nil {
			return errors.Wrapf(err, "running compaction during WAL replay")
		}
		ve.NewFiles = append(ve.NewFiles, newVE.NewFiles...)
		return nil
	}

	for {
		offset = rr.Offset()
		r, err := rr.Next()
		if err == nil {
			_, err = io.Copy(&buf, r)
		}
		if err != nil {
			// It is common to encounter a zeroed or invalid chunk due to WAL
			// preallocation and WAL recycling. We need to distinguish these
			// errors from EOF in order to recognize that the record was
			// truncated and to avoid replaying subsequent WALs, but want
			// to otherwise treat them like EOF.
			if err == io.EOF {
				break
			} else if record.IsInvalidRecord(err) && !strictWALTail {
				break
			}
			return nil, 0, errors.Wrap(err, "pebble: error when replaying WAL")
		}

		if buf.Len() < batchHeaderLen {
			return nil, 0, base.CorruptionErrorf("pebble: corrupt log file %q (num %s)",
				filename, errors.Safe(logNum))
		}

		if d.opts.ErrorIfNotPristine {
			return nil, 0, errors.WithDetailf(ErrDBNotPristine, "location: %q", d.dirname)
		}

		// Specify Batch.db so that Batch.SetRepr will compute Batch.memTableSize
		// which is used below.
		b = Batch{db: d}
		b.SetRepr(buf.Bytes())
		seqNum := b.SeqNum()
		maxSeqNum = seqNum + uint64(b.Count())

		{
			br := b.Reader()
			if kind, encodedFileNum, _, _ := br.Next(); kind == InternalKeyKindIngestSST {
				fileNums := make([]base.DiskFileNum, 0, b.Count())
				addFileNum := func(encodedFileNum []byte) {
					fileNum, n := binary.Uvarint(encodedFileNum)
					if n <= 0 {
						panic("pebble: ingest sstable file num is invalid.")
					}
					fileNums = append(fileNums, base.FileNum(fileNum).DiskFileNum())
				}
				addFileNum(encodedFileNum)

				for i := 1; i < int(b.Count()); i++ {
					kind, encodedFileNum, _, ok := br.Next()
					if kind != InternalKeyKindIngestSST {
						panic("pebble: invalid batch key kind.")
					}
					if !ok {
						panic("pebble: invalid batch count.")
					}
					addFileNum(encodedFileNum)
				}

				if _, _, _, ok := br.Next(); ok {
					panic("pebble: invalid number of entries in batch.")
				}

				paths := make([]string, len(fileNums))
				for i, n := range fileNums {
					paths[i] = base.MakeFilepath(d.opts.FS, d.dirname, fileTypeTable, n)
				}

				var meta []*manifest.FileMetadata
				meta, _, err = ingestLoad(
					d.opts, d.mu.formatVers.vers, paths, d.cacheID, fileNums,
				)
				if err != nil {
					return nil, 0, err
				}

				if uint32(len(meta)) != b.Count() {
					panic("pebble: couldn't load all files in WAL entry.")
				}

				entry, err = d.newIngestedFlushableEntry(
					meta, seqNum, logNum,
				)
				if err != nil {
					return nil, 0, err
				}

				if d.opts.ReadOnly {
					d.mu.mem.queue = append(d.mu.mem.queue, entry)
					// We added the IngestSST flushable to the queue. But there
					// must be at least one WAL entry waiting to be replayed. We
					// have to ensure this newer WAL entry isn't replayed into
					// the current value of d.mu.mem.mutable because the current
					// mutable memtable exists before this flushable entry in
					// the memtable queue. To ensure this, we just need to unset
					// d.mu.mem.mutable. When a newer WAL is replayed, we will
					// set d.mu.mem.mutable to a newer value.
					d.mu.mem.mutable = nil
				} else {
					toFlush = append(toFlush, entry)
					// During WAL replay, the lsm only has L0, hence, the
					// baseLevel is 1. For the sake of simplicity, we place the
					// ingested files in L0 here, instead of finding their
					// target levels. This is a simplification for the sake of
					// simpler code. It is expected that WAL replay should be
					// rare, and that flushables of type ingestedFlushable
					// should also be rare. So, placing the ingested files in L0
					// is alright.
					//
					// TODO(bananabrick): Maybe refactor this function to allow
					// us to easily place ingested files in levels as low as
					// possible during WAL replay. It would require breaking up
					// the application of ve to the manifest into chunks and is
					// not pretty w/o a refactor to this function and how it's
					// used.
					c := newFlush(
						d.opts, d.mu.versions.currentVersion(),
						1, /* base level */
						[]*flushableEntry{entry},
					)
					for _, file := range c.flushing[0].flushable.(*ingestedFlushable).files {
						ve.NewFiles = append(ve.NewFiles, newFileEntry{Level: 0, Meta: file.FileMetadata})
					}
				}
				return toFlush, maxSeqNum, nil
			}
		}

		if b.memTableSize >= uint64(d.largeBatchThreshold) {
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
				// We added the flushable batch to the flushable to the queue.
				// But there must be at least one WAL entry waiting to be
				// replayed. We have to ensure this newer WAL entry isn't
				// replayed into the current value of d.mu.mem.mutable because
				// the current mutable memtable exists before this flushable
				// entry in the memtable queue. To ensure this, we just need to
				// unset d.mu.mem.mutable. When a newer WAL is replayed, we will
				// set d.mu.mem.mutable to a newer value.
				d.mu.mem.mutable = nil
			} else {
				toFlush = append(toFlush, entry)
			}
		} else {
			ensureMem(seqNum)
			if err = mem.prepare(&b); err != nil && err != arenaskl.ErrArenaFull {
				return nil, 0, err
			}
			// We loop since DB.newMemTable() slowly grows the size of allocated memtables, so the
			// batch may not initially fit, but will eventually fit (since it is smaller than
			// largeBatchThreshold).
			for err == arenaskl.ErrArenaFull {
				flushMem()
				ensureMem(seqNum)
				err = mem.prepare(&b)
				if err != nil && err != arenaskl.ErrArenaFull {
					return nil, 0, err
				}
			}
			if err = mem.apply(&b, seqNum); err != nil {
				return nil, 0, err
			}
			mem.writerUnref()
		}
		buf.Reset()
	}
	flushMem()
	// mem is nil here.
	if !d.opts.ReadOnly {
		err = updateVE()
		if err != nil {
			return nil, 0, err
		}
	}
	return toFlush, maxSeqNum, err
}

func checkOptions(opts *Options, path string) (strictWALTail bool, err error) {
	f, err := opts.FS.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return false, err
	}
	return opts.checkOptions(string(data))
}

// DBDesc briefly describes high-level state about a database.
type DBDesc struct {
	// Exists is true if an existing database was found.
	Exists bool
	// FormatMajorVersion indicates the database's current format
	// version.
	FormatMajorVersion FormatMajorVersion
	// ManifestFilename is the filename of the current active manifest,
	// if the database exists.
	ManifestFilename string
}

// Peek looks for an existing database in dirname on the provided FS. It
// returns a brief description of the database. Peek is read-only and
// does not open the database
func Peek(dirname string, fs vfs.FS) (*DBDesc, error) {
	vers, versMarker, err := lookupFormatMajorVersion(fs, dirname)
	if err != nil {
		return nil, err
	}
	// TODO(jackson): Immediately closing the marker is clunky. Add a
	// PeekMarker variant that avoids opening the directory.
	if err := versMarker.Close(); err != nil {
		return nil, err
	}

	// Find the currently active manifest, if there is one.
	manifestMarker, manifestFileNum, exists, err := findCurrentManifest(vers, fs, dirname)
	if err != nil {
		return nil, err
	}
	// TODO(jackson): Immediately closing the marker is clunky. Add a
	// PeekMarker variant that avoids opening the directory.
	if err := manifestMarker.Close(); err != nil {
		return nil, err
	}

	desc := &DBDesc{
		Exists:             exists,
		FormatMajorVersion: vers,
	}
	if exists {
		desc.ManifestFilename = base.MakeFilepath(fs, dirname, fileTypeManifest, manifestFileNum)
	}
	return desc, nil
}

// ErrDBDoesNotExist is generated when ErrorIfNotExists is set and the database
// does not exist.
//
// Note that errors can be wrapped with more details; use errors.Is().
var ErrDBDoesNotExist = errors.New("pebble: database does not exist")

// ErrDBAlreadyExists is generated when ErrorIfExists is set and the database
// already exists.
//
// Note that errors can be wrapped with more details; use errors.Is().
var ErrDBAlreadyExists = errors.New("pebble: database already exists")

// ErrDBNotPristine is generated when ErrorIfNotPristine is set and the database
// already exists and is not pristine.
//
// Note that errors can be wrapped with more details; use errors.Is().
var ErrDBNotPristine = errors.New("pebble: database already exists and is not pristine")

func checkConsistency(v *manifest.Version, dirname string, objProvider objstorage.Provider) error {
	var buf bytes.Buffer
	var args []interface{}

	dedup := make(map[base.DiskFileNum]struct{})
	for level, files := range v.Levels {
		iter := files.Iter()
		for f := iter.First(); f != nil; f = iter.Next() {
			backingState := f.FileBacking
			if _, ok := dedup[backingState.DiskFileNum]; ok {
				continue
			}
			dedup[backingState.DiskFileNum] = struct{}{}
			fileNum := backingState.DiskFileNum
			fileSize := backingState.Size
			meta, err := objProvider.Lookup(base.FileTypeTable, fileNum)
			var size int64
			if err == nil {
				size, err = objProvider.Size(meta)
			}
			if err != nil {
				buf.WriteString("L%d: %s: %v\n")
				args = append(args, errors.Safe(level), errors.Safe(fileNum), err)
				continue
			}

			if size != int64(fileSize) {
				buf.WriteString("L%d: %s: object size mismatch (%s): %d (disk) != %d (MANIFEST)\n")
				args = append(args, errors.Safe(level), errors.Safe(fileNum), objProvider.Path(meta),
					errors.Safe(size), errors.Safe(fileSize))
				continue
			}
		}
	}

	if buf.Len() == 0 {
		return nil
	}
	return errors.Errorf(buf.String(), args...)
}
