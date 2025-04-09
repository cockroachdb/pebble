// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"slices"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/batchrepr"
	"github.com/cockroachdb/pebble/internal/arenaskl"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/constants"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/manual"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/objstorage/remote"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/wal"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	initialMemTableSize = 256 << 10 // 256 KB

	// The max batch size is limited by the uint32 offsets stored in
	// internal/batchskl.node, DeferredBatchOp, and flushableBatchEntry.
	//
	// We limit the size to MaxUint32 (just short of 4GB) so that the exclusive
	// end of an allocation fits in uint32.
	//
	// On 32-bit systems, slices are naturally limited to MaxInt (just short of
	// 2GB).
	maxBatchSize = constants.MaxUint32OrInt

	// The max memtable size is limited by the uint32 offsets stored in
	// internal/arenaskl.node, DeferredBatchOp, and flushableBatchEntry.
	//
	// We limit the size to MaxUint32 (just short of 4GB) so that the exclusive
	// end of an allocation fits in uint32.
	//
	// On 32-bit systems, slices are naturally limited to MaxInt (just short of
	// 2GB).
	maxMemTableSize = constants.MaxUint32OrInt
)

// FileCacheSize can be used to determine the file
// cache size for a single db, given the maximum open
// files which can be used by a file cache which is
// only used by a single db.
func FileCacheSize(maxOpenFiles int) int {
	fileCacheSize := maxOpenFiles - numNonFileCacheFiles
	if fileCacheSize < minFileCacheSize {
		fileCacheSize = minFileCacheSize
	}
	return fileCacheSize
}

// Open opens a DB whose files live in the given directory.
//
// IsCorruptionError() can be use to determine if the error is caused by on-disk
// corruption.
func Open(dirname string, opts *Options) (db *DB, err error) {
	// Make a copy of the options so that we don't mutate the passed in options.
	opts = opts.Clone()
	opts.EnsureDefaults()
	if opts.Experimental.CompactionScheduler == nil {
		opts.Experimental.CompactionScheduler = newConcurrencyLimitScheduler(defaultTimeSource{})
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	if opts.LoggerAndTracer == nil {
		opts.LoggerAndTracer = &base.LoggerWithNoopTracer{Logger: opts.Logger}
	} else {
		opts.Logger = opts.LoggerAndTracer
	}

	if invariants.Sometimes(5) {
		assertComparer := base.MakeAssertComparer(*opts.Comparer)
		opts.Comparer = &assertComparer
	}

	// In all error cases, we return db = nil; this is used by various
	// deferred cleanups.

	// Open the database and WAL directories first.
	walDirname, dataDir, err := prepareAndOpenDirs(dirname, opts)
	if err != nil {
		return nil, errors.Wrapf(err, "error opening database at %q", dirname)
	}
	defer func() {
		if db == nil {
			dataDir.Close()
		}
	}()

	// Lock the database directory.
	var fileLock *Lock
	if opts.Lock != nil {
		// The caller already acquired the database lock. Ensure that the
		// directory matches.
		if err := opts.Lock.pathMatches(dirname); err != nil {
			return nil, err
		}
		if err := opts.Lock.refForOpen(); err != nil {
			return nil, err
		}
		fileLock = opts.Lock
	} else {
		fileLock, err = LockDirectory(dirname, opts.FS)
		if err != nil {
			return nil, err
		}
	}
	defer func() {
		if db == nil {
			_ = fileLock.Close()
		}
	}()

	// List the directory contents. This also happens to include WAL log files, if
	// they are in the same dir, but we will ignore those below. The provider is
	// also given this list, but it ignores non sstable files.
	ls, err := opts.FS.List(dirname)
	if err != nil {
		return nil, err
	}

	// Establish the format major version.
	formatVersion, formatVersionMarker, err := lookupFormatMajorVersion(opts.FS, dirname, ls)
	if err != nil {
		return nil, err
	}
	defer func() {
		if db == nil {
			_ = formatVersionMarker.Close()
		}
	}()

	noFormatVersionMarker := formatVersion == FormatDefault
	if noFormatVersionMarker {
		// We will initialize the store at the minimum possible format, then upgrade
		// the format to the desired one. This helps test the format upgrade code.
		formatVersion = FormatMinSupported
		if opts.Experimental.CreateOnShared != remote.CreateOnSharedNone {
			formatVersion = FormatMinForSharedObjects
		}
		// There is no format version marker file. There are three cases:
		//  - we are trying to open an existing store that was created at
		//    FormatMostCompatible (the only one without a version marker file)
		//  - we are creating a new store;
		//  - we are retrying a failed creation.
		//
		// To error in the first case, we set ErrorIfNotPristine.
		opts.ErrorIfNotPristine = true
		defer func() {
			if err != nil && errors.Is(err, ErrDBNotPristine) {
				// We must be trying to open an existing store at FormatMostCompatible.
				// Correct the error in this case -we
				err = errors.Newf(
					"pebble: database %q written in format major version 1 which is no longer supported",
					dirname)
			}
		}()
	} else {
		if opts.Experimental.CreateOnShared != remote.CreateOnSharedNone && formatVersion < FormatMinForSharedObjects {
			return nil, errors.Newf(
				"pebble: database %q configured with shared objects but written in too old format major version %d",
				dirname, formatVersion)
		}
	}

	// Find the currently active manifest, if there is one.
	manifestMarker, manifestFileNum, manifestExists, err := findCurrentManifest(opts.FS, dirname, ls)
	if err != nil {
		return nil, errors.Wrapf(err, "pebble: database %q", dirname)
	}
	defer func() {
		if db == nil {
			_ = manifestMarker.Close()
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
		opts.Cache = cache.New(opts.CacheSize)
		defer opts.Cache.Unref()
	}

	d := &DB{
		cacheHandle:         opts.Cache.NewHandle(),
		dirname:             dirname,
		opts:                opts,
		cmp:                 opts.Comparer.Compare,
		equal:               opts.Comparer.Equal,
		merge:               opts.Merger.Merge,
		split:               opts.Comparer.Split,
		abbreviatedKey:      opts.Comparer.AbbreviatedKey,
		largeBatchThreshold: (opts.MemTableSize - uint64(memTableEmptySize)) / 2,
		fileLock:            fileLock,
		dataDir:             dataDir,
		closed:              new(atomic.Value),
		closedCh:            make(chan struct{}),
	}
	d.mu.versions = &versionSet{}
	d.diskAvailBytes.Store(math.MaxUint64)
	d.problemSpans.Init(manifest.NumLevels, opts.Comparer.Compare)

	defer func() {
		// If an error or panic occurs during open, attempt to release the manually
		// allocated memory resources. Note that rather than look for an error, we
		// look for the return of a nil DB pointer.
		if r := recover(); db == nil {
			// If there's an unused, recycled memtable, we need to release its memory.
			if obsoleteMemTable := d.memTableRecycle.Swap(nil); obsoleteMemTable != nil {
				d.freeMemTable(obsoleteMemTable)
			}

			if d.fileCache != nil {
				_ = d.fileCache.Close()
			}
			d.cacheHandle.Close()

			for _, mem := range d.mu.mem.queue {
				switch t := mem.flushable.(type) {
				case *memTable:
					manual.Free(manual.MemTable, t.arenaBuf)
					t.arenaBuf = manual.Buf{}
				}
			}
			if d.cleanupManager != nil {
				d.cleanupManager.Close()
			}
			if d.objProvider != nil {
				_ = d.objProvider.Close()
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
	d.mu.nextJobID = 1
	d.mu.mem.nextSize = opts.MemTableSize
	if d.mu.mem.nextSize > initialMemTableSize {
		d.mu.mem.nextSize = initialMemTableSize
	}
	d.mu.compact.cond.L = &d.mu.Mutex
	d.mu.compact.inProgress = make(map[*compaction]struct{})
	d.mu.compact.noOngoingFlushStartTime = crtime.NowMono()
	d.mu.snapshots.init()
	// logSeqNum is the next sequence number that will be assigned.
	// Start assigning sequence numbers from base.SeqNumStart to leave
	// room for reserved sequence numbers (see comments around
	// SeqNumStart).
	d.mu.versions.logSeqNum.Store(base.SeqNumStart)
	d.mu.formatVers.vers.Store(uint64(formatVersion))
	d.mu.formatVers.marker = formatVersionMarker

	d.timeNow = time.Now
	d.openedAt = d.timeNow()

	d.mu.Lock()
	defer d.mu.Unlock()

	jobID := d.newJobIDLocked()

	providerSettings := objstorageprovider.Settings{
		Logger:              opts.Logger,
		FS:                  opts.FS,
		FSDirName:           dirname,
		FSDirInitialListing: ls,
		FSCleaner:           opts.Cleaner,
		NoSyncOnClose:       opts.NoSyncOnClose,
		BytesPerSync:        opts.BytesPerSync,
	}
	providerSettings.Local.ReadaheadConfig = opts.Local.ReadaheadConfig
	providerSettings.Remote.StorageFactory = opts.Experimental.RemoteStorage
	providerSettings.Remote.CreateOnShared = opts.Experimental.CreateOnShared
	providerSettings.Remote.CreateOnSharedLocator = opts.Experimental.CreateOnSharedLocator
	providerSettings.Remote.CacheSizeBytes = opts.Experimental.SecondaryCacheSizeBytes

	d.objProvider, err = objstorageprovider.Open(providerSettings)
	if err != nil {
		return nil, err
	}

	if !manifestExists {
		// DB does not exist.
		if d.opts.ErrorIfNotExists || d.opts.ReadOnly {
			return nil, errors.Wrapf(ErrDBDoesNotExist, "dirname=%q", dirname)
		}

		// Create the DB.
		if err := d.mu.versions.create(
			jobID, dirname, d.objProvider, opts, manifestMarker, d.FormatMajorVersion, &d.mu.Mutex); err != nil {
			return nil, err
		}
	} else {
		if opts.ErrorIfExists {
			return nil, errors.Wrapf(ErrDBAlreadyExists, "dirname=%q", dirname)
		}
		// Load the version set.
		if err := d.mu.versions.load(
			dirname, d.objProvider, opts, manifestFileNum, manifestMarker, d.FormatMajorVersion, &d.mu.Mutex); err != nil {
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
		d.mu.mem.mutable, entry = d.newMemTable(0 /* logNum */, d.mu.versions.logSeqNum.Load(), 0 /* minSize */)
		d.mu.mem.queue = append(d.mu.mem.queue, entry)
	}

	d.mu.log.metrics.fsyncLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Buckets: FsyncLatencyBuckets,
	})
	walOpts := wal.Options{
		Primary:              wal.Dir{FS: opts.FS, Dirname: walDirname},
		Secondary:            wal.Dir{},
		MinUnflushedWALNum:   wal.NumWAL(d.mu.versions.minUnflushedLogNum),
		MaxNumRecyclableLogs: opts.MemTableStopWritesThreshold + 1,
		NoSyncOnClose:        opts.NoSyncOnClose,
		BytesPerSync:         opts.WALBytesPerSync,
		PreallocateSize:      d.walPreallocateSize,
		MinSyncInterval:      opts.WALMinSyncInterval,
		FsyncLatency:         d.mu.log.metrics.fsyncLatency,
		QueueSemChan:         d.commit.logSyncQSem,
		Logger:               opts.Logger,
		EventListener:        walEventListenerAdaptor{l: opts.EventListener},
		WriteWALSyncOffsets:  func() bool { return d.FormatMajorVersion() >= FormatWALSyncChunks },
	}
	if opts.WALFailover != nil {
		walOpts.Secondary = opts.WALFailover.Secondary
		walOpts.FailoverOptions = opts.WALFailover.FailoverOptions
		walOpts.FailoverWriteAndSyncLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
			Buckets: FsyncLatencyBuckets,
		})
	}
	walDirs := append(walOpts.Dirs(), opts.WALRecoveryDirs...)
	wals, err := wal.Scan(walDirs...)
	if err != nil {
		return nil, err
	}
	walManager, err := wal.Init(walOpts, wals)
	if err != nil {
		return nil, err
	}
	defer func() {
		if db == nil {
			_ = walManager.Close()
		}
	}()

	d.mu.log.manager = walManager

	d.cleanupManager = openCleanupManager(opts, d.objProvider, d.onObsoleteTableDelete, d.getDeletionPacerInfo)

	if manifestExists && !opts.DisableConsistencyCheck {
		curVersion := d.mu.versions.currentVersion()
		if err := checkConsistency(curVersion, d.objProvider); err != nil {
			return nil, err
		}
	}

	fileCacheSize := FileCacheSize(opts.MaxOpenFiles)
	if opts.FileCache == nil {
		opts.FileCache = NewFileCache(opts.Experimental.FileCacheShards, fileCacheSize)
		defer opts.FileCache.Unref()
	}
	d.fileCache = opts.FileCache.newHandle(d.cacheHandle, d.objProvider, d.opts.LoggerAndTracer, d.opts.MakeReaderOptions(), d.reportCorruption)
	d.newIters = d.fileCache.newIters
	d.tableNewRangeKeyIter = tableNewRangeKeyIter(d.newIters)

	d.mu.annotators.totalSize = d.makeFileSizeAnnotator(func(f *manifest.TableMetadata) bool {
		return true
	})
	d.mu.annotators.remoteSize = d.makeFileSizeAnnotator(func(f *manifest.TableMetadata) bool {
		meta, err := d.objProvider.Lookup(base.FileTypeTable, f.FileBacking.DiskFileNum)
		if err != nil {
			return false
		}
		return meta.IsRemote()
	})
	d.mu.annotators.externalSize = d.makeFileSizeAnnotator(func(f *manifest.TableMetadata) bool {
		meta, err := d.objProvider.Lookup(base.FileTypeTable, f.FileBacking.DiskFileNum)
		if err != nil {
			return false
		}
		return meta.IsRemote() && meta.Remote.CleanupMethod == objstorage.SharedNoCleanup
	})

	var previousOptionsFileNum base.DiskFileNum
	var previousOptionsFilename string
	for _, filename := range ls {
		ft, fn, ok := base.ParseFilename(opts.FS, filename)
		if !ok {
			continue
		}

		// Don't reuse any obsolete file numbers to avoid modifying an
		// ingested sstable's original external file.
		d.mu.versions.markFileNumUsed(fn)

		switch ft {
		case base.FileTypeLog:
			// Ignore.
		case base.FileTypeOptions:
			if previousOptionsFileNum < fn {
				previousOptionsFileNum = fn
				previousOptionsFilename = filename
			}
		case base.FileTypeTemp, base.FileTypeOldTemp:
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
	if n := len(wals); n > 0 {
		// Don't reuse any obsolete file numbers to avoid modifying an
		// ingested sstable's original external file.
		d.mu.versions.markFileNumUsed(base.DiskFileNum(wals[n-1].Num))
	}

	// Ratchet d.mu.versions.nextFileNum ahead of all known objects in the
	// objProvider. This avoids FileNum collisions with obsolete sstables.
	objects := d.objProvider.List()
	for _, obj := range objects {
		d.mu.versions.markFileNumUsed(obj.DiskFileNum)
	}

	// Validate the most-recent OPTIONS file, if there is one.
	if previousOptionsFilename != "" {
		path := opts.FS.PathJoin(dirname, previousOptionsFilename)
		previousOptions, err := readOptionsFile(opts, path)
		if err != nil {
			return nil, err
		}
		if err := opts.CheckCompatibility(previousOptions); err != nil {
			return nil, err
		}
	}

	// Replay any newer log files than the ones named in the manifest.
	var replayWALs wal.Logs
	for i, w := range wals {
		if base.DiskFileNum(w.Num) >= d.mu.versions.minUnflushedLogNum {
			replayWALs = wals[i:]
			break
		}
	}
	var flushableIngests []*ingestedFlushable
	for i, lf := range replayWALs {
		// WALs other than the last one would have been closed cleanly.
		//
		// Note: we used to never require strict WAL tails when reading from older
		// versions: RocksDB 6.2.1 and the version of Pebble included in CockroachDB
		// 20.1 do not guarantee that closed WALs end cleanly. But the earliest
		// compatible Pebble format is newer and guarantees a clean EOF.
		strictWALTail := i < len(replayWALs)-1
		fi, maxSeqNum, err := d.replayWAL(jobID, lf, strictWALTail)
		if err != nil {
			return nil, err
		}
		if len(fi) > 0 {
			flushableIngests = append(flushableIngests, fi...)
		}
		if d.mu.versions.logSeqNum.Load() < maxSeqNum {
			d.mu.versions.logSeqNum.Store(maxSeqNum)
		}
	}
	if d.mu.mem.mutable == nil {
		// Recreate the mutable memtable if replayWAL got rid of it.
		var entry *flushableEntry
		d.mu.mem.mutable, entry = d.newMemTable(d.mu.versions.getNextDiskFileNum(), d.mu.versions.logSeqNum.Load(), 0 /* minSize */)
		d.mu.mem.queue = append(d.mu.mem.queue, entry)
	}
	d.mu.versions.visibleSeqNum.Store(d.mu.versions.logSeqNum.Load())

	// Register with the CompactionScheduler before calling
	// d.maybeScheduleFlush, since completion of the flush can trigger
	// compactions.
	d.opts.Experimental.CompactionScheduler.Register(2, d)
	if !d.opts.ReadOnly {
		d.maybeScheduleFlush()
		for d.mu.compact.flushing {
			d.mu.compact.cond.Wait()
		}

		// Create an empty .log file for the mutable memtable.
		newLogNum := d.mu.versions.getNextDiskFileNum()
		d.mu.log.writer, err = d.mu.log.manager.Create(wal.NumWAL(newLogNum), int(jobID))
		if err != nil {
			return nil, err
		}

		// This isn't strictly necessary as we don't use the log number for
		// memtables being flushed, only for the next unflushed memtable.
		d.mu.mem.queue[len(d.mu.mem.queue)-1].logNum = newLogNum
	}
	d.updateReadStateLocked(d.opts.DebugCheck)

	if !d.opts.ReadOnly {
		// If the Options specify a format major version higher than the
		// loaded database's, upgrade it. If this is a new database, this
		// code path also performs an initial upgrade from the starting
		// implicit MinSupported version.
		//
		// We ratchet the version this far into Open so that migrations have a read
		// state available. Note that this also results in creating/updating the
		// format version marker file.
		if opts.FormatMajorVersion > d.FormatMajorVersion() {
			if err := d.ratchetFormatMajorVersionLocked(opts.FormatMajorVersion); err != nil {
				return nil, err
			}
		} else if noFormatVersionMarker {
			// We are creating a new store. Create the format version marker file.
			if err := d.writeFormatVersionMarker(d.FormatMajorVersion()); err != nil {
				return nil, err
			}
		}

		// Write the current options to disk.
		d.optionsFileNum = d.mu.versions.getNextDiskFileNum()
		tmpPath := base.MakeFilepath(opts.FS, dirname, base.FileTypeTemp, d.optionsFileNum)
		optionsPath := base.MakeFilepath(opts.FS, dirname, base.FileTypeOptions, d.optionsFileNum)

		// Write them to a temporary file first, in case we crash before
		// we're done. A corrupt options file prevents opening the
		// database.
		optionsFile, err := opts.FS.Create(tmpPath, vfs.WriteCategoryUnspecified)
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
		// Get a fresh list of files, in case some of the earlier flushes/compactions
		// have deleted some files.
		ls, err := opts.FS.List(dirname)
		if err != nil {
			return nil, err
		}
		d.scanObsoleteFiles(ls, flushableIngests)
		d.deleteObsoleteFiles(jobID)
	}
	// Else, nothing is obsolete.

	d.mu.tableStats.cond.L = &d.mu.Mutex
	d.mu.tableValidation.cond.L = &d.mu.Mutex
	if !d.opts.ReadOnly {
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
	// newIters, fileCache, versions, etc. Each of this individually cause a
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
) (walDirname string, dataDir vfs.File, err error) {
	walDirname = opts.WALDir
	if opts.WALDir == "" {
		walDirname = dirname
	}

	// Create directories if needed.
	if !opts.ReadOnly {
		f, err := mkdirAllAndSyncParents(opts.FS, dirname)
		if err != nil {
			return "", nil, err
		}
		f.Close()
		if walDirname != dirname {
			f, err := mkdirAllAndSyncParents(opts.FS, walDirname)
			if err != nil {
				return "", nil, err
			}
			f.Close()
		}
		if opts.WALFailover != nil {
			secondary := opts.WALFailover.Secondary
			f, err := mkdirAllAndSyncParents(secondary.FS, secondary.Dirname)
			if err != nil {
				return "", nil, err
			}
			f.Close()
		}
	}

	dataDir, err = opts.FS.OpenDir(dirname)
	if err != nil {
		if opts.ReadOnly && oserror.IsNotExist(err) {
			return "", nil, errors.Errorf("pebble: database %q does not exist", dirname)
		}
		return "", nil, err
	}
	if opts.ReadOnly && walDirname != dirname {
		// Check that the wal dir exists.
		walDir, err := opts.FS.OpenDir(walDirname)
		if err != nil {
			dataDir.Close()
			return "", nil, err
		}
		walDir.Close()
	}

	return walDirname, dataDir, nil
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
	lastOptionsSeen := base.DiskFileNum(0)
	for _, filename := range ls {
		ft, fn, ok := base.ParseFilename(fs, filename)
		if !ok {
			continue
		}
		switch ft {
		case base.FileTypeOptions:
			// If this file has a higher number than the last options file
			// processed, reset version. This is because rocksdb often
			// writes multiple options files without deleting previous ones.
			// Otherwise, skip parsing this options file.
			if fn > lastOptionsSeen {
				version = ""
				lastOptionsSeen = fn
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
			err = parseOptions(string(data), parseOptionsFuncs{
				visitKeyValue: func(i, j int, section, key, value string) error {
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
				},
			})
			if err != nil {
				return "", err
			}
		}
	}
	return version, nil
}

func (d *DB) replayIngestedFlushable(
	b *Batch, logNum base.DiskFileNum,
) (entry *flushableEntry, err error) {
	br := b.Reader()
	seqNum := b.SeqNum()

	fileNums := make([]base.DiskFileNum, 0, b.Count())
	var exciseSpan KeyRange
	addFileNum := func(encodedFileNum []byte) {
		fileNum, n := binary.Uvarint(encodedFileNum)
		if n <= 0 {
			panic("pebble: ingest sstable file num is invalid")
		}
		fileNums = append(fileNums, base.DiskFileNum(fileNum))
	}

	for i := 0; i < int(b.Count()); i++ {
		kind, key, val, ok, err := br.Next()
		if err != nil {
			return nil, err
		}
		if kind != InternalKeyKindIngestSST && kind != InternalKeyKindExcise {
			panic("pebble: invalid batch key kind")
		}
		if !ok {
			panic("pebble: invalid batch count")
		}
		if kind == base.InternalKeyKindExcise {
			if exciseSpan.Valid() {
				panic("pebble: multiple excise spans in a single batch")
			}
			exciseSpan.Start = slices.Clone(key)
			exciseSpan.End = slices.Clone(val)
			continue
		}
		addFileNum(key)
	}

	if _, _, _, ok, err := br.Next(); err != nil {
		return nil, err
	} else if ok {
		panic("pebble: invalid number of entries in batch")
	}

	meta := make([]*tableMetadata, len(fileNums))
	var lastRangeKey keyspan.Span
	for i, n := range fileNums {
		readable, err := d.objProvider.OpenForReading(context.TODO(), base.FileTypeTable, n,
			objstorage.OpenOptions{MustExist: true})
		if err != nil {
			return nil, errors.Wrap(err, "pebble: error when opening flushable ingest files")
		}
		// NB: ingestLoad1 will close readable.
		meta[i], lastRangeKey, err = ingestLoad1(context.TODO(), d.opts, d.FormatMajorVersion(),
			readable, d.cacheHandle, base.PhysicalTableFileNum(n), disableRangeKeyChecks())
		if err != nil {
			return nil, errors.Wrap(err, "pebble: error when loading flushable ingest files")
		}
	}
	if lastRangeKey.Valid() && d.opts.Comparer.Split.HasSuffix(lastRangeKey.End) {
		return nil, errors.AssertionFailedf("pebble: last ingest sstable has suffixed range key end %s",
			d.opts.Comparer.FormatKey(lastRangeKey.End))
	}

	numFiles := len(meta)
	if exciseSpan.Valid() {
		numFiles++
	}
	if uint32(numFiles) != b.Count() {
		panic("pebble: couldn't load all files in WAL entry")
	}

	return d.newIngestedFlushableEntry(meta, seqNum, logNum, exciseSpan)
}

// replayWAL replays the edits in the specified WAL. If the DB is in read
// only mode, then the WALs are replayed into memtables and not flushed. If
// the DB is not in read only mode, then the contents of the WAL are
// guaranteed to be flushed when a flush is scheduled after this method is run.
// Note that this flushing is very important for guaranteeing durability:
// the application may have had a number of pending
// fsyncs to the WAL before the process crashed, and those fsyncs may not have
// happened but the corresponding data may now be readable from the WAL (while
// sitting in write-back caches in the kernel or the storage device). By
// reading the WAL (including the non-fsynced data) and then flushing all
// these changes (flush does fsyncs), we are able to guarantee that the
// initial state of the DB is durable.
//
// This method mutates d.mu.mem.queue and possibly d.mu.mem.mutable and replays
// WALs into the flushable queue. Flushing of the queue is expected to be handled
// by callers. A list of flushable ingests (but not memtables) replayed is returned.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) replayWAL(
	jobID JobID, ll wal.LogicalLog, strictWALTail bool,
) (flushableIngests []*ingestedFlushable, maxSeqNum base.SeqNum, err error) {
	rr := ll.OpenForRead()
	defer func() { _ = rr.Close() }()
	var (
		b               Batch
		buf             bytes.Buffer
		mem             *memTable
		entry           *flushableEntry
		offset          wal.Offset
		lastFlushOffset int64
		keysReplayed    int64 // number of keys replayed
		batchesReplayed int64 // number of batches replayed
	)

	// TODO(jackson): This function is interspersed with panics, in addition to
	// corruption error propagation. Audit them to ensure we're truly only
	// panicking where the error points to Pebble bug and not user or
	// hardware-induced corruption.

	// "Flushes" (ie. closes off) the current memtable, if not nil.
	flushMem := func() {
		if mem == nil {
			return
		}
		mem.writerUnref()
		if d.mu.mem.mutable == mem {
			d.mu.mem.mutable = nil
		}
		entry.flushForced = !d.opts.ReadOnly
		var logSize uint64
		mergedOffset := offset.Physical + offset.PreviousFilesBytes
		if mergedOffset >= lastFlushOffset {
			logSize = uint64(mergedOffset - lastFlushOffset)
		}
		// Else, this was the initial memtable in the read-only case which must have
		// been empty, but we need to flush it since we don't want to add to it later.
		lastFlushOffset = mergedOffset
		entry.logSize = logSize
		mem, entry = nil, nil
	}

	mem = d.mu.mem.mutable
	if mem != nil {
		entry = d.mu.mem.queue[len(d.mu.mem.queue)-1]
		if !d.opts.ReadOnly {
			flushMem()
		}
	}

	// Creates a new memtable if there is no current memtable.
	ensureMem := func(seqNum base.SeqNum) {
		if mem != nil {
			return
		}
		mem, entry = d.newMemTable(base.DiskFileNum(ll.Num), seqNum, 0 /* minSize */)
		d.mu.mem.mutable = mem
		d.mu.mem.queue = append(d.mu.mem.queue, entry)
	}

	defer func() {
		if err != nil {
			err = errors.WithDetailf(err, "replaying wal %d, offset %s", ll.Num, offset)
		}
	}()

	for {
		var r io.Reader
		var err error
		r, offset, err = rr.NextRecord()
		if err == nil {
			_, err = io.Copy(&buf, r)
		}
		if err != nil {
			// It is common to encounter a zeroed or invalid chunk due to WAL
			// preallocation and WAL recycling. However zeroed or invalid chunks
			// can also be a consequence of corruption / disk rot. When the log
			// reader encounters one of these cases, it attempts to disambiguate
			// by reading ahead looking for a future record. If a future chunk
			// indicates the chunk at the original offset should've been valid, it
			// surfaces record.ErrInvalidChunk or record.ErrZeroedChunk. These
			// errors are always indicative of corruption and data loss.
			//
			// Otherwise, the reader surfaces io.ErrUnexpectedEOF indicating that
			// the WAL terminated uncleanly and ambiguously. If the WAL is the
			// most recent logical WAL, the caller passes in (strictWALTail=false),
			// indicating we should tolerate the unclean ending. If the WAL is an
			// older WAL, the caller passes in (strictWALTail=true), indicating that
			// the WAL should have been closed cleanly, and we should interpret
			// the `io.ErrUnexpectedEOF` as corruption and stop recovery.
			if errors.Is(err, io.EOF) {
				break
			} else if errors.Is(err, io.ErrUnexpectedEOF) && !strictWALTail {
				break
			} else if errors.Is(err, record.ErrInvalidChunk) || errors.Is(err, record.ErrZeroedChunk) {
				// If a read-ahead returns one of these errors, they should be marked with corruption.
				// Other I/O related errors should not be marked with corruption and simply returned.
				err = errors.Mark(err, ErrCorruption)
			}

			return nil, 0, errors.Wrap(err, "pebble: error when replaying WAL")
		}

		if buf.Len() < batchrepr.HeaderLen {
			return nil, 0, base.CorruptionErrorf("pebble: corrupt wal %s (offset %s)",
				errors.Safe(base.DiskFileNum(ll.Num)), offset)
		}

		if d.opts.ErrorIfNotPristine {
			return nil, 0, errors.WithDetailf(ErrDBNotPristine, "location: %q", d.dirname)
		}

		// Specify Batch.db so that Batch.SetRepr will compute Batch.memTableSize
		// which is used below.
		b = Batch{}
		b.db = d
		if err := b.SetRepr(buf.Bytes()); err != nil {
			return nil, 0, err
		}
		seqNum := b.SeqNum()
		maxSeqNum = seqNum + base.SeqNum(b.Count())
		keysReplayed += int64(b.Count())
		batchesReplayed++
		{
			br := b.Reader()
			if kind, _, _, ok, err := br.Next(); err != nil {
				return nil, 0, err
			} else if ok && (kind == InternalKeyKindIngestSST || kind == InternalKeyKindExcise) {
				// We're in the flushable ingests (+ possibly excises) case.
				//
				// Ingests require an up-to-date view of the LSM to determine the target
				// level of ingested sstables, and to accurately compute excises. Instead of
				// doing an ingest in this function, we just enqueue a flushable ingest
				// in the flushables queue and run a regular flush.
				flushMem()
				// mem is nil here.
				entry, err = d.replayIngestedFlushable(&b, base.DiskFileNum(ll.Num))
				if err != nil {
					return nil, 0, err
				}
				fi := entry.flushable.(*ingestedFlushable)
				flushableIngests = append(flushableIngests, fi)
				d.mu.mem.queue = append(d.mu.mem.queue, entry)
				// A flushable ingest is always followed by a WAL rotation.
				break
			}
		}

		if b.memTableSize >= uint64(d.largeBatchThreshold) {
			flushMem()
			// Make a copy of the data slice since it is currently owned by buf and will
			// be reused in the next iteration.
			b.data = slices.Clone(b.data)
			b.flushable, err = newFlushableBatch(&b, d.opts.Comparer)
			if err != nil {
				return nil, 0, err
			}
			entry := d.newFlushableEntry(b.flushable, base.DiskFileNum(ll.Num), b.SeqNum())
			// Disable memory accounting by adding a reader ref that will never be
			// removed.
			entry.readerRefs.Add(1)
			d.mu.mem.queue = append(d.mu.mem.queue, entry)
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

	d.opts.Logger.Infof("[JOB %d] WAL %s stopped reading at offset: %s; replayed %d keys in %d batches",
		jobID, ll.String(), offset, keysReplayed, batchesReplayed)
	if !d.opts.ReadOnly {
		flushMem()
	}

	// mem is nil here, if !ReadOnly.
	return flushableIngests, maxSeqNum, err
}

func readOptionsFile(opts *Options, path string) (string, error) {
	f, err := opts.FS.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return "", err
	}
	return string(data), nil
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
	// OptionsFilename is the filename of the most recent OPTIONS file, if it
	// exists.
	OptionsFilename string
}

// String implements fmt.Stringer.
func (d *DBDesc) String() string {
	if !d.Exists {
		return "uninitialized"
	}
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "initialized at format major version %s\n", d.FormatMajorVersion)
	fmt.Fprintf(&buf, "manifest: %s\n", d.ManifestFilename)
	fmt.Fprintf(&buf, "options: %s", d.OptionsFilename)
	return buf.String()
}

// Peek looks for an existing database in dirname on the provided FS. It
// returns a brief description of the database. Peek is read-only and
// does not open the database
func Peek(dirname string, fs vfs.FS) (*DBDesc, error) {
	ls, err := fs.List(dirname)
	if err != nil {
		return nil, err
	}

	vers, versMarker, err := lookupFormatMajorVersion(fs, dirname, ls)
	if err != nil {
		return nil, err
	}
	// TODO(jackson): Immediately closing the marker is clunky. Add a
	// PeekMarker variant that avoids opening the directory.
	if err := versMarker.Close(); err != nil {
		return nil, err
	}

	// Find the currently active manifest, if there is one.
	manifestMarker, manifestFileNum, exists, err := findCurrentManifest(fs, dirname, ls)
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

	// Find the OPTIONS file with the highest file number within the list of
	// directory entries.
	var previousOptionsFileNum base.DiskFileNum
	for _, filename := range ls {
		ft, fn, ok := base.ParseFilename(fs, filename)
		if !ok || ft != base.FileTypeOptions || fn < previousOptionsFileNum {
			continue
		}
		previousOptionsFileNum = fn
		desc.OptionsFilename = fs.PathJoin(dirname, filename)
	}

	if exists {
		desc.ManifestFilename = base.MakeFilepath(fs, dirname, base.FileTypeManifest, manifestFileNum)
	}
	return desc, nil
}

// LockDirectory acquires the database directory lock in the named directory,
// preventing another process from opening the database. LockDirectory returns a
// handle to the held lock that may be passed to Open through Options.Lock to
// subsequently open the database, skipping lock acquistion during Open.
//
// LockDirectory may be used to expand the critical section protected by the
// database lock to include setup before the call to Open.
func LockDirectory(dirname string, fs vfs.FS) (*Lock, error) {
	fileLock, err := fs.Lock(base.MakeFilepath(fs, dirname, base.FileTypeLock, base.DiskFileNum(0)))
	if err != nil {
		return nil, err
	}
	l := &Lock{dirname: dirname, fileLock: fileLock}
	l.refs.Store(1)
	invariants.SetFinalizer(l, func(obj interface{}) {
		if refs := obj.(*Lock).refs.Load(); refs > 0 {
			panic(errors.AssertionFailedf("lock for %q finalized with %d refs", dirname, refs))
		}
	})
	return l, nil
}

// Lock represents a file lock on a directory. It may be passed to Open through
// Options.Lock to elide lock aquisition during Open.
type Lock struct {
	dirname  string
	fileLock io.Closer
	// refs is a count of the number of handles on the lock. refs must be 0, 1
	// or 2.
	//
	// When acquired by the client and passed to Open, refs = 1 and the Open
	// call increments it to 2. When the database is closed, it's decremented to
	// 1. Finally when the original caller, calls Close on the Lock, it's
	// drecemented to zero and the underlying file lock is released.
	//
	// When Open acquires the file lock, refs remains at 1 until the database is
	// closed.
	refs atomic.Int32
}

func (l *Lock) refForOpen() error {
	// During Open, when a user passed in a lock, the reference count must be
	// exactly 1. If it's zero, the lock is no longer held and is invalid. If
	// it's 2, the lock is already in use by another database within the
	// process.
	if !l.refs.CompareAndSwap(1, 2) {
		return errors.Errorf("pebble: unexpected Lock reference count; is the lock already in use?")
	}
	return nil
}

// Close releases the lock, permitting another process to lock and open the
// database. Close must not be called until after a database using the Lock has
// been closed.
func (l *Lock) Close() error {
	if l.refs.Add(-1) > 0 {
		return nil
	}
	defer func() { l.fileLock = nil }()
	return l.fileLock.Close()
}

func (l *Lock) pathMatches(dirname string) error {
	if dirname == l.dirname {
		return nil
	}
	// Check for relative paths, symlinks, etc. This isn't ideal because we're
	// circumventing the vfs.FS interface here.
	//
	// TODO(jackson): We could add support for retrieving file inodes through Stat
	// calls in the VFS interface on platforms where it's available and use that
	// to differentiate.
	dirStat, err1 := os.Stat(dirname)
	lockDirStat, err2 := os.Stat(l.dirname)
	if err1 == nil && err2 == nil && os.SameFile(dirStat, lockDirStat) {
		return nil
	}
	return errors.Join(
		errors.Newf("pebble: opts.Lock acquired in %q not %q", l.dirname, dirname),
		err1, err2)
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

func checkConsistency(v *manifest.Version, objProvider objstorage.Provider) error {
	var errs []error
	dedup := make(map[base.DiskFileNum]struct{})
	for level, files := range v.Levels {
		for f := range files.All() {
			backingState := f.FileBacking
			if _, ok := dedup[backingState.DiskFileNum]; ok {
				continue
			}
			dedup[backingState.DiskFileNum] = struct{}{}
			fileNum := backingState.DiskFileNum
			fileSize := backingState.Size
			// We skip over remote objects; those are instead checked asynchronously
			// by the table stats loading job.
			meta, err := objProvider.Lookup(base.FileTypeTable, fileNum)
			var size int64
			if err == nil {
				if meta.IsRemote() {
					continue
				}
				size, err = objProvider.Size(meta)
			}
			if err != nil {
				errs = append(errs, errors.Wrapf(err, "L%d: %s", errors.Safe(level), fileNum))
				continue
			}

			if size != int64(fileSize) {
				errs = append(errs, errors.Errorf(
					"L%d: %s: object size mismatch (%s): %d (disk) != %d (MANIFEST)",
					errors.Safe(level), fileNum, objProvider.Path(meta),
					errors.Safe(size), errors.Safe(fileSize)))
				continue
			}
		}
	}
	return errors.Join(errs...)
}

type walEventListenerAdaptor struct {
	l *EventListener
}

func (l walEventListenerAdaptor) LogCreated(ci wal.CreateInfo) {
	// TODO(sumeer): extend WALCreateInfo for the failover case in case the path
	// is insufficient to infer whether primary or secondary.
	wci := WALCreateInfo{
		JobID:           ci.JobID,
		Path:            ci.Path,
		FileNum:         base.DiskFileNum(ci.Num),
		RecycledFileNum: ci.RecycledFileNum,
		Err:             ci.Err,
	}
	l.l.WALCreated(wci)
}
