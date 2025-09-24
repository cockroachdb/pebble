// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/inflight"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/manual"
	"github.com/cockroachdb/pebble/internal/tombspan"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/remote"
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
	maxBatchSize = min(math.MaxUint32, math.MaxInt)

	// The max memtable size is limited by the uint32 offsets stored in
	// internal/arenaskl.node, DeferredBatchOp, and flushableBatchEntry.
	//
	// We limit the size to MaxUint32 (just short of 4GB) so that the exclusive
	// end of an allocation fits in uint32.
	//
	// On 32-bit systems, slices are naturally limited to MaxInt (just short of
	// 2GB).
	maxMemTableSize = min(math.MaxUint32, math.MaxInt)
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
	maybeCleanUp := func(fn func() error) {
		if db == nil {
			err = errors.CombineErrors(err, fn())
		}
	}

	// Recover the current database state.
	rs, err := recoverState(opts, dirname)
	if err != nil {
		return nil, err
	}
	defer maybeCleanUp(rs.Close)

	formatVersion := rs.fmv
	noFormatVersionMarker := rs.fmv == FormatDefault
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
	}

	if !opts.ReadOnly {
		if err := rs.RemoveObsolete(opts); err != nil {
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
		dirs:                rs.dirs,
		objProvider:         rs.objProvider,
		closed:              new(atomic.Value),
		closedCh:            make(chan struct{}),
	}
	d.mu.versions = &versionSet{}
	d.diskAvailBytes.Store(math.MaxUint64)
	d.problemSpans.Init(manifest.NumLevels, opts.Comparer.Compare)
	if opts.Experimental.CompactionScheduler != nil {
		d.compactionScheduler = opts.Experimental.CompactionScheduler()
	} else {
		d.compactionScheduler = newConcurrencyLimitScheduler(defaultTimeSource{})
	}
	if iterTrackOpts := opts.Experimental.IteratorTracking; iterTrackOpts.PollInterval > 0 && iterTrackOpts.MaxAge > 0 {
		d.iterTracker = inflight.NewPollingTracker(iterTrackOpts.PollInterval, iterTrackOpts.MaxAge, func(report string) {
			d.opts.Logger.Infof("Long-lived iterators detected:\n%s", report)
		})
	}

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
			if d.deletePacer != nil {
				d.deletePacer.Close()
			}
			if d.mu.versions.manifestFile != nil {
				_ = d.mu.versions.manifestFile.Close()
			}
			if d.iterTracker != nil {
				d.iterTracker.Close()
				d.iterTracker = nil
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
	d.mu.mem.nextSize = min(opts.MemTableSize, initialMemTableSize)
	d.mu.compact.cond.L = &d.mu.Mutex
	d.mu.compact.inProgress = make(map[compaction]struct{})
	d.mu.compact.noOngoingFlushStartTime = crtime.NowMono()
	d.mu.snapshots.snapshotList.init()
	d.mu.snapshots.ongoingExcises = make(map[SeqNum]KeyRange)
	d.mu.snapshots.ongoingExcisesRemovedCond = sync.NewCond(&d.mu.Mutex)
	d.mu.formatVers.vers.Store(uint64(formatVersion))
	d.mu.formatVers.marker = rs.fmvMarker
	d.openedAt = d.opts.private.timeNow()
	d.mu.compact.wideTombstones = tombspan.Make(opts.Comparer)

	d.mu.Lock()
	defer d.mu.Unlock()

	jobID := d.newJobIDLocked()

	if rs.recoveredVersion == nil {
		// DB does not exist.
		if d.opts.ErrorIfNotExists || d.opts.ReadOnly {
			return nil, errors.Wrapf(ErrDBDoesNotExist, "dirname=%q", dirname)
		}
		// Create a fresh version set and create an initial manifest file.
		blobRewriteHeuristic := manifest.BlobRewriteHeuristic{
			CurrentTime: d.opts.private.timeNow,
			MinimumAge:  opts.Experimental.ValueSeparationPolicy().RewriteMinimumAge,
		}
		if err := d.mu.versions.initNewDB(
			jobID, dirname, d.objProvider, opts, rs.manifestMarker, d.FormatMajorVersion, blobRewriteHeuristic, &d.mu.Mutex); err != nil {
			return nil, err
		}
	} else {
		if opts.ErrorIfExists {
			return nil, errors.Wrapf(ErrDBAlreadyExists, "dirname=%q", dirname)
		}
		// Initialize the version set from the recovered version.
		if err := d.mu.versions.initRecoveredDB(
			dirname, d.objProvider, opts, rs.recoveredVersion, rs.manifestMarker,
			d.FormatMajorVersion, &d.mu.Mutex,
		); err != nil {
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

	// Initialize WAL metrics structure (histograms populated below)
	d.mu.log.metrics = WALMetrics{}

	walOpts := wal.Options{
		Primary:              rs.dirs.WALPrimary,
		Secondary:            rs.dirs.WALSecondary,
		MinUnflushedWALNum:   wal.NumWAL(d.mu.versions.minUnflushedLogNum),
		MaxNumRecyclableLogs: opts.MemTableStopWritesThreshold + 1,
		NoSyncOnClose:        opts.NoSyncOnClose,
		BytesPerSync:         opts.WALBytesPerSync,
		PreallocateSize:      d.walPreallocateSize,
		MinSyncInterval:      opts.WALMinSyncInterval,
		QueueSemChan:         d.commit.logSyncQSem,
		Logger:               opts.Logger,
		EventListener:        walEventListenerAdaptor{l: opts.EventListener},
		WriteWALSyncOffsets:  func() bool { return d.FormatMajorVersion() >= FormatWALSyncChunks },
	}

	// Create and assign WAL file operation histograms
	walPrimaryFileOpHistogram := prometheus.NewHistogram(prometheus.HistogramOpts{
		Buckets: FsyncLatencyBuckets,
	})
	d.mu.log.metrics.PrimaryFileOpLatency = walPrimaryFileOpHistogram
	walOpts.PrimaryFileOpHistogram = walPrimaryFileOpHistogram

	// Configure failover-specific histograms and options
	if !opts.ReadOnly && opts.WALFailover != nil {
		if walOpts.Secondary.ID == "" {
			walOpts.Secondary.ID = opts.WALFailover.Secondary.ID
		} else if opts.WALFailover.Secondary.ID != "" && walOpts.Secondary.ID != opts.WALFailover.Secondary.ID {
			return nil, errors.Errorf("WAL failover secondary identifier mismatch: OPTIONS file has %q but %q was provided - wrong disk may be mounted",
				walOpts.Secondary.ID, opts.WALFailover.Secondary.ID)
		}
		walSecondaryFileOpHistogram := prometheus.NewHistogram(prometheus.HistogramOpts{
			Buckets: FsyncLatencyBuckets,
		})
		d.mu.log.metrics.SecondaryFileOpLatency = walSecondaryFileOpHistogram
		walOpts.SecondaryFileOpHistogram = walSecondaryFileOpHistogram

		walFailoverWriteAndSyncHistogram := prometheus.NewHistogram(prometheus.HistogramOpts{
			Buckets: FsyncLatencyBuckets,
		})
		walOpts.FailoverWriteAndSyncLatency = walFailoverWriteAndSyncHistogram

		walOpts.FailoverOptions = opts.WALFailover.FailoverOptions

		walDir, err := wal.ValidateOrInitWALDir(walOpts.Secondary)
		if err != nil {
			return nil, err
		}
		walOpts.Secondary = walDir
		opts.WALFailover.Secondary.ID = walDir.ID
	}
	walManager, err := wal.Init(walOpts, rs.walsReplay)
	if err != nil {
		return nil, err
	}

	defer maybeCleanUp(walManager.Close)
	d.mu.log.manager = walManager

	// The delete pacer will call calculateDiskAvailableBytes (and thus
	// GetDiskUsage) O(number of file deletions) times. This may seem inefficient,
	// but in practice this was observed to take constant time, regardless of
	// volume size used, at least on linux with ext4 and zfs. All invocations take
	// 10 microseconds or less.
	d.deletePacer = openDeletePacer(d.opts, d.objProvider, d.calculateDiskAvailableBytes)

	fileCacheSize := FileCacheSize(opts.MaxOpenFiles)
	if opts.FileCache == nil {
		opts.FileCache = NewFileCache(opts.Experimental.FileCacheShards, fileCacheSize)
		defer opts.FileCache.Unref()
	}
	fileCacheReaderOpts := d.opts.MakeReaderOptions()
	fileCacheReaderOpts.CompressionCounters = &d.compressionCounters.Decompressed
	d.fileCache = opts.FileCache.newHandle(d.cacheHandle, d.objProvider, d.opts.LoggerAndTracer, fileCacheReaderOpts, d.reportCorruption)
	d.newIters = d.fileCache.newIters
	d.tableNewRangeKeyIter = tableNewRangeKeyIter(d.newIters)

	d.tableDiskUsageAnnotator = d.makeTableDiskSpaceUsageAnnotator()
	d.blobFileDiskUsageAnnotator = d.makeBlobFileDiskSpaceUsageAnnotator()

	d.mu.versions.markFileNumUsed(rs.maxFilenumUsed)

	// Replay any newer log files than the ones named in the manifest.
	var flushableIngests []*ingestedFlushable
	for i, w := range rs.walsReplay {
		// WALs other than the last one would have been closed cleanly.
		//
		// Note: we used to never require strict WAL tails when reading from older
		// versions: RocksDB 6.2.1 and the version of Pebble included in CockroachDB
		// 20.1 do not guarantee that closed WALs end cleanly. But the earliest
		// compatible Pebble format is newer and guarantees a clean EOF.
		strictWALTail := i < len(rs.walsReplay)-1
		fi, maxSeqNum, err := d.replayWAL(jobID, w, strictWALTail)
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

	if !d.opts.ReadOnly {
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
		if err := d.dirs.DataDir.Sync(); err != nil {
			return nil, err
		}

		// Delete any obsolete files.
		d.scanObsoleteFiles(rs.ls, flushableIngests)
		d.deleteObsoleteFiles(jobID)
	}

	// Register with the CompactionScheduler before calling
	// d.maybeScheduleFlush, since completion of the flush can trigger
	// compactions.
	d.compactionScheduler.Register(2, d)
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
	}

	d.mu.tableStats.cond.L = &d.mu.Mutex
	d.mu.tableValidation.cond.L = &d.mu.Mutex
	if !d.opts.ReadOnly {
		d.maybeCollectTableStatsLocked()
	}
	d.calculateDiskAvailableBytes()

	d.maybeScheduleFlush()
	d.maybeScheduleCompaction()

	// Locks in RecoveryDirLocks can be closed as soon as we've finished opening
	// the database.
	if err = rs.dirs.RecoveryDirLocks.Close(); err != nil {
		return nil, err
	}

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

// resolvedDirs is a set of resolved directory paths and locks.
type resolvedDirs struct {
	DirLocks         base.DirLockSet
	RecoveryDirLocks base.DirLockSet
	DataDir          vfs.File
	WALPrimary       wal.Dir
	WALSecondary     wal.Dir
	WALRecovery      []wal.Dir
}

// WALDirs returns the set of resolved directories that may contain WAL files
// relevant to recovery.
func (d *resolvedDirs) WALDirs() []wal.Dir {
	dirs := []wal.Dir{d.WALPrimary}
	if d.WALSecondary.Dirname != "" {
		dirs = append(dirs, d.WALSecondary)
	}
	dirs = append(dirs, d.WALRecovery...)
	return dirs
}

// Close closes the data directory and the directory locks.
func (d *resolvedDirs) Close() error {
	var err error
	if d.DataDir != nil {
		err = errors.CombineErrors(err, d.DataDir.Close())
	}
	err = errors.CombineErrors(err, d.DirLocks.Close())
	err = errors.CombineErrors(err, d.RecoveryDirLocks.Close())
	*d = resolvedDirs{}
	return err
}

// prepareOpenAndLockDirs resolves the various directory paths indicated within
// Options (substituting {store_path} relative paths as necessary), creates the
// directories if they don't exist, and acquires directory locks as necessary.
//
// Returns an error if ReadOnly is set and the directories don't exist. Always
// returns a non-nil resolvedDirs that may be closed to release all resources
// acquired before any error was encountered.
func prepareOpenAndLockDirs(dirname string, opts *Options) (dirs *resolvedDirs, err error) {
	dirs = &resolvedDirs{}
	dirs.WALPrimary.Dirname = dirname
	dirs.WALPrimary.FS = opts.FS
	if opts.WALDir != "" {
		dirs.WALPrimary.Dirname = resolveStorePath(dirname, opts.WALDir)
	}
	if opts.WALFailover != nil {
		dirs.WALSecondary.Dirname = resolveStorePath(dirname, opts.WALFailover.Secondary.Dirname)
		dirs.WALSecondary.FS = opts.WALFailover.Secondary.FS
	}

	// Create directories if needed.
	if !opts.ReadOnly {
		f, err := mkdirAllAndSyncParents(opts.FS, dirname)
		if err != nil {
			return dirs, err
		}
		f.Close()
		if dirs.WALPrimary.Dirname != dirname {
			f, err := mkdirAllAndSyncParents(opts.FS, dirs.WALPrimary.Dirname)
			if err != nil {
				return dirs, err
			}
			f.Close()
		}
		if opts.WALFailover != nil {
			f, err := mkdirAllAndSyncParents(opts.WALFailover.Secondary.FS, dirs.WALSecondary.Dirname)
			if err != nil {
				return dirs, err
			}
			f.Close()
		}
	}

	dirs.DataDir, err = opts.FS.OpenDir(dirname)
	if err != nil {
		if opts.ReadOnly && oserror.IsNotExist(err) {
			return dirs, errors.Errorf("pebble: database %q does not exist", dirname)
		}
		return dirs, err
	}
	if opts.ReadOnly && dirs.WALPrimary.Dirname != dirname {
		// Check that the wal dir exists.
		walDir, err := opts.FS.OpenDir(dirs.WALPrimary.Dirname)
		if err != nil {
			return dirs, err
		}
		walDir.Close()
	}

	// Lock the database directory.
	_, err = dirs.DirLocks.AcquireOrValidate(opts.Lock, dirname, opts.FS)
	if err != nil {
		return dirs, err
	}
	// Lock the dedicated WAL directory, if configured.
	if dirs.WALPrimary.Dirname != dirname {
		dirs.WALPrimary.Lock, err = dirs.DirLocks.AcquireOrValidate(opts.WALDirLock, dirs.WALPrimary.Dirname, opts.FS)
		if err != nil {
			return dirs, err
		}
	}
	// Lock the secondary WAL directory, if distinct from the data directory
	// and primary WAL directory.
	if opts.WALFailover != nil && dirs.WALSecondary.Dirname != dirname && dirs.WALSecondary.Dirname != dirs.WALPrimary.Dirname {
		dirs.WALSecondary.Lock, err = dirs.DirLocks.AcquireOrValidate(
			opts.WALFailover.Secondary.Lock, dirs.WALSecondary.Dirname, dirs.WALSecondary.FS)
		if err != nil {
			return dirs, err
		}
	}

	// Resolve path names and acquire locks for the WAL recovery directories.
	for _, dir := range opts.WALRecoveryDirs {
		dir.Dirname = resolveStorePath(dirname, dir.Dirname)
		if dir.Dirname != dirname {
			// Acquire a lock on the WAL recovery directory.
			dir.Lock, err = dirs.RecoveryDirLocks.AcquireOrValidate(dir.Lock, dir.Dirname, dir.FS)
			if err != nil {
				return dirs, errors.Wrapf(err, "error acquiring lock on WAL recovery directory %q", dir.Dirname)
			}
		}
		dirs.WALRecovery = append(dirs.WALRecovery, dir)
	}
	return dirs, nil
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
			backingState := f.TableBacking
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
