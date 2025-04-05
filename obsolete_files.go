// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"cmp"
	"context"
	"runtime/pprof"
	"slices"
	"sync"
	"time"

	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/wal"
	"github.com/cockroachdb/tokenbucket"
)

// Cleaner exports the base.Cleaner type.
type Cleaner = base.Cleaner

// DeleteCleaner exports the base.DeleteCleaner type.
type DeleteCleaner = base.DeleteCleaner

// ArchiveCleaner exports the base.ArchiveCleaner type.
type ArchiveCleaner = base.ArchiveCleaner

type cleanupManager struct {
	opts                       *Options
	objProvider                objstorage.Provider
	onTableDeleteFn            func(fileSize uint64, isLocal bool)
	deletePacer                *deletionPacer
	deletedFileSizePendingSync uint64

	// jobsCh is used as the cleanup job queue.
	jobsCh chan *cleanupJob
	// waitGroup is used to wait for the background goroutine to exit.
	waitGroup sync.WaitGroup

	mu struct {
		sync.Mutex
		// totalJobs is the total number of enqueued jobs (completed or in progress).
		totalJobs              int
		completedJobs          int
		completedJobsCond      sync.Cond
		jobsQueueWarningIssued bool
	}
}

// We can queue this many jobs before we have to block EnqueueJob.
const jobsQueueDepth = 1000

const deleteThresholdForSync uint64 = 64 * 1024 * 1024 // 64 MB

// deletableFile is used for non log files.
type deletableFile struct {
	dir      string
	fileNum  base.DiskFileNum
	fileSize uint64
	isLocal  bool
}

// obsoleteFile holds information about a file that needs to be deleted soon.
type obsoleteFile struct {
	fileType base.FileType
	// nonLogFile is populated when fileType != fileTypeLog.
	nonLogFile deletableFile
	// logFile is populated when fileType == fileTypeLog.
	logFile wal.DeletableLog
}

type cleanupJob struct {
	jobID         JobID
	obsoleteFiles []obsoleteFile
}

// openCleanupManager creates a cleanupManager and starts its background goroutine.
// The cleanupManager must be Close()d.
func openCleanupManager(
	opts *Options,
	objProvider objstorage.Provider,
	onTableDeleteFn func(fileSize uint64, isLocal bool),
	getDeletePacerInfo func() deletionPacerInfo,
) *cleanupManager {
	cm := &cleanupManager{
		opts:            opts,
		objProvider:     objProvider,
		onTableDeleteFn: onTableDeleteFn,
		deletePacer: newDeletionPacer(
			crtime.NowMono(),
			opts.FreeSpaceThresholdBytes,
			int64(opts.TargetByteDeletionRate),
			opts.FreeSpaceTimeframe,
			opts.ObsoleteBytesMaxRatio,
			opts.ObsoleteBytesTimeframe,
			getDeletePacerInfo,
		),
		jobsCh: make(chan *cleanupJob, jobsQueueDepth),
	}
	cm.mu.completedJobsCond.L = &cm.mu.Mutex
	cm.waitGroup.Add(1)

	go func() {
		pprof.Do(context.Background(), gcLabels, func(context.Context) {
			cm.mainLoop()
		})
	}()

	return cm
}

// Close stops the background goroutine, waiting until all queued jobs are completed.
// Delete pacing is disabled for the remaining jobs.
func (cm *cleanupManager) Close() {
	close(cm.jobsCh)
	cm.waitGroup.Wait()
}

// EnqueueJob adds a cleanup job to the manager's queue.
func (cm *cleanupManager) EnqueueJob(jobID JobID, obsoleteFiles []obsoleteFile) {
	job := &cleanupJob{
		jobID:         jobID,
		obsoleteFiles: obsoleteFiles,
	}

	// Report deleted bytes to the pacer, which can use this data to potentially
	// increase the deletion rate to keep up. We want to do this at enqueue time
	// rather than when we get to the job, otherwise the reported bytes will be
	// subject to the throttling rate which defeats the purpose.
	var pacingBytes uint64
	for _, of := range obsoleteFiles {
		if cm.needsPacing(of.fileType, of.nonLogFile.fileNum) {
			pacingBytes += of.nonLogFile.fileSize
		}
	}
	if pacingBytes > 0 {
		cm.deletePacer.ReportDeletion(crtime.NowMono(), pacingBytes)
	}

	cm.mu.Lock()
	cm.mu.totalJobs++
	cm.maybeLogLocked()
	cm.mu.Unlock()

	cm.jobsCh <- job
}

// Wait until the completion of all jobs that were already queued.
//
// Does not wait for jobs that are enqueued during the call.
//
// Note that DB.mu should not be held while calling this method; the background
// goroutine needs to acquire DB.mu to update deleted table metrics.
func (cm *cleanupManager) Wait() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	n := cm.mu.totalJobs
	for cm.mu.completedJobs < n {
		cm.mu.completedJobsCond.Wait()
	}
}

// mainLoop runs the manager's background goroutine.
func (cm *cleanupManager) mainLoop() {
	defer cm.waitGroup.Done()

	var tb tokenbucket.TokenBucket
	// Use a token bucket with 1 token / second refill rate and 1 token burst.
	tb.Init(1.0, 1.0)
	for job := range cm.jobsCh {
		for _, of := range job.obsoleteFiles {
			var fileSize uint64
			switch of.fileType {
			case base.FileTypeTable:
				cm.maybePace(&tb, of.fileType, of.nonLogFile.fileNum, of.nonLogFile.fileSize)
				cm.onTableDeleteFn(of.nonLogFile.fileSize, of.nonLogFile.isLocal)
				cm.deleteObsoleteObject(of.fileType, job.jobID, of.nonLogFile.fileNum)
				fileSize = of.nonLogFile.fileSize
			case base.FileTypeBlob:
				cm.maybePace(&tb, of.fileType, of.nonLogFile.fileNum, of.nonLogFile.fileSize)
				cm.deleteObsoleteObject(of.fileType, job.jobID, of.nonLogFile.fileNum)
				fileSize = of.nonLogFile.fileSize
			case base.FileTypeLog:
				cm.deleteObsoleteFile(of.logFile.FS, base.FileTypeLog, job.jobID, of.logFile.Path,
					base.DiskFileNum(of.logFile.NumWAL))
				fileSize = of.logFile.ApproxFileSize
			default:
				path := base.MakeFilepath(cm.opts.FS, of.nonLogFile.dir, of.fileType, of.nonLogFile.fileNum)
				cm.deleteObsoleteFile(
					cm.opts.FS, of.fileType, job.jobID, path, of.nonLogFile.fileNum)
				fileSize = of.nonLogFile.fileSize
			}
			cm.syncIfAboveThreshold(fileSize)
		}
		cm.mu.Lock()
		cm.mu.completedJobs++
		cm.mu.completedJobsCond.Broadcast()
		cm.maybeLogLocked()
		cm.mu.Unlock()
	}
}

// fileNumIfSST is read iff fileType is fileTypeTable.
func (cm *cleanupManager) needsPacing(fileType base.FileType, fileNumIfSST base.DiskFileNum) bool {
	if fileType != base.FileTypeTable && fileType != base.FileTypeBlob {
		return false
	}
	meta, err := cm.objProvider.Lookup(fileType, fileNumIfSST)
	if err != nil {
		// The object was already removed from the provider; we won't actually
		// delete anything, so we don't need to pace.
		return false
	}
	// Don't throttle deletion of remote objects.
	return !meta.IsRemote()
}

// maybePace sleeps before deleting an object if appropriate. It is always
// called from the background goroutine.
func (cm *cleanupManager) maybePace(
	tb *tokenbucket.TokenBucket, fileType base.FileType, fileNum base.DiskFileNum, fileSize uint64,
) {
	if !cm.needsPacing(fileType, fileNum) {
		return
	}

	tokens := cm.deletePacer.PacingDelay(crtime.NowMono(), fileSize)
	if tokens == 0.0 {
		// The token bucket might be in debt; it could make us wait even for 0
		// tokens. We don't want that if the pacer decided throttling should be
		// disabled.
		return
	}
	// Wait for tokens. We use a token bucket instead of sleeping outright because
	// the token bucket accumulates up to one second of unused tokens.
	for {
		ok, d := tb.TryToFulfill(tokenbucket.Tokens(tokens))
		if ok {
			break
		}
		time.Sleep(d)
	}
}

func (cm *cleanupManager) syncIfAboveThreshold(fileSize uint64) {
	cm.deletedFileSizePendingSync += fileSize
	if cm.deletedFileSizePendingSync < deleteThresholdForSync {
		return
	}

	if err := cm.objProvider.Sync(); err != nil {
		cm.opts.Logger.Errorf("objProvider Sync failed: %v", err)
	}
	cm.deletedFileSizePendingSync = 0
}

// deleteObsoleteFile deletes a (non-object) file that is no longer needed.
func (cm *cleanupManager) deleteObsoleteFile(
	fs vfs.FS, fileType base.FileType, jobID JobID, path string, fileNum base.DiskFileNum,
) {
	// TODO(peter): need to handle this error, probably by re-adding the
	// file that couldn't be deleted to one of the obsolete slices map.
	err := cm.opts.Cleaner.Clean(fs, fileType, path)
	if oserror.IsNotExist(err) {
		return
	}

	switch fileType {
	case base.FileTypeLog:
		cm.opts.EventListener.WALDeleted(WALDeleteInfo{
			JobID:   int(jobID),
			Path:    path,
			FileNum: fileNum,
			Err:     err,
		})
	case base.FileTypeManifest:
		cm.opts.EventListener.ManifestDeleted(ManifestDeleteInfo{
			JobID:   int(jobID),
			Path:    path,
			FileNum: fileNum,
			Err:     err,
		})
	case base.FileTypeTable, base.FileTypeBlob:
		panic("invalid deletion of object file")
	}
}

func (cm *cleanupManager) deleteObsoleteObject(
	fileType base.FileType, jobID JobID, fileNum base.DiskFileNum,
) {
	if fileType != base.FileTypeTable && fileType != base.FileTypeBlob {
		panic("not an object")
	}

	var path string
	meta, err := cm.objProvider.Lookup(fileType, fileNum)
	if err != nil {
		path = "<nil>"
	} else {
		path = cm.objProvider.Path(meta)
		err = cm.objProvider.Remove(fileType, fileNum)
	}
	if cm.objProvider.IsNotExistError(err) {
		return
	}

	switch fileType {
	case base.FileTypeTable:
		cm.opts.EventListener.TableDeleted(TableDeleteInfo{
			JobID:   int(jobID),
			Path:    path,
			FileNum: fileNum,
			Err:     err,
		})
		// TODO(jackson): Add BlobFileDeleted event.
	}
}

// maybeLogLocked issues a log if the job queue gets 75% full and issues a log
// when the job queue gets back to less than 10% full.
//
// Must be called with cm.mu locked.
func (cm *cleanupManager) maybeLogLocked() {
	const highThreshold = jobsQueueDepth * 3 / 4
	const lowThreshold = jobsQueueDepth / 10

	jobsInQueue := cm.mu.totalJobs - cm.mu.completedJobs

	if !cm.mu.jobsQueueWarningIssued && jobsInQueue > highThreshold {
		cm.mu.jobsQueueWarningIssued = true
		cm.opts.Logger.Infof("cleanup falling behind; job queue has over %d jobs", highThreshold)
	}

	if cm.mu.jobsQueueWarningIssued && jobsInQueue < lowThreshold {
		cm.mu.jobsQueueWarningIssued = false
		cm.opts.Logger.Infof("cleanup back to normal; job queue has under %d jobs", lowThreshold)
	}
}

func (d *DB) getDeletionPacerInfo() deletionPacerInfo {
	var pacerInfo deletionPacerInfo
	// Call GetDiskUsage after every file deletion. This may seem inefficient,
	// but in practice this was observed to take constant time, regardless of
	// volume size used, at least on linux with ext4 and zfs. All invocations
	// take 10 microseconds or less.
	pacerInfo.freeBytes = d.calculateDiskAvailableBytes()
	d.mu.Lock()
	pacerInfo.obsoleteBytes = d.mu.versions.metrics.Table.ObsoleteSize
	pacerInfo.liveBytes = uint64(d.mu.versions.metrics.Total().Size)
	d.mu.Unlock()
	return pacerInfo
}

// onObsoleteTableDelete is called to update metrics when an sstable is deleted.
func (d *DB) onObsoleteTableDelete(fileSize uint64, isLocal bool) {
	d.mu.Lock()
	d.mu.versions.metrics.Table.ObsoleteCount--
	d.mu.versions.metrics.Table.ObsoleteSize -= fileSize
	if isLocal {
		d.mu.versions.metrics.Table.Local.ObsoleteSize -= fileSize
	}
	d.mu.Unlock()
}

// scanObsoleteFiles scans the filesystem for files that are no longer needed
// and adds those to the internal lists of obsolete files. Note that the files
// are not actually deleted by this method. A subsequent call to
// deleteObsoleteFiles must be performed. Must be not be called concurrently
// with compactions and flushes. db.mu must be held when calling this function.
func (d *DB) scanObsoleteFiles(list []string, flushableIngests []*ingestedFlushable) {
	// Disable automatic compactions temporarily to avoid concurrent compactions /
	// flushes from interfering. The original value is restored on completion.
	disabledPrev := d.opts.DisableAutomaticCompactions
	defer func() {
		d.opts.DisableAutomaticCompactions = disabledPrev
	}()
	d.opts.DisableAutomaticCompactions = true

	// Wait for any ongoing compaction to complete before continuing.
	for d.mu.compact.compactingCount > 0 || d.mu.compact.downloadingCount > 0 || d.mu.compact.flushing {
		d.mu.compact.cond.Wait()
	}

	liveFileNums := make(map[base.DiskFileNum]struct{})
	d.mu.versions.addLiveFileNums(liveFileNums)
	// Protect against files which are only referred to by the ingestedFlushable
	// from being deleted. These are added to the flushable queue on WAL replay
	// and handle their own obsoletion/deletion. We exclude them from this obsolete
	// file scan to avoid double-deleting these files.
	for _, f := range flushableIngests {
		for _, file := range f.files {
			liveFileNums[file.FileBacking.DiskFileNum] = struct{}{}
		}
	}

	manifestFileNum := d.mu.versions.manifestFileNum

	var obsoleteTables []objectInfo
	var obsoleteBlobs []objectInfo
	var obsoleteManifests []fileInfo
	var obsoleteOptions []fileInfo

	for _, filename := range list {
		fileType, diskFileNum, ok := base.ParseFilename(d.opts.FS, filename)
		if !ok {
			continue
		}
		switch fileType {
		case base.FileTypeManifest:
			if diskFileNum >= manifestFileNum {
				continue
			}
			fi := fileInfo{FileNum: diskFileNum}
			if stat, err := d.opts.FS.Stat(filename); err == nil {
				fi.FileSize = uint64(stat.Size())
			}
			obsoleteManifests = append(obsoleteManifests, fi)
		case base.FileTypeOptions:
			if diskFileNum >= d.optionsFileNum {
				continue
			}
			fi := fileInfo{FileNum: diskFileNum}
			if stat, err := d.opts.FS.Stat(filename); err == nil {
				fi.FileSize = uint64(stat.Size())
			}
			obsoleteOptions = append(obsoleteOptions, fi)
		case base.FileTypeTable, base.FileTypeBlob:
			// Objects are handled through the objstorage provider below.
		default:
			// Don't delete files we don't know about.
		}
	}

	objects := d.objProvider.List()
	for _, obj := range objects {
		if _, ok := liveFileNums[obj.DiskFileNum]; ok {
			continue
		}
		makeObjectInfo := func() objectInfo {
			fileInfo := fileInfo{FileNum: obj.DiskFileNum}
			if size, err := d.objProvider.Size(obj); err == nil {
				fileInfo.FileSize = uint64(size)
			}
			return objectInfo{
				fileInfo: fileInfo,
				isLocal:  !obj.IsRemote(),
			}
		}

		switch obj.FileType {
		case base.FileTypeTable:
			obsoleteTables = append(obsoleteTables, makeObjectInfo())
		case base.FileTypeBlob:
			obsoleteBlobs = append(obsoleteBlobs, makeObjectInfo())
		default:
			// Ignore object types we don't know about.
		}
	}

	d.mu.versions.obsoleteTables = mergeObjectInfos(d.mu.versions.obsoleteTables, obsoleteTables)
	d.mu.versions.obsoleteBlobs = mergeObjectInfos(d.mu.versions.obsoleteBlobs, obsoleteBlobs)
	d.mu.versions.updateObsoleteTableMetricsLocked()
	d.mu.versions.obsoleteManifests = merge(d.mu.versions.obsoleteManifests, obsoleteManifests)
	d.mu.versions.obsoleteOptions = merge(d.mu.versions.obsoleteOptions, obsoleteOptions)
}

// disableFileDeletions disables file deletions and then waits for any
// in-progress deletion to finish. The caller is required to call
// enableFileDeletions in order to enable file deletions again. It is ok for
// multiple callers to disable file deletions simultaneously, though they must
// all invoke enableFileDeletions in order for file deletions to be re-enabled
// (there is an internal reference count on file deletion disablement).
//
// d.mu must be held when calling this method.
func (d *DB) disableFileDeletions() {
	d.mu.disableFileDeletions++
	d.mu.Unlock()
	defer d.mu.Lock()
	d.cleanupManager.Wait()
}

// enableFileDeletions enables previously disabled file deletions. A cleanup job
// is queued if necessary.
//
// d.mu must be held when calling this method.
func (d *DB) enableFileDeletions() {
	if d.mu.disableFileDeletions <= 0 {
		panic("pebble: file deletion disablement invariant violated")
	}
	d.mu.disableFileDeletions--
	if d.mu.disableFileDeletions > 0 {
		return
	}
	d.deleteObsoleteFiles(d.newJobIDLocked())
}

type fileInfo = base.FileInfo

// deleteObsoleteFiles enqueues a cleanup job to the cleanup manager, if necessary.
//
// d.mu must be held when calling this. The function will release and re-aquire the mutex.
//
// Does nothing if file deletions are disabled (see disableFileDeletions). A
// cleanup job will be scheduled when file deletions are re-enabled.
func (d *DB) deleteObsoleteFiles(jobID JobID) {
	if d.mu.disableFileDeletions > 0 {
		return
	}
	_, noRecycle := d.opts.Cleaner.(base.NeedsFileContents)

	// NB: d.mu.versions.minUnflushedLogNum is the log number of the earliest
	// log that has not had its contents flushed to an sstable.
	obsoleteLogs, err := d.mu.log.manager.Obsolete(wal.NumWAL(d.mu.versions.minUnflushedLogNum), noRecycle)
	if err != nil {
		panic(err)
	}

	obsoleteTables := slices.Clone(d.mu.versions.obsoleteTables)
	d.mu.versions.obsoleteTables = d.mu.versions.obsoleteTables[:0]
	obsoleteBlobs := slices.Clone(d.mu.versions.obsoleteBlobs)
	d.mu.versions.obsoleteBlobs = d.mu.versions.obsoleteBlobs[:0]

	for _, tbl := range obsoleteTables {
		delete(d.mu.versions.zombieTables, tbl.FileNum)
	}

	// Sort the manifests cause we want to delete some contiguous prefix
	// of the older manifests.
	slices.SortFunc(d.mu.versions.obsoleteManifests, func(a, b fileInfo) int {
		return cmp.Compare(a.FileNum, b.FileNum)
	})

	var obsoleteManifests []fileInfo
	manifestsToDelete := len(d.mu.versions.obsoleteManifests) - d.opts.NumPrevManifest
	if manifestsToDelete > 0 {
		obsoleteManifests = d.mu.versions.obsoleteManifests[:manifestsToDelete]
		d.mu.versions.obsoleteManifests = d.mu.versions.obsoleteManifests[manifestsToDelete:]
		if len(d.mu.versions.obsoleteManifests) == 0 {
			d.mu.versions.obsoleteManifests = nil
		}
	}

	obsoleteOptions := d.mu.versions.obsoleteOptions
	d.mu.versions.obsoleteOptions = nil

	// Release d.mu while preparing the cleanup job and possibly waiting.
	// Note the unusual order: Unlock and then Lock.
	d.mu.Unlock()
	defer d.mu.Lock()

	filesToDelete := make([]obsoleteFile, 0, len(obsoleteLogs)+len(obsoleteTables)+len(obsoleteManifests)+len(obsoleteOptions))
	for _, f := range obsoleteLogs {
		filesToDelete = append(filesToDelete, obsoleteFile{fileType: base.FileTypeLog, logFile: f})
	}
	// We sort to make the order of deletions deterministic, which is nice for
	// tests.
	slices.SortFunc(obsoleteTables, func(a, b objectInfo) int {
		return cmp.Compare(a.FileNum, b.FileNum)
	})
	slices.SortFunc(obsoleteBlobs, func(a, b objectInfo) int {
		return cmp.Compare(a.FileNum, b.FileNum)
	})
	for _, f := range obsoleteTables {
		d.fileCache.Evict(f.FileNum, base.FileTypeTable)
		filesToDelete = append(filesToDelete, obsoleteFile{
			fileType: base.FileTypeTable,
			nonLogFile: deletableFile{
				dir:      d.dirname,
				fileNum:  f.FileNum,
				fileSize: f.FileSize,
				isLocal:  f.isLocal,
			},
		})
	}
	for _, f := range obsoleteBlobs {
		d.fileCache.Evict(f.FileNum, base.FileTypeBlob)
		filesToDelete = append(filesToDelete, obsoleteFile{
			fileType: base.FileTypeBlob,
			nonLogFile: deletableFile{
				dir:      d.dirname,
				fileNum:  f.FileNum,
				fileSize: f.FileSize,
				isLocal:  f.isLocal,
			},
		})
	}

	files := [2]struct {
		fileType base.FileType
		obsolete []fileInfo
	}{
		{base.FileTypeManifest, obsoleteManifests},
		{base.FileTypeOptions, obsoleteOptions},
	}
	for _, f := range files {
		// We sort to make the order of deletions deterministic, which is nice for
		// tests.
		slices.SortFunc(f.obsolete, func(a, b fileInfo) int {
			return cmp.Compare(a.FileNum, b.FileNum)
		})
		for _, fi := range f.obsolete {
			dir := d.dirname
			filesToDelete = append(filesToDelete, obsoleteFile{
				fileType: f.fileType,
				nonLogFile: deletableFile{
					dir:      dir,
					fileNum:  fi.FileNum,
					fileSize: fi.FileSize,
					isLocal:  true,
				},
			})
		}
	}
	if len(filesToDelete) > 0 {
		d.cleanupManager.EnqueueJob(jobID, filesToDelete)
	}
	if d.opts.private.testingAlwaysWaitForCleanup {
		d.cleanupManager.Wait()
	}
}

func (d *DB) maybeScheduleObsoleteTableDeletion() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.maybeScheduleObsoleteTableDeletionLocked()
}

func (d *DB) maybeScheduleObsoleteTableDeletionLocked() {
	if len(d.mu.versions.obsoleteTables) > 0 {
		d.deleteObsoleteFiles(d.newJobIDLocked())
	}
}

func merge(a, b []fileInfo) []fileInfo {
	if len(b) == 0 {
		return a
	}

	a = append(a, b...)
	slices.SortFunc(a, func(a, b fileInfo) int {
		return cmp.Compare(a.FileNum, b.FileNum)
	})
	return slices.CompactFunc(a, func(a, b fileInfo) bool {
		return a.FileNum == b.FileNum
	})
}

func mergeObjectInfos(a, b []objectInfo) []objectInfo {
	if len(b) == 0 {
		return a
	}

	a = append(a, b...)
	slices.SortFunc(a, func(a, b objectInfo) int {
		return cmp.Compare(a.FileNum, b.FileNum)
	})
	return slices.CompactFunc(a, func(a, b objectInfo) bool {
		return a.FileNum == b.FileNum
	})
}
