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
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/v2/internal/base"
	"github.com/cockroachdb/pebble/v2/internal/invariants"
	"github.com/cockroachdb/pebble/v2/objstorage"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/cockroachdb/pebble/v2/wal"
	"github.com/cockroachdb/tokenbucket"
)

// Cleaner exports the base.Cleaner type.
type Cleaner = base.Cleaner

// DeleteCleaner exports the base.DeleteCleaner type.
type DeleteCleaner = base.DeleteCleaner

// ArchiveCleaner exports the base.ArchiveCleaner type.
type ArchiveCleaner = base.ArchiveCleaner

type cleanupManager struct {
	opts        *Options
	objProvider objstorage.Provider
	deletePacer *deletionPacer

	// jobsCh is used as the cleanup job queue.
	jobsCh chan *cleanupJob
	// waitGroup is used to wait for the background goroutine to exit.
	waitGroup sync.WaitGroup

	mu struct {
		sync.Mutex
		// totalJobs is the total number of enqueued jobs (completed or in progress).
		totalJobs              int
		completedStats         obsoleteObjectStats
		completedJobs          int
		completedJobsCond      sync.Cond
		jobsQueueWarningIssued bool
	}
}

// CompletedStats returns the stats summarizing objects deleted. The returned
// stats increase monotonically over the lifetime of the DB.
func (m *cleanupManager) CompletedStats() obsoleteObjectStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mu.completedStats
}

// We can queue this many jobs before we have to block EnqueueJob.
const jobsQueueDepth = 1000

// obsoleteFile holds information about a file that needs to be deleted soon.
type obsoleteFile struct {
	fileType base.FileType
	fs       vfs.FS
	path     string
	fileNum  base.DiskFileNum
	fileSize uint64 // approx for log files
	isLocal  bool
}

func (of *obsoleteFile) needsPacing() bool {
	// We only need to pace local objects--sstables and blob files.
	return of.isLocal && (of.fileType == base.FileTypeTable || of.fileType == base.FileTypeBlob)
}

type cleanupJob struct {
	jobID         JobID
	obsoleteFiles []obsoleteFile
	stats         obsoleteObjectStats
}

// openCleanupManager creates a cleanupManager and starts its background goroutine.
// The cleanupManager must be Close()d.
func openCleanupManager(
	opts *Options, objProvider objstorage.Provider, getDeletePacerInfo func() deletionPacerInfo,
) *cleanupManager {
	cm := &cleanupManager{
		opts:        opts,
		objProvider: objProvider,
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
func (cm *cleanupManager) EnqueueJob(
	jobID JobID, obsoleteFiles []obsoleteFile, stats obsoleteObjectStats,
) {
	job := &cleanupJob{
		jobID:         jobID,
		obsoleteFiles: obsoleteFiles,
		stats:         stats,
	}

	// Report deleted bytes to the pacer, which can use this data to potentially
	// increase the deletion rate to keep up. We want to do this at enqueue time
	// rather than when we get to the job, otherwise the reported bytes will be
	// subject to the throttling rate which defeats the purpose.
	var pacingBytes uint64
	for _, of := range obsoleteFiles {
		if of.needsPacing() {
			pacingBytes += of.fileSize
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
			switch of.fileType {
			case base.FileTypeTable:
				cm.maybePace(&tb, &of)
				cm.deleteObsoleteObject(of.fileType, job.jobID, of.fileNum)
			case base.FileTypeBlob:
				cm.maybePace(&tb, &of)
				cm.deleteObsoleteObject(of.fileType, job.jobID, of.fileNum)
			default:
				cm.deleteObsoleteFile(of.fs, of.fileType, job.jobID, of.path, of.fileNum)
			}
		}
		cm.mu.Lock()
		cm.mu.completedJobs++
		cm.mu.completedStats.Add(job.stats)
		cm.mu.completedJobsCond.Broadcast()
		cm.maybeLogLocked()
		cm.mu.Unlock()
	}
}

// maybePace sleeps before deleting an object if appropriate. It is always
// called from the background goroutine.
func (cm *cleanupManager) maybePace(tb *tokenbucket.TokenBucket, of *obsoleteFile) {
	if !of.needsPacing() {
		return
	}

	tokens := cm.deletePacer.PacingDelay(crtime.NowMono(), of.fileSize)
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
	case base.FileTypeBlob:
		cm.opts.EventListener.BlobFileDeleted(BlobFileDeleteInfo{
			JobID:   int(jobID),
			Path:    path,
			FileNum: fileNum,
			Err:     err,
		})
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
	total := d.mu.versions.metrics.Total()
	d.mu.Unlock()
	pacerInfo.liveBytes = uint64(total.AggregateSize())
	return pacerInfo
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
			liveFileNums[file.TableBacking.DiskFileNum] = struct{}{}
		}
	}

	manifestFileNum := d.mu.versions.manifestFileNum

	var obsoleteTables []obsoleteFile
	var obsoleteBlobs []obsoleteFile
	var obsoleteOptions []obsoleteFile
	var obsoleteManifests []obsoleteFile

	for _, filename := range list {
		fileType, diskFileNum, ok := base.ParseFilename(d.opts.FS, filename)
		if !ok {
			continue
		}
		makeObsoleteFile := func() obsoleteFile {
			of := obsoleteFile{
				fileType: fileType,
				fs:       d.opts.FS,
				path:     d.opts.FS.PathJoin(d.dirname, filename),
				fileNum:  diskFileNum,
				isLocal:  true,
			}
			if stat, err := d.opts.FS.Stat(filename); err == nil {
				of.fileSize = uint64(stat.Size())
			}
			return of
		}
		switch fileType {
		case base.FileTypeManifest:
			if diskFileNum >= manifestFileNum {
				continue
			}
			obsoleteManifests = append(obsoleteManifests, makeObsoleteFile())
		case base.FileTypeOptions:
			if diskFileNum >= d.optionsFileNum {
				continue
			}
			obsoleteOptions = append(obsoleteOptions, makeObsoleteFile())
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
		if obj.FileType != base.FileTypeTable && obj.FileType != base.FileTypeBlob {
			// Ignore object types we don't know about.
			continue
		}
		of := obsoleteFile{
			fileType: obj.FileType,
			fs:       d.opts.FS,
			path:     base.MakeFilepath(d.opts.FS, d.dirname, obj.FileType, obj.DiskFileNum),
			fileNum:  obj.DiskFileNum,
			isLocal:  true,
		}
		if size, err := d.objProvider.Size(obj); err == nil {
			of.fileSize = uint64(size)
		}
		if obj.FileType == base.FileTypeTable {
			obsoleteTables = append(obsoleteTables, of)
		} else {
			obsoleteBlobs = append(obsoleteBlobs, of)
		}
	}

	d.mu.versions.obsoleteTables = mergeObsoleteFiles(d.mu.versions.obsoleteTables, obsoleteTables)
	d.mu.versions.obsoleteBlobs = mergeObsoleteFiles(d.mu.versions.obsoleteBlobs, obsoleteBlobs)
	d.mu.versions.obsoleteManifests = mergeObsoleteFiles(d.mu.versions.obsoleteManifests, obsoleteManifests)
	d.mu.versions.obsoleteOptions = mergeObsoleteFiles(d.mu.versions.obsoleteOptions, obsoleteOptions)
	d.mu.versions.updateObsoleteObjectMetricsLocked()
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
	d.mu.fileDeletions.disableCount++
	d.mu.Unlock()
	defer d.mu.Lock()
	d.cleanupManager.Wait()
}

// enableFileDeletions enables previously disabled file deletions. A cleanup job
// is queued if necessary.
//
// d.mu must be held when calling this method.
func (d *DB) enableFileDeletions() {
	if d.mu.fileDeletions.disableCount <= 0 {
		panic("pebble: file deletion disablement invariant violated")
	}
	d.mu.fileDeletions.disableCount--
	if d.mu.fileDeletions.disableCount > 0 {
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
	if d.mu.fileDeletions.disableCount > 0 {
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

	// Ensure everything is already sorted. We want determinism for testing, and
	// we need the manifests to be sorted because we want to delete some
	// contiguous prefix of the older manifests.
	if invariants.Enabled {
		switch {
		case !slices.IsSortedFunc(d.mu.versions.obsoleteManifests, cmpObsoleteFileNumbers):
			d.opts.Logger.Fatalf("obsoleteManifests is not sorted")
		case !slices.IsSortedFunc(d.mu.versions.obsoleteOptions, cmpObsoleteFileNumbers):
			d.opts.Logger.Fatalf("obsoleteOptions is not sorted")
		case !slices.IsSortedFunc(obsoleteTables, cmpObsoleteFileNumbers):
			d.opts.Logger.Fatalf("obsoleteTables is not sorted")
		case !slices.IsSortedFunc(obsoleteBlobs, cmpObsoleteFileNumbers):
			d.opts.Logger.Fatalf("obsoleteBlobs is not sorted")
		}
	}

	var obsoleteManifests []obsoleteFile
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

	// Compute the stats for the files being queued for deletion and add them to
	// the running total. These stats will be used during DB.Metrics() to
	// calculate the count and size of pending obsolete files by diffing these
	// stats and the stats reported by the cleanup manager.
	var objectStats obsoleteObjectStats
	objectStats.tablesAll, objectStats.tablesLocal = calculateObsoleteObjectStats(obsoleteTables)
	objectStats.blobFilesAll, objectStats.blobFilesLocal = calculateObsoleteObjectStats(obsoleteBlobs)
	d.mu.fileDeletions.queuedStats.Add(objectStats)
	d.mu.versions.updateObsoleteObjectMetricsLocked()

	// Release d.mu while preparing the cleanup job and possibly waiting.
	// Note the unusual order: Unlock and then Lock.
	d.mu.Unlock()
	defer d.mu.Lock()

	n := len(obsoleteLogs) + len(obsoleteTables) + len(obsoleteBlobs) + len(obsoleteManifests) + len(obsoleteOptions)
	filesToDelete := make([]obsoleteFile, 0, n)
	filesToDelete = append(filesToDelete, obsoleteManifests...)
	filesToDelete = append(filesToDelete, obsoleteOptions...)
	filesToDelete = append(filesToDelete, obsoleteTables...)
	filesToDelete = append(filesToDelete, obsoleteBlobs...)
	for _, f := range obsoleteLogs {
		filesToDelete = append(filesToDelete, obsoleteFile{
			fileType: base.FileTypeLog,
			fs:       f.FS,
			path:     f.Path,
			fileNum:  base.DiskFileNum(f.NumWAL),
			fileSize: f.ApproxFileSize,
			isLocal:  true,
		})
	}
	for _, f := range obsoleteTables {
		d.fileCache.Evict(f.fileNum, base.FileTypeTable)
	}
	for _, f := range obsoleteBlobs {
		d.fileCache.Evict(f.fileNum, base.FileTypeBlob)
	}
	if len(filesToDelete) > 0 {
		d.cleanupManager.EnqueueJob(jobID, filesToDelete, objectStats)
	}
	if d.opts.private.testingAlwaysWaitForCleanup {
		d.cleanupManager.Wait()
	}
}

func (d *DB) maybeScheduleObsoleteObjectDeletion() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.mu.versions.obsoleteTables) > 0 || len(d.mu.versions.obsoleteBlobs) > 0 {
		d.deleteObsoleteFiles(d.newJobIDLocked())
	}
}

func mergeObsoleteFiles(a, b []obsoleteFile) []obsoleteFile {
	if len(b) == 0 {
		return a
	}

	a = append(a, b...)
	slices.SortFunc(a, cmpObsoleteFileNumbers)
	return slices.CompactFunc(a, func(a, b obsoleteFile) bool {
		return a.fileNum == b.fileNum
	})
}

func cmpObsoleteFileNumbers(a, b obsoleteFile) int {
	return cmp.Compare(a.fileNum, b.fileNum)
}

// objectInfo describes an object in object storage (either a sstable or a blob
// file).
type objectInfo struct {
	fileInfo
	isLocal bool
}

func (o objectInfo) asObsoleteFile(fs vfs.FS, fileType base.FileType, dirname string) obsoleteFile {
	return obsoleteFile{
		fileType: fileType,
		fs:       fs,
		path:     base.MakeFilepath(fs, dirname, fileType, o.FileNum),
		fileNum:  o.FileNum,
		fileSize: o.FileSize,
		isLocal:  o.isLocal,
	}
}

func calculateObsoleteObjectStats(files []obsoleteFile) (total, local countAndSize) {
	for _, of := range files {
		if of.isLocal {
			local.count++
			local.size += of.fileSize
		}
		total.count++
		total.size += of.fileSize
	}
	return total, local
}

type obsoleteObjectStats struct {
	tablesLocal    countAndSize
	tablesAll      countAndSize
	blobFilesLocal countAndSize
	blobFilesAll   countAndSize
}

func (s *obsoleteObjectStats) Add(other obsoleteObjectStats) {
	s.tablesLocal.Add(other.tablesLocal)
	s.tablesAll.Add(other.tablesAll)
	s.blobFilesLocal.Add(other.blobFilesLocal)
	s.blobFilesAll.Add(other.blobFilesAll)
}

func (s *obsoleteObjectStats) Sub(other obsoleteObjectStats) {
	s.tablesLocal.Sub(other.tablesLocal)
	s.tablesAll.Sub(other.tablesAll)
	s.blobFilesLocal.Sub(other.blobFilesLocal)
	s.blobFilesAll.Sub(other.blobFilesAll)
}

type countAndSize struct {
	count uint64
	size  uint64
}

func (c *countAndSize) Add(other countAndSize) {
	c.count += other.count
	c.size += other.size
}

func (c *countAndSize) Sub(other countAndSize) {
	c.count = invariants.SafeSub(c.count, other.count)
	c.size = invariants.SafeSub(c.size, other.size)
}

func makeZombieObjects() zombieObjects {
	return zombieObjects{
		objs: make(map[base.DiskFileNum]objectInfo),
	}
}

// zombieObjects tracks a set of objects that are no longer required by the most
// recent version of the LSM, but may still need to be accessed by an open
// iterator. Such objects are 'dead,' but cannot be deleted until iterators that
// may access them are closed.
type zombieObjects struct {
	objs       map[base.DiskFileNum]objectInfo
	totalSize  uint64
	localSize  uint64
	localCount uint64
}

// Add adds an object to the set of zombie objects.
func (z *zombieObjects) Add(obj objectInfo) {
	if _, ok := z.objs[obj.FileNum]; ok {
		panic(errors.AssertionFailedf("zombie object %s already exists", obj.FileNum))
	}
	z.objs[obj.FileNum] = obj
	z.totalSize += obj.FileSize
	if obj.isLocal {
		z.localSize += obj.FileSize
		z.localCount++
	}
}

// AddMetadata is like Add, but takes an ObjectMetadata and the object's size.
func (z *zombieObjects) AddMetadata(meta *objstorage.ObjectMetadata, size uint64) {
	z.Add(objectInfo{
		fileInfo: fileInfo{
			FileNum:  meta.DiskFileNum,
			FileSize: size,
		},
		isLocal: !meta.IsRemote(),
	})
}

// Count returns the number of zombie objects.
func (z *zombieObjects) Count() int {
	return len(z.objs)
}

// Extract removes an object from the set of zombie objects, returning the
// object that was removed.
func (z *zombieObjects) Extract(fileNum base.DiskFileNum) objectInfo {
	obj, ok := z.objs[fileNum]
	if !ok {
		panic(errors.AssertionFailedf("zombie object %s not found", fileNum))
	}
	delete(z.objs, fileNum)

	// Detect underflow in case we have a bug that causes an object's size to be
	// mutated.
	if z.totalSize < obj.FileSize {
		panic(errors.AssertionFailedf("zombie object %s size %d is greater than total size %d", fileNum, obj.FileSize, z.totalSize))
	}
	if obj.isLocal && z.localSize < obj.FileSize {
		panic(errors.AssertionFailedf("zombie object %s size %d is greater than local size %d", fileNum, obj.FileSize, z.localSize))
	}

	z.totalSize -= obj.FileSize
	if obj.isLocal {
		z.localSize -= obj.FileSize
		z.localCount--
	}
	return obj
}

// TotalSize returns the size of all objects in the set.
func (z *zombieObjects) TotalSize() uint64 {
	return z.totalSize
}

// LocalStats returns the count and size of all local objects in the set.
func (z *zombieObjects) LocalStats() (count uint64, size uint64) {
	return z.localCount, z.localSize
}
