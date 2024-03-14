// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"runtime/pprof"
	"sync"
	"time"

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
	opts            *Options
	objProvider     objstorage.Provider
	onTableDeleteFn func(fileSize uint64, isLocal bool)
	deletePacer     *deletionPacer

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

// deletableFile is used for non log files.
type deletableFile struct {
	dir      string
	fileNum  base.DiskFileNum
	fileSize uint64
	isLocal  bool
}

// obsoleteFile holds information about a file that needs to be deleted soon.
type obsoleteFile struct {
	fileType fileType
	// nonLogFile is populated when fileType != fileTypeLog.
	nonLogFile deletableFile
	// logFile is populated when fileType == fileTypeLog.
	logFile wal.DeletableLog
}

type cleanupJob struct {
	jobID         int
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
		deletePacer:     newDeletionPacer(time.Now(), int64(opts.TargetByteDeletionRate), getDeletePacerInfo),
		jobsCh:          make(chan *cleanupJob, jobsQueueDepth),
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
func (cm *cleanupManager) EnqueueJob(jobID int, obsoleteFiles []obsoleteFile) {
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
		cm.deletePacer.ReportDeletion(time.Now(), pacingBytes)
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
			case fileTypeTable:
				cm.maybePace(&tb, of.fileType, of.nonLogFile.fileNum, of.nonLogFile.fileSize)
				cm.onTableDeleteFn(of.nonLogFile.fileSize, of.nonLogFile.isLocal)
				cm.deleteObsoleteObject(fileTypeTable, job.jobID, of.nonLogFile.fileNum)
			case fileTypeLog:
				cm.deleteObsoleteFile(of.logFile.FS, fileTypeLog, job.jobID, of.logFile.Path,
					base.DiskFileNum(of.logFile.NumWAL), of.logFile.ApproxFileSize)
			default:
				path := base.MakeFilepath(cm.opts.FS, of.nonLogFile.dir, of.fileType, of.nonLogFile.fileNum)
				cm.deleteObsoleteFile(
					cm.opts.FS, of.fileType, job.jobID, path, of.nonLogFile.fileNum, of.nonLogFile.fileSize)
			}
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
	if fileType != fileTypeTable {
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

	tokens := cm.deletePacer.PacingDelay(time.Now(), fileSize)
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
	fs vfs.FS, fileType fileType, jobID int, path string, fileNum base.DiskFileNum, fileSize uint64,
) {
	// TODO(peter): need to handle this error, probably by re-adding the
	// file that couldn't be deleted to one of the obsolete slices map.
	err := cm.opts.Cleaner.Clean(fs, fileType, path)
	if oserror.IsNotExist(err) {
		return
	}

	switch fileType {
	case fileTypeLog:
		cm.opts.EventListener.WALDeleted(WALDeleteInfo{
			JobID:   jobID,
			Path:    path,
			FileNum: fileNum,
			Err:     err,
		})
	case fileTypeManifest:
		cm.opts.EventListener.ManifestDeleted(ManifestDeleteInfo{
			JobID:   jobID,
			Path:    path,
			FileNum: fileNum,
			Err:     err,
		})
	case fileTypeTable:
		panic("invalid deletion of object file")
	}
}

func (cm *cleanupManager) deleteObsoleteObject(
	fileType fileType, jobID int, fileNum base.DiskFileNum,
) {
	if fileType != fileTypeTable {
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
	case fileTypeTable:
		cm.opts.EventListener.TableDeleted(TableDeleteInfo{
			JobID:   jobID,
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
