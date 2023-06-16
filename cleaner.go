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
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/objstorage"
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
	onTableDeleteFn func(fileSize uint64)
	deletePacer     *deletionPacer

	// jobsCh is used as the cleanup job queue.
	jobsCh chan *cleanupJob
	// waitGroup is used to wait for the background goroutine to exit.
	waitGroup sync.WaitGroup

	mu struct {
		sync.Mutex
		queuedJobs        int
		completedJobs     int
		completedJobsCond sync.Cond
	}
}

// In practice, we should rarely have more than a couple of jobs (in most cases
// we Wait() after queueing a job).
const jobsChLen = 10000

// obsoleteFile holds information about a file that needs to be deleted soon.
type obsoleteFile struct {
	dir      string
	fileNum  base.DiskFileNum
	fileType fileType
	fileSize uint64
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
	onTableDeleteFn func(fileSize uint64),
	getDeletePacerInfo func() deletionPacerInfo,
) *cleanupManager {
	cm := &cleanupManager{
		opts:            opts,
		objProvider:     objProvider,
		onTableDeleteFn: onTableDeleteFn,
		deletePacer:     newDeletionPacer(getDeletePacerInfo),
		jobsCh:          make(chan *cleanupJob, jobsChLen),
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
//
// Note that logs in obsoleteFiles might be recycled instead of deleted.
//
// Returns a channel that is closed when the job completes.
func (cm *cleanupManager) EnqueueJob(jobID int, obsoleteFiles []obsoleteFile) {
	job := &cleanupJob{
		jobID:         jobID,
		obsoleteFiles: obsoleteFiles,
	}
	cm.mu.Lock()
	defer cm.mu.Unlock()
	select {
	case cm.jobsCh <- job:
		cm.mu.queuedJobs++

	default:
		if invariants.Enabled {
			panic("cleanup jobs queue full")
		}
		// Something is terribly wrong... Just drop the job.
		cm.opts.Logger.Infof("cleanup jobs queue full")
	}
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
	n := cm.mu.queuedJobs
	for cm.mu.completedJobs < n {
		cm.mu.completedJobsCond.Wait()
	}
}

// mainLoop runs the manager's background goroutine.
func (cm *cleanupManager) mainLoop() {
	defer cm.waitGroup.Done()
	useLimiter := false
	var limiter tokenbucket.TokenBucket

	if r := cm.opts.TargetByteDeletionRate; r != 0 {
		useLimiter = true
		limiter.Init(tokenbucket.TokensPerSecond(r), tokenbucket.Tokens(r))
	}

	for job := range cm.jobsCh {
		for _, of := range job.obsoleteFiles {
			if of.fileType != fileTypeTable {
				path := base.MakeFilepath(cm.opts.FS, of.dir, of.fileType, of.fileNum)
				cm.deleteObsoleteFile(of.fileType, job.jobID, path, of.fileNum, of.fileSize)
			} else {
				if useLimiter {
					cm.maybePace(&limiter, of.fileType, of.fileNum, of.fileSize)
				}
				cm.onTableDeleteFn(of.fileSize)
				cm.deleteObsoleteObject(fileTypeTable, job.jobID, of.fileNum)
			}
		}
		cm.mu.Lock()
		cm.mu.completedJobs++
		cm.mu.completedJobsCond.Broadcast()
		cm.mu.Unlock()
	}
}

// maybePace sleeps before deleting an object if appropriate. It is always
// called from the background goroutine.
func (cm *cleanupManager) maybePace(
	limiter *tokenbucket.TokenBucket,
	fileType base.FileType,
	fileNum base.DiskFileNum,
	fileSize uint64,
) {
	meta, err := cm.objProvider.Lookup(fileType, fileNum)
	if err != nil {
		// The object was already removed from the provider; we won't actually
		// delete anything, so we don't need to pace.
		return
	}
	if meta.IsShared() {
		// Don't throttle deletion of shared objects.
		return
	}
	if !cm.deletePacer.shouldPace() {
		// The deletion pacer decided that we shouldn't throttle; account
		// for the operation but don't wait for tokens.
		limiter.Adjust(-tokenbucket.Tokens(fileSize))
		return
	}
	// Wait for tokens.
	for {
		ok, d := limiter.TryToFulfill(tokenbucket.Tokens(fileSize))
		if ok {
			break
		}
		time.Sleep(d)
	}
}

// deleteObsoleteFile deletes a (non-object) file that is no longer needed.
func (cm *cleanupManager) deleteObsoleteFile(
	fileType fileType, jobID int, path string, fileNum base.DiskFileNum, fileSize uint64,
) {
	// TODO(peter): need to handle this error, probably by re-adding the
	// file that couldn't be deleted to one of the obsolete slices map.
	err := cm.opts.Cleaner.Clean(cm.opts.FS, fileType, path)
	if oserror.IsNotExist(err) {
		return
	}

	switch fileType {
	case fileTypeLog:
		cm.opts.EventListener.WALDeleted(WALDeleteInfo{
			JobID:   jobID,
			Path:    path,
			FileNum: fileNum.FileNum(),
			Err:     err,
		})
	case fileTypeManifest:
		cm.opts.EventListener.ManifestDeleted(ManifestDeleteInfo{
			JobID:   jobID,
			Path:    path,
			FileNum: fileNum.FileNum(),
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
			FileNum: fileNum.FileNum(),
			Err:     err,
		})
	}
}
