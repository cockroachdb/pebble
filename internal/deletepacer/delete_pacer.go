// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package deletepacer

import (
	"context"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/crlib/fifo"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/metrics"
)

// DiskFreeSpaceFn returns the current amount of free space on the disk in
// bytes. This is used to determine whether the deletion rate needs to be
// increased to prevent running out of disk space.
type DiskFreeSpaceFn func() uint64

// DeleteFn is called to perform the actual deletion of an obsolete file. It
// should not panic. Any errors must be handled internally. The delete pacer
// does not retry on deletion failures.
type DeleteFn func(of ObsoleteFile, jobID int)

// DeletePacer rate-limits deletions of obsolete files to prevent disk
// performance degradation. On some SSDs, deleting large amounts of data too
// quickly can negatively impact overall disk performance due to internal garbage
// collection overhead.
//
// The delete pacer maintains a queue of obsolete files and processes them at a
// controlled rate. The deletion rate adapts dynamically based on:
//   - A configured baseline rate (minimum deletion throughput)
//   - Recent deletion patterns (smoothing out bursts over a 5-minute window)
//   - Queue backlog (accelerating when the queue grows too large)
//   - Available disk space (accelerating when running low on space)
//
// This ensures files are deleted at a steady pace that matches the workload
// while preventing sudden bursts that could overload the disk, particularly
// after large compactions or when closing iterators.
//
// DeletePacer is safe for concurrent use. It must be created with Open() and
// closed with Close().
type DeletePacer struct {
	opts            Options
	diskFreeSpaceFn DiskFreeSpaceFn
	deleteFn        DeleteFn
	logger          base.Logger

	mu struct {
		sync.Mutex

		queue             fifo.Queue[queueEntry]
		queuedPacingBytes uint64
		// queuedHistory keeps track of pacing bytes added to the queue within the
		// last 5 minutes.
		queuedHistory history
		metrics       Metrics
		// deletedCond is signaled when we delete a file; used by WaitForTesting().
		deletedCond sync.Cond
		closed      bool
	}
	// notifyCh is used to wake up the background goroutine. Used when new files
	// are enqueued, and on Close().
	notifyCh chan struct{}
	// waitGroup is used in Close() to wait for the background goroutine to exit.
	waitGroup sync.WaitGroup
}

// RecentRateWindow is the timeframe over which we smooth out deletion bursts.
// The delete pacer uses this window to calculate the recent deletion rate and
// spread out large bursts over time.
//
// Files that have been in the queue for longer than this window are considered
// backlog, triggering accelerated deletion rates.
const RecentRateWindow = 5 * time.Minute

// maxQueueSize is a safety valve to prevent unbounded queue growth. If the
// queue exceeds this size, pacing is temporarily disabled to drain the queue
// quickly. This prevents memory exhaustion and ensures deletions don't fall too
// far behind.
//
// When this limit is hit, an error is logged (at most once per minute).
const maxQueueSize = 1000

// Open creates a DeletePacer and starts its background goroutine.
// The DeletePacer must be Close()d.
func Open(
	opts Options, logger base.Logger, diskFreeSpaceFn DiskFreeSpaceFn, deleteFn DeleteFn,
) *DeletePacer {
	opts.EnsureDefaults()
	dp := &DeletePacer{
		opts:            opts,
		logger:          logger,
		diskFreeSpaceFn: diskFreeSpaceFn,
		deleteFn:        deleteFn,
		notifyCh:        make(chan struct{}, 1),
	}
	dp.mu.queue = fifo.MakeQueue(&queueBackingPool)
	dp.mu.queuedHistory.Init(crtime.NowMono(), RecentRateWindow)
	dp.mu.deletedCond.L = &dp.mu.Mutex
	dp.waitGroup.Add(1)
	go func() {
		pprof.Do(context.Background(), pprof.Labels("pebble", "gc"), func(context.Context) {
			dp.mainLoop()
		})
	}()
	return dp
}

var queueBackingPool = fifo.MakeQueueBackingPool[queueEntry]()

type queueEntry struct {
	ObsoleteFile
	JobID int
}

// Close stops the background goroutine, waiting until all queued jobs are completed.
// Delete pacing is disabled for the remaining jobs.
//
// EnqueueJob() must not be called after Close().
func (dp *DeletePacer) Close() {
	dp.mu.Lock()
	dp.mu.closed = true
	dp.mu.Unlock()
	select {
	case dp.notifyCh <- struct{}{}:
	default:
	}
	dp.waitGroup.Wait()
}

// mainLoop is the background goroutine that processes the delete queue.
//
// We keep track of a pacing rate in bytes/sec. When we delete a file, we add its
// size to a "debt" counter. The debt is then paid off at the pacing rate. If the
// debt is greater than zero, we wait until it is paid off before deleting the
// next file.
//
// The pacing rate is recalculated every time we are about to delete a file or
// when a new file gets enqueued. The pacing rate is based on:
//   - the baseline rate configured by the user;
//   - the recent rate of incoming deletions (over the last 5 minutes);
//   - the backlog (deletions that have been in the queue for more than 5 minutes);
//   - whether we are running low on free space.
func (dp *DeletePacer) mainLoop() {
	defer dp.waitGroup.Done()

	timer := time.NewTimer(time.Duration(0))
	defer timer.Stop()

	rateCalc := makeRateCalculator(&dp.opts, dp.diskFreeSpaceFn, crtime.NowMono())

	var lastMaxQueueLog crtime.Mono
	dp.mu.Lock()
	defer dp.mu.Unlock()
	for {
		if dp.mu.closed && dp.mu.queue.Len() == 0 {
			return
		}
		now := crtime.NowMono()
		disablePacing := dp.mu.closed
		if dp.mu.queue.Len() > maxQueueSize {
			// The queue is getting out of hand; disable pacing.
			disablePacing = true
			if lastMaxQueueLog == 0 || now.Sub(lastMaxQueueLog) > time.Minute {
				lastMaxQueueLog = now
				dp.logger.Errorf("excessive delete pacer queue size %d; pacing temporarily disabled", dp.mu.queue.Len())
			}
		}

		rateCalc.Update(now, dp.mu.queuedHistory.Sum(now), dp.mu.queuedPacingBytes, disablePacing)

		// Processing priority:
		//   1. Exit if closed and queue empty;
		//   2. Wait for pacing debt to clear;
		//   3. Otherwise, delete next file.
		switch {
		case dp.mu.queue.Len() == 0:
			// Nothing to do.
			dp.mu.Unlock()
			<-dp.notifyCh
			dp.mu.Lock()

		case rateCalc.InDebt():
			// We have files in the queue but we must wait.
			dp.mu.Unlock()
			waitTime := rateCalc.DebtWaitTime()
			// Don't wait more than 10 seconds; we want a chance to recalculate the
			// rate (and check if we're running low on free space).
			waitTime = min(waitTime, 10*time.Second)
			timer.Reset(waitTime)
			select {
			case <-timer.C:
			case <-dp.notifyCh:
				timer.Stop()
			}
			dp.mu.Lock()

		default:
			// Delete a file.
			file := *dp.mu.queue.PeekFront()
			dp.mu.queue.PopFront()
			if b := file.pacingBytes(); b != 0 {
				dp.mu.queuedPacingBytes = invariants.SafeSub(dp.mu.queuedPacingBytes, b)
				rateCalc.AddDebt(b)
			}
			func() {
				dp.mu.Unlock()
				defer dp.mu.Lock()
				dp.deleteFn(file.ObsoleteFile, file.JobID)
			}()
			dp.mu.metrics.InQueue.Dec(file.FileType, file.FileSize, file.Placement)
			dp.mu.metrics.Deleted.Inc(file.FileType, file.FileSize, file.Placement)
			dp.mu.deletedCond.Broadcast()
		}
	}
}

// Enqueue adds the given files to the delete queue. Enqueue never blocks.
func (dp *DeletePacer) Enqueue(jobID int, files ...ObsoleteFile) {
	dp.mu.Lock()
	defer dp.mu.Unlock()
	if dp.mu.closed {
		if invariants.Enabled {
			panic("Enqueue called after Close")
		}
		return
	}
	now := crtime.NowMono()
	for _, file := range files {
		if b := file.pacingBytes(); b > 0 {
			dp.mu.queuedPacingBytes += b
			dp.mu.queuedHistory.Add(now, b)
		}
		dp.mu.metrics.InQueue.Inc(file.FileType, file.FileSize, file.Placement)
		dp.mu.queue.PushBack(queueEntry{
			ObsoleteFile: file,
			JobID:        jobID,
		})
	}
	select {
	case dp.notifyCh <- struct{}{}:
	default:
	}
}

// Metrics returns the current metrics.
func (dp *DeletePacer) Metrics() Metrics {
	dp.mu.Lock()
	defer dp.mu.Unlock()
	return dp.mu.metrics
}

// Metrics tracks statistics about files in the delete pacer queue and files
// that have been deleted. This provides visibility into deletion throughput and
// queue depth.
type Metrics struct {
	// InQueue contains the count and total size of files currently waiting in the
	// delete queue, broken down by file type (tables vs blob files) and locality
	// (all vs local only).
	InQueue metrics.FileCountsAndSizes

	// Deleted contains the count and total size of files that have been deleted
	// since the DeletePacer was started, broken down by file type and locality.
	Deleted metrics.FileCountsAndSizes
}

// WaitForTesting waits until the deletion of all files that were already
// queued. Does not wait for jobs that are enqueued during the call.
func (dp *DeletePacer) WaitForTesting() {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	n := dp.mu.metrics.Deleted.Total().Count + dp.mu.metrics.InQueue.Total().Count
	for dp.mu.metrics.Deleted.Total().Count < n {
		dp.mu.deletedCond.Wait()
	}
}
