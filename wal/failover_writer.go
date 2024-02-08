// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package wal

import (
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/prometheus/client_golang/prometheus"
)

// recordQueueEntry is an entry in recordQueue.
type recordQueueEntry struct {
	p    []byte
	opts SyncOptions
}

const initialBufferLen = 8192

// recordQueue is a variable-size single-producer multiple-consumer queue. It
// is not lock-free, but most operations only need mu.RLock. It needs a mutex
// to grow the size, since there is no upper bound on the number of queued
// records (which are all the records that are not synced, and will need to be
// written again in case of failover). Additionally, it needs a mutex to
// atomically grab a snapshot of the queued records and provide them to a new
// LogWriter that is being switched to.
type recordQueue struct {
	// Only held for reading for all pop operations and most push operations.
	// Held for writing when buffer needs to be grown or when switching to a new
	// writer.
	mu sync.RWMutex

	// queue is [tail, head). tail is the oldest entry and head is the index for
	// the next entry.
	//
	// Consumers: atomically read and write tail in pop (using
	// compare-and-swap). This is not the usual kind of queue consumer since
	// they already know the index that they are popping exists, hence don't
	// need to look at head.
	//
	// Producer: atomically reads tail in push. Writes to head.
	//
	// Based on the above we only need tail to be atomic. However, the producer
	// also populates entries in buffer, whose values need to be seen by the
	// consumers when doing a pop, which means they need to synchronize using a
	// release and acquire memory barrier pair, where the push does the release
	// and the pop does the acquire. For this reason we make head also atomic
	// and merge head and tail into a single atomic, so that the store of head
	// in push and the load of tail in pop accomplishes this release-acquire
	// pair.
	//
	// All updates to headTail hold mu at least for reading. So when mu is held
	// for writing, there is a guarantee that headTail is not being updated.
	//
	// head is most-significant 32 bits and tail is least-significant 32 bits.
	headTail atomic.Uint64

	// Access to buffer requires at least RLock.
	buffer []recordQueueEntry

	lastTailObservedByProducer uint32

	// Read requires RLock.
	writer *record.LogWriter

	// When writer != nil, this is the return value of the last call to
	// SyncRecordGeneralized. It is updated in (a) WriteRecord calls push, using
	// only RLock (since WriteRecord is externally synchronized), (b)
	// snapshotAndSwitchWriter, using Lock. (b) excludes (a).
	lastLogSize int64
}

func (q *recordQueue) init() {
	*q = recordQueue{
		buffer: make([]recordQueueEntry, initialBufferLen),
	}
}

// NB: externally synchronized, i.e., no concurrent push calls.
func (q *recordQueue) push(
	p []byte,
	opts SyncOptions,
	latestLogSizeInWriteRecord int64,
	latestWriterInWriteRecord *record.LogWriter,
) (index uint32, writer *record.LogWriter, lastLogSize int64) {
	ht := q.headTail.Load()
	h, t := unpackHeadTail(ht)
	n := int(h - t)
	if len(q.buffer) == n {
		// Full
		m := 2 * n
		newBuffer := make([]recordQueueEntry, m)
		for i := int(t); i < int(h); i++ {
			newBuffer[i%m] = q.buffer[i%n]
		}
		q.mu.Lock()
		q.buffer = newBuffer
		q.mu.Unlock()
	}
	q.mu.RLock()
	q.buffer[h] = recordQueueEntry{
		p:    p,
		opts: opts,
	}
	// Reclaim memory for consumed entries. We couldn't do that in pop since
	// multiple consumers are popping using CAS and that immediately transfers
	// ownership to the producer.
	for i := q.lastTailObservedByProducer; i < t; i++ {
		q.buffer[i] = recordQueueEntry{}
	}
	q.lastTailObservedByProducer = t
	q.headTail.Add(1 << headTailBits)
	writer = q.writer
	if writer == latestWriterInWriteRecord {
		// WriteRecord has written to this writer since the switch.
		q.lastLogSize = latestLogSizeInWriteRecord
	}
	// Else writer is a new writer that was switched to, so ignore the
	// latestLogSizeInWriteRecord.

	lastLogSize = q.lastLogSize
	q.mu.RUnlock()
	return h, writer, lastLogSize
}

func (q *recordQueue) length() int {
	ht := q.headTail.Load()
	h, t := unpackHeadTail(ht)
	return int(h - t)
}

// Pops all entries. Must be called only after the last push returns.
func (q *recordQueue) popAll(err error) (numRecords int, numSyncsPopped int) {
	ht := q.headTail.Load()
	h, t := unpackHeadTail(ht)
	n := int(h - t)
	if n == 0 {
		return 0, 0
	}
	return n, q.pop(h-1, err, false)
}

// Pops all entries up to and including index. The remaining queue is
// [index+1, head).
//
// NB: we could slightly simplify to only have the latest writer be able to
// pop. This would avoid the CAS below, but it seems better to reduce the
// amount of queued work regardless of who has successfully written it.
func (q *recordQueue) pop(index uint32, err error, runCb bool) (numSyncsPopped int) {
	var buf [512]SyncOptions
	ht := q.headTail.Load()
	h, t := unpackHeadTail(ht)
	tail := int(t)
	maxEntriesToPop := int(index) - tail + 1
	if maxEntriesToPop <= 0 {
		return 0
	}
	var b []SyncOptions
	if maxEntriesToPop <= len(buf) {
		b = buf[:maxEntriesToPop]
	} else {
		b = make([]SyncOptions, maxEntriesToPop)
	}
	// Allocations were done before acquiring the mutex.
	q.mu.RLock()
	for i := 0; i < maxEntriesToPop; i++ {
		// Grab all the possible entries before doing CAS, since successful CAS
		// will also release those buffer slots to the producer.
		n := len(q.buffer)
		b[i] = q.buffer[(i+tail)%n].opts
	}
	for {
		newHT := makeHeadTail(h, index+1)
		if q.headTail.CompareAndSwap(ht, newHT) {
			break
		}
		ht := q.headTail.Load()
		h, t = unpackHeadTail(ht)
		tail = int(t)
		maxEntriesToPop = int(index) - tail + 1
		if maxEntriesToPop <= 0 {
			break
		}
	}
	q.mu.RUnlock()

	numEntriesPopped := maxEntriesToPop
	if numEntriesPopped <= 0 {
		return 0
	}
	n := len(b)
	for i := n - numEntriesPopped; i < n; i++ {
		if b[i].Done != nil {
			numSyncsPopped++
			if err != nil {
				*b[i].Err = err
			}
			b[i].Done.Done()
		}
	}
	return numSyncsPopped
}

func (q *recordQueue) snapshotAndSwitchWriter(
	writer *record.LogWriter,
	snapshotFunc func(firstIndex uint32, entries []recordQueueEntry) (logSize int64),
) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.writer = writer
	h, t := unpackHeadTail(q.headTail.Load())
	n := h - t
	if n > 0 {
		m := uint32(len(q.buffer))
		b := make([]recordQueueEntry, n)
		for i := t; i < h; i++ {
			b[i-t] = q.buffer[i%m]
		}
		q.lastLogSize = snapshotFunc(t, b)
	}
}

// getLastIndex is used by failoverWriter.Close.
func (q *recordQueue) getLastIndex() (lastIndex int64) {
	h, _ := unpackHeadTail(q.headTail.Load())
	return int64(h) - 1
}

const headTailBits = 32

func unpackHeadTail(ht uint64) (head, tail uint32) {
	const mask = 1<<headTailBits - 1
	head = uint32((ht >> headTailBits) & mask)
	tail = uint32(ht & mask)
	return head, tail
}

func makeHeadTail(head, tail uint32) uint64 {
	return (uint64(head) << headTailBits) | uint64(tail)
}

// Maximum number of physical log files when writing a virtual WAL. Arbitrarily
// chosen value. Setting this to 2 will not simplify the code. We make this a
// constant since we want a fixed size array for writer.writers.
const maxPhysicalLogs = 10

// failoverWriter is the implementation of Writer in failover mode. No Writer
// method blocks for IO, except for Close.
//
// Loosely speaking, Close blocks until all records are successfully written
// and synced to some log writer. Monitoring of log writer latency and errors
// continues after Close is called, which means failoverWriter can be switched
// to a new log writer after Close is called, to unblock Close.
//
// More precisely, Close does not block if there is an error in creating or
// closing the latest LogWriter when close was called. This is because errors
// are considered indicative of misconfiguration, and the user of
// failoverWriter can dampen switching when observing errors (e.g. see
// failoverMonitor), so close does not assume any liveness of calls to
// switchToNewDir when such errors occur. Since the caller (see db.go) treats
// an error on Writer.Close as fatal, this does mean that failoverWriter has
// limited ability to mask errors (its primary task is to mask high latency).
type failoverWriter struct {
	opts    failoverWriterOpts
	q       recordQueue
	writers [maxPhysicalLogs]logWriterAndRecorder
	mu      struct {
		sync.Mutex
		// cond is signaled when the latest LogWriter is set in writers (or there
		// is a creation error), or when the latest LogWriter is successfully
		// closed. It is waited on in Close. We don't use channels and select
		// since what Close is waiting on is dynamic based on the local state in
		// Close, so using Cond is simpler.
		cond *sync.Cond
		// nextWriterIndex is advanced before creating the *LogWriter. That is, a
		// slot is reserved by taking the current value of nextWriterIndex and
		// incrementing it, and then the *LogWriter for that slot is created. When
		// newFailoverWriter returns, nextWriterIndex = 1.
		//
		// The latest *LogWriter is (will be) at nextWriterIndex-1.
		//
		// INVARIANT: nextWriterIndex <= len(writers)
		nextWriterIndex logNameIndex
		closed          bool
		// metrics is initialized in Close. Currently we just use the metrics from
		// the latest writer after it is closed, since in the common case with
		// only one writer, that writer's flush loop will have finished and the
		// metrics will be current. With multiple writers, these metrics can be
		// quite inaccurate. The WriteThroughput metric includes an IdleDuration,
		// which can be high for a writer that was switched away from, and
		// therefore not indicative of overall work being done by the
		// failoverWriter. The PendingBufferLen and SyncQueueLen are similarly
		// inaccurate once there is no more work being given to a writer. We could
		// add a method to LogWriter to stop sampling metrics when it is not the
		// latest writer. Then we could aggregate all these metrics across all
		// writers.
		//
		// Note that CockroachDB does not use these metrics in any meaningful way.
		//
		// TODO(sumeer): do the improved solution outlined above.
		metrics record.LogWriterMetrics
	}
	// State for computing logical offset. The cumulative offset state is in
	// offset. Each time we call SyncRecordGeneralized from WriteRecord, we
	// compute the delta from the size returned by this LogWriter now, and the
	// size returned by this LogWriter in the previous call to
	// SyncRecordGeneralized. That previous call to SyncRecordGeneralized may
	// have happened from WriteRecord, or asynchronously during a switch. So
	// that previous call state requires synchronization and is maintained in
	// recordQueue. The offset is incremented by this delta without any
	// synchronization, since we rely on external synchronization (like the
	// standaloneWriter).
	logicalOffset struct {
		latestWriterInWriteRecord  *record.LogWriter
		latestLogSizeInWriteRecord int64
		offset                     int64
		// Transitions once from false => true when there is a non-nil writer.
		notEstimatedOffset bool
	}
	psiForWriteRecordBacking record.PendingSyncIndex
	psiForSwitchBacking      record.PendingSyncIndex
}

type logWriterAndRecorder struct {
	// This may never become non-nil, if when the LogWriter was finally created,
	// it was no longer the latest writer. Additionally, if there was an error
	// in creating the writer, w will remain nil and createError will be set.
	w *record.LogWriter
	// createError is set if there is an error creating the writer. This is
	// useful in Close since we need to know when the work for creating the
	// latest writer is done, whether it resulted in success or not.
	createError error
	r           latencyAndErrorRecorder
}

var _ Writer = &failoverWriter{}

var _ switchableWriter = &failoverWriter{}

type failoverWriterOpts struct {
	wn     NumWAL
	logger base.Logger

	// Options that feed into SyncingFileOptions.
	noSyncOnClose   bool
	bytesPerSync    int
	preallocateSize func() int

	// Options for record.LogWriter.
	minSyncInterval func() time.Duration
	fsyncLatency    prometheus.Histogram
	queueSemChan    chan struct{}
	stopper         *stopper

	writerCreatedForTest chan<- struct{}
}

func newFailoverWriter(
	opts failoverWriterOpts, initialDir dirAndFileHandle,
) (*failoverWriter, error) {
	ww := &failoverWriter{
		opts: opts,
	}
	ww.q.init()
	ww.mu.cond = sync.NewCond(&ww.mu)
	// The initial record.LogWriter creation also happens via a
	// switchToNewWriter since we don't want it to block newFailoverWriter.
	err := ww.switchToNewDir(initialDir)
	if err != nil {
		// Switching limit cannot be exceeded when creating.
		panic(err)
	}
	return ww, nil
}

// WriteRecord implements Writer.
func (ww *failoverWriter) WriteRecord(p []byte, opts SyncOptions) (logicalOffset int64, err error) {
	recordIndex, writer, lastLogSize := ww.q.push(
		p, opts, ww.logicalOffset.latestLogSizeInWriteRecord, ww.logicalOffset.latestWriterInWriteRecord)
	if writer == nil {
		// Don't have a record.LogWriter yet, so use an estimate. This estimate
		// will get overwritten.
		ww.logicalOffset.offset += int64(len(p))
		return ww.logicalOffset.offset, nil
	}
	// INVARIANT: writer != nil.
	notEstimatedOffset := ww.logicalOffset.notEstimatedOffset
	if !notEstimatedOffset {
		ww.logicalOffset.notEstimatedOffset = true
	}
	ww.psiForWriteRecordBacking = record.PendingSyncIndex{Index: record.NoSyncIndex}
	if opts.Done != nil {
		ww.psiForWriteRecordBacking.Index = int64(recordIndex)
	}
	ww.logicalOffset.latestLogSizeInWriteRecord, err = writer.SyncRecordGeneralized(p, &ww.psiForWriteRecordBacking)
	ww.logicalOffset.latestWriterInWriteRecord = writer
	if notEstimatedOffset {
		delta := ww.logicalOffset.latestLogSizeInWriteRecord - lastLogSize
		ww.logicalOffset.offset += delta
	} else {
		// Overwrite the estimate. This is a best-effort improvement in that it is
		// accurate for the common case where writer is the first LogWriter.
		// Consider a failover scenario where there was no LogWriter for the first
		// 10 records, so they are all accumulated as an estimate. Then the first
		// LogWriter successfully writes and syncs the first 5 records and gets
		// stuck. A switch happens to a second LogWriter that is handed the
		// remaining 5 records, and the the 11th record arrives via a WriteRecord.
		// The transition from !notEstimatedOffset to notEstimatedOffset will
		// happen on this 11th record, and the logic here will use the length of
		// the second LogWriter, that does not reflect the full length.
		//
		// TODO(sumeer): try to make this more correct, without adding much more
		// complexity, and without adding synchronization.
		ww.logicalOffset.offset = ww.logicalOffset.latestLogSizeInWriteRecord
	}
	return ww.logicalOffset.offset, err
}

// switchToNewDir starts switching to dir. It implements switchableWriter. All
// work is async, and a non-nil error is returned only if the switching limit
// is exceeded.
func (ww *failoverWriter) switchToNewDir(dir dirAndFileHandle) error {
	ww.mu.Lock()
	// Can have a late switchToNewDir call is the failoverMonitor has not yet
	// been told that the writer is closed. Ignore.
	if ww.mu.closed {
		ww.mu.Unlock()
		if ww.opts.writerCreatedForTest != nil {
			ww.opts.writerCreatedForTest <- struct{}{}
		}
		return nil
	}
	// writerIndex is the slot for this writer.
	writerIndex := ww.mu.nextWriterIndex
	if int(writerIndex) == len(ww.writers) {
		ww.mu.Unlock()
		return errors.Errorf("exceeded switching limit")
	}
	ww.mu.nextWriterIndex++
	ww.mu.Unlock()

	// Creation is async.
	ww.opts.stopper.runAsync(func() {
		// TODO(sumeer): recycling of logs.
		filename := dir.FS.PathJoin(dir.Dirname, makeLogFilename(ww.opts.wn, writerIndex))
		recorderAndWriter := &ww.writers[writerIndex].r
		var file vfs.File
		// handleErrFunc is called when err != nil. It handles the multiple IO error
		// cases below.
		handleErrFunc := func(err error) {
			if file != nil {
				file.Close()
			}
			ww.mu.Lock()
			defer ww.mu.Unlock()
			ww.writers[writerIndex].createError = err
			ww.mu.cond.Signal()
			if ww.opts.writerCreatedForTest != nil {
				ww.opts.writerCreatedForTest <- struct{}{}
			}
		}
		var err error
		// Create file.
		recorderAndWriter.writeStart()
		file, err = dir.FS.Create(filename)
		recorderAndWriter.writeEnd(err)
		// TODO(sumeer): should we fatal if primary dir? At some point it is better
		// to fatal instead of continuing to failover.
		// base.MustExist(dir.FS, filename, ww.opts.logger, err)
		if err != nil {
			handleErrFunc(err)
			return
		}
		// Sync dir.
		recorderAndWriter.writeStart()
		err = dir.Sync()
		recorderAndWriter.writeEnd(err)
		if err != nil {
			handleErrFunc(err)
			return
		}
		// Wrap in a syncingFile.
		syncingFile := vfs.NewSyncingFile(file, vfs.SyncingFileOptions{
			NoSyncOnClose:   ww.opts.noSyncOnClose,
			BytesPerSync:    ww.opts.bytesPerSync,
			PreallocateSize: ww.opts.preallocateSize(),
		})
		// Wrap in the latencyAndErrorRecorder.
		recorderAndWriter.setWriter(syncingFile)

		// Using NumWAL as the DiskFileNum is fine since it is used only as
		// EOF trailer for safe log recycling. Even though many log files can
		// map to a single NumWAL, a file used for NumWAL n at index m will
		// never get recycled for NumWAL n at a later index (since recycling
		// happens when n as a whole is obsolete).
		w := record.NewLogWriter(recorderAndWriter, base.DiskFileNum(ww.opts.wn),
			record.LogWriterConfig{
				WALMinSyncInterval:        ww.opts.minSyncInterval,
				WALFsyncLatency:           ww.opts.fsyncLatency,
				QueueSemChan:              ww.opts.queueSemChan,
				ExternalSyncQueueCallback: ww.doneSyncCallback,
			})
		closeWriter := func() bool {
			ww.mu.Lock()
			defer ww.mu.Unlock()
			if writerIndex+1 != ww.mu.nextWriterIndex || ww.mu.closed {
				// Not the latest writer or the writer was closed while this async
				// creation was ongoing.
				if ww.opts.writerCreatedForTest != nil {
					ww.opts.writerCreatedForTest <- struct{}{}
				}
				return true
			}
			// Latest writer.
			ww.writers[writerIndex].w = w
			ww.mu.cond.Signal()
			// NB: snapshotAndSwitchWriter does not block on IO, since
			// SyncRecordGeneralized does no IO.
			ww.q.snapshotAndSwitchWriter(w,
				func(firstIndex uint32, entries []recordQueueEntry) (logSize int64) {
					for i := range entries {
						ww.psiForSwitchBacking = record.PendingSyncIndex{Index: record.NoSyncIndex}
						if entries[i].opts.Done != nil {
							ww.psiForSwitchBacking.Index = int64(firstIndex) + int64(i)
						}
						var err error
						logSize, err = w.SyncRecordGeneralized(entries[i].p, &ww.psiForSwitchBacking)
						if err != nil {
							// TODO(sumeer): log periodically. The err will also surface via
							// the latencyAndErrorRecorder, so if a switch is possible, it
							// will be done.
							ww.opts.logger.Errorf("%s", err)
						}
					}
					return logSize
				})
			if ww.opts.writerCreatedForTest != nil {
				ww.opts.writerCreatedForTest <- struct{}{}
			}
			return false
		}()
		if closeWriter {
			// Never wrote anything to this writer so don't care about the
			// returned error.
			ww.opts.stopper.runAsync(func() {
				_ = w.Close()
			})
		}
	})
	return nil
}

// doneSyncCallback is the record.ExternalSyncQueueCallback called by
// record.LogWriter.
//
// recordQueue is popped from only when some work requests a sync (and
// successfully syncs). In the worst case, if no syncs are requested, we could
// queue all the records needed to fill up a memtable in the recordQueue. This
// can have two negative effects: (a) in the case of failover, we need to
// replay all the data in the current mutable memtable, which takes more time,
// (b) the memory usage is proportional to the size of the memtable. We ignore
// these negatives since, (a) users like CockroachDB regularly sync, and (b)
// the default memtable size is only 64MB.
func (ww *failoverWriter) doneSyncCallback(doneSync record.PendingSyncIndex, err error) {
	if err != nil {
		// Don't pop anything since we can retry after switching to a new
		// LogWriter.
		return
	}
	// NB: harmless after Close returns since numSyncsPopped will be 0.
	numSyncsPopped := ww.q.pop(uint32(doneSync.Index), err, true)
	if ww.opts.queueSemChan != nil {
		for i := 0; i < numSyncsPopped; i++ {
			<-ww.opts.queueSemChan
		}
	}
}

// ongoingLatencyOrErrorForCurDir implements switchableWriter.
func (ww *failoverWriter) ongoingLatencyOrErrorForCurDir() (time.Duration, error) {
	ww.mu.Lock()
	defer ww.mu.Unlock()
	if ww.mu.closed {
		return 0, nil
	}
	return ww.writers[ww.mu.nextWriterIndex-1].r.ongoingLatencyOrError()
}

// Close implements Writer.
//
// NB: getOngoingLatencyOrErrorForLatestWriter and switchToNewDir can be
// called after Close is called, and there is also a possibility that they get
// called after Close returns and before failoverMonitor knows that the
// failoverWriter is closed.
//
// doneSyncCallback can be called anytime after Close returns since there
// could be stuck writes that finish arbitrarily later.
//
// See the long comment about Close behavior where failoverWriter is declared.
func (ww *failoverWriter) Close() (logicalOffset int64, err error) {
	logicalOffset = ww.logicalOffset.offset
	// [0, closeCalledCount) have had LogWriter.Close called (though may not
	// have finished) or the LogWriter will never be non-nil. Either way, they
	// have been "processed".
	closeCalledCount := logNameIndex(0)
	// lastWriterState is the state for the last writer, for which we are
	// waiting for LogWriter.Close to finish or for creation to be unsuccessful.
	// What is considered the last writer can change. All state is protected by
	// ww.mu.
	type lastWriterState struct {
		index   logNameIndex
		closed  bool
		err     error
		metrics record.LogWriterMetrics
	}
	var lastWriter lastWriterState
	lastRecordIndex := record.PendingSyncIndex{Index: ww.q.getLastIndex()}
	ww.mu.Lock()
	defer ww.mu.Unlock()
	// Every iteration starts and ends with the mutex held.
	//
	// Invariant: ww.mu.nextWriterIndex >= 1.
	//
	// We will loop until we have closed the lastWriter (and use
	// lastPossibleWriter.err). We also need to call close on all LogWriters
	// that will not close themselves, i.e., those that have already been
	// created and installed in failoverWriter.writers (this set may change
	// while failoverWriter.Close runs).
	for !lastWriter.closed {
		numWriters := ww.mu.nextWriterIndex
		if numWriters > closeCalledCount {
			// INVARIANT: numWriters > closeCalledCount.
			lastWriter = lastWriterState{
				index: numWriters - 1,
			}
			// Try to process [closeCalledCount, numWriters). Will surely process
			// [closeCalledCount, numWriters-1), since those writers are either done
			// initializing, or will close themselves. The writer at numWriters-1 we
			// can only process if it is done initializing, else we will iterate
			// again.
			for i := closeCalledCount; i < numWriters; i++ {
				w := ww.writers[i].w
				cErr := ww.writers[i].createError
				// Is the current index the last writer. If yes, this is also the last
				// loop iteration.
				isLastWriter := i == lastWriter.index
				if w != nil {
					// Can close it, so extend closeCalledCount.
					closeCalledCount = i + 1
					if isLastWriter {
						// We may care about its error and when it finishes closing.
						index := i
						ww.opts.stopper.runAsync(func() {
							// Last writer(s) (since new writers can be created and become
							// last, as we iterate) are guaranteed to have seen the last
							// record (since it was queued before Close was called). It is
							// possible that a writer got created after the last record was
							// dequeued and before this fact was realized by Close. In that
							// case we will harmlessly tell it that it synced that last
							// record, though it has already been written and synced by
							// another writer.
							err := w.CloseWithLastQueuedRecord(lastRecordIndex)
							ww.mu.Lock()
							defer ww.mu.Unlock()
							if lastWriter.index == index {
								lastWriter.closed = true
								lastWriter.err = err
								lastWriter.metrics = w.Metrics()
								ww.mu.cond.Signal()
							}
						})
					} else {
						// Don't care about the returned error since all the records we
						// relied on this writer for were already successfully written.
						ww.opts.stopper.runAsync(func() {
							_ = w.CloseWithLastQueuedRecord(record.PendingSyncIndex{Index: record.NoSyncIndex})
						})
					}
				} else if cErr != nil {
					// Have processed it, so extend closeCalledCount.
					closeCalledCount = i + 1
					if isLastWriter {
						lastWriter.closed = true
						lastWriter.err = cErr
						lastWriter.metrics = record.LogWriterMetrics{}
					}
					// Else, ignore.
				} else {
					if !isLastWriter {
						// Not last writer, so will close itself.
						closeCalledCount = i + 1
					}
					// Else, last writer, so we may have to close it.
				}
			}
		}
		if !lastWriter.closed {
			// Either waiting for creation of last writer or waiting for the close
			// to finish, or something else to become the last writer.
			ww.mu.cond.Wait()
		}
	}
	err = lastWriter.err
	ww.mu.metrics = lastWriter.metrics
	ww.mu.closed = true
	_, _ = ww.q.popAll(err)
	return logicalOffset, err
}

// Metrics implements writer.
func (ww *failoverWriter) Metrics() record.LogWriterMetrics {
	ww.mu.Lock()
	defer ww.mu.Unlock()
	return ww.mu.metrics
}

// latencyAndErrorRecorder records ongoing write and sync operations and errors
// in those operations. record.LogWriter cannot continue functioning after any
// error, so all errors are considered permanent.
//
// writeStart/writeEnd are used directly when creating a file. After the file
// is successfully created, setWriter turns latencyAndErrorRecorder into an
// implementation of writerSyncerCloser that will record for the Write and
// Sync methods.
type latencyAndErrorRecorder struct {
	ongoingOperationStart atomic.Int64
	error                 atomic.Pointer[error]
	writerSyncerCloser
}

type writerSyncerCloser interface {
	io.Writer
	io.Closer
	Sync() error
}

func (r *latencyAndErrorRecorder) writeStart() {
	r.ongoingOperationStart.Store(time.Now().UnixNano())
}

func (r *latencyAndErrorRecorder) writeEnd(err error) {
	if err != nil {
		ptr := &err
		r.error.Store(ptr)
	}
	r.ongoingOperationStart.Store(0)
}

func (r *latencyAndErrorRecorder) setWriter(w writerSyncerCloser) {
	r.writerSyncerCloser = w
}

func (r *latencyAndErrorRecorder) ongoingLatencyOrError() (time.Duration, error) {
	startTime := r.ongoingOperationStart.Load()
	var latency time.Duration
	if startTime != 0 {
		l := time.Now().UnixNano() - startTime
		if l < 0 {
			l = 0
		}
		latency = time.Duration(l)
	}
	errPtr := r.error.Load()
	var err error
	if errPtr != nil {
		err = *errPtr
	}
	return latency, err
}

// Sync implements writerSyncerCloser.
func (r *latencyAndErrorRecorder) Sync() error {
	r.writeStart()
	err := r.writerSyncerCloser.Sync()
	r.writeEnd(err)
	return err
}

// Write implements io.Writer.
func (r *latencyAndErrorRecorder) Write(p []byte) (n int, err error) {
	r.writeStart()
	n, err = r.writerSyncerCloser.Write(p)
	r.writeEnd(err)
	return n, err
}
