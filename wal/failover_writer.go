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

// TODO(sumeer): remove these notes after ensuring they are done, and
// incorporated into code comments.
//
// Misc temporary notes:
//
// recordQueue size: should we inject fsyncs to keep the memory bounded?
// Doesn't seem necessary for CRDB since there will be frequent fsyncs due to
// raft log appends. Also, it can't grow beyond the memtable size (though
// failing over 64MB, the memtable size, will not be fast).
//
// If switch to secondary failed because of misconfiguration, we should switch
// back to primary and make a note of it. If primary is still unavailable we
// would switch again, so repeated switching which is bad. Misconfiguration
// should result in an error. Consider an error as worse than slowness, so
// don't switch back?
//
// recycle: recycle only those in primary dir. Keep track of those where
// record.logwriter closed. Those are the only ones we can delete or recycle.
//
// Gap and de-duping logic on read:
// Gap can be there in adjacent logs but not between log n and the union of logs 0..n-1.
// Entries 0..50 sent to log 0.
// Entries 0..60 sent to log 1
// Log 0 commits up to 50
// Entries 50..100 sent to log 2
// Log 2 commits all if these.
// Log 1 only commits 0..10.
//
// Write stall threshold: Decay the write stall threshold slowly after
// switching back to primary dir, as flush may take some time. Say 10
// memtables. Could take 20s to flush. Good thing is that it will only add 1
// sublevel.

// recordQueueEntry is an entry in recordQueue.
type recordQueueEntry struct {
	p    []byte
	opts SyncOptions
}

const initialBufferLen = 8192

// recordQueue is a variable size single producer multiple consumer queue. It
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

	// Protected by mu.
	writer *record.LogWriter
}

func (q *recordQueue) init() {
	*q = recordQueue{
		buffer: make([]recordQueueEntry, initialBufferLen),
	}
}

// NB: externally synchronized, i.e., no concurrent push calls.
func (q *recordQueue) push(p []byte, opts SyncOptions) (index uint32, writer *record.LogWriter) {
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
	q.mu.RUnlock()
	return h, writer
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
	return n, q.pop(h-1, err)
}

// Pops all entries up to and including index. The remaining queue is
// [index+1, head).
//
// NB: we could slightly simplify to only have the latest writer be able to
// pop. This would avoid the CAS below, but it seems better to reduce the
// amount of queued work regardless of who has successfully written it.
func (q *recordQueue) pop(index uint32, err error) (numSyncsPopped int) {
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
	writer *record.LogWriter, snapshotFunc func(firstIndex uint32, entries []recordQueueEntry),
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
		snapshotFunc(t, b)
	}
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
// method blocks for IO, except for Close. Close will block until all records
// are successfully written and synced to some log writer. Monitoring of log
// writer latency and errors continues after Close is called, which means
// failoverWriter can be switched to a new log writer after Close is called,
// so as to unblock Close.
type failoverWriter struct {
	wm      *failoverManager
	opts    failoverWriterOpts
	q       recordQueue
	writers [maxPhysicalLogs]logWriterAndRecorder
	mu      struct {
		sync.Mutex
		// cond is signaled when the latest *LogWriter is set in writers, or when
		// what was probably the latest *LogWriter is successfully closed. It is
		// waited on in Close.
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
	}
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
	opts failoverWriterOpts, initialDir dirAndFileHandle, wm *failoverManager,
) (*failoverWriter, error) {
	ww := &failoverWriter{
		wm:   wm,
		opts: opts,
	}
	ww.q.init()
	ww.mu.cond = sync.NewCond(&ww.mu)
	// The initial record.LogWriter creation also happens via a
	// switchToNewWriter since we don't want it to block newFailoverWriter.
	err := ww.switchToNewDir(initialDir)
	if err != nil {
		return nil, err
	}
	return ww, nil
}

// Size implements Writer.
func (ww *failoverWriter) Size() uint64 {
	// TODO(sumeer):
	return 0
}

// FileSize implements Writer.
func (ww *failoverWriter) FileSize() uint64 {
	// TODO(sumeer):
	return 0
}

// WriteRecord implements Writer.
func (ww *failoverWriter) WriteRecord(p []byte, opts SyncOptions) error {
	recordIndex, writer := ww.q.push(p, opts)
	if writer == nil {
		// Don't have a record.LogWriter yet.
		return nil
	}
	ps := record.PendingSyncIndex{Index: record.NoSyncIndex}
	if opts.Done != nil {
		ps.Index = int64(recordIndex)
	}
	_, err := writer.SyncRecordGeneralized(p, ps)
	return err
}

// switchToNewDir starts switching to dir. It implements switchableWriter.
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
	writerIndex := ww.mu.nextWriterIndex
	if int(writerIndex) == len(ww.writers) {
		ww.mu.Unlock()
		return errors.Errorf("exceeded switching limit")
	}
	ww.mu.nextWriterIndex++
	ww.mu.Unlock()

	ww.opts.stopper.runAsync(func() {
		// TODO(sumeer): recycling of logs.
		filename := dir.FS.PathJoin(dir.Dirname, makeLogFilename(ww.opts.wn, writerIndex))
		recorderAndWriter := &ww.writers[writerIndex].r
		recorderAndWriter.writeStart()
		file, err := dir.FS.Create(filename)
		recorderAndWriter.writeEnd(err)
		// TODO(sumeer): should we fatal if primary dir? At some point it is better
		// to fatal instead of continuing to failover.
		// base.MustExist(dir.FS, filename, ww.opts.logger, err)
		handleErrFunc := func() {
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
		if err != nil {
			handleErrFunc()
			return
		}
		{
			recorderAndWriter.writeStart()
			err = dir.Sync()
			recorderAndWriter.writeEnd(err)
		}
		if err != nil {
			handleErrFunc()
			return
		}
		syncingFile := vfs.NewSyncingFile(file, vfs.SyncingFileOptions{
			NoSyncOnClose:   ww.opts.noSyncOnClose,
			BytesPerSync:    ww.opts.bytesPerSync,
			PreallocateSize: ww.opts.preallocateSize(),
		})
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
			ww.q.snapshotAndSwitchWriter(w, func(firstIndex uint32, entries []recordQueueEntry) {
				for i := range entries {
					ps := record.PendingSyncIndex{Index: record.NoSyncIndex}
					if entries[i].opts.Done != nil {
						ps.Index = int64(firstIndex) + int64(i)
					}
					_, err := w.SyncRecordGeneralized(entries[i].p, ps)
					if err != nil {
						// TODO(sumeer): log periodically. The err will also surface via
						// the latencyAndErrorRecorder, so if a switch is possible, it
						// will be done.
						ww.opts.logger.Errorf("%s", err)
					}
				}
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
func (ww *failoverWriter) doneSyncCallback(doneSync record.PendingSyncIndex, err error) {
	if err != nil {
		// Don't pop anything since we can retry after switching to a new
		// LogWriter.
		return
	}
	// NB: harmless after Close returns since numSyncsPopped will be 0.
	numSyncsPopped := ww.q.pop(uint32(doneSync.Index), err)
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
func (ww *failoverWriter) Close() error {
	// [0, closeCalledCount) have had LogWriter.Close called (though may not
	// have finished) or the LogWriter will never be non-nil. Either way, they
	// have been "processed".
	closeCalledCount := logNameIndex(0)
	done := false
	var err error
	ww.mu.Lock()
	// Every iteration starts and ends with the mutex held.
	//
	// Invariant: ww.mu.nextWriterIndex >= 1.
	//
	// TODO(sumeer): write a high level comment for the logic below.
	for !done {
		numWriters := ww.mu.nextWriterIndex
		// Want to process [closeCalledCount, numWriters).
		// Invariant: numWriters - closeCalledCount >= 1.
		if numWriters-closeCalledCount <= 0 {
			panic("invariant violation")
		}
		// Wait until writers[numWriters-1] is either created or has a creation
		// error or numWriters advances. We are waiting on IO here, since failover
		// is continuing to happen if IO is taking too long. If we run out of
		// maxPhysicalLogs, then we will truly block on IO here, but there is
		// nothing that can be done in that case. Note that there is a very rare
		// case that the recordQueue is already empty, and waiting is unnecessary,
		// but so be it.
		for {
			if ww.writers[numWriters-1].w == nil && ww.writers[numWriters-1].createError == nil {
				ww.mu.cond.Wait()
			} else {
				break
			}
			numWriters = ww.mu.nextWriterIndex
		}
		// Invariant: [closeCalledCount, numWriters) have their *LogWriters in the
		// final state (the createError may not be in its final state): nil and
		// will stay nil, or non-nil. Additionally, index numWriters-1 creation
		// has completed, so both the *LogWriter and createError is in its final
		// state.

		// Unlock, so monitoring and switching can continue happening.
		ww.mu.Unlock()
		// Process [closeCalledCount, numWriters).

		// Iterate over everything except the latest writer, i.e.,
		// [closeCalledCount, numWriters-1). These are either non-nil, or will
		// forever stay nil. From the previous invariant, we know that
		// numWriters-1-closeCalledCount >= 0.
		for i := closeCalledCount; i < numWriters-1; i++ {
			w := ww.writers[i].w
			if w != nil {
				// Don't care about the returned error since all the records we relied
				// on this writer for were already successfully written.
				ww.opts.stopper.runAsync(func() {
					_ = w.Close()
				})
			}
		}
		closeCalledCount = numWriters - 1
		ww.mu.Lock()
		numWriters = ww.mu.nextWriterIndex
		if closeCalledCount < numWriters-1 {
			// Haven't processed some non-latest writers. Process them first.
			continue
		}
		// Latest writer, for now. And we have already waited for its creation.
		latestWriterIndex := closeCalledCount
		w := ww.writers[latestWriterIndex].w
		createErr := ww.writers[latestWriterIndex].createError
		// closedLatest and closeErr also protected by ww.mu.
		closedLatest := false
		var closeErr error
		if w == nil && createErr == nil {
			panic("invariant violation")
		}
		closeCalledCount++
		if createErr == nil {
			// INVARIANT: w != nil
			ww.opts.stopper.runAsync(func() {
				// Write to iteration local variable closeErr, since we may write to
				// a different closeErr in a different iteration. We only read this
				// value in the same iteration (below).
				cErr := w.Close()
				ww.mu.Lock()
				closeErr = cErr
				closedLatest = true
				ww.mu.cond.Signal()
				ww.mu.Unlock()
			})
		} else {
			closeErr = createErr
			closedLatest = true
		}
		// We start this inner loop with latestWriterIndex being the latest writer
		// and one of the following cases:
		//
		// A. w != nil and waiting for the goroutine that is closing w.
		// B. w == nil and closedLatest is already true because of creation error,
		// which is reflected in closeErr.
		for !done {
			if !closedLatest {
				// Case A. Will unblock on close or when a newer writer creation
				// happens (or fails).
				ww.mu.cond.Wait()
			}
			if latestWriterIndex < ww.mu.nextWriterIndex-1 {
				// Doesn't matter if closed or not, since no longer the latest.
				break
				// Continue outer for loop.
			}
			// Still the latest writer.
			if closedLatest {
				// Case A has finished close, or case B.
				if closeErr == nil {
					// Case A closed with no error.
					_, numSyncsPopped := ww.q.popAll(nil)
					if numSyncsPopped != 0 {
						// The client requested syncs are required to be popped by the
						// record.LogWriter. The only records we expect to pop now are the
						// tail that did not request a sync, and have been successfully
						// synced implicitly by the record.LogWriter.
						//
						// NB: popAll is not really necessary for correctness. We do this
						// to free memory.
						panic(errors.AssertionFailedf(
							"%d syncs not popped by the record.LogWriter", numSyncsPopped))
					}
					// Exit all loops.
					done = true
					ww.mu.closed = true
				} else {
					// Case A closed with error, or case B. The err is in closeErr.
					//
					// If there are no more slots left, or the queue is empty (an
					// earlier writer may have seen all the records and finally written
					// them), we can be done.
					if len(ww.writers) == int(latestWriterIndex+1) || ww.q.length() == 0 {
						// If there are any records queued, we have no guarantee that they
						// were written, so propagate the error.
						numRecords, _ := ww.q.popAll(closeErr)
						err = closeErr
						if numRecords == 0 {
							err = nil
						}
						// Exit all loops.
						done = true
						ww.mu.closed = true
					}
					// Else, this will eventually not be the latest writer, so continue
					// this inner loop.
				}
			}
			// Else still waiting on case A to close.
		}
	}
	ww.mu.Unlock()
	// Only nil in some unit tests.
	if ww.wm != nil {
		ww.wm.writerClosed()
	}
	return err
}

func (ww *failoverWriter) Metrics() *record.LogWriterMetrics {
	// TODO(sumeer):
	return nil
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
