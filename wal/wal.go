// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package wal

import (
	"fmt"
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

// TODO(sumeer): write a high-level comment describing the approach.

// Dir is used for storing log files.
type Dir struct {
	FS      vfs.FS
	Dirname string
}

// NumWAL is the number of the virtual WAL. It can map to one or more physical
// log files. In standalone mode, it will map to exactly one log file. In
// failover mode, it can map to many log files, which are totally ordered
// (using a dense logNameIndex).
//
// In general, WAL refers to the virtual WAL, and file refers to a log file.
// The Pebble MANIFEST only knows about virtual WALs and assigns numbers to
// them. Additional mapping to one or more files happens in this package. If a
// WAL maps to multiple files, the source of truth regarding that mapping is
// the contents of the directories.
type NumWAL base.DiskFileNum

// logNameIndex numbers log files within a WAL.
type logNameIndex uint32

// TODO(sumeer): parsing func. And remove attempts to parse log files outside
// the wal package (including tools).

// makeLogFilename makes a log filename.
func makeLogFilename(wn NumWAL, index logNameIndex) string {
	if index == 0 {
		// Use a backward compatible name, for simplicity.
		return base.MakeFilename(base.FileTypeLog, base.DiskFileNum(wn))
	}
	return fmt.Sprintf("%s-%03d.log", base.DiskFileNum(wn).String(), index)
}

// Options provides configuration for the Manager.
type Options struct {
	// Primary dir for storing WAL files.
	Primary Dir
	// Secondary is used for failover. Optional.
	Secondary Dir

	// Recyling configuration. Only files in the primary dir are recycled.

	// MinRecycleLogNum is the minimum log file number that is allowed to be
	// recycled. Log file numbers smaller than this will be deleted. This is
	// used to prevent recycling a log written by a previous instance of the DB
	// which may not have had log recycling enabled.
	MinRecycleLogNum base.DiskFileNum
	// MaxNumRecyclableLogs is the maximum number of log files to maintain for
	// recycling.
	MaxNumRecyclableLogs int

	// Configuration for calling vfs.NewSyncingFile.

	// NoSyncOnClose is documented in SyncingFileOptions.
	NoSyncOnClose bool
	// BytesPerSync is documented in SyncingFileOptions.
	BytesPerSync int
	// PreallocateSize is documented in SyncingFileOptions.
	PreallocateSize func() int

	// MinSyncInterval is documented in Options.WALMinSyncInterval.
	MinSyncInterval func() time.Duration
	// FsyncLatency records fsync latency. This doesn't differentiate between
	// fsyncs on the primary and secondary dir.
	//
	// TODO(sumeer): consider separating out into two histograms.
	FsyncLatency prometheus.Histogram
	// QueueSemChan is the channel to pop from when popping from queued records
	// that have requested a sync. It's original purpose was to function as a
	// semaphore that prevents the record.LogWriter.flusher.syncQueue from
	// overflowing (which will cause a panic). It is still useful in that role
	// when the WALManager is configured in standalone mode. In failover mode
	// there is no syncQueue, so the pushback into the commit pipeline is
	// unnecessary, but possibly harmless.
	QueueSemChan chan struct{}
	// Logger for logging.
	Logger base.Logger
}

// Stats exposes stats used in Pebble metrics.
type Stats struct {
	// ObsoleteFileCount is the number of obsolete log files.
	ObsoleteFileCount int
	// ObsoleteFileSize is the total size of obsolete log files.
	ObsoleteFileSize uint64
	// LiveFileCount is the number of live log files.
	LiveFileCount int
	// LiveFileSize is the total size of live log files. This can be higher than
	// LiveSize due to log recycling (a live log file may be larger than the
	// size used in its latest incarnation), or failover (resulting in multiple
	// log files containing the same records).
	LiveFileSize uint64
	// LiveSize is the total size of the live data in log files.
	LiveSize uint64
}

// Manager handles all WAL work.
//
// It is an interface for now, but if we end up with a single implementation
// for both standalone mode and failover mode, we will get rid of the
// interface.
type Manager interface {
	// Init initializes the Manager.
	//
	// Implementation notes:
	// - lists and stats the directories, so that Stats are up to date (assuming
	//   no obsolete files yet), and the list of WALs and their constituent log
	//   files is initialized.
	// - ensures dirs are created and synced.
	Init(o Options) error
	// List returns the virtual WALs in ascending order.
	List() ([]NumWAL, error)
	// Delete deletes all virtual WALs up to highestObsoleteNum. The
	// underlying physical WAL files may be recycled.
	Delete(highestObsoleteNum NumWAL) error
	// OpenForRead opens a virtual WAL for read.
	OpenForRead(wn NumWAL, strictWALTail bool) (Reader, error)
	// Create creates a new virtual WAL.
	//
	// WALNums passed to successive Create calls must be monotonically
	// increasing, and be greater than any NumWAL seen earlier. The caller must
	// close the previous Writer before calling Create.
	Create(wn NumWAL) (Writer, error)
	// Stats returns the latest Stats.
	Stats() Stats
	// Close the manager.
	// REQUIRES: Writers and Readers have already been closed.
	Close() error
}

// SyncOptions has non-nil Done and Err when fsync is requested, else both are
// nil.
type SyncOptions struct {
	Done *sync.WaitGroup
	Err  *error
}

// Writer writes to a virtual WAL. A Writer in standalone mode maps to a
// single record.LogWriter. In failover mode, it can failover across multiple
// physical log files.
type Writer interface {
	// Size based on writes.
	Size() uint64
	// FileSize is the size of the file(s) underlying this WAL. FileSize
	// >= Size because of recycling and failover. This is an estimate.
	FileSize() uint64
	// WriteRecord writes a complete record. The record is asynchronously
	// persisted to the underlying writer. If SyncOptions.Done != nil, the wait
	// group will be notified when durability is guaranteed or an error has
	// occurred (set in SyncOptions.Err). External synchronisation provided by
	// commitPipeline.mu guarantees that WriteRecord calls are serialized.
	WriteRecord(p []byte, opts SyncOptions) error
	// Close the writer.
	Close() error
	// Metrics must be called after Close. The callee will no longer modify the
	// returned LogWriterMetrics.
	Metrics() *record.LogWriterMetrics
}

// Reader reads a virtual WAL.
type Reader interface {
	// NextRecord returns the next record, or error.
	NextRecord() (io.Reader, error)
	// LogicalOffset is the monotonically increasing offset in the WAL. When the
	// WAL corresponds to a single log file, this is the offset in that log
	// file.
	LogicalOffset() int64
	// Close the reader.
	Close() error
}

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
// switch: may need to switch after close is called on wal.Writer.
// monitoring should also impose a minimum delay since last switch to avoid
// too many (say 2 min). Delay should be after 2 switches:
// - switched from primary to secondary and back quickly. Remaining slower.
// - switched from secondary to primary ...
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
// Probe: Do full lifecycle. Delete. Create. Write 10kb. Fsync.
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

// Pops all entries. Must be called only after the last push returns.
func (q *recordQueue) popAll() (numSyncsPopped int) {
	ht := q.headTail.Load()
	h, t := unpackHeadTail(ht)
	n := int(h - t)
	if n == 0 {
		return 0
	}
	return q.pop(h-1, nil)
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
const maxPhysicalLogs = 20

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
		nextWriterIndex logNameIndex
		closed          bool
	}
}

type logWriterAndRecorder struct {
	// This may never become non-nil, if when the LogWriter was finally created,
	// it was no longer the latest writer.
	w *record.LogWriter
	r latencyAndErrorRecorder
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
}

func newFailoverWriter(
	opts failoverWriterOpts, initialDir Dir, wm *failoverManager,
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
func (ww *failoverWriter) switchToNewDir(dir Dir) error {
	ww.mu.Lock()
	if ww.mu.closed {
		return nil
	}
	writerIndex := ww.mu.nextWriterIndex
	ww.mu.nextWriterIndex++
	ww.mu.Unlock()

	if int(writerIndex) >= len(ww.writers) {
		return errors.Errorf("exceeded switching limit")
	}
	go func() {
		// TODO(sumeer): recycling of logs.
		filename := dir.FS.PathJoin(dir.Dirname, makeLogFilename(ww.opts.wn, writerIndex))
		recorderAndWriter := &ww.writers[writerIndex].r
		recorderAndWriter.writeStart()
		file, err := dir.FS.Create(filename)
		recorderAndWriter.writeEnd(err)
		// TODO(sumeer): should we fatal if primary dir? At some point it is better
		// to fatal instead of continuing to failover.
		// base.MustExist(dir.FS, filename, ww.opts.logger, err)
		if err != nil {
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
			if writerIndex+1 != ww.mu.nextWriterIndex {
				// Not the latest writer.
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
						// TODO(sumeer): log periodically. The err will also
						// surface via the latencyAndErrorRecorder, so if a
						// switch is possible, it will be done.
					}
				}
			})
			return false
		}()
		if closeWriter {
			// Never wrote anything to this writer so don't care about the
			// returned error.
			go w.Close()
		}
	}()
	return nil
}

// doneSyncCallback is the record.ExternalSyncQueueCallback called by
// record.LogWriter.
func (ww *failoverWriter) doneSyncCallback(doneSync record.PendingSyncIndex, err error) {
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
// doneSyncCallback can be called anytime after Close returns since there could be
// stuck writes that finish arbitrarily later.
func (ww *failoverWriter) Close() error {
	// [0, closeCalledCount) have had LogWriter.Close called (though may not
	// have finished).
	closeCalledCount := logNameIndex(0)
	done := false
	ww.mu.Lock()
	// Every iteration starts and ends with the mutex held.
	for !done {
		numWriters := ww.mu.nextWriterIndex
		// Unlock, so monitoring and switching can continue happening.
		ww.mu.Unlock()
		// Iterate over everything except the latest writer. These are either
		// non-nil, or will forever stay nil.
		for i := closeCalledCount; i < numWriters-1; i++ {
			w := ww.writers[i].w
			if w != nil {
				// Don't care about the returned error since all the records we relied
				// on this writer for were already successfully written.
				go w.Close()
			}
		}
		if closeCalledCount > numWriters-1 {
			panic("invariant violation")
		}
		closeCalledCount = numWriters - 1
		ww.mu.Lock()
		numWriters = ww.mu.nextWriterIndex
		if closeCalledCount < numWriters-1 {
			// Haven't processed some non-latest writers. Process them first.
			continue
		}
		// Latest writer, for now.
		latestWriterIndex := closeCalledCount
		w := ww.writers[latestWriterIndex].w
		var err error
		closingLatest := false
		closedLatest := false
		if w != nil {
			closingLatest = true
			closeCalledCount++
			go func() {
				err = w.Close()
				closedLatest = true
				ww.mu.Lock()
				ww.mu.cond.Signal()
				ww.mu.Unlock()
			}()
		}
		// Inner loop that continues while latestWriterIndex is the latest writer,
		// and (the writer there is uninitialized, or successfully closed by the
		// preceding code).
		for {
			// Will wait until latestWriterIndex closes, or the latest writer is
			// initialized, or a new writer becomes the latest writer.
			ww.mu.cond.Wait()
			if latestWriterIndex < ww.mu.nextWriterIndex-1 {
				// Doesn't matter if closed or not, since no longer the latest.
				break
				// Will continue outer for loop.
			}
			if closingLatest {
				if closedLatest {
					done = true
					ww.mu.closed = true
					numSyncsPopped := ww.q.popAll()
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
					break
				} else {
					// Nothing happened, so continue this inner loop, to wait for
					// closedLatest.
					continue
				}
			} else {
				// !closingLatest, because latest writer had not been initialized.
				// It may be initialized now.
				break
				// Will continue outer for loop.
			}
		}
	}
	ww.mu.Unlock()
	ww.wm.writerClosed()
	return nil
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

// dirProber, to probe the primary dir.
type dirProber struct {
	fs       vfs.FS
	filename string
	interval time.Duration

	mu struct {
		sync.Mutex
		enabledCond     *sync.Cond
		enabled         bool
		close           bool
		closed          chan struct{}
		history         [probeHistoryLength]time.Duration
		firstProbeIndex int
		nextProbeIndex  int
	}
}

const probeHistoryLength = 128

// Large value.
const failedProbeDuration = 24 * 60 * 60 * time.Second

func (p *dirProber) init(fs vfs.FS, filename string, interval time.Duration) {
	*p = dirProber{
		fs:       fs,
		filename: filename,
		interval: interval,
	}
	p.mu.enabledCond = sync.NewCond(&p.mu)
	p.mu.closed = make(chan struct{})
	go p.probeLoop()
}

func (p *dirProber) probeLoop() {
	for {
		// Wait for enabled or closed.
		close := func() bool {
			p.mu.Lock()
			defer p.mu.Unlock()
			for {
				if p.mu.enabled {
					return false
				}
				if p.mu.close {
					return true
				}
				p.mu.enabledCond.Wait()
			}
		}()
		if close {
			p.mu.closed <- struct{}{}
			return
		}
		// Probe.
		//
		// NB: If disableProbing is followed soon by enableProbing such that
		// probeLoop does not notice the disabling, the disabling will be a noop.
		// This is acceptable.
		done := false
		for !done {
			var probeDur time.Duration
			// TODO: delete, create, write, sync.

			p.mu.Lock()
			if p.mu.enabled {
				nextIndex := p.mu.nextProbeIndex % probeHistoryLength
				p.mu.history[nextIndex] = probeDur
				firstIndex := p.mu.firstProbeIndex % probeHistoryLength
				if firstIndex == nextIndex {
					// Wrapped around
					p.mu.firstProbeIndex++
				}
				p.mu.nextProbeIndex++
			} else {
				p.mu.firstProbeIndex = 0
				p.mu.nextProbeIndex = 0
				done = true
			}
			p.mu.Unlock()
			if !done {
				time.Sleep(p.interval)
			}
		}
	}
}

func (p *dirProber) enableProbing() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.enabled = true
	p.mu.enabledCond.Signal()
}

func (p *dirProber) disableProbing() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.enabled = false
}

func (p *dirProber) close() {
	p.disableProbing()

	p.mu.Lock()
	p.mu.close = true
	p.mu.enabledCond.Signal()
	p.mu.Unlock()

	<-p.mu.closed
}

func (p *dirProber) getMeanMax(interval time.Duration) (time.Duration, time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	numSamples := p.mu.nextProbeIndex - p.mu.firstProbeIndex
	samplesNeeded := int((interval + p.interval - 1) / p.interval)
	if samplesNeeded == 0 {
		panic("interval is too short")
	} else if samplesNeeded > probeHistoryLength {
		panic("interval is too long")
	}
	if samplesNeeded < numSamples {
		return failedProbeDuration, failedProbeDuration
	}
	offset := numSamples - samplesNeeded
	var sum, max time.Duration
	for i := p.mu.firstProbeIndex + offset; i < p.mu.nextProbeIndex; i++ {
		sampleDur := p.mu.history[i%probeHistoryLength]
		sum += sampleDur
		if max < sampleDur {
			max = sampleDur
		}
	}
	mean := sum / time.Duration(samplesNeeded)
	return mean, max
}

type failoverMonitorOptions struct {
	primary   Dir
	secondary Dir

	dirProbeInterval             time.Duration
	healthyProbeLatencyThreshold time.Duration

	unhealthySamplingInterval          time.Duration
	unhealthyOperationLatencyThreshold time.Duration
}

// switchableWriter is a subset of failoverWriter needed by failoverMonitor.
type switchableWriter interface {
	switchToNewDir(dir Dir) error
	ongoingLatencyOrErrorForCurDir() (time.Duration, error)
}

// failoverMonitor monitors ...
type failoverMonitor struct {
	opts failoverMonitorOptions

	prober dirProber
	mu     struct {
		sync.Mutex
		useSecondary bool
		writer       switchableWriter
	}
}

func newFailoverMonitor(opts failoverMonitorOptions) *failoverMonitor {
	m := &failoverMonitor{
		opts: opts,
	}
	m.prober.init(opts.primary.FS,
		opts.primary.FS.PathJoin(opts.primary.Dirname, "probe-file"), opts.dirProbeInterval)
	return m
}

func (m *failoverMonitor) close() {
	m.prober.close()
	// TODO: close monitorLoop.
}

func (m *failoverMonitor) getDirForNextWriter() Dir {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.mu.useSecondary {
		return m.opts.secondary
	}
	return m.opts.primary
}

// Called when previous writer is closed
func (m *failoverMonitor) noWriter() {
	// TODO
	//
	// TODO: call getCurDir and switch immediately if stale. This can happen if
	// probe indicated primary has become healthy, since the call to
	// getDirForNextWriter.
}

// writerCreateFunc is allowed to return nil.
func (m *failoverMonitor) newWriter(writerCreateFunc func(dir Dir) switchableWriter) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.mu.writer != nil {
		panic("previous writer not closed")
	}
	dir := m.opts.primary
	if m.mu.useSecondary {
		dir = m.opts.secondary
	}
	m.mu.writer = writerCreateFunc(dir)
}

func (m *failoverMonitor) monitorLoop() {
	// TODO
}

type failoverManager struct {
	opts Options
	// TODO(jackson/sumeer): read-path etc.

	monitor *failoverMonitor
}

var _ Manager = &failoverManager{}

// Init implements Manager.
func (wm *failoverManager) Init(o Options) error {
	fmOpts := failoverMonitorOptions{
		primary:   o.Primary,
		secondary: o.Secondary,
		// TODO(sumeer): make configurable.
		dirProbeInterval:                   time.Second,
		healthyProbeLatencyThreshold:       100 * time.Millisecond,
		unhealthySamplingInterval:          100 * time.Millisecond,
		unhealthyOperationLatencyThreshold: 200 * time.Millisecond,
	}
	monitor := newFailoverMonitor(fmOpts)
	// TODO(jackson): list dirs and assemble a list of all WALNums and
	// corresponding log files.

	*wm = failoverManager{
		opts:    o,
		monitor: monitor,
	}
	return nil
}

// List implements Manager.
func (wm *failoverManager) List() ([]NumWAL, error) {
	// TODO(jackson):
	return nil, nil
}

// Delete implements Manager.
func (wm *failoverManager) Delete(highestObsoleteNum NumWAL) error {
	// TODO(sumeer):
	return nil
}

// OpenForRead implements Manager.
func (wm *failoverManager) OpenForRead(wn NumWAL, strictWALTail bool) (Reader, error) {
	// TODO(jackson):
	return nil, nil
}

// Create implements Manager.
func (wm *failoverManager) Create(wn NumWAL) (Writer, error) {
	fwOpts := failoverWriterOpts{
		wn:              wn,
		logger:          wm.opts.Logger,
		noSyncOnClose:   wm.opts.NoSyncOnClose,
		bytesPerSync:    wm.opts.BytesPerSync,
		preallocateSize: wm.opts.PreallocateSize,
		minSyncInterval: wm.opts.MinSyncInterval,
		fsyncLatency:    wm.opts.FsyncLatency,
		queueSemChan:    wm.opts.QueueSemChan,
	}
	var err error
	var ww *failoverWriter
	writerCreateFunc := func(dir Dir) switchableWriter {
		ww, err = newFailoverWriter(fwOpts, wm.monitor.getDirForNextWriter(), wm)
		if err != nil {
			return nil
		}
		return ww
	}
	wm.monitor.newWriter(writerCreateFunc)
	return ww, err
}

func (wm *failoverManager) writerClosed() {
	wm.monitor.noWriter()
}

// Stats implements Manager.
func (wm *failoverManager) Stats() Stats {
	// TODO(sumeer):
	return Stats{}
}

// Close implements Manager.
func (wm *failoverManager) Close() error {
	wm.monitor.close()
	return nil
}

// TODO(sumeer):
// - clean shutdown of all goroutines.
//   - waiting for all LogWriters to close.
