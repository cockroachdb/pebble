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
	fs      vfs.FS
	dirname string
}

// WALNum is the number of the virtual WAL. It can map to one or more physical
// log files. In standalone mode, it will map to exactly one log file. In
// failover mode, it can map to many log files, which are totally ordered
// (using a dense logIndex).
//
// In general, WAL refers to the virtual WAL, and file refers to a log file.
// The Pebble MANIFEST only knows about virtual WALs and assigns numbers to
// them. Additional mapping to one or more files happens in this package. If a
// WAL maps to multiple files, the source of truth regarding that mapping is
// the contents of the directories.
type WALNum base.DiskFileNum

// logIndex numbers log files within a WAL.
type logIndex uint32

// TODO(sumeer): parsing func. And remove attempts to parse log files outside
// the wal package (including tools).

// makeLogFilename makes a log filename.
func makeLogFilename(wn WALNum, index logIndex) string {
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

	// SyncingFileOptions is the configuration when calling vfs.NewSyncingFile.
	SyncingFileOpts vfs.SyncingFileOptions

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
	List() ([]WALNum, error)
	// Delete deletes all virtual WALs up to highestObsoleteNum. The
	// underlying physical WAL files may be recycled.
	Delete(highestObsoleteNum WALNum) error
	// OpenForRead opens a virtual WAL for read.
	OpenForRead(wn WALNum, strictWALTail bool) (Reader, error)
	// Create creates a new virtual WAL.
	//
	// WALNums passed to successive Create calls must be monotonically
	// increasing, and be greater than any WALNum seen earlier. The caller must
	// close the previous Writer before calling Create.
	Create(wn WALNum) (Writer, error)
	// Stats returns the latest Stats.
	Stats() Stats
	// Close the manager.
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

// Misc temporary notes:
//
// recordQueue size: should we inject fsyncs to keep the memory bounded?
// Doesn't seem necessary for CRDB since there will be frequent fsyncs due to
// raft log appends. Also, it can't grow beyond the memtable size (though
// failing over 64MB, the memtable size, will not be fast).
//
// switch: may need to switch after close is called on wal.Writer.
// monitoring should also impose a minimum delay since last switch to avoid
// too many (say 2 min).
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
	q.buffer = make([]recordQueueEntry, initialBufferLen)
	q.writer = nil
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

// failoverWriter is the implementation of Writer in failover mode.
//
// TODO: add the logic to call switchToNewWriter, by spinning up a goroutine
// that every duration D (say 100ms) reads the latencyAndErrorRecorder of
// nextWriterIndex-1. May situate part of that logic outside the writer since
// there will also be writing to a probe file in primary Dir to decide when to
// switch back, which is better done by the manager.
type failoverWriter struct {
	wm      *failoverManager
	wn      WALNum
	q       recordQueue
	writers [maxPhysicalLogs]logWriterPlus
	mu      struct {
		sync.Mutex
		nextWriterIndex int
	}
}

type logWriterPlus struct {
	w *record.LogWriter
	r latencyAndErrorRecorder
}

var _ Writer = &failoverWriter{}

type recordWriterOpts struct {
	wn WALNum
	// TODO: other things needed when creating a record.LogWriter.
}

func newFailoverWriter(
	opts recordWriterOpts, initialDir Dir, wm *failoverManager,
) (*failoverWriter, error) {
	ww := &failoverWriter{
		wm: wm,
		wn: opts.wn,
	}
	ww.q.init()
	// The initial record.LogWriter creation also happens via a
	// switchToNewWriter since it could block.
	err := ww.switchToNewDir(initialDir)
	if err != nil {
		return nil, err
	}
	return ww, nil
}

func (ww *failoverWriter) Size() uint64 {
	// TODO
	return 0
}

func (ww *failoverWriter) FileSize() uint64 {
	// TODO
	return 0
}

// NB: externally synchronized such that no concurrent calls to WriteRecord.
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

func (ww *failoverWriter) switchToNewDir(dir Dir) error {
	ww.mu.Lock()
	writerIndex := ww.mu.nextWriterIndex
	ww.mu.nextWriterIndex++
	ww.mu.Unlock()
	if writerIndex >= len(ww.writers) {
		return errors.Errorf("exceeded switching limit")
	}
	go func() {
		var w *record.LogWriter
		// TODO: create file. Ongoing file creation latency has to be available in
		// getOngoingLatencyOrErrorForLatestWriter.
		//
		// TODO: create record.LogWriter with writer.doneSyncCallback.
		//
		// TODO: handle error by plumbing through to
		// getOngoingLatencyOrErrorForLatestWriter.
		ww.mu.Lock()
		defer ww.mu.Unlock()
		// Still the latest writer.
		if writerIndex+1 == ww.mu.nextWriterIndex {
			ww.writers[writerIndex].w = w
			ww.q.snapshotAndSwitchWriter(w, func(firstIndex uint32, entries []recordQueueEntry) {
				for i := range entries {
					ps := record.PendingSyncIndex{Index: record.NoSyncIndex}
					if entries[i].opts.Done != nil {
						ps.Index = int64(firstIndex) + int64(i)
					}
					_, err := w.SyncRecordGeneralized(entries[i].p, ps)
					if err != nil {
						// Do nothing? The only non-nil err case we will also surface via
						// getOngoing... so that a switch can be done.
					}
				}
			})
		}
	}()
	return nil
}

func (ww *failoverWriter) getCurDir() Dir {
	// TODO
	return Dir{}
}

func (ww *failoverWriter) doneSyncCallback(doneSync record.PendingSyncIndex, err error) {
	numSyncsPopped := ww.q.pop(uint32(doneSync.Index), err)
	for i := 0; i < numSyncsPopped; i++ {
		// TODO: signal queueSemChan as it is still used by the commit pipeline
		// (though unnecessary).
	}
}

// This is the latency of ongoing IO operation on nextWriterIndex-1.
func (ww *failoverWriter) ongoingLatencyOrErrorForCurDir() (time.Duration, error) {
	// TODO:

	// ww.mu.Lock()
	// latestWriterIndex := ww.mu.nextWriterIndex-1
	// ww.mu.Unlock()
	// Use ww.writers[latestWriterIndex].r
	return 0, nil
}

// Implementation note: getOngoingLatencyOrErrorForLatestWriter and
// switchToNewWriter can be called after Close. So as soon as q is empty, can
// start returning 0 there.
func (ww *failoverWriter) Close() error {
	// TODO
	ww.wm.writerClosed(ww)
	return nil
}

func (ww *failoverWriter) Metrics() *record.LogWriterMetrics {
	// TODO
	return nil
}

// latencyAndErrorRecorder is an implementation of
// record.LatencyAndErrorRecorder.
//
// One per record.LogWriter. Used first when creating the file and then passed
// to the LogWriter for all its operations.
type latencyAndErrorRecorder struct {
	ongoingOperationStart atomic.Uint64
	error                 atomic.Pointer[error]
}

var _ record.LatencyAndErrorRecorder = &latencyAndErrorRecorder{}

func (r *latencyAndErrorRecorder) WriteStart() {
	// TODO
}

func (r *latencyAndErrorRecorder) WriteEnd() {
	// TODO
}

func (r *latencyAndErrorRecorder) SetPermanentError(err error) {
	// TODO
}

func (r *latencyAndErrorRecorder) ongoingLatencyOrError() (time.Duration, error) {
	// TODO
	return 0, nil
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
	getCurDir() Dir
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
	m.prober.init(opts.primary.fs, opts.primary.fs.PathJoin(opts.primary.dirname, "probe-file"), opts.dirProbeInterval)
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

// setWriter should be called with nil when previous writer is closed.
func (m *failoverMonitor) setWriter(w switchableWriter) {
	// TODO
	//
	// TODO: call getCurDir and switch immediately if stale. This can happen if
	// probe indicated primary has become healthy, since the call to
	// getDirForNextWriter.
}

func (m *failoverMonitor) monitorLoop() {
	// TODO
}

type failoverManager struct {
	opts Options
	// TODO: other stuff.

	monitor *failoverMonitor
	ww      *failoverWriter
}

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
	wm.monitor = newFailoverMonitor(fmOpts)
	// TODO: list dirs and assemble a list of all WALNums and corresponding log files.
	return nil
}

var _ Manager = &failoverManager{}

func (wm *failoverManager) List() ([]WALNum, error) {
	// TODO:
	return nil, nil
}

func (wm *failoverManager) Delete(highestObsoleteNum WALNum) error {
	// TODO:
	return nil
}

func (wm *failoverManager) OpenForRead(wn WALNum, strictWALTail bool) (Reader, error) {
	// TODO:
	return nil, nil
}

func (wm *failoverManager) Create(wn WALNum) (Writer, error) {
	if wm.ww != nil {
		panic("previous writer not closed")
	}
	ww, err := newFailoverWriter(recordWriterOpts{wn: wn}, wm.monitor.getDirForNextWriter(), wm)
	if err != nil {
		return nil, err
	}
	wm.ww = ww
	wm.monitor.setWriter(wm.ww)
	return ww, nil
}

func (wm *failoverManager) writerClosed(ww *failoverWriter) {
	if wm.ww != ww {
		panic("writer mismatch")
	}
	wm.ww = nil
	wm.monitor.setWriter(nil)
}

func (wm *failoverManager) Stats() Stats {
	// TODO:
	return Stats{}
}

func (wm *failoverManager) Close() error {
	wm.monitor.close()
	// TODO: close latest writer.
	return nil
}
