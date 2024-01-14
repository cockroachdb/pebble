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

// Dir is usef for storing WAL files.
type Dir struct {
	fs      vfs.FS
	dirname string
}

// WALNum is the number of the virtual WAL.
type WALNum base.DiskFileNum

type Options struct {
	Primary Dir
	// Secondary is used for failover. Optional.
	Secondary Dir

	// Recyling configuration. Only files in the primary dir are recycled.
	MinRecycleLogNum     base.DiskFileNum
	MaxNumRecyclableLogs int

	NoSyncOnClose       bool
	WALBytesPerSync     int
	PreallocateFileSize int
	WALMinSyncInterval  func() time.Duration
	WALFsyncLatency     prometheus.Histogram
	// QueueSemChan is the channel to pop from when popping from queued records
	// that have requested a sync. It's original purpose was to function as a
	// semaphore that prevents the record.LogWriter.flusher.syncQueue from
	// overflowing (which will cause a panic). It is still useful in that role
	// when the WALManager is configured in standalone mode. In failover mode
	// there is no syncQueue, so the pushback into the commit pipeline is
	// unnecessary, but possibly harmless.
	QueueSemChan chan struct{}
}

type WALStats struct {
	ObsoleteFileCount int
	ObsoleteFileSize  uint64
	LiveFileCount     int
	LiveFileSize      uint64
	LiveSize          uint64
}

// WALManager handles all WAL work. It is an interface for now, but if we end
// up with a single implementation for both standalone mode and failover mode,
// we will get rid of the interface.
type WALManager interface {
	// Init ...
	// Also ensures dirs are created and synced.
	Init(o Options) error
	// ListWALs returns the virtual WALs in ascending order.
	ListWALs() ([]WALNum, error)
	// DeleteWALs deletes all virtual WALs up to highestObsoleteNum. The
	// underlying physical WAL files may be recycled.
	DeleteWALs(highestObsoleteNum WALNum) error
	// OpenWALForRead opens a virtual WAL for read.
	OpenWALForRead(wn WALNum, strictWALTail bool) (WALReader, error)
	// CreateWAL ...
	//
	// WALNums passed to successive CreateWAL calls must be monotonically
	// increasing, and be greater than any WALNum seen earlier.
	CreateWAL(wn WALNum) (WALWriter, error)

	GetStats() WALStats
}

// SyncOptions has non-nil Done and Err when fsync is requested, else both are
// nil.
type SyncOptions struct {
	Done *sync.WaitGroup
	Err  *error
}

// WALWriter writes to a virtual WAL. When a WALWriter is in standalone mode,
// it maps to a single record.LogWriter. When in failover mode, it can
// failover across multiple physical log files.
type WALWriter interface {
	// Size based on writes.
	Size() uint64
	// FileSize is the size of the file(s) underlying this WAL. FileSize
	// >= Size because of recycling and failover. This is an estimate.
	FileSize() uint64
	// WriteRecord writes a complete record. The record is asynchronously
	// persisted to the underlying writer. If SyncOptions.Done != nil, the wait
	// group will be notified when durability is guaranteed or an error has
	// occurred (set in SyncOptions.Err). External synchronisation is provided
	// by commitPipeline.mu.
	WriteRecord(p []byte, opts SyncOptions) error
	// Close the writer.
	Close() error
	// Metrics must be called after Close. The callee will no longer modify the
	// returned LogWriterMetrics.
	Metrics() *record.LogWriterMetrics
}

// WALReader reads a virtual WAL.
type WALReader interface {
	NextRecord() (io.Reader, error)
	LogicalOffset() int64
	Close() error
}

// TODO: parsing func. And remove attempts to parse log files outside the
// wal package (including tools).
func makeLogFilename(wn WALNum, index uint32) string {
	if index == 0 {
		// Use a backward compatible name, for simplicity.
		return base.MakeFilename(base.FileTypeLog, base.DiskFileNum(wn))
	}
	return fmt.Sprintf("%s-%03d.logf", base.DiskFileNum(wn).String(), index)
}

// Misc temporary notes:
//
// recordQueue size: should we inject fsyncs to keep the memory bounded?
// Doesn't seem necessary for CRDB since there will be frequent fsyncs due to
// raft log appends. Also, it can't grow beyond the memtable size (though
// failing over 64MB, the memtable size, will not be fast).

// switch: may need to switch after close is called on wal.logwriter.
// monitoring should also impose a minimum delay since last switch to avoid
// too many (say 2 min).
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

// recordQueueEntry is an entry in recordQueue.
type recordQueueEntry struct {
	p    []byte
	opts SyncOptions
}

const initialBufferLen = 8192

// recordQueue is a variable size single producer multiple consumer queue. It
// is not lock-free, but most operations only need mu.RLock. It needs a mutex
// is to grow the size, since there is no upper bound on the number of queued
// records (these are all the records that are not synced). Additionally, it
// needs a mutex to atomically grab a snapshot of the queued records and
// provide them to a new LogWriter that is being switched to.
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

// Maximum number of physical logs when writing a virtual WAL. Arbitrarily
// chosen value. Setting this to 2 will not simplify the code. We make this a
// constant since we want a fixed size array for walWriter.writers.
const maxPhysicalLogs = 20

// walWriter is the implementation of WALWriter in failover mode.
//
// TODO: add the logic to call switchToNewWriter, by spinning up a goroutine
// that every duration D (say 100ms) reads the latencyAndErrorRecorder of
// nextWriterIndex-1. May situate part of that logic outside the walWriter since
// there will also be writing to a probe file in primary Dir to decide when to
// switch back, which is better done by the manager.
type walWriter struct {
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

var _ WALWriter = &walWriter{}

type recordWriterOpts struct {
	wn WALNum
	// TODO: other things needed when creating a record.LogWriter.
}

func newWALWriter(opts recordWriterOpts, initialDir Dir) (*walWriter, error) {
	ww := &walWriter{
		wn: opts.wn,
	}
	ww.q.init()
	// The initial record.LogWriter creation also happens via a
	// switchToNewWriter since it could block.
	err := ww.switchToNewWriter(initialDir)
	if err != nil {
		return nil, err
	}
	return ww, nil
}

func (ww *walWriter) Size() uint64 {
	// TODO
	return 0
}

func (ww *walWriter) FileSize() uint64 {
	// TODO
	return 0
}

// NB: externally synchronized such that no concurrent calls to WriteRecord.
func (ww *walWriter) WriteRecord(p []byte, opts SyncOptions) error {
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

func (ww *walWriter) switchToNewWriter(dir Dir) error {
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
		// TODO: create record.LogWriter with walWriter.doneSyncCallback.
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

func (ww *walWriter) doneSyncCallback(doneSync record.PendingSyncIndex, err error) {
	numSyncsPopped := ww.q.pop(uint32(doneSync.Index), err)
	for i := 0; i < numSyncsPopped; i++ {
		// TODO: signal queueSemChan as it is still used by the commit pipeline
		// (though unnecessary).
	}
}

// This is the latency of ongoing IO operation on nextWriterIndex-1.
func (ww *walWriter) getOngoingLatencyOrErrorForLatestWriter() (time.Duration, error) {
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
func (ww *walWriter) Close() error {
	// TODO
	return nil
}

func (ww *walWriter) Metrics() *record.LogWriterMetrics {
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

func (r *latencyAndErrorRecorder) shouldFailover(latencyThreshold time.Duration) bool {
	// TODO
	return false
}

// dirProber. One of these in WALManager to probe the primary dir.
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
