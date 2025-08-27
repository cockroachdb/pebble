// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package record

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2/internal/humanize"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/cockroachdb/pebble/v2/vfs/errorfs"
	"github.com/cockroachdb/pebble/v2/vfs/vfstest"
	"github.com/prometheus/client_golang/prometheus"
	prometheusgo "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

type syncErrorFile struct {
	vfs.File
	err error
}

func (f syncErrorFile) Sync() error {
	return f.err
}

func TestSyncQueue(t *testing.T) {
	var q syncQueue
	var closed atomic.Bool

	var flusherWG sync.WaitGroup
	flusherWG.Add(1)
	go func() {
		defer flusherWG.Done()
		for {
			if closed.Load() {
				return
			}
			head, tail, _ := q.load()
			q.pop(head, tail, nil, nil)
		}
	}()

	var commitMu sync.Mutex
	var doneWG sync.WaitGroup
	for i := 0; i < SyncConcurrency; i++ {
		doneWG.Add(1)
		go func(i int) {
			defer doneWG.Done()
			for j := 0; j < 1000; j++ {
				wg := &sync.WaitGroup{}
				wg.Add(1)
				// syncQueue is a single-producer, single-consumer queue. We need to
				// provide mutual exclusion on the producer side.
				commitMu.Lock()
				q.push(wg, new(error))
				commitMu.Unlock()
				wg.Wait()
			}
		}(i)
	}
	doneWG.Wait()

	closed.Store(true)
	flusherWG.Wait()
}

func TestFlusherCond(t *testing.T) {
	var mu sync.Mutex
	var c flusherCond
	var closed bool

	psq := &pendingSyncsWithSyncQueue{}
	c.init(&mu, psq)
	q := &psq.syncQueue

	var flusherWG sync.WaitGroup
	flusherWG.Add(1)
	go func() {
		defer flusherWG.Done()

		mu.Lock()
		defer mu.Unlock()

		for {
			for {
				if closed {
					return
				}
				if !q.empty() {
					break
				}
				c.Wait()
			}

			head, tail, _ := q.load()
			q.pop(head, tail, nil, nil)
		}
	}()

	var commitMu sync.Mutex
	var doneWG sync.WaitGroup
	// NB: we're testing with low concurrency here, because what we want to
	// stress is that signalling of the flusherCond works
	// correctly. Specifically, we want to make sure that a signal is "lost",
	// causing the test to wedge.
	for i := 0; i < 2; i++ {
		doneWG.Add(1)
		go func(i int) {
			defer doneWG.Done()
			for j := 0; j < 10000; j++ {
				wg := &sync.WaitGroup{}
				wg.Add(1)
				// syncQueue is a single-producer, single-consumer queue. We need to
				// provide mutual exclusion on the producer side.
				commitMu.Lock()
				q.push(wg, new(error))
				commitMu.Unlock()
				c.Signal()
				wg.Wait()
			}
		}(i)
	}
	doneWG.Wait()

	mu.Lock()
	closed = true
	c.Signal()
	mu.Unlock()
	flusherWG.Wait()
}

func TestSyncError(t *testing.T) {
	mem := vfs.NewMem()
	f, err := mem.Create("log", vfs.WriteCategoryUnspecified)
	require.NoError(t, err)

	injectedErr := errors.New("injected error")
	w := NewLogWriter(syncErrorFile{f, injectedErr}, 0, LogWriterConfig{
		WALFsyncLatency:     prometheus.NewHistogram(prometheus.HistogramOpts{}),
		WriteWALSyncOffsets: func() bool { return false },
	})

	syncRecord := func() {
		var syncErr error
		var syncWG sync.WaitGroup
		syncWG.Add(1)
		_, err = w.SyncRecord([]byte("hello"), &syncWG, &syncErr)
		require.NoError(t, err)
		syncWG.Wait()
		if injectedErr != syncErr {
			t.Fatalf("expected %v but found %v", injectedErr, syncErr)
		}
	}
	// First waiter receives error.
	syncRecord()
	// All subsequent waiters also receive the error.
	syncRecord()
	syncRecord()
}

type syncFile struct {
	writePos atomic.Int64
	syncPos  atomic.Int64
	buffer   bytes.Buffer
}

func (f *syncFile) Write(buf []byte) (int, error) {
	n := len(buf)
	f.writePos.Add(int64(n))
	f.buffer.Write(buf)
	return n, nil
}

func (f *syncFile) Sync() error {
	f.syncPos.Store(f.writePos.Load())
	return nil
}

func TestSyncRecord(t *testing.T) {
	f := &syncFile{}
	w := NewLogWriter(f, 0, LogWriterConfig{
		WALFsyncLatency:     prometheus.NewHistogram(prometheus.HistogramOpts{}),
		WriteWALSyncOffsets: func() bool { return false },
	})

	var syncErr error
	for i := 0; i < 100000; i++ {
		var syncWG sync.WaitGroup
		syncWG.Add(1)
		offset, err := w.SyncRecord([]byte("hello"), &syncWG, &syncErr)
		require.NoError(t, err)
		syncWG.Wait()
		require.NoError(t, syncErr)
		if v := f.writePos.Load(); offset != v {
			t.Fatalf("expected write pos %d, but found %d", offset, v)
		}
		if v := f.syncPos.Load(); offset != v {
			t.Fatalf("expected sync pos %d, but found %d", offset, v)
		}
	}
}

func TestSyncRecordWithSignalChan(t *testing.T) {
	f := &syncFile{}
	semChan := make(chan struct{}, 5)
	for i := 0; i < cap(semChan); i++ {
		semChan <- struct{}{}
	}
	w := NewLogWriter(f, 0, LogWriterConfig{
		WALFsyncLatency:     prometheus.NewHistogram(prometheus.HistogramOpts{}),
		QueueSemChan:        semChan,
		WriteWALSyncOffsets: func() bool { return false },
	})
	require.Equal(t, cap(semChan), len(semChan))
	var syncErr error
	for i := 0; i < 5; i++ {
		var syncWG sync.WaitGroup
		syncWG.Add(1)
		_, err := w.SyncRecord([]byte("hello"), &syncWG, &syncErr)
		require.NoError(t, err)
		syncWG.Wait()
		require.NoError(t, syncErr)
		// The waitgroup is released before the channel is read, so wait if
		// necessary.
		require.Eventually(t, func() bool {
			return cap(semChan)-(i+1) == len(semChan)
		}, 10*time.Second, time.Millisecond)
	}
}

type fakeTimer struct {
	f func()
}

func (t *fakeTimer) Reset(d time.Duration) bool {
	return false
}

func (t *fakeTimer) Stop() bool {
	return false
}

func try(initialSleep, maxTotalSleep time.Duration, f func() error) error {
	totalSleep := time.Duration(0)
	for d := initialSleep; ; d *= 2 {
		time.Sleep(d)
		totalSleep += d
		if err := f(); err == nil || totalSleep >= maxTotalSleep {
			return err
		}
	}
}

func TestMinSyncInterval(t *testing.T) {
	const minSyncInterval = 100 * time.Millisecond

	f := &syncFile{}
	w := NewLogWriter(f, 0, LogWriterConfig{
		WALMinSyncInterval: func() time.Duration {
			return minSyncInterval
		},
		WALFsyncLatency:     prometheus.NewHistogram(prometheus.HistogramOpts{}),
		WriteWALSyncOffsets: func() bool { return false },
	})

	var timer fakeTimer
	w.afterFunc = func(d time.Duration, f func()) syncTimer {
		if d != minSyncInterval {
			t.Fatalf("expected minSyncInterval %s, but found %s", minSyncInterval, d)
		}
		timer.f = f
		timer.Reset(d)
		return &timer
	}

	syncRecord := func(n int) *sync.WaitGroup {
		wg := &sync.WaitGroup{}
		wg.Add(1)
		_, err := w.SyncRecord(bytes.Repeat([]byte{'a'}, n), wg, new(error))
		require.NoError(t, err)
		return wg
	}

	// Sync one record which will cause the sync timer to kick in.
	syncRecord(1).Wait()

	startWritePos := f.writePos.Load()
	startSyncPos := f.syncPos.Load()

	// Write a bunch of large records. The sync position should not change
	// because we haven't triggered the timer. But note that the writes should
	// not block either even though syncing isn't being done.
	var wg *sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg = syncRecord(10000)
		if v := f.syncPos.Load(); startSyncPos != v {
			t.Fatalf("expected syncPos %d, but found %d", startSyncPos, v)
		}
		// NB: we can't use syncQueue.load() here as that will return 0,0 while the
		// syncQueue is blocked.
		syncQ := w.flusher.pendingSyncs.(*pendingSyncsWithSyncQueue)
		head, tail := syncQ.unpack(syncQ.headTail.Load())
		waiters := head - tail
		if waiters != uint32(i+1) {
			t.Fatalf("expected %d waiters, but found %d", i+1, waiters)
		}
	}

	err := try(time.Millisecond, 5*time.Second, func() error {
		v := f.writePos.Load()
		if v > startWritePos {
			return nil
		}
		return errors.Errorf("expected writePos > %d, but found %d", startWritePos, v)
	})
	require.NoError(t, err)

	// Fire the timer, and then wait for the last record to sync.
	timer.f()
	wg.Wait()

	if w, s := f.writePos.Load(), f.syncPos.Load(); w != s {
		t.Fatalf("expected syncPos %d, but found %d", s, w)
	}
}

func TestMinSyncIntervalClose(t *testing.T) {
	const minSyncInterval = 100 * time.Millisecond

	f := &syncFile{}
	w := NewLogWriter(f, 0, LogWriterConfig{
		WALMinSyncInterval: func() time.Duration {
			return minSyncInterval
		},
		WALFsyncLatency:     prometheus.NewHistogram(prometheus.HistogramOpts{}),
		WriteWALSyncOffsets: func() bool { return false },
	})

	var timer fakeTimer
	w.afterFunc = func(d time.Duration, f func()) syncTimer {
		if d != minSyncInterval {
			t.Fatalf("expected minSyncInterval %s, but found %s", minSyncInterval, d)
		}
		timer.f = f
		timer.Reset(d)
		return &timer
	}

	syncRecord := func(n int) *sync.WaitGroup {
		wg := &sync.WaitGroup{}
		wg.Add(1)
		_, err := w.SyncRecord(bytes.Repeat([]byte{'a'}, n), wg, new(error))
		require.NoError(t, err)
		return wg
	}

	// Sync one record which will cause the sync timer to kick in.
	syncRecord(1).Wait()

	// Syncing another record will not complete until the timer is fired OR the
	// writer is closed.
	wg := syncRecord(1)
	require.NoError(t, w.Close())
	wg.Wait()
}

type syncFileWithWait struct {
	f       syncFile
	writeWG sync.WaitGroup
	syncWG  sync.WaitGroup
}

func (f *syncFileWithWait) Write(buf []byte) (int, error) {
	f.writeWG.Wait()
	return f.f.Write(buf)
}

func (f *syncFileWithWait) Sync() error {
	f.syncWG.Wait()
	return f.f.Sync()
}

func TestMetricsWithoutSync(t *testing.T) {
	f := &syncFileWithWait{}
	f.writeWG.Add(1)
	w := NewLogWriter(f, 0, LogWriterConfig{
		WALFsyncLatency:     prometheus.NewHistogram(prometheus.HistogramOpts{}),
		WriteWALSyncOffsets: func() bool { return false },
	})
	offset, err := w.SyncRecord([]byte("hello"), nil, nil)
	require.NoError(t, err)
	const recordSize = 16
	require.EqualValues(t, recordSize, offset)
	// We have 512KB of buffer capacity, and 5 bytes + overhead = 16 bytes for
	// each record. Write 28 * 1024 records to fill it up to 87.5%. This
	// constitutes ~14 blocks (each 32KB).
	const numRecords = 28 << 10
	for i := 0; i < numRecords; i++ {
		_, err = w.SyncRecord([]byte("hello"), nil, nil)
		require.NoError(t, err)
	}
	// Unblock the flush loop. It will run once or twice to write these blocks,
	// plus may run one more time due to the Close, so up to 3 runs. So ~14
	// blocks flushed over up to 3 runs.
	f.writeWG.Done()
	w.Close()
	m := w.Metrics()
	// Mean is >= 4 filled blocks.
	require.LessOrEqual(t, float64(4), m.PendingBufferLen.Mean())
	// None of these writes asked to be synced.
	require.EqualValues(t, 0, int(m.SyncQueueLen.Mean()))
	require.Less(t, int64(numRecords*recordSize), m.WriteThroughput.Bytes)
}

func TestMetricsWithSync(t *testing.T) {
	f := &syncFileWithWait{}
	f.syncWG.Add(1)
	syncLatencyMicros := prometheus.NewHistogram(prometheus.HistogramOpts{
		Buckets: []float64{0,
			float64(time.Millisecond),
			float64(2 * time.Millisecond),
			float64(3 * time.Millisecond),
			float64(4 * time.Millisecond),
			float64(5 * time.Millisecond),
			float64(6 * time.Millisecond),
			float64(7 * time.Millisecond),
			float64(8 * time.Millisecond),
			float64(9 * time.Millisecond),
			float64(10 * time.Millisecond)},
	})

	w := NewLogWriter(f, 0, LogWriterConfig{
		WALFsyncLatency:     syncLatencyMicros,
		WriteWALSyncOffsets: func() bool { return false },
	},
	)
	var wg sync.WaitGroup
	wg.Add(100)
	for i := 0; i < 100; i++ {
		var syncErr error
		_, err := w.SyncRecord([]byte("hello"), &wg, &syncErr)
		require.NoError(t, err)
	}

	const syncLatency = 100 * time.Millisecond
	go func() {
		time.Sleep(syncLatency)
		// Unblock the flush loop. It may have run once or twice for these writes,
		// plus may run one more time due to the Close, so up to 3 runs. So 100
		// elements in the sync queue, spread over up to 3 runs.
		f.syncWG.Done()
	}()

	// Close() will only return after flushing is finished.
	require.NoError(t, w.Close())

	m := w.Metrics()
	require.LessOrEqual(t, float64(30), m.SyncQueueLen.Mean())

	writeTo := &prometheusgo.Metric{}
	require.NoError(t, syncLatencyMicros.Write(writeTo))
	for i := 0; i < 100; i += 10 {
		t.Logf("%d%%: %v", i, valueAtQuantileWindowed(writeTo.Histogram, float64(i)))
	}
	// Allow for some inaccuracy in sleep and for two syncs, one of which was
	// fast.
	require.LessOrEqual(t, float64(syncLatency/(2*time.Microsecond)),
		valueAtQuantileWindowed(writeTo.Histogram, 90))
	require.LessOrEqual(t, syncLatency/2, m.WriteThroughput.WorkDuration)
}

func valueAtQuantileWindowed(histogram *prometheusgo.Histogram, q float64) float64 {
	buckets := histogram.Bucket
	n := float64(*histogram.SampleCount)
	if n == 0 {
		return 0
	}

	// NB: The 0.5 is added for rounding purposes; it helps in cases where
	// SampleCount is small.
	rank := uint64(((q / 100) * n) + 0.5)

	// Since we are missing the +Inf bucket, CumulativeCounts may never exceed
	// rank. By omitting the highest bucket we have from the search, the failed
	// search will land on that last bucket and we don't have to do any special
	// checks regarding landing on a non-existent bucket.
	b := sort.Search(len(buckets)-1, func(i int) bool { return *buckets[i].CumulativeCount >= rank })

	var (
		bucketStart float64 // defaults to 0, which we assume is the lower bound of the smallest bucket
		bucketEnd   = *buckets[b].UpperBound
		count       = *buckets[b].CumulativeCount
	)

	// Calculate the linearly interpolated value within the bucket.
	if b > 0 {
		bucketStart = *buckets[b-1].UpperBound
		count -= *buckets[b-1].CumulativeCount
		rank -= *buckets[b-1].CumulativeCount
	}
	val := bucketStart + (bucketEnd-bucketStart)*(float64(rank)/float64(count))
	if math.IsNaN(val) || math.IsInf(val, -1) {
		return 0
	}

	// Should not extrapolate past the upper bound of the largest bucket.
	//
	// NB: SampleCount includes the implicit +Inf bucket but the
	// buckets[len(buckets)-1].UpperBound refers to the largest bucket defined
	// by us -- the client library doesn't give us access to the +Inf bucket
	// which Prometheus uses under the hood. With a high enough quantile, the
	// val computed further below surpasses the upper bound of the largest
	// bucket. Using that interpolated value feels wrong since we'd be
	// extrapolating. Also, for specific metrics if we see our q99 values to be
	// hitting the top-most bucket boundary, that's an indication for us to
	// choose better buckets for more accuracy. It's also worth noting that the
	// prometheus client library does the same thing when the resulting value is
	// in the +Inf bucket, whereby they return the upper bound of the second
	// last bucket -- see [1].
	//
	// [1]: https://github.com/prometheus/prometheus/blob/d9162189/promql/quantile.go#L103.
	if val > *buckets[len(buckets)-1].UpperBound {
		return *buckets[len(buckets)-1].UpperBound
	}

	return val
}

// TestQueueWALBlocks tests queueing many un-flushed WAL blocks when syncing is
// blocked.
func TestQueueWALBlocks(t *testing.T) {
	blockWriteCh := make(chan struct{}, 1)
	f := errorfs.WrapFile(vfstest.DiscardFile, errorfs.InjectorFunc(func(op errorfs.Op) error {
		if op.Kind == errorfs.OpFileWrite {
			<-blockWriteCh
		}
		return nil
	}))
	w := NewLogWriter(f, 0, LogWriterConfig{
		WALFsyncLatency:     prometheus.NewHistogram(prometheus.HistogramOpts{}),
		WriteWALSyncOffsets: func() bool { return false },
	})
	const numBlocks = 1024
	var b [blockSize]byte
	var logSize int64
	for i := 0; i < numBlocks; i++ {
		var err error
		logSize, err = w.SyncRecord(b[:], nil, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	close(blockWriteCh)
	require.NoError(t, w.Close())

	m := w.Metrics()
	t.Logf("LogSize is %s", humanize.Bytes.Int64(logSize))
	t.Logf("Mean pending buffer len is %.2f", m.PendingBufferLen.Mean())
	require.GreaterOrEqual(t, logSize, int64(numBlocks*blockSize))
}

func TestPendingSyncsWithHighestSyncIndex(t *testing.T) {
	var psi PendingSyncIndex
	psi.Index = NoSyncIndex
	require.True(t, psi.empty())
	require.False(t, psi.syncRequested())
	psi.Index = 0
	require.False(t, psi.empty())
	require.True(t, psi.syncRequested())

	type indexAndErr struct {
		PendingSyncIndex
		error
	}
	var cbValues []indexAndErr
	var q pendingSyncsWithHighestSyncIndex
	q.init(func(doneSync PendingSyncIndex, err error) {
		cbValues = append(cbValues, indexAndErr{PendingSyncIndex: doneSync, error: err})
	})
	require.True(t, q.empty())
	q.push(&psi)
	require.False(t, q.empty())
	require.Equal(t, int64(0), q.load())
	require.Equal(t, int64(0), q.snapshotForPop().(*PendingSyncIndex).Index)
	q.setBlocked()
	require.True(t, q.empty())
	require.Equal(t, int64(NoSyncIndex), q.load())
	require.Equal(t, int64(NoSyncIndex), q.snapshotForPop().(*PendingSyncIndex).Index)
	q.clearBlocked()
	require.False(t, q.empty())
	require.Equal(t, int64(0), q.load())

	const highestIndex = 100
	testErr := errors.New("test error")
	var popDone sync.WaitGroup
	popDone.Add(1)
	// Goroutine that pops.
	go func() {
		var poppedIndex int64
		for poppedIndex != highestIndex {
			if !q.empty() {
				snap := q.snapshotForPop()
				require.False(t, snap.empty())
				poppedIndex = snap.(*PendingSyncIndex).Index
				require.NotEqual(t, int64(NoSyncIndex), poppedIndex)
				var err error
				// Inject error for all even index pops.
				if poppedIndex%2 == 0 {
					err = testErr
				}
				require.NoError(t, q.pop(snap, err))
			}
		}
		popDone.Done()
	}()
	// Goroutine that pushes.
	go func() {
		// Already pushed 0, so start at index 1.
		for i := 1; i <= highestIndex; i++ {
			psi.Index = int64(i)
			q.push(&psi)
			// Randomly inject sleep, to allow pop to catch up, so that highestIndex
			// doesn't overwrite everything. With this, we see some sparseness of
			// indices in pops, but we get enough popped indices.
			if rand.IntN(2) == 0 {
				time.Sleep(time.Millisecond)
			}
		}
	}()
	popDone.Wait()
	latestValue := int64(NoSyncIndex)
	for _, cbVal := range cbValues {
		if cbVal.Index%2 == 0 {
			require.Equal(t, testErr, cbVal.error)
		} else {
			require.NoError(t, cbVal.error)
		}
		require.Less(t, latestValue, cbVal.Index)
		latestValue = cbVal.Index
	}
	require.Equal(t, int64(highestIndex), latestValue)
}

func TestSyncRecordGeneralized(t *testing.T) {
	f := &syncFile{}
	// Write two records, where the first one requests a sync. The callback
	// should execute for both since the second is synced on close.
	lastSync := int64(-1)
	cbChan := make(chan struct{}, 2)
	w := NewLogWriter(f, 0, LogWriterConfig{
		WALFsyncLatency: prometheus.NewHistogram(prometheus.HistogramOpts{}),
		ExternalSyncQueueCallback: func(doneSync PendingSyncIndex, err error) {
			require.NoError(t, err)
			require.Equal(t, lastSync+1, doneSync.Index)
			lastSync++
			cbChan <- struct{}{}
		},
		WriteWALSyncOffsets: func() bool { return false },
	})
	offset, err := w.SyncRecordGeneralized([]byte("hello"), &PendingSyncIndex{})
	require.NoError(t, err)
	<-cbChan
	require.Equal(t, offset, f.writePos.Load())
	require.Equal(t, offset, f.syncPos.Load())

	offset, err = w.SyncRecordGeneralized([]byte("world"), &PendingSyncIndex{Index: NoSyncIndex})
	require.NoError(t, err)
	time.Sleep(5 * time.Millisecond)
	select {
	case <-cbChan:
		t.Fatalf("should not sync")
	default:
	}
	if f.writePos.Load() == offset {
		require.NotEqual(t, f.writePos.Load(), f.syncPos.Load())
	}
	require.NoError(t, w.CloseWithLastQueuedRecord(PendingSyncIndex{Index: 1}))
	<-cbChan
	require.Equal(t, int64(1), lastSync)
	require.Equal(t, f.syncPos.Load(), f.writePos.Load())
}

type syncFileWithError struct {
	syncFile
	err error
}

func (f *syncFileWithError) Sync() error {
	if f.err != nil {
		return f.err
	}
	return f.syncFile.Sync()
}

func TestSyncRecordGeneralizedWithCloseError(t *testing.T) {
	f := &syncFileWithError{}
	// Write two records, where the first one requests a sync. The callback
	// should execute for both since the second attempts to sync on close. The
	// sync on close gets an error, so the callback has an error too.
	lastSync := int64(-1)
	cbChan := make(chan struct{}, 2)
	w := NewLogWriter(f, 0, LogWriterConfig{
		WALFsyncLatency: prometheus.NewHistogram(prometheus.HistogramOpts{}),
		ExternalSyncQueueCallback: func(doneSync PendingSyncIndex, err error) {
			if doneSync.Index == 1 {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, lastSync+1, doneSync.Index)
			lastSync++
			cbChan <- struct{}{}
		},
		WriteWALSyncOffsets: func() bool { return false },
	})
	offset, err := w.SyncRecordGeneralized([]byte("hello"), &PendingSyncIndex{})
	require.NoError(t, err)
	<-cbChan
	require.Equal(t, offset, f.writePos.Load())
	require.Equal(t, offset, f.syncPos.Load())

	// Set error for next sync.
	f.err = errorfs.ErrInjected
	offset, err = w.SyncRecordGeneralized([]byte("world"), &PendingSyncIndex{Index: NoSyncIndex})
	require.NoError(t, err)
	time.Sleep(5 * time.Millisecond)
	select {
	case <-cbChan:
		t.Fatalf("should not sync")
	default:
	}
	if f.writePos.Load() == offset {
		require.NotEqual(t, f.writePos.Load(), f.syncPos.Load())
	}
	require.Error(t, w.CloseWithLastQueuedRecord(PendingSyncIndex{Index: 1}))
	<-cbChan
	require.Equal(t, int64(1), lastSync)
	require.Less(t, f.syncPos.Load(), f.writePos.Load())
}

func writeWALSyncRecords(t *testing.T, numRecords int, recordSizes []int) *syncFile {
	f := &syncFile{}
	w := NewLogWriter(f, 1, LogWriterConfig{
		WALFsyncLatency:     prometheus.NewHistogram(prometheus.HistogramOpts{}),
		WriteWALSyncOffsets: func() bool { return true },
	})
	var syncErr error
	for i := 0; i < numRecords; i++ {
		var syncWG sync.WaitGroup
		syncWG.Add(1)
		data := []byte(strings.Repeat(fmt.Sprintf("%d", i%10), recordSizes[i]))
		offset, err := w.SyncRecord(data, &syncWG, &syncErr)
		require.NoError(t, err)
		syncWG.Wait()
		require.NoError(t, syncErr)
		if v := f.writePos.Load(); offset != v {
			t.Fatalf("expected write pos %d, but found %d", offset, v)
		}
		if v := f.syncPos.Load(); offset != v {
			t.Fatalf("expected sync pos %d, but found %d", offset, v)
		}
	}
	return f
}

func validateWALSyncRecords(t *testing.T, buf *bytes.Buffer) {
	var largestOffset uint64 = 0
	i := 0
	bufBytes := (*buf).Bytes()
	for i < len(bufBytes) {
		if blockSize-(i%blockSize) < walSyncHeaderSize {
			i += blockSize - (i % blockSize)
			continue
		}

		checksum := binary.LittleEndian.Uint32(bufBytes[i+0 : i+4])
		length := binary.LittleEndian.Uint16(bufBytes[i+4 : i+6])
		chunkEncoding := bufBytes[i+6]
		logNum := binary.LittleEndian.Uint32(bufBytes[i+7 : i+11])

		// Reader and Writer have a logNum of 1, so a logNum of 2 is EOF.
		if logNum == 2 {
			// reached EOF trailer
			if checksum != 0 && length != 0 {
				t.Fatal("Mismatched logNum but not EOF trailer")
			}
			break
		}
		offset := binary.LittleEndian.Uint64(bufBytes[i+11 : i+19])
		if offset < largestOffset {
			t.Fatal("Expected monotonitcally increasing offsets.")
		}
		largestOffset = offset
		headerFormat := headerFormatMappings[chunkEncoding]
		headerSize := headerFormat.headerSize
		i += headerSize + int(length)
	}

	r := NewReader(bytes.NewBuffer(bufBytes), 1)
	for {
		rr, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Error Next(): %v", err)
		}
		_, err = io.ReadAll(rr)
		if err != nil {
			t.Fatalf("Error Next(): %v", err)
		}
	}
}

func TestWALSyncOffsetSimple(t *testing.T) {
	recordSizes := []int{}
	for i := 0; i < 1000; i++ {
		recordSizes = append(recordSizes, blockSize-walSyncHeaderSize)
	}
	f := writeWALSyncRecords(t, len(recordSizes), recordSizes)
	validateWALSyncRecords(t, &f.buffer)
}

func TestWALSyncOffsetRandom(t *testing.T) {
	recordSizes := []int{}
	for i := 0; i < 1000; i++ {
		recordSizes = append(recordSizes, 1+rand.IntN(blockSize-walSyncHeaderSize))
	}
	f := writeWALSyncRecords(t, len(recordSizes), recordSizes)
	validateWALSyncRecords(t, &f.buffer)
}

// BenchmarkQueueWALBlocks exercises queueing within the LogWriter. It can be
// useful to measure allocations involved when flushing is slow enough to
// accumulate a large backlog fo queued blocks.
func BenchmarkQueueWALBlocks(b *testing.B) {
	const dataVolume = 64 << 20 /* 64 MB */
	for _, writeSize := range []int64{64, 512, 1024, 2048, 32768} {
		b.Run(fmt.Sprintf("record-size=%s", humanize.Bytes.Int64(writeSize)), func(b *testing.B) {
			record := make([]byte, writeSize)
			numRecords := int(dataVolume / writeSize)

			for j := 0; j < b.N; j++ {
				b.StopTimer()
				blockWriteCh := make(chan struct{}, 1)
				f := errorfs.WrapFile(vfstest.DiscardFile, errorfs.InjectorFunc(func(op errorfs.Op) error {
					if op.Kind == errorfs.OpFileWrite {
						<-blockWriteCh
					}
					return nil
				}))
				w := NewLogWriter(f, 0, LogWriterConfig{
					WALFsyncLatency:     prometheus.NewHistogram(prometheus.HistogramOpts{}),
					WriteWALSyncOffsets: func() bool { return false },
				})

				b.StartTimer()
				for n := numRecords; n > 0; n-- {
					if _, err := w.SyncRecord(record[:], nil, nil); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()

				b.SetBytes(dataVolume)
				close(blockWriteCh)
				require.NoError(b, w.Close())
			}
		})
	}
}

// BenchmarkWriteWALBlocksAllocs exercises the PendingSync and related
// interfaces for the generalized write path, to ensure there are no extra
// allocations.
func BenchmarkWriteWALBlocksAllocs(b *testing.B) {
	const dataVolume = 64 << 20 /* 64 MB */
	writeSize := 64
	record := make([]byte, writeSize)
	numRecords := dataVolume / writeSize

	for j := 0; j < b.N; j++ {
		b.StopTimer()
		f := vfstest.DiscardFile
		w := NewLogWriter(f, 0, LogWriterConfig{
			WALFsyncLatency:           prometheus.NewHistogram(prometheus.HistogramOpts{}),
			ExternalSyncQueueCallback: func(doneSync PendingSyncIndex, err error) {},
			WriteWALSyncOffsets:       func() bool { return false },
		})

		var psi PendingSyncIndex
		b.StartTimer()
		for i := 0; i < numRecords; i++ {
			psi.Index = int64(i)
			if _, err := w.SyncRecordGeneralized(record[:], &psi); err != nil {
				b.Fatal(err)
			}
		}
		// Close to ensure everything is written.
		require.NoError(b, w.CloseWithLastQueuedRecord(psi))
		b.StopTimer()
		b.SetBytes(dataVolume)
	}
}
