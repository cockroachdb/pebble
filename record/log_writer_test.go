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
	"github.com/cockroachdb/pebble/internal/humanize"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/errorfs"
	"github.com/cockroachdb/pebble/vfs/vfstest"
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
	flusherWG.Go(func() {
		for {
			if closed.Load() {
				return
			}
			head, tail, _ := q.load()
			q.pop(head, tail, nil, nil)
		}
	})

	var commitMu sync.Mutex
	var doneWG sync.WaitGroup
	for range SyncConcurrency {
		doneWG.Go(func() {
			for range 1000 {
				wg := &sync.WaitGroup{}
				wg.Add(1)
				// syncQueue is a single-producer, single-consumer queue. We need to
				// provide mutual exclusion on the producer side.
				commitMu.Lock()
				q.push(wg, new(error))
				commitMu.Unlock()
				wg.Wait()
			}
		})
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
	flusherWG.Go(func() {
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
	})

	var commitMu sync.Mutex
	// NB: we're testing with low concurrency here, because what we want to
	// stress is that signalling of the flusherCond works
	// correctly. Specifically, we want to make sure that a signal is "lost",
	// causing the test to wedge.
	var doneWG sync.WaitGroup
	for range 2 {
		doneWG.Go(func() {
			for range 10000 {
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
		})
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

// failingWriteFile is a syncFile whose writes fail once fail is set.
type failingWriteFile struct {
	syncFile
	fail atomic.Bool
}

func (f *failingWriteFile) Write(buf []byte) (int, error) {
	if f.fail.Load() {
		return 0, errors.New("injected write error")
	}
	return f.syncFile.Write(buf)
}

// TestFlushedOffset verifies that FlushedOffset reports the prefix of the log
// that has been written to the file, and that it stops advancing once a write
// fails.
func TestFlushedOffset(t *testing.T) {
	f := &failingWriteFile{}
	w := NewLogWriter(f, 0, LogWriterConfig{
		WriteWALSyncOffsets: func() bool { return true },
	})
	syncRecord := func(p []byte) (int64, error) {
		var syncErr error
		var syncWG sync.WaitGroup
		syncWG.Add(1)
		offset, err := w.SyncRecord(p, &syncWG, &syncErr)
		require.NoError(t, err)
		syncWG.Wait()
		return offset, syncErr
	}
	require.Zero(t, w.FlushedOffset())
	// The records span several blocks.
	for range 10 {
		offset, err := syncRecord(bytes.Repeat([]byte("a"), 10000))
		require.NoError(t, err)
		require.Equal(t, offset, w.FlushedOffset())
		require.Equal(t, f.writePos.Load(), w.FlushedOffset())
	}
	flushed := w.FlushedOffset()
	f.fail.Store(true)
	_, err := syncRecord([]byte("hello"))
	require.Error(t, err)
	require.Equal(t, flushed, w.FlushedOffset())
	require.Error(t, w.Close())
	require.Equal(t, flushed, w.FlushedOffset())
}

// TestFlushedOffsetWholeRecords verifies that FlushedOffset does not cover a
// record whose first blocks have been written to the file while the writer is
// still emitting the rest of it, and that a reader of the prefix it reports
// reads whole records followed by a clean end.
func TestFlushedOffsetWholeRecords(t *testing.T) {
	for _, walSync := range []bool{false, true} {
		t.Run(fmt.Sprintf("wal-sync=%t", walSync), func(t *testing.T) {
			f := &syncFile{}
			w := NewLogWriter(f, 1, LogWriterConfig{
				WriteWALSyncOffsets: func() bool { return walSync },
			})
			syncRecord := func(p []byte) int64 {
				var syncErr error
				var syncWG sync.WaitGroup
				syncWG.Add(1)
				offset, err := w.SyncRecord(p, &syncWG, &syncErr)
				require.NoError(t, err)
				syncWG.Wait()
				require.NoError(t, syncErr)
				return offset
			}
			firstEnd := syncRecord([]byte("first"))
			require.Equal(t, firstEnd, w.FlushedOffset())

			// Emit the first two fragments of a record that spans three blocks, as
			// SyncRecordGeneralized does. Each fills a block and queues it, and the
			// flush loop writes the queued blocks while the record is incomplete.
			p := bytes.Repeat([]byte("x"), 3*blockSize)
			p = w.emitFragment(0, p)
			p = w.emitFragment(1, p)
			// The flush loop updates the metrics once FlushedOffset is updated.
			require.Eventually(t, func() bool {
				return w.Metrics().WriteThroughput.Bytes >= 2*blockSize
			}, 10*time.Second, time.Millisecond)
			require.Equal(t, firstEnd, w.FlushedOffset())

			for i := 2; len(p) > 0; i++ {
				p = w.emitFragment(i, p)
			}
			// The record is now complete, but only the next call to SyncRecord
			// publishes its end, along with its own.
			lastEnd := syncRecord([]byte("last"))
			require.Equal(t, lastEnd, w.FlushedOffset())
			require.NoError(t, w.Close())

			buf := f.buffer.Bytes()
			for _, tc := range []struct {
				prefix  int64
				records int
			}{{firstEnd, 1}, {lastEnd, 3}} {
				r := NewReader(bytes.NewReader(buf[:tc.prefix]), 1)
				var n int
				for {
					rec, err := r.Next()
					if err == io.EOF {
						break
					}
					require.NoError(t, err)
					_, err = io.Copy(io.Discard, rec)
					require.NoError(t, err)
					n++
				}
				require.Equal(t, tc.records, n, "records in the first %d bytes", tc.prefix)
			}
		})
	}
}

// syncRecordForTest writes p to w as a record, waits for its sync, and returns
// the offset SyncRecord returned.
func syncRecordForTest(t *testing.T, w *LogWriter, p []byte) int64 {
	t.Helper()
	var syncErr error
	var syncWG sync.WaitGroup
	syncWG.Add(1)
	offset, err := w.SyncRecord(p, &syncWG, &syncErr)
	require.NoError(t, err)
	syncWG.Wait()
	require.NoError(t, syncErr)
	return offset
}

// TestFlushedOffsetExcludesPadding verifies that FlushedOffset ends with the
// last record even when the rest of the record's block is zero padding, and
// that a reader of the prefix it reports finds the end of the log exactly
// there: a record.Reader does not count a tail shorter than a chunk header in
// its offset.
func TestFlushedOffsetExcludesPadding(t *testing.T) {
	for _, walSync := range []bool{false, true} {
		headerSize := recyclableHeaderSize
		if walSync {
			headerSize = walSyncHeaderSize
		}
		for gap := range headerSize + 1 {
			t.Run(fmt.Sprintf("wal-sync=%t,gap=%d", walSync, gap), func(t *testing.T) {
				f := &syncFile{}
				w := NewLogWriter(f, 1, LogWriterConfig{
					WriteWALSyncOffsets: func() bool { return walSync },
				})
				// A record that ends gap bytes short of the end of the first block.
				end := int64(blockSize - gap)
				offset := syncRecordForTest(t, w, bytes.Repeat([]byte("x"), int(end)-headerSize))
				require.Equal(t, end, w.FlushedOffset())
				if gap < headerSize {
					// No other chunk fits in the block, so it is padded and queued.
					require.Equal(t, int64(blockSize), offset)
				} else {
					require.Equal(t, end, offset)
				}
				require.NoError(t, w.Close())

				r := NewReader(bytes.NewReader(f.buffer.Bytes()[:end]), 1)
				rec, err := r.Next()
				require.NoError(t, err)
				_, err = io.Copy(io.Discard, rec)
				require.NoError(t, err)
				_, err = r.Next()
				require.Equal(t, io.EOF, err)
				require.Equal(t, end, r.Offset())
			})
		}
	}
}

// invertingFile is a syncFile that modifies the buffers passed to Write once it
// has written them, which io.Writer forbids, to catch a LogWriter that reads a
// buffer again after writing it.
type invertingFile struct {
	syncFile
}

func (f *invertingFile) Write(p []byte) (int, error) {
	n, err := f.syncFile.Write(p)
	for i := range p {
		p[i] ^= 0xff
	}
	return n, err
}

// TestFlushedOffsetFilledBlock verifies FlushedOffset when the flush loop writes
// a block that the writer has filled but not queued yet, which leaves the flush
// loop to find where records end in the block from its chunks. The block's
// first record has been written before, and the file modifies what is written
// to it, so those chunks can no longer be read from the block.
func TestFlushedOffsetFilledBlock(t *testing.T) {
	for _, walSync := range []bool{false, true} {
		for _, endsRecord := range []bool{false, true} {
			t.Run(fmt.Sprintf("wal-sync=%t,ends-record=%t", walSync, endsRecord), func(t *testing.T) {
				headerSize := recyclableHeaderSize
				if walSync {
					headerSize = walSyncHeaderSize
				}
				w := NewLogWriter(&invertingFile{}, 1, LogWriterConfig{
					WriteWALSyncOffsets: func() bool { return walSync },
				})
				firstEnd := syncRecordForTest(t, w, []byte("first"))
				require.Equal(t, firstEnd, w.FlushedOffset())

				// The second record fills the rest of the block, and either ends there
				// or continues in the next block.
				size := blockSize - int(firstEnd) - headerSize
				if !endsRecord {
					size += 100
				}
				// The writer queues a block it has filled only once it gets hold of
				// the free list, which holds it back.
				w.free.Lock()
				done := make(chan error, 1)
				go func() {
					_, err := w.SyncRecord(make([]byte, size), nil, nil)
					done <- err
				}()
				require.Eventually(t, func() bool {
					return w.block.written.Load() == blockSize
				}, 10*time.Second, time.Millisecond)
				// Have the flush loop write the block; it updates the metrics after
				// FlushedOffset.
				w.flusher.ready.Signal()
				require.Eventually(t, func() bool {
					return w.Metrics().WriteThroughput.Bytes == blockSize
				}, 10*time.Second, time.Millisecond)
				if endsRecord {
					require.Equal(t, int64(blockSize), w.FlushedOffset())
				} else {
					require.Equal(t, firstEnd, w.FlushedOffset())
				}
				w.free.Unlock()
				require.NoError(t, <-done)
				require.NoError(t, w.Close())
			})
		}
	}
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
		WriteWALSyncOffsets: func() bool { return false },
		WALFileOpHistogram:  syncLatencyMicros,
	})
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
	// Goroutine that pops.
	var popDone sync.WaitGroup
	popDone.Go(func() {
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
	})
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

// writeWALSyncRecords writes and synchronously syncs numRecords records with
// the given sizes, using the WAL sync chunk format. It returns the resulting
// file along with the end offset of each record.
func writeWALSyncRecords(t *testing.T, numRecords int, recordSizes []int) (*syncFile, []uint64) {
	f := &syncFile{}
	w := NewLogWriter(f, 1, LogWriterConfig{
		WriteWALSyncOffsets: func() bool { return true },
	})
	var syncErr error
	recordEnd := make([]uint64, numRecords)
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
		// Everything written so far is now durable, so the writer's durability
		// watermark must have caught up with the file's sync position.
		if v := w.syncedOffset.Load(); v != uint64(offset) {
			t.Fatalf("record %d: expected syncedOffset %d, but found %d", i, offset, v)
		}
		// FlushedOffset ends with the record, before the zero padding that
		// completes its block if no other chunk fits in it.
		if v := w.FlushedOffset(); v != offset {
			padding := f.buffer.Bytes()[min(v, offset):offset]
			if offset%blockSize != 0 || len(padding) == 0 || len(padding) >= walSyncHeaderSize ||
				!bytes.Equal(padding, make([]byte, len(padding))) {
				t.Fatalf("record %d: expected FlushedOffset %d, or the start of its block's padding, but found %d",
					i, offset, v)
			}
		}
		recordEnd[i] = uint64(offset)
	}
	flushed := w.FlushedOffset()
	require.NoError(t, w.Close())
	// FlushedOffset covers whole records only, not the EOF trailer that Close
	// writes.
	require.Equal(t, flushed, w.FlushedOffset())
	require.Less(t, flushed, f.writePos.Load())
	return f, recordEnd
}

// validateWALSyncRecords checks that every chunk in the file is stamped with
// the offset that was durably synced when the chunk was written, and that the
// file reads back cleanly. recordEnd holds the end offset of each record; since
// writeWALSyncRecords syncs each record before writing the next one, a chunk
// belonging to record i must be stamped with recordEnd[i-1].
func validateWALSyncRecords(t *testing.T, buf *bytes.Buffer, recordEnd []uint64) {
	// record is the index of the record that the chunk at offset i belongs to.
	record := 0
	i := 0
	foundEOFTrailer := false
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
			if checksum != 0 || length != 0 {
				t.Fatal("Mismatched logNum but not EOF trailer")
			}
			foundEOFTrailer = true
			break
		}
		for record < len(recordEnd) && uint64(i) >= recordEnd[record] {
			record++
		}
		if record >= len(recordEnd) {
			t.Fatalf("chunk at offset %d is past the last record", i)
		}
		var expected uint64
		if record > 0 {
			expected = recordEnd[record-1]
		}
		offset := binary.LittleEndian.Uint64(bufBytes[i+11 : i+19])
		if offset != expected {
			t.Fatalf("chunk at offset %d (record %d): expected stamped offset %d, but found %d",
				i, record, expected, offset)
		}
		headerFormat := headerFormatMappings[chunkEncoding]
		headerSize := headerFormat.headerSize
		i += headerSize + int(length)
	}
	if !foundEOFTrailer {
		t.Fatal("did not reach the EOF trailer")
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
	f, recordEnd := writeWALSyncRecords(t, len(recordSizes), recordSizes)
	validateWALSyncRecords(t, &f.buffer, recordEnd)
}

func TestWALSyncOffsetRandom(t *testing.T) {
	recordSizes := []int{}
	for i := 0; i < 1000; i++ {
		recordSizes = append(recordSizes, 1+rand.IntN(blockSize-walSyncHeaderSize))
	}
	f, recordEnd := writeWALSyncRecords(t, len(recordSizes), recordSizes)
	validateWALSyncRecords(t, &f.buffer, recordEnd)
}

// TestWALSyncOffsetMultiBlock exercises records that span multiple blocks, so
// that a record is split across first/middle/last chunks and its bytes reach
// the file through queued blocks rather than through the tail of the block
// currently being filled.
func TestWALSyncOffsetMultiBlock(t *testing.T) {
	recordSizes := []int{}
	for i := 0; i < 20; i++ {
		recordSizes = append(recordSizes, 1+rand.IntN(10*blockSize))
	}
	f, recordEnd := writeWALSyncRecords(t, len(recordSizes), recordSizes)
	validateWALSyncRecords(t, &f.buffer, recordEnd)
}

// TestWALSyncOffsetBatchedBlocks verifies the durability watermark when a
// single flush drains many queued blocks at once. The flush loop is held inside
// Write while the writer fills up a backlog of blocks, so the flush that
// follows has to account for all of them.
func TestWALSyncOffsetBatchedBlocks(t *testing.T) {
	f := &syncFileWithWait{}
	f.writeWG.Add(1)
	w := NewLogWriter(f, 1, LogWriterConfig{
		WriteWALSyncOffsets: func() bool { return true },
	})
	// Build up a backlog of roughly 20 queued blocks. None of these request a
	// sync, and the flush loop cannot drain them while writeWG is held.
	const numRecords = 40
	for i := 0; i < numRecords; i++ {
		_, err := w.SyncRecord([]byte(strings.Repeat("x", blockSize/2)), nil, nil)
		require.NoError(t, err)
	}
	f.writeWG.Done()

	// Sync a final record; everything written so far must be covered by the
	// watermark.
	var syncWG sync.WaitGroup
	var syncErr error
	syncWG.Add(1)
	offset, err := w.SyncRecord([]byte("last"), &syncWG, &syncErr)
	require.NoError(t, err)
	syncWG.Wait()
	require.NoError(t, syncErr)
	require.EqualValues(t, offset, f.f.syncPos.Load())
	require.EqualValues(t, offset, w.syncedOffset.Load())
	require.NoError(t, w.Close())

	// Confirm the backlog actually materialized; otherwise the flush loop kept
	// up and this test would silently degenerate into the single-block case,
	// which would drain one block per flush and yield a mean of at most 1.
	m := w.Metrics()
	require.GreaterOrEqual(t, m.PendingBufferLen.Mean(), float64(3))
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
