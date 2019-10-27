// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package record

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/codahale/hdrhistogram"
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
	var closed int32

	var flusherWG sync.WaitGroup
	flusherWG.Add(1)
	go func() {
		defer flusherWG.Done()
		for {
			if atomic.LoadInt32(&closed) == 1 {
				return
			}
			head, tail := q.load()
			q.pop(head, tail, nil)
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

	atomic.StoreInt32(&closed, 1)
	flusherWG.Wait()
}

func TestFlusherCond(t *testing.T) {
	var mu sync.Mutex
	var q syncQueue
	var c flusherCond
	var closed bool

	c.init(&mu, &q)

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

			head, tail := q.load()
			q.pop(head, tail, nil)
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
	f, err := mem.Create("log")
	if err != nil {
		t.Fatal(err)
	}
	injectedErr := fmt.Errorf("injected error")
	w := NewLogWriter(syncErrorFile{f, injectedErr}, 0)

	var syncErr error
	var syncWG sync.WaitGroup
	syncWG.Add(1)
	if _, err := w.SyncRecord([]byte("hello"), &syncWG, &syncErr); err != nil {
		t.Fatal(err)
	}
	syncWG.Wait()
	if injectedErr != syncErr {
		t.Fatalf("unexpected %v but found %v", injectedErr, syncErr)
	}
}

type syncFile struct {
	writePos int64
	syncPos  int64
}

func (f *syncFile) Write(buf []byte) (int, error) {
	n := len(buf)
	atomic.AddInt64(&f.writePos, int64(n))
	return n, nil
}

func (f *syncFile) Sync() error {
	atomic.StoreInt64(&f.syncPos, atomic.LoadInt64(&f.writePos))
	return nil
}

func TestSyncRecord(t *testing.T) {
	f := &syncFile{}
	w := NewLogWriter(f, 0)

	var syncErr error
	for i := 0; i < 100000; i++ {
		var syncWG sync.WaitGroup
		syncWG.Add(1)
		offset, err := w.SyncRecord([]byte("hello"), &syncWG, &syncErr)
		if err != nil {
			t.Fatal(err)
		}
		syncWG.Wait()
		if syncErr != nil {
			t.Fatal(syncErr)
		}
		if v := atomic.LoadInt64(&f.writePos); offset != v {
			t.Fatalf("expected write pos %d, but found %d", offset, v)
		}
		if v := atomic.LoadInt64(&f.syncPos); offset != v {
			t.Fatalf("expected sync pos %d, but found %d", offset, v)
		}
	}
}

type syncTracker struct {
	writePos int64
	syncPos  int64
	hist     *hdrhistogram.Histogram
}

func (f *syncTracker) Write(buf []byte) (int, error) {
	n := len(buf)
	atomic.AddInt64(&f.writePos, int64(n))
	return n, nil
}

func (f *syncTracker) Sync() error {
	writePos := atomic.LoadInt64(&f.writePos)
	size := writePos - atomic.LoadInt64(&f.syncPos)
	if size == 0 {
		// Don't record "empty" syncs.
		return nil
	}
	f.hist.RecordValue(size)
	atomic.StoreInt64(&f.syncPos, writePos)
	return nil
}

func TestMinSyncInterval(t *testing.T) {
	const numWorkers = 10

	// TODO(peter): this is flaky under stress.
	run := func(interval time.Duration) int64 {
		f := &syncTracker{
			hist: hdrhistogram.New(0, 1000, 1),
		}
		w := NewLogWriter(f, 0)
		w.SetMinSyncInterval(func() time.Duration {
			return interval
		})
		data := []byte("hello")

		doneCh := make(chan error, numWorkers)
		var mu sync.Mutex
		for i := 0; i < cap(doneCh); i++ {
			go func() {
				var syncErr error
				for j := 0; j < 1000; j++ {
					var syncWG sync.WaitGroup
					syncWG.Add(1)
					mu.Lock()
					_, err := w.SyncRecord(data, &syncWG, &syncErr)
					mu.Unlock()
					if err != nil {
						doneCh <- err
						return
					}
					syncWG.Wait()
				}
				doneCh <- nil
			}()
		}

		for i := 0; i < cap(doneCh); i++ {
			if err := <-doneCh; err != nil {
				t.Fatal(err)
			}
		}

		return f.hist.ValueAtQuantile(0.95)
	}

	base := run(0)
	delayed := run(100 * time.Microsecond)
	if expected := base * (numWorkers - 1); expected > delayed {
		t.Fatalf("expected p95 sync groups of at least %d, but found %d", expected, delayed)
	}
}
