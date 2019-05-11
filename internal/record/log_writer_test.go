// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package record

import (
	"sync"
	"sync/atomic"
	"testing"
)

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
			q.pop(head, tail)
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
				q.push(wg)
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
			q.pop(head, tail)
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
				q.push(wg)
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
