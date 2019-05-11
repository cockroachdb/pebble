// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/petermattis/pebble/internal/arenaskl"
	"github.com/petermattis/pebble/internal/record"
	"golang.org/x/exp/rand"
)

type testCommitEnv struct {
	logSeqNum     uint64
	visibleSeqNum uint64
	writePos      int64
	writeCount    uint64
	applyBuf      struct {
		sync.Mutex
		buf []uint64
	}
}

func (e *testCommitEnv) env() commitEnv {
	return commitEnv{
		logSeqNum:     &e.logSeqNum,
		visibleSeqNum: &e.visibleSeqNum,
		apply:         e.apply,
		write:         e.write,
	}
}

func (e *testCommitEnv) apply(b *Batch, mem *memTable) error {
	e.applyBuf.Lock()
	e.applyBuf.buf = append(e.applyBuf.buf, b.seqNum())
	e.applyBuf.Unlock()
	return nil
}

func (e *testCommitEnv) write(b *Batch, _ *sync.WaitGroup) (*memTable, error) {
	n := int64(len(b.storage.data))
	atomic.AddInt64(&e.writePos, n)
	atomic.AddUint64(&e.writeCount, 1)
	return nil, nil
}

func TestCommitPipeline(t *testing.T) {
	var e testCommitEnv
	p := newCommitPipeline(e.env())

	const n = 10000
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			var b Batch
			_ = b.Set([]byte(fmt.Sprint(i)), nil, nil)
			_ = p.Commit(&b, false)
		}(i)
	}
	wg.Wait()

	if s := atomic.LoadUint64(&e.writeCount); n != s {
		t.Fatalf("expected %d written batches, but found %d", n, s)
	}
	if n != len(e.applyBuf.buf) {
		t.Fatalf("expected %d written batches, but found %d",
			n, len(e.applyBuf.buf))
	}
	if s := atomic.LoadUint64(&e.logSeqNum); n != s {
		t.Fatalf("expected %d, but found %d", n, s)
	}
	if s := atomic.LoadUint64(&e.visibleSeqNum); n != s {
		t.Fatalf("expected %d, but found %d", n, s)
	}
}

func TestCommitPipelineAllocateSeqNum(t *testing.T) {
	var e testCommitEnv
	p := newCommitPipeline(e.env())

	const n = 10
	var wg sync.WaitGroup
	wg.Add(n)
	var prepareCount uint64
	var applyCount uint64
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			p.AllocateSeqNum(func() {
				atomic.AddUint64(&prepareCount, 1)
			}, func(seqNum uint64) {
				atomic.AddUint64(&applyCount, 1)
			})
		}(i)
	}
	wg.Wait()

	if s := atomic.LoadUint64(&prepareCount); n != s {
		t.Fatalf("expected %d prepares, but found %d", n, s)
	}
	if s := atomic.LoadUint64(&applyCount); n != s {
		t.Fatalf("expected %d applies, but found %d", n, s)
	}
	// AllocateSeqNum always returns a non-zero sequence number causing the
	// values we see to be offset from 1.
	if s := atomic.LoadUint64(&e.logSeqNum); n+1 != s {
		t.Fatalf("expected %d, but found %d", n+1, s)
	}
	if s := atomic.LoadUint64(&e.visibleSeqNum); n+1 != s {
		t.Fatalf("expected %d, but found %d", n+1, s)
	}
}

func BenchmarkCommitPipeline(b *testing.B) {
	for _, parallelism := range []int{1, 2, 4, 8, 16, 32, 64, 128} {
		b.Run(fmt.Sprintf("parallel=%d", parallelism), func(b *testing.B) {
			b.SetParallelism(parallelism)
			mem := newMemTable(nil)
			wal := record.NewLogWriter(ioutil.Discard, 0 /* logNum */)

			nullCommitEnv := commitEnv{
				logSeqNum:     new(uint64),
				visibleSeqNum: new(uint64),
				apply: func(b *Batch, mem *memTable) error {
					err := mem.apply(b, b.seqNum())
					if err != nil {
						return err
					}
					mem.unref()
					return nil
				},
				write: func(b *Batch, wg *sync.WaitGroup) (*memTable, error) {
					for {
						err := mem.prepare(b)
						if err == arenaskl.ErrArenaFull {
							mem = newMemTable(nil)
							continue
						}
						if err != nil {
							return nil, err
						}
						break
					}

					_, err := wal.SyncRecord(b.storage.data, wg)
					return mem, err
				},
			}
			p := newCommitPipeline(nullCommitEnv)

			const keySize = 8
			b.SetBytes(2 * keySize)
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
				buf := make([]byte, keySize)

				for pb.Next() {
					batch := newBatch(nil)
					binary.BigEndian.PutUint64(buf, rng.Uint64())
					batch.Set(buf, buf, nil)
					if err := p.Commit(batch, true /* sync */); err != nil {
						b.Fatal(err)
					}
					batch.release()
				}
			})
		})
	}
}
