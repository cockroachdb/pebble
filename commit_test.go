package pebble

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/petermattis/pebble/arenaskl"
	"github.com/petermattis/pebble/crc"
	"github.com/petermattis/pebble/record"
)

type testCommitEnv struct {
	logSeqNum     uint64
	visibleSeqNum uint64
	writePos      uint64
	writeCount    uint64
	applyBuf      struct {
		sync.Mutex
		buf []uint64
	}
}

func (e *testCommitEnv) env() commitEnv {
	return commitEnv{
		apply:   e.apply,
		reserve: e.reserve,
		sync:    e.sync,
		write:   e.write,
	}
}

func (e *testCommitEnv) apply(b *Batch) error {
	e.applyBuf.Lock()
	e.applyBuf.buf = append(e.applyBuf.buf, b.seqNum())
	e.applyBuf.Unlock()
	return nil
}

func (e *testCommitEnv) reserve(n int) uint64 {
	return atomic.AddUint64(&e.writePos, uint64(n)) - uint64(n)
}

func (e *testCommitEnv) sync() error {
	return nil
}

func (e *testCommitEnv) write(pos uint64, data []byte) error {
	atomic.AddUint64(&e.writeCount, 1)
	return nil
}

func TestCommitPipeline(t *testing.T) {
	var e testCommitEnv
	p := newCommitPipeline(e.env(), &e.logSeqNum, &e.visibleSeqNum)
	defer p.close()

	const n = 10000
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			var b Batch
			_ = b.Set([]byte(fmt.Sprint(i)), nil, nil)
			_ = p.commit(&b, false)
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

func BenchmarkCommitPipeline(b *testing.B) {
	for _, parallelism := range []int{1, 2, 4, 8, 16, 32, 64, 128} {
		b.Run(fmt.Sprintf("parallel=%d", parallelism), func(b *testing.B) {
			b.SetParallelism(parallelism)
			var mem struct {
				sync.RWMutex
				*memTable
			}
			var wal struct {
				*record.Writer
			}
			wal.Writer = record.NewWriter(ioutil.Discard)

			nullCommitEnv := commitEnv{
				apply: func(b *Batch) error {
					for {
						mem.RLock()
						err := arenaskl.ErrArenaFull
						if mem.memTable != nil {
							err = mem.apply(b, b.seqNum())
						}
						mem.RUnlock()

						if err == arenaskl.ErrArenaFull {
							mem.Lock()
							mem.memTable = newMemTable(nil)
							mem.Unlock()
							continue
						}
						return err
					}
				},
				reserve: func(n int) uint64 {
					return 0
					// return wal.Reserve(n)
				},
				sync: func() error {
					// return wal.Sync()
					return nil
				},
				write: func(pos uint64, data []byte) error {
					_ = crc.New(data).Value()
					// return wal.Fill(pos, data)
					return nil
				},
			}
			var logSeqNum, visibleSeqNum uint64
			p := newCommitPipeline(nullCommitEnv, &logSeqNum, &visibleSeqNum)
			defer p.close()

			b.SetBytes(16)
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				buf := make([]byte, 8)

				for pb.Next() {
					batch := newBatch(nil)
					binary.BigEndian.PutUint64(buf, rng.Uint64())
					batch.Set(buf, buf, nil)
					if err := p.commit(batch, false /* sync */); err != nil {
						b.Fatal(err)
					}
					batch.release()
				}
			})
		})
	}
}
