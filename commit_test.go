package pebble

import (
	"fmt"
	"sync"
	"testing"
)

type testCommitEnv struct {
	applyBuf struct {
		sync.Mutex
		buf []uint64
	}
	publishBuf []uint64
	writeBuf   []uint64
}

func (e *testCommitEnv) env() commitEnv {
	return commitEnv{
		apply:   e.apply,
		publish: e.publish,
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

func (e *testCommitEnv) publish(seqNum uint64) {
	e.publishBuf = append(e.publishBuf, seqNum)
}

func (e *testCommitEnv) sync() error {
	return nil
}

func (e *testCommitEnv) write(group commitList) error {
	for b := group.head; b != nil; b = b.next {
		e.writeBuf = append(e.writeBuf, b.seqNum())
	}
	return nil
}

func TestCommitPipeline(t *testing.T) {
	var e testCommitEnv
	p := newCommitPipeline(e.env(), 1)
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

	if n != len(e.writeBuf) {
		t.Fatalf("expected %d written batches, but found %d",
			n, len(e.writeBuf))
	}
	if n != len(e.applyBuf.buf) {
		t.Fatalf("expected %d written batches, but found %d",
			n, len(e.applyBuf.buf))
	}
	if n != len(e.publishBuf) {
		t.Fatalf("expected %d published batches, but found %d",
			n, len(e.publishBuf))
	}

	for i := 0; i < n; i++ {
		if e.writeBuf[i] != uint64(i+1) {
			t.Fatalf("batches written out of sequence order: %d != %d",
				i+1, e.writeBuf[i])
		}
		if e.publishBuf[i] != uint64(i+1) {
			t.Fatalf("batches published out of sequence order: %d != %d",
				i+1, e.publishBuf[i])
		}
	}
}

func BenchmarkCommitPipeline(b *testing.B) {
	for _, parallelism := range []int{1, 2, 4, 8, 16, 32, 64, 128} {
		b.Run(fmt.Sprintf("parallel=%d", parallelism), func(b *testing.B) {
			b.SetParallelism(parallelism)

			nullCommitEnv := commitEnv{
				apply: func(b *Batch) error {
					// time.Sleep(10 * time.Microsecond)
					return nil
				},
				publish: func(seqNum uint64) {
				},
				sync: func() error {
					// time.Sleep(time.Millisecond)
					return nil
				},
				write: func(group commitList) error {
					// time.Sleep(50 * time.Microsecond)
					return nil
				},
			}
			p := newCommitPipeline(nullCommitEnv, 1)
			defer p.close()

			b.RunParallel(func(pb *testing.PB) {
				batch := newBatch(nil)
				batch.Set([]byte("hello"), nil, nil)

				for pb.Next() {
					batch.setSeqNum(0)
					if err := p.commit(batch, true); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
