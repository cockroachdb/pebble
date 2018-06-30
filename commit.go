package pebble

import (
	"fmt"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type commitEnv struct {
	// Apply the batch to the in-memory state. Called concurrently.
	apply func(b *Batch) error
	// Reserve n bytes in the WAL, returning the start offset in the WAL at which
	// those bytes will be written. Must be matched with a call to
	// write(). Called serially with the write mutex held.
	reserve func(n int) uint64
	// Sync the WAL. Called serially.
	sync func() error
	// Write the batch to the WAL. A previous call to reserve(len(data)) needs to
	// be performed to retrieve the supplied position. Called concurrently.
	write func(pos uint64, data []byte) error
}

// A commitPipeline manages the commit commitPipeline: writing batches to the
// WAL, optionally syncing the WAL, and applying the batches to the memtable. A
// commitPipeline groups batches together before writing them to the WAL to
// optimize the WAL write behavior. After a batch has been written to the WAL,
// if the batch requested syncing it will wait for the next WAL sync to
// occur. Next, the commitPipeline applies the written (and synced) batches to
// the memtable concurrently (using the goroutine that called
// commitPipeline.commit). Lastly, the commitPipeline publishes that visible
// sequence number ensuring that the sequence number only ratchets up.
type commitPipeline struct {
	env commitEnv

	// The next sequence number to give to a batch. Only mutated atomically the
	// current WAL writer.
	logSeqNum     *uint64
	visibleSeqNum *uint64

	// State for writing to the WAL.
	write struct {
		syncutil.Mutex
		head *Batch
		tail *Batch
	}
}

func newCommitPipeline(env commitEnv, logSeqNum, visibleSeqNum *uint64) *commitPipeline {
	p := &commitPipeline{
		env:           env,
		logSeqNum:     logSeqNum,
		visibleSeqNum: visibleSeqNum,
	}
	return p
}

func (p *commitPipeline) close() {
}

// Commit the specified batch, writing it to the WAL, optionally syncing the
// WAL, and applying the batch to the memtable. Upon successful return the
// batch's mutations will be visible for reading.
func (p *commitPipeline) commit(b *Batch, syncWAL bool) error {
	b.published.Add(1)
	n := uint64(b.count())
	w := &p.write
	w.Lock()

	// Assign the batch a sequence number.
	b.setSeqNum(atomic.AddUint64(p.logSeqNum, n) - n)

	// Reserve the WAL position.
	pos := p.env.reserve(len(b.data))

	if w.head == nil {
		w.head = b
		w.tail = b
	} else {
		w.tail.next = b
		w.tail = b
	}

	w.Unlock()

	// Fill the WAL reservation.
	if err := p.env.write(pos, b.data); err != nil {
		// TODO(peter): what to do on error? the pipeline will be horked at this
		// point.
		return err
	}

	// Apply the batch to the memtable.
	if err := p.env.apply(b); err != nil {
		// TODO(peter): what to do on error? the pipeline will be horked at this
		// point.
		return err
	}

	w.Lock()
	b.applied = true
	if w.head == b {
		// Our batch is the head of the queue. Publish it and any subsequent
		// batches that have already been applied.
		for w.head != nil {
			t := w.head
			if !t.applied {
				break
			}
			w.head = w.head.next
			if w.head == nil {
				w.tail = nil
			}
			oldSeqNum := t.seqNum()
			newSeqNum := oldSeqNum + uint64(t.count())
			if !atomic.CompareAndSwapUint64(p.visibleSeqNum, oldSeqNum, newSeqNum) {
				panic(fmt.Sprintf("bad visible sequence number transition: visible=%d old=%d new=%d",
					atomic.LoadUint64(p.visibleSeqNum), oldSeqNum, newSeqNum))
			}
			t.next = nil
			t.published.Done()
		}
		w.Unlock()
	} else {
		w.Unlock()
		// Wait for our batch to be published.
		b.published.Wait()
	}

	// TODO(peter): wait for the WAL to sync.
	return nil
}
