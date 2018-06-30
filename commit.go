package pebble

import (
	"sync/atomic"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type commitQueueNode struct {
	value *Batch
	next  unsafe.Pointer
}

type commitQueue struct {
	head unsafe.Pointer
	tail unsafe.Pointer
}

func (q *commitQueue) init() {
	dummy := &commitQueueNode{}
	q.head = unsafe.Pointer(dummy)
	q.tail = q.head
}

func (q *commitQueue) load(p *unsafe.Pointer) *commitQueueNode {
	return (*commitQueueNode)(atomic.LoadPointer(p))
}

func (q *commitQueue) cas(p *unsafe.Pointer, old, new *commitQueueNode) (ok bool) {
	return atomic.CompareAndSwapPointer(
		p, unsafe.Pointer(old), unsafe.Pointer(new))
}

func (q *commitQueue) enqueue(node *commitQueueNode) {
	for {
		last := q.load(&q.tail)
		next := q.load(&last.next)
		if last == q.load(&q.tail) {
			if next == nil {
				if q.cas(&last.next, next, node) {
					q.cas(&q.tail, last, node)
					return
				}
			} else {
				q.cas(&q.tail, last, next)
			}
		}
	}
}

func (q *commitQueue) dequeue() *Batch {
	for {
		first := q.load(&q.head)
		last := q.load(&q.tail)
		next := q.load(&first.next)
		if first == q.load(&q.head) {
			if first == last {
				if next == nil {
					// Queue is empty.
					return nil
				}
				q.cas(&q.tail, last, next)
			} else {
				v := next.value
				if atomic.LoadUint32(&v.applied) == 0 {
					// The first batch in the queue has not been applied.
					return nil
				}
				if q.cas(&q.head, first, next) {
					return v
				}
			}
		}
	}
}

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
		pending commitQueue
		nodes   []commitQueueNode
	}
}

func newCommitPipeline(env commitEnv, logSeqNum, visibleSeqNum *uint64) *commitPipeline {
	p := &commitPipeline{
		env:           env,
		logSeqNum:     logSeqNum,
		visibleSeqNum: visibleSeqNum,
	}
	p.write.pending.init()
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

	if len(w.nodes) == 0 {
		w.nodes = make([]commitQueueNode, 1024)
	}
	node := &w.nodes[0]
	w.nodes = w.nodes[1:]
	node.value = b

	// Enqueue the batch in the pending queue. Note that while the pending queue
	// is lock-free, we want the order of batches to be the same as the sequence
	// number order.
	w.pending.enqueue(node)

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

	// Mark the batch as applied.
	atomic.StoreUint32(&b.applied, 1)

	// Loop dequeuing applied batches from the pending queue. If our batch was
	// the head of the pending queue we are guaranteed that either we'll publish
	// it or someone else will dequeue and publish it. If our batch is not the
	// head of the queue then either we'll dequeue applied batches and reach our
	// batch or there is an unapplied batch blocking us. When that unapplied
	// batch applies it will go through the same process and publish our batch
	// for us.
	for {
		t := w.pending.dequeue()
		if t == nil {
			// Wait for another goroutine to publish us.
			b.published.Wait()
			break
		}

		// We're responsible for publishing the sequence number for batch t, but
		// another goroutine might have already increased the visible sequence
		// number past t's, so only ratchet if t's visible sequence number is past
		// the current visible sequence number.
		for {
			curSeqNum := atomic.LoadUint64(p.visibleSeqNum)
			newSeqNum := t.seqNum() + uint64(t.count())
			if newSeqNum <= curSeqNum {
				break
			}
			if atomic.CompareAndSwapUint64(p.visibleSeqNum, curSeqNum, newSeqNum) {
				break
			}
		}
		t.published.Done()
	}

	// TODO(peter): wait for the WAL to sync.
	return nil
}
