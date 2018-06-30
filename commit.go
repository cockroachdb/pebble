package pebble

import (
	"math/bits"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

type commitQueueNode struct {
	position uint64
	value    unsafe.Pointer
	_        [6]uint64
}

// commitQueue maintains a circular ring buffer for a fixed-size
// single-producer, multiple-consumer queue.
type commitQueue struct {
	// The padding members are here to ensure each item is on a separate cache
	// line. This prevents false sharing and improves performance.
	_     [8]uint64
	write uint64
	_     [8]uint64
	read  uint64
	_     [8]uint64
	mask  uint64
	_     [8]uint64
	nodes []commitQueueNode
}

func (q *commitQueue) init() {
	// Size the commitQueue to 8x the number of CPUs. Note that this works around
	// a limitation (bug?) when the size of the queue is 1 (enqueue does not
	// properly block waiting for the sequence to advance).
	n := runtime.NumCPU()
	n = 1 << uint((64 - bits.LeadingZeros(uint(n-1))))
	n *= 8

	q.mask = uint64(n - 1)
	q.nodes = make([]commitQueueNode, n)
	for i := range q.nodes {
		q.nodes[i].position = uint64(i)
	}
}

// Enqueue a single batch. Wait on cond if the queue is full.
func (q *commitQueue) enqueue(b *Batch, cond *sync.Cond) {
	// Note that this is a single-producer, multi-consumer queue. The q.write
	// field is protected by commitPipeline.write.mu.
	for {
		pos := q.write
		n := &q.nodes[pos&q.mask]
		seq := atomic.LoadUint64(&n.position)
		if seq != pos {
			cond.Wait()
			continue
		}

		atomic.StoreUint64(&q.write, pos+1)
		atomic.StorePointer(&n.value, unsafe.Pointer(b))
		atomic.StoreUint64(&n.position, pos+1)
		return
	}
}

// Dequeue removes and returns the batch at the head of the queue if that batch
// has been applied (Batch.applied != 0). Returns nil if either the queue empty
// or the head batch is not been applied.
func (q *commitQueue) dequeue(cond *sync.Cond) *Batch {
	for {
		pos := atomic.LoadUint64(&q.read)
		n := &q.nodes[pos&q.mask]
		seq := atomic.LoadUint64(&n.position)
		if seq != pos+1 {
			if pos == atomic.LoadUint64(&q.write) {
				// The queue is empty.
				return nil
			}
			// The element at pos is being written.
			runtime.Gosched()
			continue
		}

		b := (*Batch)(atomic.LoadPointer(&n.value))
		if b == nil || atomic.LoadUint32(&b.applied) == 0 {
			// The batch at the read index has either already been published or has
			// not been applied.
			return nil
		}

		// Dequeue the element by bumping the read pointer.
		if atomic.CompareAndSwapUint64(&q.read, pos, pos+1) {
			atomic.StorePointer(&n.value, nil)
			atomic.StoreUint64(&n.position, pos+q.mask+1)
			cond.Signal()
			return b
		}

		// We failed to bump the read pointer. Loop and try again.
		runtime.Gosched() // free up the cpu before the next iteration
	}
}

type commitEnv struct {
	// Apply the batch to the in-memory state. Called concurrently.
	apply func(b *Batch) error
	// Sync the WAL up to pos+n. Called concurrently.
	sync func(pos, n int64) error
	// Write the batch to the WAL. The data is not persisted until a call to
	// sync() is performed. Returns the WAL position at which the data was
	// written which can be used in a subsequent call to sync(). Called serially.
	write func(data []byte) (int64, error)
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
		sync.Mutex
		cond    sync.Cond
		pending commitQueue
	}
}

func newCommitPipeline(env commitEnv, logSeqNum, visibleSeqNum *uint64) *commitPipeline {
	p := &commitPipeline{
		env:           env,
		logSeqNum:     logSeqNum,
		visibleSeqNum: visibleSeqNum,
	}
	p.write.cond.L = &p.write.Mutex
	p.write.pending.init()
	return p
}

// Commit the specified batch, writing it to the WAL, optionally syncing the
// WAL, and applying the batch to the memtable. Upon successful return the
// batch's mutations will be visible for reading.
func (p *commitPipeline) commit(b *Batch, syncWAL bool) error {
	// Prepare the batch for committing: enqueuing the batch in the pending
	// queue, determining the batch sequence number and writing the data to the
	// WAL.
	pos, err := p.prepare(b)
	if err != nil {
		// TODO(peter): what to do on error? the pipeline will be horked at this
		// point.
	}

	// Apply the batch to the memtable.
	if err := p.env.apply(b); err != nil {
		// TODO(peter): what to do on error? the pipeline will be horked at this
		// point.
		return err
	}

	// Publish the batch sequence number.
	p.publish(b)

	if syncWAL {
		if err := p.env.sync(pos, int64(len(b.data))); err != nil {
			return err
		}
	}
	return nil
}

func (p *commitPipeline) prepare(b *Batch) (int64, error) {
	b.published.Add(1)
	n := uint64(b.count())

	w := &p.write
	w.Lock()

	// Enqueue the batch in the pending queue. Note that while the pending queue
	// is lock-free, we want the order of batches to be the same as the sequence
	// number order.
	w.pending.enqueue(b, &w.cond)

	// Assign the batch a sequence number.
	b.setSeqNum(atomic.AddUint64(p.logSeqNum, n) - n)

	// Write the data to the WAL.
	pos, err := p.env.write(b.data)

	w.Unlock()

	return pos, err
}

func (p *commitPipeline) publish(b *Batch) {
	w := &p.write

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
		t := w.pending.dequeue(&w.cond)
		if t == nil {
			// Wait for another goroutine to publish us.
			b.published.Wait()
			break
		}

		// We're responsible for publishing the sequence number for batch t, but
		// another concurrent goroutine might sneak in a publish the sequence
		// number for a subsequent batch. That's ok as all we're guaranteeing is
		// that the sequence number ratchets up.
		for {
			curSeqNum := atomic.LoadUint64(p.visibleSeqNum)
			newSeqNum := t.seqNum() + uint64(t.count())
			if newSeqNum <= curSeqNum {
				// t's sequence number has already been published.
				break
			}
			if atomic.CompareAndSwapUint64(p.visibleSeqNum, curSeqNum, newSeqNum) {
				// We successfully published t's sequence number.
				break
			}
		}

		t.published.Done()
	}
}
