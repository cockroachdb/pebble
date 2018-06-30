package pebble

import (
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type commitQueueNode struct {
	position uint64
	value    unsafe.Pointer
}

// commitQueue maintains a circular ring buffer.
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
	roundUp := func(v uint64) uint64 {
		v--
		v |= v >> 1
		v |= v >> 2
		v |= v >> 4
		v |= v >> 8
		v |= v >> 16
		v |= v >> 32
		v++
		return v
	}

	// Size the commitQueue to 8x the number of CPUs. Note that this works around
	// a limitation (bug?) when the size of the queue is 1 (enqueue does not
	// properly block waiting for the sequence to advance).
	n := roundUp(uint64(runtime.NumCPU() * 8))

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

func (q *commitQueue) dequeue() *Batch {
	for {
		pos := atomic.LoadUint64(&q.read)
		if v := atomic.LoadUint64(&q.write); pos == v {
			return nil
		}
		n := &q.nodes[pos&q.mask]
		seq := atomic.LoadUint64(&n.position)
		if seq != pos+1 {
			runtime.Gosched()
			continue
		}

		b := (*Batch)(atomic.LoadPointer(&n.value))
		if b == nil || atomic.LoadUint32(&b.applied) == 0 {
			// The batch at the read index has either already been published or has
			// not been applied.
			return nil
		}

		if atomic.CompareAndSwapUint64(&q.read, pos, pos+1) {
			atomic.StorePointer(&n.value, nil)
			atomic.StoreUint64(&n.position, pos+q.mask+1)
			return b
		}

		runtime.Gosched() // free up the cpu before the next iteration
	}
}

type commitEnv struct {
	// Apply the batch to the in-memory state. Called concurrently.
	apply func(b *Batch) error
	// Reserve n bytes in the WAL, returning the start offset in the WAL at which
	// those bytes will be written. Must be matched with a call to
	// write(). Called serially with the write mutex held.
	reserve func(n int) uint64
	// Sync the WAL up to pos+n.
	sync func(pos uint64, n int) error
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
	// Prepare the batch for committing: determine the batch sequence number and
	// WAL position and enqueue the batch in the pending queue.
	pos := p.prepare(b)

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

	// Publish the batch sequence number.
	p.publish(b)

	if syncWAL {
		if err := p.env.sync(pos, len(b.data)); err != nil {
			return err
		}
	}
	return nil
}

func (p *commitPipeline) prepare(b *Batch) uint64 {
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

	// Reserve the WAL position.
	pos := p.env.reserve(len(b.data))

	w.Unlock()

	return pos
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
		w.cond.Signal()
	}
}
