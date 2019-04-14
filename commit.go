// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

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

// commitEnv contains the environment that a commitPipeline interacts
// with. This allows fine-grained testing of commitPipeline behavior without
// construction of an entire DB.
type commitEnv struct {
	// The next sequence number to give to a batch. Protected by
	// commitPipeline.mu.
	logSeqNum *uint64
	// The visible sequence number at which reads should be performed. Ratcheted
	// upwards atomically as batches are applied to the memtable.
	visibleSeqNum *uint64

	// Apply the batch to the specified memtable. Called concurrently.
	apply func(b *Batch, mem *memTable) error
	// Sync the WAL. Called serially by the sync goroutine.
	sync func() error
	// Write the batch to the WAL. The data is not persisted until a call to
	// sync() is performed. Returns the memtable the batch should be applied
	// to. Serial execution enforced by commitPipeline.mu.
	write func(b *Batch) (*memTable, error)
}

// A commitPipeline manages the stages of committing a set of mutations
// (contained in a single Batch) atomically to the DB. The steps are
// conceptually:
//
//   1. Write the batch to the WAL and optionally sync the WAL
//   2. Apply the mutations in the batch to the memtable
//
// These two simple steps are made complicated by the desire for high
// performance. In the absence of concurrency, performance is limited by how
// fast a batch can be written (and synced) to the WAL and then added to the
// memtable, both of which are outside the purview of the commit
// pipeline. Performance under concurrency is the primary concern of the commit
// pipeline, though it also needs to maintain two invariants:
//
//   1. Batches need to be written to the WAL in sequence number order.
//   2. Batches need to be made visible for reads in sequence number order. This
//      invariant arises from the use of a single sequence number which
//      indicates which mutations are visible.
//
// Taking these invariants into account, let's revisit the work the commit
// pipeline needs to perform. Writing the batch to the WAL is necessarily
// serialized as there is a single WAL object. The order of the entries in the
// WAL defines the sequence number order. Note that writing to the WAL is
// extremely fast, usually just a memory copy. Applying the mutations in a
// batch to the memtable can occur concurrently as the underlying skiplist
// supports concurrent insertions. Publishing the visible sequence number is
// another serialization point, but one with a twist: the visible sequence
// number cannot be bumped until the mutations for earlier batches have
// finished applying to the memtable (the visible sequence number only ratchets
// up). Lastly, if requested, the commit waits for the WAL to sync. Note that
// waiting for the WAL sync after ratcheting the visible sequence number allows
// another goroutine to read committed data before the WAL has synced. This is
// similar behavior to RocksDB's manual WAL flush functionality. Application
// code needs to protect against this if necessary.
//
// The full outline of the commit pipeline operation is as follows:
//
//   with commitPipeline mutex locked:
//     assign batch sequence number
//     write batch to WAL
//   (optionally) add batch to WAL sync list
//   apply batch to memtable (concurrently)
//   wait for earlier batches to apply
//   ratchet read sequence number
//   (optionally) wait for the WAL to sync
//
// As soon as a batch has been written to the WAL, the commitPipeline mutex is
// released allowing another batch to write to the WAL. Each commit operation
// individually applies its batch to the memtable providing concurrency. The
// WAL sync happens concurrently with applying to the memtable (see
// commitPipeline.syncLoop).
//
// The "waits for earlier batches to apply" work is more complicated than might
// be expected. The obvious approach would be to keep a queue of pending
// batches and for each batch to wait for the previous batch to finish
// committing. This approach was tried initially and turned out to be too
// slow. The problem is that it causes excessive goroutine activity as each
// committing goroutine needs to wake up in order for the next goroutine to be
// unblocked. The approach taken in the current code is conceptually similar,
// though it avoids waking a goroutine to perform work that another goroutine
// can perform. A commitQueue (a single-producer, multiple-consumer queue)
// holds the ordered list of committing batches. Addition to the queue is done
// while holding commitPipeline.mutex ensuring the same ordering of batches in
// the queue as the ordering in the WAL. When a batch finishes applying to the
// memtable, it atomically updates its Batch.applied field. Ratcheting of the
// visible sequence number is done by commitPipeline.publish which loops
// dequeueing "applied" batches and ratcheting the visible sequence number. If
// we hit an unapplied batch at the head of the queue we can block as we know
// that committing of that unapplied batch will eventually find our (applied)
// batch in the queue. See commitPipeline.publish for additional commentary.
type commitPipeline struct {
	env commitEnv
	// The mutex to use for synchronizing access to logSeqNum and serializing
	// calls to commitEnv.write().
	mu sync.Mutex
	// Condition var to signal upon changes to the pending queue.
	cond sync.Cond
	// Queue of pending batches to commit.
	pending commitQueue

	syncer struct {
		sync.Mutex
		cond    sync.Cond
		closed  bool
		pending []*Batch
	}
}

func newCommitPipeline(env commitEnv) *commitPipeline {
	p := &commitPipeline{
		env: env,
	}
	p.cond.L = &p.mu
	p.pending.init()
	p.syncer.cond.L = &p.syncer.Mutex
	go p.syncLoop()
	return p
}

func (p *commitPipeline) syncLoop() {
	s := &p.syncer
	s.Lock()
	defer s.Unlock()

	for {
		for len(s.pending) == 0 && !s.closed {
			s.cond.Wait()
		}
		if s.closed {
			return
		}

		pending := s.pending
		s.pending = nil

		s.Unlock()

		if err := p.env.sync(); err != nil {
			// TODO(peter): Handle error notification.
			panic(err)
		}

		for _, b := range pending {
			b.commit.Done()
		}

		s.Lock()
	}
}

func (p *commitPipeline) Close() {
	p.syncer.Lock()
	p.syncer.closed = true
	p.syncer.cond.Broadcast()
	p.syncer.Unlock()
}

// Commit the specified batch, writing it to the WAL, optionally syncing the
// WAL, and applying the batch to the memtable. Upon successful return the
// batch's mutations will be visible for reading.
func (p *commitPipeline) Commit(b *Batch, syncWAL bool) error {
	if len(b.storage.data) == 0 {
		return nil
	}

	// Prepare the batch for committing: enqueuing the batch in the pending
	// queue, determining the batch sequence number and writing the data to the
	// WAL.
	mem, err := p.prepare(b, syncWAL)
	if err != nil {
		// TODO(peter): what to do on error? the pipeline will be horked at this
		// point.
		panic(err)
	}

	// Apply the batch to the memtable.
	if err := p.env.apply(b, mem); err != nil {
		// TODO(peter): what to do on error? the pipeline will be horked at this
		// point.
		panic(err)
	}

	// Publish the batch sequence number.
	p.publish(b)

	return nil
}

// AllocateSeqNum allocates a sequence number, invokes the prepare callback,
// then the apply callback, and then publishes the sequence
// number. AllocateSeqNum does not write to the WAL or add entries to the
// memtable. AllocateSeqNum can be used to sequence an operation such as
// sstable ingestion within the commit pipeline. The prepare callback is
// invoked with commitPipeline.mu held, but note that DB.mu is not held and
// must be locked if necessary.
func (p *commitPipeline) AllocateSeqNum(prepare func(), apply func(seqNum uint64)) {
	// This method is similar to Commit and prepare. Be careful about trying to
	// share additional code with those methods because Commit and prepare are
	// performance critical code paths.

	b := newBatch(nil)
	defer b.release()

	// Give the batch a count of 1 so that the log and visible sequence number
	// are incremented correctly.
	b.storage.data = make([]byte, batchHeaderLen)
	b.setCount(1)
	b.commit.Add(1)

	p.mu.Lock()

	// Enqueue the batch in the pending queue. Note that while the pending queue
	// is lock-free, we want the order of batches to be the same as the sequence
	// number order.
	p.pending.enqueue(b, &p.cond)

	// Assign the batch a sequence number.
	seqNum := *p.env.logSeqNum
	*p.env.logSeqNum++
	if seqNum == 0 {
		seqNum = *p.env.logSeqNum
		*p.env.logSeqNum++
		b.setCount(2)
	}
	b.setSeqNum(seqNum)

	// Invoke the prepare callback. Note the lack of error reporting. Even if the
	// callback internally fails, the sequence number needs to be published in
	// order to allow the commit pipeline to proceed.
	prepare()

	p.mu.Unlock()

	// Invoke the apply callback.
	apply(b.seqNum())

	// Publish the sequence number.
	p.publish(b)
}

func (p *commitPipeline) prepare(b *Batch, syncWAL bool) (*memTable, error) {
	n := uint64(b.count())
	if n == invalidBatchCount {
		return nil, ErrInvalidBatch
	}
	count := 1
	if syncWAL {
		count++
	}
	b.commit.Add(count)

	p.mu.Lock()

	// Enqueue the batch in the pending queue. Note that while the pending queue
	// is lock-free, we want the order of batches to be the same as the sequence
	// number order.
	p.pending.enqueue(b, &p.cond)

	// Assign the batch a sequence number.
	b.setSeqNum(*p.env.logSeqNum)
	*p.env.logSeqNum += n

	// Write the data to the WAL.
	mem, err := p.env.write(b)

	p.mu.Unlock()

	if syncWAL {
		s := &p.syncer
		s.Lock()
		s.pending = append(s.pending, b)
		s.cond.Signal()
		s.Unlock()
	}

	return mem, err
}

func (p *commitPipeline) publish(b *Batch) {
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
		t := p.pending.dequeue(&p.cond)
		if t == nil {
			// Wait for another goroutine to publish us. We might also be waiting for
			// the WAL sync to finish.
			b.commit.Wait()
			break
		}

		// We're responsible for publishing the sequence number for batch t, but
		// another concurrent goroutine might sneak in and publish the sequence
		// number for a subsequent batch. That's ok as all we're guaranteeing is
		// that the sequence number ratchets up.
		for {
			curSeqNum := atomic.LoadUint64(p.env.visibleSeqNum)
			newSeqNum := t.seqNum() + uint64(t.count())
			if newSeqNum <= curSeqNum {
				// t's sequence number has already been published.
				break
			}
			if atomic.CompareAndSwapUint64(p.env.visibleSeqNum, curSeqNum, newSeqNum) {
				// We successfully published t's sequence number.
				break
			}
		}

		t.commit.Done()
	}
}
