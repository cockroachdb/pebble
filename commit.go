package pebble

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type commitState struct {
	writeWG sync.WaitGroup
	applyWG *sync.WaitGroup
	// TODO(peter): Is there a faster approach to achieving this end? Rather than
	// a per-batch wait-group, we could use a per-group wait-group. That would
	// use slightly less memory and involve somewhat less synchronization. A
	// wait-group per group is essentially what RocksDB provides with its
	// concurrent memtable inserts code.
	prevWG *sync.WaitGroup
	next   *Batch
	sync   bool
	err    error
}

// commitList holds a single-linked list of batches that are waiting to be
// written to the WAL or synced to disk.
type commitList struct {
	head *Batch
	tail *Batch
}

func (l *commitList) clear() {
	l.head = nil
	l.tail = nil
}

func (l *commitList) push(e *Batch) {
	if l.head == nil {
		l.head = e
		l.tail = e
	} else {
		l.tail.commit.next = e
		l.tail = e
	}
}

func (l *commitList) splice(other commitList) {
	if l.head == nil {
		*l = other
	} else {
		l.tail.commit.next = other.head
		l.tail = other.tail
	}
}

func (l *commitList) empty() bool {
	return l.head == nil
}

type commitEnv struct {
	// Apply the batch to the in-memory state. Called concurrently.
	apply func(b *Batch) error
	// Sync the WAL. Called serially.
	sync func() error
	// Write the batch group to the WAL. Called serially.
	write func(group commitList) error
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
		writing bool
		pending commitList
		// A buffer for chunked allocation of apply wait-groups which must outlive
		// the lifetime of the Batch they are assigned to.
		buf []sync.WaitGroup
	}

	// State for syncing the WAL.
	syncer struct {
		syncutil.Mutex
		cond    sync.Cond
		closed  bool
		pending commitList
	}
}

func newCommitPipeline(env commitEnv, logSeqNum, visibleSeqNum *uint64) *commitPipeline {
	p := &commitPipeline{
		env:           env,
		logSeqNum:     logSeqNum,
		visibleSeqNum: visibleSeqNum,
	}
	p.write.cond.L = &p.write.Mutex
	p.syncer.cond.L = &p.syncer.Mutex
	go p.syncLoop()
	return p
}

func (p *commitPipeline) syncLoop() {
	s := &p.syncer
	s.Lock()
	defer s.Unlock()

	for {
		for s.pending.empty() && !s.closed {
			s.cond.Wait()
		}
		if s.closed {
			return
		}
		pending := s.pending
		s.pending.clear()

		s.Unlock()

		err := p.env.sync()

		for b, next := pending.head, (*Batch)(nil); b != nil; b = next {
			// Clear the batch's next link before signalling the commit wait group.
			next, b.commit.next = b.commit.next, nil
			b.commit.err = err
			b.commit.writeWG.Done()
		}

		s.Lock()
	}
}

func (p *commitPipeline) close() {
	p.syncer.Lock()
	p.syncer.closed = true
	p.syncer.cond.Signal()
	p.syncer.Unlock()
}

// Commit the specified batch, writing it to the WAL, optionally syncing the
// WAL, and applying the batch to the memtable. Upon successful return the
// batch's mutations will be visible for reading.
func (p *commitPipeline) commit(b *Batch, syncWAL bool) error {
	b.commit.writeWG.Add(1)
	b.commit.sync = syncWAL

	w := &p.write
	w.Lock()

	leader := w.pending.empty()
	w.pending.push(b)

	if leader {
		// We're the leader. Wait for any running commit to finish.
		for w.writing {
			w.cond.Wait()
		}
		pending := w.pending
		w.pending.clear()
		w.writing = true
		w.Unlock()

		// Set the sequence number for each member of the group.
		for b := pending.head; b != nil; b = b.commit.next {
			n := uint64(b.count())
			b.setSeqNum(atomic.AddUint64(p.logSeqNum, n) - n)

			if len(w.buf) > 0 {
				b.commit.prevWG = &w.buf[0]
				w.buf = w.buf[1:]
			}
			if len(w.buf) == 0 {
				w.buf = make([]sync.WaitGroup, 256)
			}
			b.commit.applyWG = &w.buf[0]
			b.commit.applyWG.Add(1)
		}

		// Write the group to the WAL.
		err := p.env.write(pending)

		// We're done writing the group, let the next group of batches proceed.
		w.Lock()
		w.writing = false
		w.cond.Signal()
		w.Unlock()

		// Propagate the error to all of the group's batches. If a batch requires
		// syncing and the WAL write was successful, add it to the syncing list.
		var syncing commitList
		for b, next := pending.head, (*Batch)(nil); b != nil; b = next {
			// Clear the batch's next link before signalling the commit wait group.
			next, b.commit.next = b.commit.next, nil
			if err != nil || !b.commit.sync {
				b.commit.err = err
				b.commit.writeWG.Done()
			} else {
				syncing.push(b)
			}
		}

		if !syncing.empty() {
			// The WAL write was successful and one or more of the batches requires
			// syncing: notify the sync goroutine.
			s := &p.syncer
			s.Lock()
			s.pending.splice(syncing)
			s.cond.Signal()
			s.Unlock()
		}
	} else {
		w.Unlock()
	}

	// Wait for the write/sync to finish.
	b.commit.writeWG.Wait()
	if b.commit.err != nil {
		return b.commit.err
	}

	// Apply this batch to the memtable.
	if err := p.env.apply(b); err != nil {
		return err
	}

	// Wait for the previous batch (if any) to have applied. This prevents this
	// batch's writes from becoming visible before the previous batch has
	// finished applying.
	if b.commit.prevWG != nil {
		b.commit.prevWG.Wait()
	}

	// Publish this batch's writes and then notify any waiter.
	oldSeqNum := b.seqNum()
	newSeqNum := oldSeqNum + uint64(b.count())
	if !atomic.CompareAndSwapUint64(p.visibleSeqNum, oldSeqNum, newSeqNum) {
		panic(fmt.Sprintf("bad visible sequence number transition: visible=%d old=%d new=%d",
			atomic.LoadUint64(p.visibleSeqNum), oldSeqNum, newSeqNum))
	}
	b.commit.applyWG.Done()

	return nil
}
