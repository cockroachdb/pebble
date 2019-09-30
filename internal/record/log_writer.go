// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package record

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble/internal/crc"
)

type block struct {
	// buf[:written] has already been filled with fragments. Updated atomically.
	written int32
	// buf[:flushed] has already been flushed to w.
	flushed int32
	buf     [blockSize]byte
}

type flusher interface {
	Flush() error
}

type syncer interface {
	Sync() error
}

const (
	syncConcurrencyBits = 9

	// SyncConcurrency is the maximum number of concurrent sync operations that
	// can be performed. Note that a sync operation is initiated either by a call
	// to SyncRecord or by a call to Close. Exported as this value also limits
	// the commit concurrency in commitPipeline.
	SyncConcurrency = 1 << syncConcurrencyBits
)

type syncSlot struct {
	wg  *sync.WaitGroup
	err *error
}

// syncQueue is a lock-free fixed-size single-producer, single-consumer
// queue. The single-producer can push to the head, and the single-consumer can
// pop multiple values from the tail. Popping calls Done() on each of the
// available *sync.WaitGroup elements.
type syncQueue struct {
	// headTail packs together a 32-bit head index and a 32-bit tail index. Both
	// are indexes into slots modulo len(slots)-1.
	//
	// tail = index of oldest data in queue
	// head = index of next slot to fill
	//
	// Slots in the range [tail, head) are owned by consumers.  A consumer
	// continues to own a slot outside this range until it nils the slot, at
	// which point ownership passes to the producer.
	//
	// The head index is stored in the most-significant bits so that we can
	// atomically add to it and the overflow is harmless.
	headTail uint64

	// slots is a ring buffer of values stored in this queue. The size must be a
	// power of 2. A slot is in use until the tail index has moved beyond it.
	slots [SyncConcurrency]syncSlot
}

const dequeueBits = 32

func (q *syncQueue) unpack(ptrs uint64) (head, tail uint32) {
	const mask = 1<<dequeueBits - 1
	head = uint32((ptrs >> dequeueBits) & mask)
	tail = uint32(ptrs & mask)
	return
}

func (q *syncQueue) push(wg *sync.WaitGroup, err *error) {
	ptrs := atomic.LoadUint64(&q.headTail)
	head, tail := q.unpack(ptrs)
	if (tail+uint32(len(q.slots)))&(1<<dequeueBits-1) == head {
		panic("pebble: queue is full")
	}

	slot := &q.slots[head&uint32(len(q.slots)-1)]
	slot.wg = wg
	slot.err = err

	// Increment head. This passes ownership of slot to dequeue and acts as a
	// store barrier for writing the slot.
	atomic.AddUint64(&q.headTail, 1<<dequeueBits)
}

func (q *syncQueue) empty() bool {
	head, tail := q.load()
	return head == tail
}

func (q *syncQueue) load() (head, tail uint32) {
	ptrs := atomic.LoadUint64(&q.headTail)
	head, tail = q.unpack(ptrs)
	return head, tail
}

func (q *syncQueue) pop(head, tail uint32, err error) error {
	if tail == head {
		// Queue is empty.
		return nil
	}

	for ; tail != head; tail++ {
		slot := &q.slots[tail&uint32(len(q.slots)-1)]
		wg := slot.wg
		if wg == nil {
			return fmt.Errorf("nil waiter at %d", tail&uint32(len(q.slots)-1))
		}
		*slot.err = err
		slot.wg = nil
		slot.err = nil
		// We need to bump the tail count before signalling the wait group as
		// signalling the wait group can trigger release a blocked goroutine which
		// will try to enqueue before we've "freed" space in the queue.
		atomic.AddUint64(&q.headTail, 1)
		wg.Done()
	}

	return nil
}

// flusherCond is a specialized condition variable that is safe to signal for
// readiness without holding the associated mutex in some circumstances. In
// particular, when waiter is added to syncQueue, this condition variable can
// be signalled without holding flusher.Mutex.
type flusherCond struct {
	mu   *sync.Mutex
	q    *syncQueue
	cond sync.Cond
}

func (c *flusherCond) init(mu *sync.Mutex, q *syncQueue) {
	c.mu = mu
	c.q = q
	// Yes, this is a bit circular, but that is intentional. flusherCond.cond.L
	// points flusherCond so that when cond.L.Unlock is called flusherCond.Unlock
	// will be called and we can check the !syncQueue.empty() condition.
	c.cond.L = c
}

func (c *flusherCond) Signal() {
	// Pass-through to the cond var.
	c.cond.Signal()
}

func (c *flusherCond) Wait() {
	// Pass-through to the cond var. Note that internally the cond var implements
	// Wait as:
	//
	//   t := notifyListAdd()
	//   L.Unlock()
	//   notifyListWait(t)
	//   L.Lock()
	//
	// We've configured the cond var to call flusherReady.Unlock() which allows
	// us to check the !syncQueue.empty() condition without a danger of missing a
	// notification. Any call to flusherReady.Signal() after notifyListAdd() is
	// called will cause the subsequent notifyListWait() to return immediately.
	c.cond.Wait()
}

func (c *flusherCond) Lock() {
	c.mu.Lock()
}

func (c *flusherCond) Unlock() {
	c.mu.Unlock()
	if !c.q.empty() {
		// If the current goroutine is about to block on sync.Cond.Wait, this call
		// to Signal will prevent that. The comment in Wait above explains a bit
		// about what is going on here, but it is worth reiterating:
		//
		//   flusherCond.Wait()
		//     sync.Cond.Wait()
		//       t := notifyListAdd()
		//       flusherCond.Unlock()    <-- we are here
		//       notifyListWait(t)
		//       flusherCond.Lock()
		//
		// The call to Signal here results in:
		//
		//     sync.Cond.Signal()
		//       notifyListNotifyOne()
		//
		// The call to notifyListNotifyOne() will prevent the call to
		// notifyListWait(t) from blocking.
		c.cond.Signal()
	}
}

// LogWriter writes records to an underlying io.Writer. In order to support WAL
// file reuse, a LogWriter's records are tagged with the WAL's file
// number. When reading a log file a record from a previous incarnation of the
// file will return the error ErrInvalidLogNum.
type LogWriter struct {
	// w is the underlying writer.
	w io.Writer
	// c is w as a closer.
	c io.Closer
	// f is w as a flusher.
	f flusher
	// s is w as a syncer.
	s syncer
	// logNum is the low 32-bits of the log's file number.
	logNum uint32
	// blockNum is the zero based block number for the current block.
	blockNum int64
	// err is any accumulated error. TODO(peter): This needs to be protected in
	// some fashion. Perhaps using atomic.Value.
	err error
	// block is the current block being written. Protected by flusher.Mutex.
	block *block
	free  chan *block

	flusher struct {
		sync.Mutex
		// Flusher ready is a condition variable that is signalled when there are
		// blocks to flush, syncing has been requested, or the LogWriter has been
		// closed. For signalling of a sync, it is safe to call without holding
		// flusher.Mutex.
		ready flusherCond
		// Has the writer been closed?
		closed bool
		// Accumulated flush error.
		err     error
		pending []*block
		syncQ   syncQueue
	}
}

// NewLogWriter returns a new LogWriter.
func NewLogWriter(w io.Writer, logNum uint64) *LogWriter {
	c, _ := w.(io.Closer)
	f, _ := w.(flusher)
	s, _ := w.(syncer)
	r := &LogWriter{
		w: w,
		c: c,
		f: f,
		s: s,
		// NB: we truncate the 64-bit log number to 32-bits. This is ok because a)
		// we are very unlikely to reach a file number of 4 billion and b) the log
		// number is used as a validation check and using only the low 32-bits is
		// sufficient for that purpose.
		logNum: uint32(logNum),
		free:   make(chan *block, 4),
	}
	for i := 0; i < cap(r.free); i++ {
		r.free <- &block{}
	}
	r.block = <-r.free
	r.flusher.ready.init(&r.flusher.Mutex, &r.flusher.syncQ)
	go r.flushLoop()
	return r
}

func (w *LogWriter) flushLoop() {
	f := &w.flusher
	f.Lock()
	defer f.Unlock()

	for {
		var data []byte
		for {
			if f.closed {
				return
			}
			// Grab the portion of the current block that requires flushing. Note that
			// the current block can be added to the pending blocks list after we release
			// the flusher lock, but it won't be part of pending.
			written := atomic.LoadInt32(&w.block.written)
			data = w.block.buf[w.block.flushed:written]
			w.block.flushed = written
			if len(f.pending) > 0 || len(data) > 0 || !f.syncQ.empty() {
				break
			}
			f.ready.Wait()
			continue
		}

		pending := f.pending
		f.pending = f.pending[len(f.pending):]
		head, tail := f.syncQ.load()

		f.Unlock()

		var err error
		for _, b := range pending {
			if err = w.flushBlock(b); err != nil {
				break
			}
		}
		if err == nil && len(data) > 0 {
			_, err = w.w.Write(data)
		}
		if head != tail {
			if err == nil && w.s != nil {
				err = w.s.Sync()
			}
			if popErr := f.syncQ.pop(head, tail, err); popErr != nil {
				// This slightly odd code structure prevents panic'ing without the lock
				// held which would immediately hit a Go runtime error when the defer
				// tries to unlock a mutex that isn't locked.
				f.Lock()
				panic(popErr)
			}
		}

		f.Lock()
		f.err = err
		if f.err != nil {
			// TODO(peter): There might be new waiters that we should propagate f.err
			// to. Because f.err is now set, we only have to perform a single extra
			// clearing of those waiters as no new ones can arrive via SyncRecord().
			return
		}
	}
}

func (w *LogWriter) flushBlock(b *block) error {
	if _, err := w.w.Write(b.buf[b.flushed:]); err != nil {
		return err
	}
	b.written = 0
	b.flushed = 0
	w.free <- b
	return nil
}

// queueBlock queues the current block for writing to the underlying writer,
// allocates a new block and reserves space for the next header.
func (w *LogWriter) queueBlock() {
	// Allocate a new block, blocking until one is available. We do this first
	// because w.block is protected by w.flusher.Mutex.
	nextBlock := <-w.free

	f := &w.flusher
	f.Lock()
	f.pending = append(f.pending, w.block)
	w.block = nextBlock
	f.ready.Signal()
	w.err = w.flusher.err
	f.Unlock()

	w.blockNum++
}

// Close flushes and syncs any unwritten data and closes the writer.
func (w *LogWriter) Close() error {
	f := &w.flusher

	// Force a sync of any unwritten data.
	wg := &sync.WaitGroup{}
	var err error
	wg.Add(1)
	f.syncQ.push(wg, &err)
	f.ready.Signal()
	wg.Wait()

	f.Lock()
	f.closed = true
	f.ready.Signal()
	f.Unlock()

	if w.c != nil {
		if err := w.c.Close(); err != nil {
			return err
		}
	}
	w.err = errors.New("pebble/record: closed LogWriter")
	return err
}

// WriteRecord writes a complete record. Returns the offset just past the end
// of the record.
func (w *LogWriter) WriteRecord(p []byte) (int64, error) {
	return w.SyncRecord(p, nil, nil)
}

// SyncRecord writes a complete record. If wg!= nil the record will be
// asynchronously persisted to the underlying writer and done will be called on
// the wait group upon completion. Returns the offset just past the end of the
// record.
func (w *LogWriter) SyncRecord(p []byte, wg *sync.WaitGroup, err *error) (int64, error) {
	if w.err != nil {
		return -1, w.err
	}

	// The `i == 0` condition ensures we handle empty records. Such records can
	// possibly be generated for VersionEdits stored in the MANIFEST. While the
	// MANIFEST is currently written using Writer, it is good to support the same
	// semantics with LogWriter.
	for i := 0; i == 0 || len(p) > 0; i++ {
		p = w.emitFragment(i, p)
	}

	if wg != nil {
		// If we've been asked to persist the record, add the WaitGroup to the sync
		// queue and signal the flushLoop. Note that flushLoop will write partial
		// blocks to the file if syncing has been requested. The contract is that
		// any record written to the LogWriter to this point will be flushed to the
		// OS and synced to disk.
		f := &w.flusher
		f.syncQ.push(wg, err)
		f.ready.Signal()
	}

	offset := w.blockNum*blockSize + int64(w.block.written)
	// Note that we don't return w.err here as a concurrent call to Close would
	// race with our read. That's ok because the only error we could be seeing is
	// one to syncing for which the caller can receive notification of by passing
	// in a non-nil err argument.
	return offset, nil
}

// Size returns the current size of the file.
func (w *LogWriter) Size() int64 {
	return w.blockNum*blockSize + int64(w.block.written)
}

func (w *LogWriter) emitFragment(n int, p []byte) []byte {
	b := w.block
	i := b.written
	first := n == 0
	last := blockSize-i-recyclableHeaderSize >= int32(len(p))

	if last {
		if first {
			b.buf[i+6] = recyclableFullChunkType
		} else {
			b.buf[i+6] = recyclableLastChunkType
		}
	} else {
		if first {
			b.buf[i+6] = recyclableFirstChunkType
		} else {
			b.buf[i+6] = recyclableMiddleChunkType
		}
	}

	binary.LittleEndian.PutUint32(b.buf[i+7:i+11], w.logNum)

	r := copy(b.buf[i+recyclableHeaderSize:], p)
	j := i + int32(recyclableHeaderSize+r)
	binary.LittleEndian.PutUint32(b.buf[i+0:i+4], crc.New(b.buf[i+6:j]).Value())
	binary.LittleEndian.PutUint16(b.buf[i+4:i+6], uint16(r))
	atomic.StoreInt32(&b.written, j)

	if blockSize-b.written < recyclableHeaderSize {
		// There is no room for another fragment in the block, so fill the
		// remaining bytes with zeros and queue the block for flushing.
		for i := b.written; i < blockSize; i++ {
			b.buf[i] = 0
		}
		atomic.StoreInt32(&b.written, j)
		w.queueBlock()
	}
	return p[r:]
}
