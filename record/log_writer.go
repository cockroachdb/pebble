package record

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"

	"github.com/petermattis/pebble/crc"
)

type block struct {
	// buf[:written] has already been filled with fragments. Updated atomically.
	written int32
	// buf[:flushed] has already been flushed to w.
	flushed int32
	buf     [blockSize]byte
}

// LogWriter writes records to an underlying io.Writer.
type LogWriter struct {
	// w is the underlying writer.
	w io.Writer
	// c is w as a closer.
	c io.Closer
	// f is w as a flusher.
	f flusher
	// s is w as a syncer.
	s syncer
	// blockNumber is the zero based block number for the current block.
	blockNumber int64
	// err is any accumulated error. TODO(peter): This needs to be protected in
	// some fashion. Perhaps using atomic.Value.
	err error
	// block is the current block being written. Protected by flusher.Mutex.
	block *block
	free  chan *block

	// Protects against concurrent calls to Flush().
	flushMu sync.Mutex
	// The latest position in the file that has been flushed. Updated atomically.
	flushWatermark int64

	// Protects against concurrent calls to Sync().
	sync struct {
		sync.Mutex
		// The latest position in the file that has been synced.
		watermark int64
		fast1     int64
		fast2     int64
		slow      int64
	}

	flusher struct {
		sync.Mutex
		// Cond var signalled when there are blocks to flush or the Writer has been
		// closed.
		ready sync.Cond
		// Cond var signalled when flushing of pending blocks has been completed.
		done sync.Cond
		// Is flushing currently active?
		flushing bool
		// Has the writer been closed?
		closed bool
		// Accumulated flush error.
		err     error
		pending []*block
	}
}

// NewLogWriter returns a new LogWriter.
func NewLogWriter(w io.Writer) *LogWriter {
	c, _ := w.(io.Closer)
	f, _ := w.(flusher)
	s, _ := w.(syncer)
	r := &LogWriter{
		w:    w,
		c:    c,
		f:    f,
		s:    s,
		free: make(chan *block, 4),
	}
	for i := 0; i < cap(r.free); i++ {
		r.free <- &block{}
	}
	r.block = <-r.free
	r.flusher.ready.L = &r.flusher.Mutex
	r.flusher.done.L = &r.flusher.Mutex
	go r.flushLoop()
	return r
}

func (w *LogWriter) flushLoop() {
	f := &w.flusher
	f.Lock()
	defer f.Unlock()

	for {
		for {
			if f.closed {
				return
			}
			if f.flushing {
				f.done.Wait()
				continue
			}
			if len(f.pending) == 0 {
				f.ready.Wait()
				continue
			}
			break
		}

		pending := f.pending
		f.pending = nil
		f.flushing = true

		f.Unlock()

		var err error
		for _, b := range pending {
			if err = w.flushBlock(b); err != nil {
				break
			}
		}

		f.Lock()
		f.err = err
		if f.err != nil {
			return
		}
		f.flushing = false
		f.done.Signal()
	}
}

func (w *LogWriter) flushBlock(b *block) error {
	n, err := w.w.Write(b.buf[b.flushed:])
	if err != nil {
		return err
	}
	b.written = 0
	b.flushed = 0
	w.free <- b
	atomic.StoreInt64(&w.flushWatermark, w.flushWatermark+int64(n))
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

	w.blockNumber++
}

// Close flushes any unwritten data and closes the writer.
func (w *LogWriter) Close() error {
	w.flusher.Lock()
	w.flusher.closed = true
	w.flusher.ready.Signal()
	w.flusher.Unlock()

	if err := w.Sync(math.MaxInt64); err != nil {
		return err
	}
	if w.c != nil {
		if err := w.c.Close(); err != nil {
			return err
		}
	}
	w.err = errors.New("pebble/record: closed LogWriter")
	return nil
}

// Flush flushes unwritten data up to the specified position (as returned from
// WriteRecord). May be called concurrently with Write, Sync and itself.
func (w *LogWriter) Flush(pos int64) error {
	w.flushMu.Lock()
	defer w.flushMu.Unlock()

	if w.err != nil {
		return w.err
	}

	w.flusher.Lock()
	// Wait for any existing flushing to complete.
	for w.flusher.flushing {
		w.flusher.done.Wait()
	}
	// Block any new flushing from starting.
	w.flusher.flushing = true
	// Grab the list of pending blocks to be flushed.
	pending := w.flusher.pending
	w.flusher.pending = nil
	// Grab the portion of the current block that requires flushing. Note that
	// the current block can be added to the pending blocks list after we release
	// the flusher lock, but it won't be part of pending.
	written := atomic.LoadInt32(&w.block.written)
	data := w.block.buf[w.block.flushed:written]
	w.block.flushed = written
	w.flusher.Unlock()

	// Flush any pending blocks.
	var err error
	for _, t := range pending {
		if err = w.flushBlock(t); err != nil {
			break
		}
	}
	if err == nil && len(data) > 0 {
		var n int
		n, err = w.w.Write(data)
		atomic.StoreInt64(&w.flushWatermark, w.flushWatermark+int64(n))
	}

	// Release the flush loop.
	w.flusher.Lock()
	w.err = err
	w.flusher.err = err
	w.flusher.flushing = false
	w.flusher.done.Signal()
	w.flusher.Unlock()

	if w.f != nil {
		w.err = w.f.Flush()
		return w.err
	}
	return nil
}

// Sync flushes unwritten data up to the specified position synchronizes the
// underlying file. May be called concurrently with Write, Flush and itself.
func (w *LogWriter) Sync(pos int64) error {
	if err := w.Flush(pos); err != nil {
		if pos <= atomic.LoadInt64(&w.sync.watermark) {
			return nil
		}
		return err
	}

	if pos <= atomic.LoadInt64(&w.sync.watermark) {
		// Nothing to do, the position we're being asked to sync to has already
		// been synced.
		atomic.AddInt64(&w.sync.fast1, 1)
		return nil
	}

	w.sync.Lock()
	defer w.sync.Unlock()

	// Note that this check doesn't require an atomic because syncWatermark is
	// only ever set with syncMu held.
	if pos <= w.sync.watermark {
		// Nothing to do, the position we're being asked to sync to has already
		// been synced.
		w.sync.fast2++
		if true && (w.sync.fast2%10000) == 0 {
			v := atomic.LoadInt64(&w.sync.fast1)
			fmt.Printf("sync %.1f%% %0.1f%%\n",
				100.0*float64(v)/float64(v+w.sync.fast2+w.sync.slow),
				100.0*float64(w.sync.fast2)/float64(v+w.sync.fast2+w.sync.slow),
			)
		}
		return nil
	}

	newWatermark := atomic.LoadInt64(&w.flushWatermark)
	if w.s != nil {
		w.err = w.s.Sync()
		if w.err != nil {
			return w.err
		}
	}
	atomic.StoreInt64(&w.sync.watermark, newWatermark)
	w.sync.slow++
	return nil
}

// WriteRecord writes a complete record. Returns the offset just past the end
// of the record.
func (w *LogWriter) WriteRecord(p []byte) (int64, error) {
	if w.err != nil {
		return -1, w.err
	}

	for i := 0; len(p) > 0; i++ {
		p = w.emitFragment(i, p)
	}

	offset := w.blockNumber*blockSize + int64(w.block.written)
	return offset, w.err
}

func (w *LogWriter) emitFragment(n int, p []byte) []byte {
	b := w.block
	i := b.written
	first := n == 0
	last := blockSize-i-headerSize >= int32(len(p))

	if last {
		if first {
			b.buf[i+6] = fullChunkType
		} else {
			b.buf[i+6] = lastChunkType
		}
	} else {
		if first {
			b.buf[i+6] = firstChunkType
		} else {
			b.buf[i+6] = middleChunkType
		}
	}

	r := copy(b.buf[i+headerSize:], p)
	j := i + int32(headerSize+r)
	binary.LittleEndian.PutUint32(b.buf[i+0:i+4], crc.New(b.buf[i+6:j]).Value())
	binary.LittleEndian.PutUint16(b.buf[i+4:i+6], uint16(r))
	atomic.StoreInt32(&b.written, j)

	if blockSize-b.written <= headerSize {
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
