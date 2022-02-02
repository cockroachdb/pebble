package sstable

import (
	"sync"

	"github.com/cockroachdb/pebble/internal/base"
)

type writeTask struct {
	// Since writeTasks are pooled, the compressionDone channel will be re-used.
	// It is necessary that any writes to the channel have already been read,
	// before adding the writeTask back to the pool.
	compressionDone chan bool
	buf             *dataBlockBuf
	// todo(bananabrick) : Remove the indexSepKey field once Writer.addIndexEntry is
	// refactored to not require the indexSepKey.
	indexSepKey InternalKey

	// inflightSize is used to decrement Writer.coordination.sizeEstimate.inflightSize.
	inflightSize int
}

// Note that only the Writer client goroutine will be adding tasks to the writeQueue.
// Both the Writer client and the compression goroutines will be able to write to
// writeTask.compressionDone to indicate that the compression job associated with
// a writeTask has finished.
type writeQueue struct {
	tasks  chan *writeTask
	wg     sync.WaitGroup
	writer *Writer

	// err represents an error which is encountered when the write queue attempts
	// to write a block to disk. The error is stored here to skip unnecessary block
	// writes once the first error is encountered.
	err    error
	closed bool
}

func newWriteQueue(size int, writer *Writer) *writeQueue {
	w := &writeQueue{}
	w.tasks = make(chan *writeTask, size)
	w.writer = writer

	w.wg.Add(1)
	go w.runWorker()
	return w
}

func (w *writeQueue) performWrite(task *writeTask) error {
	var bh BlockHandle
	var bhp BlockHandleWithProperties

	var err error
	if bh, err = w.writer.writeCompressedBlock(task.buf.compressed, task.buf.tmp[:]); err != nil {
		return err
	}

	// Update the size estimates after writing the data block to disk.
	w.writer.coordination.sizeEstimate.dataBlockWritten(
		w.writer.meta.Size, task.inflightSize, int(bh.Length),
	)

	if bhp, err = w.writer.maybeAddBlockPropertiesToBlockHandle(bh); err != nil {
		return err
	}

	prevKey := base.DecodeInternalKey(task.buf.dataBlock.curKey)
	if err = w.writer.addIndexEntry(prevKey, task.indexSepKey, bhp, task.buf.tmp[:]); err != nil {
		return err
	}

	return nil
}

func (w *writeQueue) releaseBuffers(task *writeTask) {
	dataBlockBufPool.Put(task.buf)
	task.buf = nil
	writeTaskPool.Put(task)
}

func (w *writeQueue) runWorker() {
	for task := range w.tasks {
		<-task.compressionDone

		if w.err == nil {
			w.err = w.performWrite(task)
		}

		w.releaseBuffers(task)
	}
	w.wg.Done()
}

//lint:ignore U1000 - Will be used in a future pr.
func (w *writeQueue) add(task *writeTask) {
	w.tasks <- task
}

// addSync will perform the writeTask synchronously with the caller goroutine. Calls to addSync
// are no longer valid once writeQueue.add has been called at least once.
func (w *writeQueue) addSync(task *writeTask) error {
	// This should instantly return without blocking.
	<-task.compressionDone

	if w.err == nil {
		w.err = w.performWrite(task)
	}

	w.releaseBuffers(task)

	return w.err
}

// finish should only be called once no more tasks will be added to the writeQueue.
// finish will return any error which was encountered while tasks were processed.
func (w *writeQueue) finish() error {
	if w.closed {
		return w.err
	}

	close(w.tasks)
	w.wg.Wait()
	w.closed = true
	return w.err
}
