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
	// todo(bananabrick) : Remove the key field once Writer.addIndexEntry is
	// refactored to not require the key.
	key InternalKey
}

type writeQueue struct {
	tasks  chan *writeTask
	wg     sync.WaitGroup
	writer *Writer

	// err represents an error which is encountered when the write queue attempts
	// to write a block to disk. The error is stored here to skip unnecessary block
	// writes once the first error is encountered.
	err error
}

func newWriteQueue(size int, writer *Writer) *writeQueue {
	w := &writeQueue{}
	w.tasks = make(chan *writeTask, size)
	w.writer = writer

	w.wg.Add(1)
	go w.runWorker()
	return w
}

func (w *writeQueue) runWorker() {
	var bh BlockHandle
	var bhp BlockHandleWithProperties

	for task := range w.tasks {
		<-task.compressionDone

		var err error
		if w.err == nil {
			err = func() error {
				var err error
				if bh, err = w.writer.writeCompressedBlock(task.buf.compressed, task.buf.tmp[:]); err != nil {
					return err
				}

				if bhp, err = w.writer.maybeAddBlockPropertiesToBlockHandle(bh); err != nil {
					return err
				}

				prevKey := base.DecodeInternalKey(task.buf.dataBlock.curKey)
				if err = w.writer.addIndexEntry(prevKey, task.key, bhp, task.buf.tmp[:]); err != nil {
					return err
				}

				return nil
			}()
		}

		w.writer.dataBlockBuffers <- task.buf
		task.buf = nil
		writeQueueTasks.Put(task)

		// We check if w.err is already != nil, because we only want to write to the errCh once.
		if err != nil && w.err == nil {
			w.err = err
			w.writer.queueState.errCh <- err
		}
	}
	w.wg.Done()
}

func (w *writeQueue) add(task *writeTask) {
	w.tasks <- task
}

// finish should only be called once no more tasks will be added to the writeQueue.
func (w *writeQueue) finish() {
	close(w.tasks)
	w.wg.Wait()
}
