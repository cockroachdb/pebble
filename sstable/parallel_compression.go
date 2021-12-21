package sstable

import (
	"sync"
)

// Overall Algorithm:
// We initialize a CompressionQueue, and launch
// MaxCompressionConcurreny compression workers. Each worker
// blocks on a channel waiting for a compression job which is
// represented using a compressionTask. Once the
// compression job is complete, it writes the compressedData
// to the compressionTask.doneCh.
//
// Each sstable Writer will have a writeQueue which
// launches a single worker to write blocks to disk.
// writeTaskBuffer are queued in the writeQueue
// in the order in which the blocks need to be written to disk. The
// worker will block on the writeTask.doneCh as it waits
// for the corresponding compression job to complete. Once the compression
// job is complete, the worker will write the block to disk.

// todo(bananabrick) : historic compression ratio for size.
// Try and move indexBlock/meta.Size stuff to the main thread
// to avoid writerMu.

// todo(bananabrick): pool block writer buffers.

func copySliceToDst(dst *[]byte, src []byte) {
	if len(src) > cap(*dst) {
		// We assume that eventually dst will have enough cap
		// to not require this allocation.
		*dst = make([]byte, 2*len(src))
	}
	*dst = (*dst)[:len(src)]
	copy(*dst, src)
}

func copySlice(b []byte) []byte {
	x := make([]byte, len(b))
	copy(x, b)
	return x
}

func copyInternalKey(key InternalKey) InternalKey {
	key.UserKey = copySlice(key.UserKey)
	return key
}

// compressedData is used to send the compressed block to the
// writer thread.
type compressedData struct {
	compressed  []byte
	tmpBuf      [blockHandleLikelyMaxLen]byte
	err         error
	compressBuf []byte
}

// compressionTask is added to the compression
// queue to indicate a pending compression.
type compressionTask struct {
	toCompress  []byte
	compression Compression
	checksum    ChecksumType

	// doneCh is used to signal to the writer
	// thread that the compression has been
	// completed. doneCh is a
	// buffered channel of size 1.
	// The write to doneCh won't block because
	// the doneCh is a buffered channel of size 1.
	doneCh chan<- *compressedData
}

// writeTask is added to the write queue
// to maintain the order of block writes to disk.
type writeTask struct {
	// doneCh is used to maintain write order
	// in the write queue. doneCh is a
	// buffered channel of size 1.
	doneCh <-chan *compressedData

	indexSep        InternalKey
	props           []byte
	indexProps      []byte
	flushIndexBlock bool
}

// CompressionQueue queues up blocks
// which will get compressed in parallel.
type CompressionQueue struct {
	queue chan *compressionTask

	// The following channels contain preallocated
	// writeTask, compressionTask, and compressedData
	// structs. We don't want to preallocate these when
	// every single block is flushed.
	writeTaskBuffer       chan *writeTask
	compressionTaskBuffer chan *compressionTask
	compressedDataBuffer  chan *compressedData

	// closeWG is used to wait for the background workers
	// to finish executing.
	closeWG sync.WaitGroup
}

// NewCompressionQueue will start numWorkers goroutines to process
// compression of blocks, and return a usable *CompressionQueue.
func NewCompressionQueue(
	maxSize int, numWorkers int, bufferSize int) *CompressionQueue {
	qu := &CompressionQueue{}
	qu.queue = make(chan *compressionTask, maxSize)

	qu.writeTaskBuffer = make(chan *writeTask, bufferSize)
	qu.compressionTaskBuffer = make(chan *compressionTask, bufferSize)
	qu.compressedDataBuffer = make(chan *compressedData, bufferSize)

	// preallocate the structs, to avoid allocations on every
	// block flush.
	writeTasks := make([]writeTask, bufferSize)
	compressionTasks := make([]compressionTask, bufferSize)
	compressedDatas := make([]compressedData, bufferSize)
	for i := 0; i < bufferSize; i++ {
		qu.writeTaskBuffer <- &writeTasks[i]
		qu.compressionTaskBuffer <- &compressionTasks[i]
		qu.compressedDataBuffer <- &compressedDatas[i]
	}

	for i := 0; i < numWorkers; i++ {
		qu.closeWG.Add(1)
		go qu.runWorker()
	}

	return qu
}

func (qu *CompressionQueue) runWorker() {
	defer qu.closeWG.Done()

	var checksumData checksumData
	for {
		toCompress, ok := <-qu.queue
		if !ok {
			// The channel was closed
			break
		}
		if toCompress == nil {
			panic("a nil compression job was queued")
		}

		checksumData.checksumType = toCompress.checksum
		compressedData := <-qu.compressedDataBuffer
		b, err := compressAndChecksum(
			toCompress.toCompress, toCompress.compression,
			&compressedData.compressBuf, &compressedData.tmpBuf,
			&checksumData,
		)
		if err != nil {
			compressedData.err = err
		} else {
			// b could be just returning toCompress.toCompress. Since
			// we'll be releasing toCompress so that it can be re-used,
			// we copy over the slice here.
			copySliceToDst(&compressedData.compressed, b)
		}

		// This call will never block because the doneCh
		// must be a buffered channel of size 1.
		toCompress.doneCh <- compressedData

		// Release the compression task, for future use to compress
		// a different block.
		qu.releaseCompressionTask(toCompress)
	}
}

// todo(bananabrick) : We need to make sure that there are no references
// to any of the fields of the values we're releasing. Add a reference
// counted byte slice to make sure of this.
func (qu *CompressionQueue) releaseCompressionTask(c *compressionTask) {
	c.doneCh = nil
	qu.compressionTaskBuffer <- c
}

func (qu *CompressionQueue) releaseWriteTask(w *writeTask) {
	w.doneCh = nil
	qu.writeTaskBuffer <- w
}

func (qu *CompressionQueue) releaseCompressedData(c *compressedData) {
	qu.compressedDataBuffer <- c
}

// Close will only return after the goroutines started
// by NewCompressionQueue have stopped running.
// Items shouldn't be added to the queue once close has
// been called.
func (qu *CompressionQueue) Close() {
	close(qu.queue)
	qu.closeWG.Wait()
}

// writeQueue is used to process compressed blocks
// in parallel.
type writeQueue struct {
	// queue is sequenced by the order of the writes to
	// the file.
	queue  chan *writeTask
	writer *Writer

	// closeWG is used to wait for the background workers
	// to finish executing.
	closeWG sync.WaitGroup
}

// newWriteQueue will start a single goroutine to process
// compression of blocks, and return a usable *writeQueue.
func newWriteQueue(maxSize int, w *Writer) *writeQueue {
	qu := &writeQueue{}
	qu.queue = make(chan *writeTask, maxSize)
	qu.writer = w

	qu.closeWG.Add(1)
	go qu.runWorker()

	return qu
}

func (qu *writeQueue) releaseBuffers(w *writeTask, c *compressedData) {
	qu.writer.parallelWriterState.compressionQueueRef.releaseWriteTask(w)
	qu.writer.parallelWriterState.compressionQueueRef.releaseCompressedData(c)
}

func (qu *writeQueue) runWorker() {
	defer qu.closeWG.Done()

	var err error
	for {
		// We continue to read from qu.queue even if we've encountered an
		// error because the main compaction goroutine might not have detected
		// the error yet, and may still add to this queue. We don't want
		// the main compaction goroutine to block. Once the compaction
		// goroutine encounters the error, it will call qu.finish.
		writeTask, ok := <-qu.queue
		if !ok {
			// If qu.queue was closed, then
			// we've already processed all other blocks
			// since blocks are added to the queue in order.
			// It's okay to break here.
			break
		}
		if writeTask == nil {
			panic("a nil write job was queued")
		}

		compressedData := <-writeTask.doneCh
		// If we've already encountered an error, then we don't
		// want to write the current sstable using this worker.
		if err != nil {
			qu.releaseBuffers(writeTask, compressedData)
			continue
		}

		err = func() error {
			if compressedData.err != nil {
				return compressedData.err
			}

			bh, err := qu.writer.writeBlockPostCompression(
				compressedData.compressed, compressedData.tmpBuf,
			)
			if err != nil {
				return err
			}

			bhp := BlockHandleWithProperties{BlockHandle: bh, Props: writeTask.props}
			encodedBHP := encodeBlockHandleWithProperties(compressedData.tmpBuf[:], bhp)
			if err = qu.writer.addIndexEntry(
				writeTask.indexSep, encodedBHP, bhp,
				// We're copying indexProps here because addIndexEntry will end up holding a reference
				// to this, and we don't want two references to the same byte slice, which can happen
				// once we call qu.releaseBuffers.
				writeTask.flushIndexBlock, copySlice(writeTask.indexProps),
			); err != nil {
				return err
			}

			return nil
		}()

		if err != nil {
			qu.writer.parallelWriterState.errCh <- err
		}

		// release the writeTask, compressedData structs so
		// that they can be used for other compression/write
		// tasks in the future.
		qu.releaseBuffers(writeTask, compressedData)
	}
}

// finish will block until the last block written
// to to the compression queue has been written to
// the disk, assuming no errors occur.
// The queue is no longer usable after finish is called.
func (qu *writeQueue) finish() {
	close(qu.queue)
	qu.closeWG.Wait()
}
