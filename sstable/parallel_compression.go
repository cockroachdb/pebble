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
// to the compressionTask.compressionDone.
//
// Each sstable Writer will have a writeQueue which
// launches a single worker to write blocks to disk.
// writeTaskBuffer are queued in the writeQueue
// in the order in which the blocks need to be written to disk. The
// worker will block on the writeTask.compressionDone as it waits
// for the corresponding compression job to complete. Once the compression
// job is complete, the worker will write the block to disk.

// todo(bananabrick) : historic compression ratio for size.
// Try and move indexBlock/meta.Size stuff to the main goroutine
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
// writer goroutine.
type compressedData struct {
	compressed       []byte
	tmpBuf           [blockHandleLikelyMaxLen]byte
	err              error
	compressBuf      []byte
	compressionRatio float64
}

// compressionTask is added to the compression
// queue to indicate a pending compression.
type compressionTask struct {
	toCompress  []byte
	compression Compression
	checksum    ChecksumType

	// compressionDone is used to signal to the writer
	// goroutine that the compression has been
	// completed. compressionDone is a
	// buffered channel of size 1.
	// The write to compressionDone won't block because
	// the compressionDone is a buffered channel of size 1.
	compressionDone chan<- *compressedData
}

// writeTask is added to the write queue
// to maintain the order of block writes to disk.
type writeTask struct {
	// compressionDone is a buffered channel of size 1.
	// The writer goroutine will wait on the compressionDone
	// in the writeTask for the corresponding compressionTask
	// to complete. This ensures write order of the blocks
	// to disk since writeTasks are added to the writeQueue
	// in order.
	// This channel will be read from at most once.
	// The read from this channel will block until
	// the corresponding compressionTask is complete.
	// The compressionTask is guaranteed to eventually
	// write to the compressionDone.
	compressionDone <-chan *compressedData

	indexSep        InternalKey
	props           []byte
	indexProps      []byte
	flushIndexBlock bool
}

// CompressionQueue queues up blocks
// which will get compressed in parallel.
type CompressionQueue struct {
	// sends to queue may block, but they'll be processed
	// once the workers process the items already sent
	// successully to the queue. The send can block forever
	// if the workers stop processing compressionTasks.
	// This can only happen if the queue is closed.
	// But the queue will only be closed after all the compaction
	// goroutines terminate.
	// Reads from the queue will block if the queue is empty.
	// compressionTasks will either be added to the queue, or
	// the queue will be closed, so reads won't block forever.
	queue chan *compressionTask

	// The following buffered channels contain preallocated
	// writeTask, compressionTask, and compressedData
	// structs. We don't want to allocate these when
	// every single block is flushed.
	//
	// Writes to these channels will never block, as
	// all the writes to these channels have been previously
	// read from the channel.
	//
	// Reads from these channels may block, but they won't
	// block forever. CompressionQueue.runWorker, and
	// writeQueue.runWorker will always add items back to
	// these channels.
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

	var checksummer checksummer
	for {
		toCompress, ok := <-qu.queue
		if !ok {
			// qu.queue was closed. This can only happen
			// once all the compaction goroutines finish
			// running. This means that no more
			// compressionTasks will be sent on the queue.
			// Since none of the compaction goroutines were
			// blocked, we know that there are no blocked sends
			// on qu.queue. Since the FIFO order of channels
			// is maintained, we know that by the time
			// the close is processed, all the queue compressionTasks
			// must already have been processed.
			//
			// INVARIANT: Every single item added to qu.queue
			// is processed before the workers are shut down.
			break
		}

		checksummer.checksumType = toCompress.checksum
		compressedData := <-qu.compressedDataBuffer

		toCompressLen := len(toCompress.toCompress)

		b, err := compressAndChecksum(
			toCompress.toCompress, toCompress.compression,
			&compressedData.compressBuf, &compressedData.tmpBuf,
			&checksummer,
		)

		compressedLen := len(b)

		if err != nil {
			compressedData.err = err
		} else {
			compressedData.compressionRatio = float64(compressedLen) / float64(toCompressLen)
			// b could be just returning toCompress.toCompress. Since
			// we'll be releasing toCompress so that it can be re-used,
			// we copy over the slice here.
			copySliceToDst(&compressedData.compressed, b)
		}

		// This call will never block because the compressionDone
		// must be a buffered channel of size 1.
		toCompress.compressionDone <- compressedData

		// Release the compression task, for future use to compress
		// a different block.
		qu.releaseCompressionTask(toCompress)
	}
}

// todo(bananabrick) : We need to make sure that there are no references
// to any of the fields of the values we're releasing. Add a reference
// counted byte slice to make sure of this.
func (qu *CompressionQueue) releaseCompressionTask(c *compressionTask) {
	c.compressionDone = nil
	qu.compressionTaskBuffer <- c
}

func (qu *CompressionQueue) releaseWriteTask(w *writeTask) {
	w.compressionDone = nil
	qu.writeTaskBuffer <- w
}

func (qu *CompressionQueue) releaseCompressedData(c *compressedData) {
	qu.compressedDataBuffer <- c
}

// Close will only return after the goroutines started
// by NewCompressionQueue have stopped running.
// Items shouldn't be added to the queue once close has
// been called. Close must only be called after all
// the compaction goroutines have finished running.
func (qu *CompressionQueue) Close() {
	close(qu.queue)
	qu.closeWG.Wait()
}

// writeQueue is used to process compressed blocks
// in parallel.
type writeQueue struct {
	// writeTasks are added to the queue in the order of
	// the writes to the file.
	//
	// Writes to the queue may block if it is already full.
	// The writes will be unblocked as writeTasks are
	// processed by the workers. The write may block
	// forever, if the queue is closed before all the
	// writeTasks are processed. But this is impossible
	// as the queue will be closed by the compaction
	// go routine, only after it sends items to the queue.
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
			// It is the compaction goroutine which adds
			// writeTasks to qu.queue. When qu.finish is called,
			// we know that no more writeTasks will be added to
			// the queue.
			//
			// When qu.finish calls close(qu.queue), we know that
			// none of the sends to qu.queue are currently blocked,
			// and that there will be no more sends.
			//
			// If qu.queue was closed, then
			// we've already processed all other blocks
			// since blocks are added to the queue in order.
			// It's okay to break here.
			//
			// INVARIANT: Every single item added to qu.queue
			// is processed before the workers are shut down.
			break
		}

		compressedData := <-writeTask.compressionDone
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

			// Update average compression ratio
			qu.writer.parallelWriterState.updateAverageCompressionRatio(compressedData.compressionRatio)

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
