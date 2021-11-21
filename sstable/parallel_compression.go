package sstable

import (
	"sync"
)

// Overall Algorithm:
// We initialize a CompressionWorkersQueue, and launch
// MaxCompressionConcurreny compression workers. Each worker
// blocks on a channel waiting for a compression job which is
// represented using a BlockCompressionCoordinator. Once the
// compression job is complete, it writes the CompressedData
// to the BlockCompressionCoordinator.doneCh.
//
// Each sstable Writer will have a writeQueueContainer which
// launches a single worker to write blocks to disk.
// BlockWriteCoordinators are queued in the writeQueueContainer
// in the order in which the blocks need to be written to disk. The
// worker will block on the BlockWriteCoordinator.doneCh as it waits
// for the corresponding compression job to complete. Once the compression
// job is complete, the worker will write the block to disk.

// todo(bananabrick) : historic compression ratio for size.
// Try and move indexBlock/meta.Size stuff to the main thread
// to avoid writerMu.

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

// CompressedData is used to send the compressed block to the
// writer thread.
type CompressedData struct {
	compressed  []byte
	tmpBuf      [blockHandleLikelyMaxLen]byte
	err         error
	compressBuf []byte
}

// BlockCompressionCoordinator is added to the compression
// queue to indicate a pending compression.
type BlockCompressionCoordinator struct {
	toCompress  []byte
	compression Compression
	checksum    ChecksumType

	// doneCh is used to signal to the writer
	// thread that the compression has been
	// completed. doneCh should be a
	// buffered channel of size 1.
	doneCh chan<- *CompressedData
}

// BlockWriteCoordinator is added to the write queue
// to maintain the order of block writes to disk.
type BlockWriteCoordinator struct {
	// doneCh is used to maintain write order
	// in the write queue. doneCh should be a
	// buffered channel of size 1.
	doneCh <-chan *CompressedData

	indexSep        InternalKey
	props           []byte
	indexProps      []byte
	flushIndexBlock bool
}

// CompressionWorkersQueue is used to queue up blocks
// which will get compressed in parallel.
type CompressionWorkersQueue struct {
	queue chan *BlockCompressionCoordinator

	writeCoordinators       chan *BlockWriteCoordinator
	compressionCoordinators chan *BlockCompressionCoordinator
	compressedDataState     chan *CompressedData

	// closeWG is used to wait for the background workers
	// to finish executing.
	closeWG sync.WaitGroup
}

func (qu *CompressionWorkersQueue) allocateBuffers(bufferSize int) {
	for i := 0; i < bufferSize; i++ {
		w := &BlockWriteCoordinator{}
		c := &BlockCompressionCoordinator{}
		data := &CompressedData{}

		qu.writeCoordinators <- w
		qu.compressionCoordinators <- c
		qu.compressedDataState <- data
	}
}

// NewCompressionWorkersQueue will start numWorkers goroutines to process
// compression of blocks, and return a usable *CompressionWorkersQueue.
func NewCompressionWorkersQueue(
	maxSize int, numWorkers int, bufferSize int) *CompressionWorkersQueue {
	qu := &CompressionWorkersQueue{}
	qu.queue = make(chan *BlockCompressionCoordinator, maxSize)

	qu.writeCoordinators = make(chan *BlockWriteCoordinator, bufferSize)
	qu.compressionCoordinators = make(chan *BlockCompressionCoordinator, bufferSize)
	qu.compressedDataState = make(chan *CompressedData, bufferSize)

	qu.allocateBuffers(bufferSize)

	for i := 0; i < numWorkers; i++ {
		qu.closeWG.Add(1)
		go qu.runWorker()
	}

	return qu
}

func (qu *CompressionWorkersQueue) runWorker() {
	defer qu.closeWG.Done()

	var checksumData checksumData
	for {
		toCompress := <-qu.queue
		if toCompress == nil {
			// The channel was closed. There might still be more
			// blocks queued for compression, but we're going to ignore
			// those.
			// todo(bananabrick) : Might want to process every item
			// added to the compression queue, so that the writer worker
			// doesn't block when the the db is closed.
			break
		}
		checksumData.checksumType = toCompress.checksum
		compressedData := <-qu.compressedDataState
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

		// Release the compression coordinator.
		qu.releaseCompressionCoordinator(toCompress)
	}
}

// todo(bananabrick) : We need to make sure that there are no references
// to any of the fields of the values we're releasing. Add a reference
// counted byte slice to make sure of this.
func (qu *CompressionWorkersQueue) releaseCompressionCoordinator(c *BlockCompressionCoordinator) {
	c.doneCh = nil
	qu.compressionCoordinators <- c
}

func (qu *CompressionWorkersQueue) releaseWriteCoordinator(w *BlockWriteCoordinator) {
	w.doneCh = nil
	qu.writeCoordinators <- w
}

func (qu *CompressionWorkersQueue) releaseCompressedData(c *CompressedData) {
	qu.compressedDataState <- c
}

// Close will only return after the goroutines started
// by NewCompressionWorkersQueue have stopped running.
// Items shouldn't be added to the queue once close has
// been called.
func (qu *CompressionWorkersQueue) Close() {
	close(qu.queue)
	qu.closeWG.Wait()
}

// writeQueueContainer is used to process compressed blocks
// in parallel.
type writeQueueContainer struct {
	// queue is sequenced by the order of the writes to
	// the file.
	queue  chan *BlockWriteCoordinator
	writer *Writer

	// closeWG is used to wait for the background workers
	// to finish executing.
	closeWG sync.WaitGroup
}

// newwriteQueueContainer will start a single goroutine to process
// compression of blocks, and return a usable *writeQueueContainer.
func newWriteQueueContainer(maxSize int, w *Writer) *writeQueueContainer {
	qu := &writeQueueContainer{}
	qu.queue = make(chan *BlockWriteCoordinator, maxSize)
	qu.writer = w

	qu.closeWG.Add(1)
	go qu.runWorker()

	return qu
}

func (qu *writeQueueContainer) releaseBuffers(w *BlockWriteCoordinator, c *CompressedData) {
	qu.writer.parallelWriterState.compressionQueueRef.releaseWriteCoordinator(w)
	qu.writer.parallelWriterState.compressionQueueRef.releaseCompressedData(c)
}

func (qu *writeQueueContainer) runWorker() {
	defer qu.closeWG.Done()

	var err error
	for {
		blockWriteCoordinator := <-qu.queue
		if blockWriteCoordinator == nil {
			// If blockWriteCoordinator == nil, then
			// we've already processed all other blocks
			// since blocks are added to the queue in order.
			// It's okay to break here.
			break
		}

		compressedData := <-blockWriteCoordinator.doneCh
		// If we've already encountered an error, then we don't
		// want to write the current sstable using this worker.
		if err != nil {
			qu.releaseBuffers(blockWriteCoordinator, compressedData)
			continue
		}

		if compressedData.err != nil {
			err = compressedData.err
			qu.writer.parallelWriterState.errCh <- err
			qu.releaseBuffers(blockWriteCoordinator, compressedData)
			continue
		}

		bh, err := qu.writer.writeBlockPostCompression(
			compressedData.compressed, compressedData.tmpBuf,
		)
		if err != nil {
			qu.writer.parallelWriterState.errCh <- err
			qu.releaseBuffers(blockWriteCoordinator, compressedData)
			continue
		}

		bhp := BlockHandleWithProperties{BlockHandle: bh, Props: blockWriteCoordinator.props}
		encodedBHP := encodeBlockHandleWithProperties(compressedData.tmpBuf[:], bhp)
		if err = qu.writer.addIndexEntry(
			blockWriteCoordinator.indexSep, encodedBHP, bhp,
			// We're copying indexProps here because addIndexEntry will end up holding a reference
			// to this, and we don't want two references to the same byte slice, which can happen
			// once we call qu.releaseBuffers.
			blockWriteCoordinator.flushIndexBlock, copySlice(blockWriteCoordinator.indexProps),
		); err != nil {
			qu.writer.parallelWriterState.errCh <- err
			qu.releaseBuffers(blockWriteCoordinator, compressedData)
			continue
		}

		qu.releaseBuffers(blockWriteCoordinator, compressedData)
	}
}

// finish will block until the last block written
// to to the compression queue has been written to
// the disk, assuming no errors occur.
// The queue is no longer usable after finish is called.
func (qu *writeQueueContainer) finish() {
	close(qu.queue)
	qu.closeWG.Wait()
}
