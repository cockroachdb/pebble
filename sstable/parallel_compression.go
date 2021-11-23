package sstable

import (
	"sync"
	"sync/atomic"
)

// todo(bananabrick): rethink error handling in the writer
// in the parallel compression case

// todo(bananabrick): check for races for any fields in the writer
// which will be accessed from the writer thread.

type CompressedData struct {
	compressed []byte
	err        error
}

type BlockCompressionCoordinator struct {
	toCompress  []byte
	compression Compression

	// doneCh is used to signal to the writer
	// thread that the compression has been
	// completed. doneCh should be a
	// buffered channel of size 1.
	doneCh chan<- *CompressedData
}

type BlockWriteCoordinator struct {
	// doneCh is used to maintain write order
	// in the write queue. doneCh should be a
	// buffered channel of size 1.
	doneCh <-chan *CompressedData

	// postFunc should be called after the compressed
	// block has been written to the file.
	postFunc func()
}

// CompressionQueueContainer is used to queue up blocks
// which will get compressed in parallel.
type CompressionQueueContainer struct {
	atomic struct {
		done int32
	}

	queue chan *BlockCompressionCoordinator

	// closeWG is used to wait for the background workers
	// to finish executing.
	closeWG sync.WaitGroup
}

// NewCompressionQueueContainer will start numWorkers goroutines to process
// compression of blocks, and return a usable *CompressionQueueContainer.
func NewCompressionQueueContainer(
	maxSize int, numWorkers int) *CompressionQueueContainer {
	qu := &CompressionQueueContainer{}
	qu.queue = make(chan *BlockCompressionCoordinator, maxSize)

	for i := 0; i < numWorkers; i++ {
		qu.closeWG.Add(1)
		go qu.startWorker()
	}

	return qu
}

func (qu *CompressionQueueContainer) startWorker() {
	var trailerScratch [blockHandleLikelyMaxLen]byte
	var compressBuf []byte
	var checksumData checksumData
	for {
		if atomic.LoadInt32(&qu.atomic.done) == 1 {
			break
		}

		toCompress := <-qu.queue
		b, err := compressAndChecksum(
			toCompress.toCompress, toCompress.compression, &compressBuf, &trailerScratch, &checksumData,
		)
		compressedData := &CompressedData{}
		if err != nil {
			compressedData.err = err
		} else {
			compressedData.compressed = b
		}
		// todo(bananabrick): this should never block.
		toCompress.doneCh <- compressedData
	}
	qu.closeWG.Done()
}

// Close will only return after the goroutines started
// by NewCompressionQueueContainer have stopped running.
// Items shouldn't be added to the queue once close has
// been called.
func (qu *CompressionQueueContainer) Close() {
	atomic.StoreInt32(&qu.atomic.done, 1)
	qu.closeWG.Wait()
}

// writeQueueContainer is used to process compressed blocks
// in parallel.
type writeQueueContainer struct {
	atomic struct {
		done int32
	}

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
	go qu.startWorker()

	return qu
}

func (qu *writeQueueContainer) startWorker() {
	for {
		if atomic.LoadInt32(&qu.atomic.done) == 1 {
			break
		}

		// The write queue will block until the specific
		// block in this order has been compressed.
		blockWriteCoordinator := <-qu.queue
		compressedData := <-blockWriteCoordinator.doneCh

		if compressedData.err != nil {
			// todo(bananabrick): figure out how to asynchronously
			// handle the errors.
		} else {
			qu.writer.writeBlockPostCompression(compressedData.compressed)
		}
	}
	qu.closeWG.Done()
}

// Close will only return after the goroutine started
// by NewWompressionQueueContainer has stopped running.
// Items shouldn't be added to the queue once close has
// been called.
func (qu *writeQueueContainer) close() {
	atomic.StoreInt32(&qu.atomic.done, 1)
	qu.closeWG.Wait()
}
