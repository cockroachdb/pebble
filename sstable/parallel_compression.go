package sstable

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// todo(bananabrick): rethink error handling in the writer
// in the parallel compression case

// todo(bananabrick): check for races for any fields in the Writer
// which will be accessed from the writer thread.

// todo(bananabrick) : make sure the startWorker goroutines terminate
// when pebble is closed. Right now, they may block and leak.

// todo(bananabrick) : sequence the blocks written by a writer, which
// will be used to terminate the writer goroutine, and make sure
// that all of the blocks for a given file have been written.

type CompressedData struct {
	compressed []byte
	err        error
}

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

type BlockWriteCoordinator struct {
	// doneCh is used to maintain write order
	// in the write queue. doneCh should be a
	// buffered channel of size 1.
	doneCh <-chan *CompressedData

	key InternalKey
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
		checksumData.checksumType = toCompress.checksum
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
		done      int32
		numQueued uint64
	}

	// queue is sequenced by the order of the writes to
	// the file.
	queue  chan *BlockWriteCoordinator
	writer *Writer

	// closeWG is used to wait for the background workers
	// to finish executing.
	closeWG sync.WaitGroup

	// highestBlockSequenceNumProcessed is used to keep
	// track of the last block which has been processed.
	highestBlockSequenceNumProcessed uint64
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
		// The write queue will block until the specific
		// block in the queue has been compressed. This
		// ensures the ordering of writes in the file.
		blockWriteCoordinator := <-qu.queue
		compressedData := <-blockWriteCoordinator.doneCh

		if compressedData.err != nil {
			// todo(bananabrick): figure out how to asynchronously
			// handle the errors.
		} else {
			bh, err := qu.writer.writeBlockPostCompression(compressedData.compressed)
			if err != nil {
				// todo(bananabrick)
				fmt.Println("handle this")
			}
			err = qu.writer.addBlockPropertiesandIndexEntry(blockWriteCoordinator.key, bh)
			if err != nil {
				// todo(bananabrick)
				fmt.Println("handle this")
			}
			fmt.Println("writing", bh, err)
		}

		qu.highestBlockSequenceNumProcessed++
		highestQueued := atomic.LoadUint64(&qu.atomic.numQueued)
		if atomic.LoadInt32(&qu.atomic.done) == 1 && qu.highestBlockSequenceNumProcessed == highestQueued {
			break
		}
	}
	qu.closeWG.Done()
}

// finish will write an item to the write queue
// to indicate that this was the last block. It will
// then block until the last block has been processed
// by the queue.
// qu shouldn't be used once finish is called.
func (qu *writeQueueContainer) finish() {
	atomic.StoreInt32(&qu.atomic.done, 1)
	qu.closeWG.Wait()
}
