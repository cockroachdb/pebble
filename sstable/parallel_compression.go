package sstable

import (
	"sync"
	"sync/atomic"
)

// CompressionQueueContainer is used to queue up blocks
// which will get compressed in parallel.
type CompressionQueueContainer struct {
	atomic struct {
		done int32
	}

	// todo(bananabrick): this shouldn't be an int.
	// queue will have a single writer, and many readers.
	queue chan int

	// closeWG is used to wait for the background workers
	// to finish executing.
	closeWG sync.WaitGroup
}

// NewCompressionQueueContainer will start numWorkers goroutines to process
// compression of blocks, and return a usable *CompressionQueueContainer.
func NewCompressionQueueContainer(
	maxSize int, numWorkers int) *CompressionQueueContainer {

	qu := &CompressionQueueContainer{}
	qu.queue = make(chan int, maxSize)

	for i := 0; i < numWorkers; i++ {
		qu.closeWG.Add(1)
		qu.startWorker()
	}

	return qu
}

func (qu *CompressionQueueContainer) startWorker() {
	for {
		if atomic.LoadInt32(&qu.atomic.done) == 1 {
			break
		}

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

	queue chan int

	// closeWG is used to wait for the background workers
	// to finish executing.
	closeWG sync.WaitGroup
}

// newwriteQueueContainer will start a single goroutine to process
// compression of blocks, and return a usable *writeQueueContainer.
func newWriteQueueContainer(maxSize int) *writeQueueContainer {
	qu := &writeQueueContainer{}

	// We're using a buffered channel for the write queue. Since,
	// we never want the writes to the queue to block, we make sure that
	// its buffer size is at least equal to the maximum number of goroutines
	// which will write to it.
	qu.queue = make(chan int, maxSize)

	qu.closeWG.Add(1)
	qu.startWorker()

	return qu
}

func (qu *writeQueueContainer) startWorker() {
	for {
		if atomic.LoadInt32(&qu.atomic.done) == 1 {
			break
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
