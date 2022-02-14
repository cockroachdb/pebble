package sstable

import (
	"sync"
)

type compressionTask struct {
	compressionDone *chan bool
	buf             *dataBlockBuf
	compression     Compression
}

// Note that only the Writer client goroutine will be adding tasks to the compressionQueue.
type compressionQueue struct {
	tasks chan *compressionTask
	wg    sync.WaitGroup
}

func newCompressionQueue(size int) *compressionQueue {
	c := &compressionQueue{}
	c.tasks = make(chan *compressionTask, size)

	c.wg.Add(size)
	for i := 0; i < size; i++ {
		go c.runWorker()
	}

	return c
}

func (c *compressionQueue) runWorker() {
	for task := range c.tasks {
		task.buf.finish()
		task.buf.compressAndChecksum(task.compression)
		*task.compressionDone <- true
	}
	c.wg.Done()
}

//lint:ignore U1000 - Will be used in a future pr.
func (c *compressionQueue) add(task *compressionTask) {
	c.tasks <- task
}

// finish should only be called once no more tasks will be added to the compressionQueue.
func (c *compressionQueue) finish() {
	close(c.tasks)
	c.wg.Wait()
}
