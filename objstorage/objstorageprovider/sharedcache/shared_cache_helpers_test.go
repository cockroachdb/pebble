package sharedcache

func ShardingBlockSize() int {
	return shardingBlockSize
}

func (c *Cache) Misses() int32 {
	return c.misses.Load()
}

func (c *Cache) WaitForWritesToComplete() {
	close(c.writeWorkers.tasksCh)
	c.writeWorkers.doneWaitGroup.Wait()
	c.writeWorkers.Start(c, c.writeWorkers.numWorkers)
}
