package sharedcache

func ShardingBlockSize() int {
	return shardingBlockSize
}

func (c *Cache) Misses() int32 {
	return c.misses.Load()
}

func (c *Cache) WaitForWritesToComplete() {
	c.writeBackWaitGroup.Wait()
}
