// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorageprovider

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/vfs"
)

type sharedCache struct {
	shards []sharedCacheShard
	logger base.Logger

	// TODO(josh): Have a dedicated metrics struct. Right now, this
	// is just for testing.
	misses atomic.Int32
}

func openSharedCache(
	fs vfs.FS, fsDir string, blockSize int, sizeBytes int64,
) (*sharedCache, error) {
	// TODO(josh): Vertical scale ops can be done. IIUC, the same sstable reads won't
	// map to the same shards necessarily, emptying the cache in effect. We may need
	// a solution to this issue eventually.
	numShards := 2 * runtime.GOMAXPROCS(0)
	min := shardingBlockSize * int64(numShards)
	if sizeBytes < min {
		return nil, errors.Errorf("cache size %d lower than min %d", sizeBytes, min)
	}

	sc := &sharedCache{}
	sc.shards = make([]sharedCacheShard, numShards)
	blocksPerShard := sizeBytes / int64(numShards) / int64(blockSize)
	for i := range sc.shards {
		if err := sc.shards[i].init(fs, fsDir, i, blocksPerShard, blockSize); err != nil {
			return nil, err
		}
	}
	return sc, nil
}

func (sc *sharedCache) Close() error {
	var retErr error
	for i := range sc.shards {
		if err := sc.shards[i].Close(); err != nil && retErr == nil {
			retErr = err
		}
	}
	sc.shards = nil
	return retErr
}

// ReadAt peforms a read form an object, attempting to use cached data when
// possible.
func (sc *sharedCache) ReadAt(
	ctx context.Context, fileNum base.FileNum, p []byte, ofs int64, readable objstorage.Readable,
) error {
	n, err := sc.Get(fileNum, p, ofs)
	if err != nil {
		return err
	}
	if n == len(p) {
		// Everything was in cache!
		return nil
	}
	// Read the rest from the object.
	sc.misses.Add(1)
	if err := readable.ReadAt(ctx, p[n:], ofs+int64(n)); err != nil {
		return err
	}
	// TODO(josh): Currently, we do not read in increments of cache block size above,
	// which is a correctness issue, as a later read can read zero data from a partially
	// filled cache block. Fix this.
	// TODO(josh): Writing back to the cache should be asyc with respect to the
	// call to ReadAt.
	if err := sc.Set(fileNum, p[n:], ofs+int64(n)); err != nil {
		// TODO(josh): Would like to log at error severity, but base.Logger doesn't
		// have error severity.
		sc.logger.Infof("writing back to cache after miss failed: %v", err)
	}
	return nil
}

// Get attempts to read the requested data from the cache.
//
// If all data is available, returns n = len(p).
//
// If data is partially available, a prefix of the data is read; returns n < len(p)
// and no error. If no prefix is available, returns n = 0 and no error.
func (sc *sharedCache) Get(fileNum base.FileNum, p []byte, ofs int64) (n int, _ error) {
	// The data extent might cross shard boundaries, hence the loop. In the hot
	// path, max two iterations of this loop will be executed, since reads are sized
	// in units of sstable block size.
	for {
		shard := sc.getShard(fileNum, ofs+int64(n))
		cappedLen := len(p[n:])
		if toBoundary := int(shardingBlockSize - (ofs % shardingBlockSize)); cappedLen > toBoundary {
			cappedLen = toBoundary
		}
		numRead, err := shard.Get(fileNum, p[n:n+cappedLen], ofs+int64(n))
		if err != nil {
			return n, err
		}
		n += numRead
		if numRead < cappedLen {
			// We only read a prefix from this shard.
			return n, nil
		}
		if n == len(p) {
			// We are done.
			return n, nil
		}
		// Data extent crosses shard boundary, continue with next shard.
	}
}

// Set attempts to write the requested data to the cache.
//
// If all of p is not written to the shard, Set returns a non-nil error.
func (sc *sharedCache) Set(fileNum base.FileNum, p []byte, ofs int64) error {
	// The data extent might cross shard boundaries, hence the loop. In the hot
	// path, max two iterations of this loop will be executed, since reads are sized
	// in units of sstable block size.
	n := 0
	for {
		shard := sc.getShard(fileNum, ofs+int64(n))
		cappedLen := len(p[n:])
		if toBoundary := int(shardingBlockSize - (ofs % shardingBlockSize)); cappedLen > toBoundary {
			cappedLen = toBoundary
		}
		err := shard.Set(fileNum, p[n:n+cappedLen], ofs+int64(n))
		if err != nil {
			return err
		}
		// Set returns an error if cappedLen bytes aren't written the the shard.
		n += cappedLen
		if n == len(p) {
			// We are done.
			return nil
		}
		// Data extent crosses shard boundary, continue with next shard.
	}
}

const shardingBlockSize = 1024 * 1024

func (sc *sharedCache) getShard(fileNum base.FileNum, ofs int64) *sharedCacheShard {
	const prime64 = 1099511628211
	hash := uint64(fileNum)*prime64 + uint64(ofs)/shardingBlockSize
	return &sc.shards[hash%uint64(len(sc.shards))]
}

type sharedCacheShard struct {
	file         vfs.File
	sizeInBlocks int64
	blockSize    int
	mu           struct {
		sync.Mutex
		// TODO(josh): Neither of these datastructures are space-efficient.
		// Focusing on correctness to start.
		where map[metadataKey]int64
		free  []int64
	}
}

type metadataKey struct {
	filenum base.FileNum
	offset  int64
}

func (s *sharedCacheShard) init(
	fs vfs.FS, fsDir string, shardIdx int, sizeInBlocks int64, blockSize int,
) error {
	*s = sharedCacheShard{
		sizeInBlocks: sizeInBlocks,
	}
	if blockSize < 1024 || shardingBlockSize%blockSize != 0 {
		return errors.Newf("invalid block size %d (must divide %d)", blockSize, shardingBlockSize)
	}
	s.blockSize = blockSize
	file, err := fs.OpenReadWrite(fs.PathJoin(fsDir, fmt.Sprintf("SHARED-CACHE-%03d", shardIdx)))
	if err != nil {
		return err
	}
	// TODO(radu): truncate file if necessary (especially important if we restart
	// with more shards).
	if err := file.Preallocate(0, int64(blockSize)*sizeInBlocks); err != nil {
		return err
	}
	s.file = file

	// TODO(josh): Right now, the secondary cache is not persistent. All existing
	// cache contents will be over-written, since all metadata is only stored in
	// memory.
	s.mu.where = make(map[metadataKey]int64)
	for i := int64(0); i < sizeInBlocks; i++ {
		s.mu.free = append(s.mu.free, i)
	}

	return nil
}

func (s *sharedCacheShard) Close() error {
	defer func() {
		s.file = nil
	}()
	return s.file.Close()
}

// Get attempts to read the requested data from the shard. The data must not
// cross a shard boundary.
//
// If all data is available, returns n = len(p).
//
// If data is partially available, a prefix of the data is read; returns n < len(p)
// and no error. If no prefix is available, returns n = 0 and no error.
func (s *sharedCacheShard) Get(fileNum base.FileNum, p []byte, ofs int64) (n int, _ error) {
	if invariants.Enabled {
		if ofs/shardingBlockSize != (ofs+int64(len(p))-1)/shardingBlockSize {
			panic("Get crosses shard boundary")
		}
	}

	// TODO(josh): Make the locking more fine-grained. Do not hold locks during calls
	// to ReadAt.
	s.mu.Lock()
	defer s.mu.Unlock()

	// The data extent might cross cache block boundaries, hence the loop. In the hot
	// path, max two iterations of this loop will be executed, since reads are sized
	// in units of sstable block size.
	for {
		cacheBlockInd, ok := s.mu.where[metadataKey{
			filenum: fileNum,
			offset:  (ofs + int64(n)) / int64(s.blockSize),
		}]
		if !ok {
			return n, nil
		}

		readAt := cacheBlockInd * int64(s.blockSize)
		if n == 0 { // if first read
			readAt += ofs % int64(s.blockSize)
		}
		readSize := s.blockSize
		if n == 0 { // if first read
			// Cast to int safe since ofs is modded by block size.
			readSize -= int(ofs % int64(s.blockSize))
		}

		if len(p[n:]) <= readSize {
			numRead, err := s.file.ReadAt(p[n:], readAt)
			return n + numRead, err
		}
		numRead, err := s.file.ReadAt(p[n:n+readSize], readAt)
		if err != nil {
			return 0, err
		}

		// Note that numRead == readSize, since we checked for an error above.
		n += numRead
	}
}

// Set attempts to write the requested data to the shard. The data must not
// cross a shard boundary.
//
// If all of p is not written to the shard, Set returns a non-nil error.
func (s *sharedCacheShard) Set(fileNum base.FileNum, p []byte, ofs int64) error {
	if invariants.Enabled {
		if ofs/shardingBlockSize != (ofs+int64(len(p))-1)/shardingBlockSize {
			panic("Set crosses shard boundary")
		}
	}

	// TODO(josh): Make the locking more fine-grained. Do not hold locks during calls
	// to WriteAt.
	s.mu.Lock()
	defer s.mu.Unlock()

	// The data extent might cross cache block boundaries, hence the loop. In the hot
	// path, max two iterations of this loop will be executed, since reads are sized
	// in units of sstable block size.
	n := 0
	for {
		var cacheBlockInd int64
		if len(s.mu.free) == 0 {
			// TODO(josh): Right now, we do random eviction. Eventually, we will do something
			// more sophisticated, e.g. leverage ClockPro.
			var k metadataKey
			for k1, v := range s.mu.where {
				cacheBlockInd = v
				k = k1
				break
			}
			delete(s.mu.where, k)
		} else {
			cacheBlockInd = s.mu.free[len(s.mu.free)-1]
			s.mu.free = s.mu.free[:len(s.mu.free)-1]
		}

		s.mu.where[metadataKey{
			filenum: fileNum,
			offset:  (ofs + int64(n)) / int64(s.blockSize),
		}] = cacheBlockInd

		writeAt := cacheBlockInd * int64(s.blockSize)
		if n == 0 { // if first write
			writeAt += ofs % int64(s.blockSize)
		}
		writeSize := s.blockSize
		if n == 0 { // if first write
			// Cast to int safe since ofs is modded by block size.
			writeSize -= int(ofs % int64(s.blockSize))
		}

		if len(p[n:]) <= writeSize {
			// Ignore num written ret value, since if partial write, an error
			// is returned.
			_, err := s.file.WriteAt(p[n:], writeAt)
			return err
		}
		numWritten, err := s.file.WriteAt(p[n:n+writeSize], writeAt)
		if err != nil {
			return err
		}

		// Note that numWritten == writeSize, since we checked for an error above.
		n += numWritten
	}
}
