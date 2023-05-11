// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorageprovider

import (
	"context"
	"fmt"
	"math/bits"
	"runtime"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/vfs"
)

type sharedCache struct {
	shards []sharedCacheShard
}

func openSharedCache(
	fs vfs.FS, fsDir string, blockSize int, sizeBytes int64,
) (*sharedCache, error) {
	numShards := 2 * runtime.GOMAXPROCS(0)
	if sizeBytes < shardingBlockSize*int64(numShards) {
		return nil, errors.Errorf("cache size %d too low", sizeBytes)
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
	// TODO(radu, josh): write back to cache.
	return readable.ReadAt(ctx, p[n:], ofs+int64(n))
}

// Get attempts to read the requested data from the cache.
//
// If all data is available, returns n = len(p).
//
// If data is partially available, a prefix of the data is read; returns n < len(p)
// and no error. If no prefix is available, returns n = 0 and no error.
func (sc *sharedCache) Get(fileNum base.FileNum, p []byte, ofs int64) (n int, _ error) {
	// The data extent might cross shard boundaries, hence the loop. In the hot
	// path, only one iteration of the loop will be executed.
	for {
		shard := sc.getShard(fileNum, ofs)
		cappedLen := len(p)
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

const shardingBlockSize = 1024 * 1024

func (sc *sharedCache) getShard(fileNum base.FileNum, ofs int64) *sharedCacheShard {
	const prime64 = 1099511628211
	hash := uint64(fileNum)*prime64 + uint64(ofs)/shardingBlockSize
	return &sc.shards[hash%uint64(len(sc.shards))]
}

type sharedCacheShard struct {
	file         vfs.File
	sizeInBlocks int64
	// blockSizeExp is log2(BlockSize).
	blockSizeExp int
	mu           struct {
		sync.Mutex
		// TODO: map from fileNum / offset to block metadata.
	}
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
	s.blockSizeExp = bits.Len64(uint64(blockSize)) - 1
	file, err := fs.OpenReadWrite(fs.PathJoin(fsDir, fmt.Sprintf("SHARED-CACHE-%03d", shardIdx)))
	if err != nil {
		return err
	}
	// TODO(radu): truncate file if necessary (especially important if we restart
	// with more shards).
	if err := file.Preallocate(0, int64(blockSize)*sizeInBlocks); err != nil {
		return err
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
	// TODO
	return 0, nil
}
