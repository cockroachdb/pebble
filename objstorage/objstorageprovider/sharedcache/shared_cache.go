// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sharedcache

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/vfs"
)

const (
	// ShardingBlockSize is public to enable testing from sharedcache_test.
	ShardingBlockSize = 1024 * 1024
)

// Cache is a persistent cache backed by a local filesystem. It is intended
// to cache data that is in slower shared storage (e.g. S3), hence the
// package name 'sharedcache'.
type Cache struct {
	shards []shard
	logger base.Logger

	blockSize int

	// TODO(josh): Have a dedicated metrics struct. Right now, this
	// is just for testing.
	Misses atomic.Int32
}

// Open opens a cache. If there is no existing cache at fsDir, a new one
// is created.
func Open(
	fs vfs.FS, fsDir string, blockSize int, sizeBytes int64, numShards int, persistMetadata bool,
) (*Cache, error) {
	min := ShardingBlockSize * int64(numShards)
	if sizeBytes < min {
		return nil, errors.Errorf("cache size %d lower than min %d", sizeBytes, min)
	}

	sc := &Cache{
		blockSize: blockSize,
	}
	sc.shards = make([]shard, numShards)
	blocksPerShard := sizeBytes / int64(numShards) / int64(blockSize)
	for i := range sc.shards {
		if err := sc.shards[i].open(fs, fsDir, i, blocksPerShard, blockSize, persistMetadata); err != nil {
			return nil, err
		}
	}
	return sc, nil
}

// Close closes the cache. Methods such as ReadAt should not be called after Close is
// called.
func (c *Cache) Close() error {
	var retErr error
	for i := range c.shards {
		if err := c.shards[i].close(); err != nil && retErr == nil {
			retErr = err
		}
	}
	c.shards = nil
	return retErr
}

// ReadAt performs a read form an object, attempting to use cached data when
// possible.
func (c *Cache) ReadAt(
	ctx context.Context, fileNum base.FileNum, p []byte, ofs int64, readable objstorage.Readable,
) error {
	{
		n, err := c.get(fileNum, p, ofs)
		if err != nil {
			return err
		}
		if n == len(p) {
			// Everything was in cache!
			return nil
		}

		// Note this. The below code does not need the original ofs, as with the earlier
		// reading from the cache done, the relevant offset is ofs + int64(n). Same with p.
		ofs += int64(n)
		p = p[n:]

		if invariants.Enabled {
			if n != 0 && ofs%int64(c.blockSize) != 0 {
				panic(fmt.Sprintf("after non-zero read from cache, ofs is not block-aligned: %v %v", ofs, n))
			}
		}
	}

	// We must do reads with offset & size that are multiples of the block size. Else
	// later cache hits may return incorrect zeroed results from the cache.
	firstBlockInd := ofs / int64(c.blockSize)
	adjustedOfs := firstBlockInd * int64(c.blockSize)

	// Take the length of what is left to read plus the length of the adjustment of
	// the offset plus the size of a block minus one and divide by the size of a block
	// to get the number of blocks to read from the readable.
	sizeOfOffAdjustment := int(ofs - adjustedOfs)
	numBlocksToRead := ((len(p) + sizeOfOffAdjustment) + (c.blockSize - 1)) / c.blockSize
	adjustedLen := numBlocksToRead * c.blockSize
	adjustedP := make([]byte, adjustedLen)

	// Read the rest from the object.
	c.Misses.Add(1)
	// TODO(josh): To have proper EOF handling, we will need readable.ReadAt to return
	// the number of bytes read successfully. As is, we cannot tell if the readable.ReadAt
	// should be returned from cache.ReadAt. For now, the cache just swallows all
	// io.EOF errors.
	if err := readable.ReadAt(ctx, adjustedP, adjustedOfs); err != nil && err != io.EOF {
		return err
	}
	copy(p, adjustedP[sizeOfOffAdjustment:])

	// TODO(josh): Writing back to the cache should be async with respect to the
	// call to ReadAt.
	if err := c.set(fileNum, adjustedP, adjustedOfs); err != nil {
		// TODO(josh): Would like to log at error severity, but base.Logger doesn't
		// have error severity.
		c.logger.Infof("writing back to cache after miss failed: %v", err)
	}
	return nil
}

// get attempts to read the requested data from the cache.
//
// If all data is available, returns n = len(p).
//
// If data is partially available, a prefix of the data is read; returns n < len(p)
// and no error. If no prefix is available, returns n = 0 and no error.
func (c *Cache) get(fileNum base.FileNum, p []byte, ofs int64) (n int, _ error) {
	// The data extent might cross shard boundaries, hence the loop. In the hot
	// path, max two iterations of this loop will be executed, since reads are sized
	// in units of sstable block size.
	for {
		shard := c.getShard(fileNum, ofs+int64(n))
		cappedLen := len(p[n:])
		if toBoundary := int(ShardingBlockSize - ((ofs + int64(n)) % ShardingBlockSize)); cappedLen > toBoundary {
			cappedLen = toBoundary
		}
		numRead, err := shard.get(fileNum, p[n:n+cappedLen], ofs+int64(n))
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

// set attempts to write the requested data to the cache. Both ofs & len(p) must
// be multiples of the block size.
//
// If all of p is not written to the shard, set returns a non-nil error.
func (c *Cache) set(fileNum base.FileNum, p []byte, ofs int64) error {
	if invariants.Enabled {
		if ofs%int64(c.blockSize) != 0 || len(p)%c.blockSize != 0 {
			panic(fmt.Sprintf("set with ofs & len not multiples of block size: %v %v", ofs, len(p)))
		}
	}

	// The data extent might cross shard boundaries, hence the loop. In the hot
	// path, max two iterations of this loop will be executed, since reads are sized
	// in units of sstable block size.
	n := 0
	for {
		shard := c.getShard(fileNum, ofs+int64(n))
		cappedLen := len(p[n:])
		if toBoundary := int(ShardingBlockSize - ((ofs + int64(n)) % ShardingBlockSize)); cappedLen > toBoundary {
			cappedLen = toBoundary
		}
		err := shard.set(fileNum, p[n:n+cappedLen], ofs+int64(n))
		if err != nil {
			return err
		}
		// set returns an error if cappedLen bytes aren't written the the shard.
		n += cappedLen
		if n == len(p) {
			// We are done.
			return nil
		}
		// Data extent crosses shard boundary, continue with next shard.
	}
}

func (c *Cache) getShard(fileNum base.FileNum, ofs int64) *shard {
	const prime64 = 1099511628211
	hash := uint64(fileNum)*prime64 + uint64(ofs)/ShardingBlockSize
	// TODO(josh): Instance change ops are often run in production. Such an operation
	// updates len(c.shards); see openSharedCache. As a result, the behavior of this
	// function changes, and the cache empties out at restart time. We may want a better
	// story here eventually.
	return &c.shards[hash%uint64(len(c.shards))]
}

type shard struct {
	file              vfs.File
	numDataBlocks     int64
	numMetadataBlocks int64
	blockSize         int
	persistMetadata   bool
	mu                struct {
		sync.Mutex

		// TODO(josh): None of these datastructures are space-efficient.
		// Focusing on correctness to start.
		where map[metadataMapKey]int64
		table []*metadataMapKey
		free  []int64
		seq   int32
	}
}

type metadataMapKey struct {
	filenum         base.FileNum
	logicalBlockInd int64
}

type metadataLogEntry struct {
	filenum         base.FileNum
	logicalBlockInd int64
	cacheBlockInd   int64
}

// TODO(josh): We should make this type more space-efficient later on.
type metadataLogEntryBatch struct {
	e1 metadataLogEntry
	e2 metadataLogEntry
	// A non-full shard will not always set e2. If e2 is not valid, it should
	// not be used to create in-memory metadata from the metadata log.
	e2IsValid bool
	// seq provides a way of ordering batches written to the metadata log.
	// Each batch increases seq by exactly one. A zero-valued seq indicates
	// a non-full circular buffer of batches. The batch with the zero-valued
	// seq should be skipped over.
	seq int32
}

// TODO(josh): For now, changing parameters of the cache, such as sizeInBlocks or number
// of shards, is not supported.
func (s *shard) open(
	fs vfs.FS, fsDir string, shardIdx int, sizeInBlocks int64, blockSize int, persistMetadata bool,
) error {
	// Init the files that back the shard.
	{
		*s = shard{
			persistMetadata: persistMetadata,
		}
		if blockSize < 1024 || ShardingBlockSize%blockSize != 0 {
			return errors.Newf("invalid block size %d (must divide %d)", blockSize, ShardingBlockSize)
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
	}

	// Compute the number of cache blocks used for metadata and for data.
	var metadataTotalSizeInBytes int64
	{
		if s.persistMetadata {
			metadataBatchSizeInBytes := int64(unsafe.Sizeof(metadataLogEntryBatch{}))
			metadataTotalSizeInBytes = metadataBatchSizeInBytes * sizeInBlocks
			numMetadataBlocks := (metadataTotalSizeInBytes + int64(blockSize) - 1) / int64(blockSize)

			s.numMetadataBlocks = numMetadataBlocks
			s.numDataBlocks = sizeInBlocks - numMetadataBlocks
			if s.numMetadataBlocks == 0 {
				return errors.Errorf("shard too small as no room for metadata blocks")
			}
			if s.numDataBlocks == 0 {
				return errors.Errorf("shard too small as no room for data blocks")
			}
		} else {
			s.numMetadataBlocks = 0
			s.numDataBlocks = sizeInBlocks
		}
	}

	// Read the log from the early blocks in the file that backs the shard.
	// If the log doesn't exist, we will get a zero-valued log, which we will
	// lead us to init empty in-memory metadata, as expected.
	var log []metadataLogEntryBatch
	if s.persistMetadata {
		logAsBytes := make([]byte, metadataTotalSizeInBytes)
		_, err := s.file.ReadAt(logAsBytes, 0)
		if err != nil && err != io.EOF {
			return err
		}

		logPtr := unsafe.Pointer(&logAsBytes[0])
		log = unsafe.Slice((*metadataLogEntryBatch)(logPtr), s.numDataBlocks)
	}

	// Find the max sequence number and the index of the entry in the log with
	// the max sequence number. Since the log is circular, the entry after that
	// should be processed first.
	maxEntry := 0
	{
		maxSeq := int32(0)
		for i, e := range log {
			if e.seq > maxSeq {
				maxSeq = e.seq
				maxEntry = i
			}
		}
		// seq is inited at one. See below where a batch with a zero-valued seq
		// is skipped over.
		s.mu.seq = maxSeq + 1
	}

	// Replay the log to create s.mu.table.
	{
		s.mu.table = make([]*metadataMapKey, s.numDataBlocks)
		var prevSeq int32
		for i := 0; i < len(log); i++ {
			entry := log[(i+maxEntry+1)%len(log)]
			// seq is inited at one & increases monotonically. So a zero-valued seq
			// indicates the circular buffer is not yet full. The batch should be
			// skipped.
			if entry.seq == 0 {
				continue
			}
			if i != 0 {
				if entry.seq != prevSeq+1 {
					return errors.Errorf("sequence numbers not increasing by exactly one always: %v %v", prevSeq, entry.seq)
				}
			}
			prevSeq = entry.seq

			entries := []metadataLogEntry{entry.e1}
			// If the shard is not full, there may be no need to include metadata
			// in e2. Skip processing e2 is this is the case.
			if entry.e2IsValid {
				entries = append(entries, entry.e2)
			}
			for _, subEntry := range entries {
				k := metadataMapKey{
					filenum:         subEntry.filenum,
					logicalBlockInd: subEntry.logicalBlockInd,
				}
				s.mu.table[subEntry.cacheBlockInd] = &k
			}
		}
	}

	// Create s.mu.where & s.mu.free from s.mu.table.
	{
		s.mu.where = make(map[metadataMapKey]int64)
		for cacheBlockInd, e := range s.mu.table {
			if e != nil {
				s.mu.where[*e] = int64(cacheBlockInd)
			} else {
				s.mu.free = append(s.mu.free, int64(cacheBlockInd))
			}
		}
	}

	return nil
}

func (s *shard) close() error {
	defer func() {
		s.file = nil
	}()
	return s.file.Close()
}

// get attempts to read the requested data from the shard. The data must not
// cross a shard boundary.
//
// If all data is available, returns n = len(p).
//
// If data is partially available, a prefix of the data is read; returns n < len(p)
// and no error. If no prefix is available, returns n = 0 and no error.
func (s *shard) get(fileNum base.FileNum, p []byte, ofs int64) (n int, _ error) {
	if invariants.Enabled {
		if ofs/ShardingBlockSize != (ofs+int64(len(p))-1)/ShardingBlockSize {
			panic(fmt.Sprintf("get crosses shard boundary: %v %v", ofs, len(p)))
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
		cacheBlockInd, ok := s.mu.where[metadataMapKey{
			filenum:         fileNum,
			logicalBlockInd: (ofs + int64(n)) / int64(s.blockSize),
		}]
		if !ok {
			return n, nil
		}

		readAt := cacheBlockInd*int64(s.blockSize) + s.numMetadataBlocks*int64(s.blockSize)
		if n == 0 { // if first read
			readAt += ofs % int64(s.blockSize)
		}

		readSize := s.blockSize
		if n == 0 { // if first read
			// Cast to int safe since ofs is modded by block size.
			readSize -= int(ofs % int64(s.blockSize))
		}

		if invariants.Enabled {
			if readAt < s.numMetadataBlocks*int64(s.blockSize) {
				panic(fmt.Sprintf("reading a data block from the metadata block section: %v %v %v", readAt, s.numMetadataBlocks, s.blockSize))
			}
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

// set attempts to write the requested data to the shard. The data must not
// cross a shard boundary, and both ofs & len(p) must be multiples of the
// block size.
//
// If all of p is not written to the shard, set returns a non-nil error.
func (s *shard) set(fileNum base.FileNum, p []byte, ofs int64) error {
	if invariants.Enabled {
		if ofs/ShardingBlockSize != (ofs+int64(len(p))-1)/ShardingBlockSize {
			panic(fmt.Sprintf("set crosses shard boundary: %v %v", ofs, len(p)))
		}
		if ofs%int64(s.blockSize) != 0 || len(p)%s.blockSize != 0 {
			panic(fmt.Sprintf("set with ofs & len not multiples of block size: %v %v", ofs, len(p)))
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
			var k metadataMapKey
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

		k := metadataMapKey{
			filenum:         fileNum,
			logicalBlockInd: (ofs + int64(n)) / int64(s.blockSize),
		}
		s.mu.where[k] = cacheBlockInd
		s.mu.table[cacheBlockInd] = &k

		writeAt := cacheBlockInd*int64(s.blockSize) + s.numMetadataBlocks*int64(s.blockSize)
		writeSize := s.blockSize
		if invariants.Enabled {
			if writeAt < s.numMetadataBlocks*int64(s.blockSize) {
				panic(fmt.Sprintf("writing a data block to the metadata block section: %v %v %v", writeAt, s.numMetadataBlocks, s.blockSize))
			}
		}

		entry := metadataLogEntry{
			filenum:         k.filenum,
			logicalBlockInd: k.logicalBlockInd,
			cacheBlockInd:   cacheBlockInd,
		}

		// TODO(josh): Below code does no fsyncing. We can fix this once we land the working
		// set idea described by Radu at https://github.com/cockroachdb/pebble/issues/2542.
		if len(p[n:]) <= writeSize {
			// Ignore num written ret value, since if partial write, an error
			// is returned.
			_, err := s.file.WriteAt(p[n:], writeAt)
			if err != nil {
				return err
			}

			// TODO(josh): It is not safe to update the contents of the cache & the metadata in two
			// disk writes like this, without the working set scheme described by Radu at
			// https://github.com/cockroachdb/pebble/issues/2542. We will implement that later.
			return s.persistMetadataUpdate(entry)
		}

		numWritten, err := s.file.WriteAt(p[n:n+writeSize], writeAt)
		if err != nil {
			return err
		}

		if err := s.persistMetadataUpdate(entry); err != nil {
			return err
		}

		// Note that numWritten == writeSize, since we checked for an error above.
		n += numWritten
	}
}

func (s *shard) persistMetadataUpdate(e metadataLogEntry) error {
	if !s.persistMetadata {
		return nil
	}

	batch := metadataLogEntryBatch{
		e1:  e,
		seq: s.mu.seq,
	}

	// e2 enables writing out the full metadata every s.numDataBlocks calls to
	// persistMetadataUpdate, which implies that the full metadata is present within
	// the circular log, since it is s.numDataBlocks batches large.
	//
	// This is conceptually not different from flushing the entire table every s.numDataBlocks
	// updates. We just de-amortize and flush it bit by bit instead of a single large flush.
	ptr := int64(s.mu.seq) % s.numDataBlocks
	if s.mu.table[ptr] != nil {
		batch.e2IsValid = true
		batch.e2 = metadataLogEntry{
			filenum:         s.mu.table[ptr].filenum,
			logicalBlockInd: s.mu.table[ptr].logicalBlockInd,
			cacheBlockInd:   ptr,
		}
	}

	// TODO(josh): This serialization scheme is not intended to be portable across machine
	// architectures. We can make it portable in a later PR.
	batchAsBytes := (*[unsafe.Sizeof(metadataLogEntryBatch{})]byte)(unsafe.Pointer(&batch))[:]
	writeAt := int64(s.mu.seq) % s.numDataBlocks * int64(unsafe.Sizeof(metadataLogEntryBatch{}))

	if invariants.Enabled {
		if writeAt+int64(len(batchAsBytes)) > s.numMetadataBlocks*int64(s.blockSize) {
			panic(fmt.Sprintf("writing a metadata block to the data block section: %v %v %v %v", writeAt, len(batchAsBytes), s.numMetadataBlocks, s.blockSize))
		}
	}

	_, err := s.file.WriteAt(batchAsBytes, writeAt)
	if err != nil {
		return err
	}

	// TODO(josh): Keep seq from overflowing, by moding by something, or similar.
	s.mu.seq++ // mu is held; see Set; fine-grained locking can be done later on
	return nil
}
