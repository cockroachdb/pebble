// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

type persistentCache struct {
	mu struct {
		sync.Mutex

		files        map[base.FileNum]*persistentCacheValue
		head, tail   *persistentCacheValue
		usedCapacity uint64
	}

	files    chan *persistentCacheValue
	capacity uint64

	localFS, sharedFS   vfs.FS
	localDir, sharedDir string
	uniqueID            uint32
	wg                  sync.WaitGroup
}

type persistentCacheValue struct {
	atomic struct {
		initialized uint64
	}
	mu struct {
		sync.Mutex

		refs     int64
		evicting sync.Cond
	}
	localFile vfs.File
	fileNum   base.FileNum
	dir       string
	localFS   vfs.FS
	size      uint64

	// Protected by persistentCache.mu.
	prev, next *persistentCacheValue
}

func (p *persistentCacheValue) File() vfs.File {
	return p.localFile
}

func (p *persistentCacheValue) Ref() {
	p.mu.Lock()
	p.mu.refs++
	p.mu.Unlock()
}

func (p *persistentCacheValue) unrefInternal(waitForEviction bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.refs--
	if p.mu.refs > 0 {
		if waitForEviction {
			p.mu.evicting.Wait()
		}
		return
	} else if p.mu.refs < 0 {
		panic("inconsistent ref count")
	}
	// p.mu.refs == 0
	fmt.Printf("persistent cache: evicted %s (size = %d)\n", p.fileNum, p.size)
	if atomic.LoadUint64(&p.atomic.initialized) != 0 {
		_ = p.localFile.Close()
		_ = p.localFS.Remove(base.MakeFilepath(p.localFS, p.dir, fileTypeTable, p.fileNum))
	}
	p.mu.evicting.Broadcast()
}

func (p *persistentCacheValue) Unref() {
	p.unrefInternal(false /* waitForEviction */)
}

func newPersistentCache(
	localFS vfs.FS, localDir string, sharedFS vfs.FS, sharedDir string, uniqueID uint32, size uint64,
) *persistentCache {
	psc := &persistentCache{
		files:     make(chan *persistentCacheValue, 500),
		capacity:  size,
		localFS:   localFS,
		sharedFS:  sharedFS,
		localDir:  localDir,
		sharedDir: sharedDir,
		uniqueID:  uniqueID,
	}
	psc.mu.files = make(map[base.FileNum]*persistentCacheValue)
	return psc
}

func (l *persistentCache) MarkDeleted(fileNum base.FileNum) {
	l.mu.Lock()
	defer l.mu.Unlock()
	cached := l.mu.files[fileNum]
	if cached == nil {
		return
	}

	delete(l.mu.files, fileNum)
}

func (l *persistentCache) Get(fileNum base.FileNum) sstable.PersistentCacheValue {
	l.mu.Lock()
	cached := l.mu.files[fileNum]

	if cached == nil {
		l.mu.Unlock()
		return nil
	}

	cached.Ref()

	// Move up to head.
	if l.mu.tail == cached && l.mu.head != cached {
		l.mu.tail = cached.prev
	}
	if cached.prev != nil {
		cached.prev.next = cached.next
	}
	if cached.next != nil && l.mu.head != cached {
		cached.next.prev = cached.prev
	}
	cached.prev = nil
	if l.mu.head != cached {
		cached.next = l.mu.head
		l.mu.head.prev = cached
		l.mu.head = cached
	}

	l.mu.Unlock()

	if atomic.LoadUint64(&cached.atomic.initialized) == 0 {
		cached.Unref()
		return nil
	}
	return cached
}

func (l *persistentCache) MaybeCache(meta *manifest.FileMetadata, amountRead int64) {
	newVal := atomic.AddInt64(&meta.Atomic.BytesBeforeLocalCache, -1*amountRead)
	if newVal > 0 {
		return
	}
	atomic.StoreInt64(&meta.Atomic.BytesBeforeLocalCache, meta.InitBytesBeforeLocalCache)

	l.mu.Lock()
	cached := l.mu.files[meta.FileNum]
	if cached != nil {
		l.mu.Unlock()
		return
	}
	pcValue := &persistentCacheValue{
		localFile: nil,
		fileNum:   meta.FileNum,
		size:      meta.Size,
		dir:       l.localDir,
		localFS:   l.localFS,
	}
	pcValue.atomic.initialized = 0
	pcValue.mu.refs = 1
	pcValue.mu.evicting.L = &pcValue.mu
	l.mu.files[meta.FileNum] = pcValue

	pcValue.prev = nil
	pcValue.next = l.mu.head
	l.mu.head = pcValue
	if l.mu.tail == nil {
		l.mu.tail = pcValue
	} else {
		pcValue.next.prev = pcValue
	}
	l.mu.usedCapacity += pcValue.size
	l.mu.Unlock()

	l.files <- pcValue
}

func (l *persistentCache) copyAsync(
	localPath string, sharedPath string, val *persistentCacheValue,
) {
	defer l.wg.Done()
	defer val.Unref()

	err := vfs.CopyAcrossFS(l.sharedFS, sharedPath, l.localFS, localPath)
	if err != nil {
		// TODO: handle.
		fmt.Printf("error when copying across fs: %s\n", err)
		return
	}

	val.localFile, err = l.localFS.Open(localPath)
	if err != nil {
		// TODO: handle.
		fmt.Printf("error when opening file after copy: %s\n", err)
		return
	}
	atomic.StoreUint64(&val.atomic.initialized, 1)
	fmt.Printf("persistent cache: saved %s (size = %d)\n", val.fileNum, val.size)
}

func (l *persistentCache) runCacher() {
	defer l.wg.Done()

	for {
		file, ok := <-l.files
		if !ok {
			return
		}
		l.mu.Lock()
		skip := false
		for l.mu.usedCapacity > l.capacity {
			tail := l.mu.tail
			if tail == file {
				// Evicting ourselves. Don't cache.
				skip = true
			} else if tail == nil {
				// Empty cache.
				break
			}
			l.mu.tail = tail.prev
			if tail.prev == nil {
				l.mu.head = nil
			} else {
				tail.prev.next = nil
			}

			if !skip {
				l.mu.usedCapacity -= tail.size
			}
			tail.next = nil
			tail.prev = nil
			delete(l.mu.files, tail.fileNum)
			if skip {
				break
			}
			l.mu.Unlock()

			tail.unrefInternal(true)
			l.mu.Lock()
		}
		if skip || (file.next == nil && file.prev == nil) {
			// We were evicted.
			l.mu.Unlock()
			continue
		}
		file.Ref()
		l.mu.Unlock()
		fmt.Printf("persistent cache: added %s (size = %d)\n", file.fileNum, file.size)

		localPath := base.MakeFilepath(l.localFS, l.localDir, fileTypeTable, file.fileNum)
		sharedPath := base.MakeSharedSSTPath(l.sharedFS, l.sharedDir, l.uniqueID, file.fileNum)
		l.wg.Add(1)
		go l.copyAsync(localPath, sharedPath, file)
	}
}

func (l *persistentCache) Close() {
	close(l.files)
	l.wg.Wait()
}

func (l *persistentCache) Start() {
	l.wg.Add(1)
	go l.runCacher()
}
