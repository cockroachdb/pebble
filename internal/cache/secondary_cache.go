// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package cache

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/vfs"
)

type SecondaryCache interface {
	GetAndEvict(id uint64, fileNum base.FileNum, offset uint64) []byte
	Set(id uint64, fileNum base.FileNum, offset uint64, block []byte)
	DeleteFile(id uint64, fileNum base.FileNum)
}

// Design of secondary cache:
//
// 0) Block key = {id, filenum, offset uint64}
// 1) Slab files of max size 16mb that contain block key, size (8 bytes),
//    unused (8 bytes), followed by block contents.
// 2) In-memory map containing key (above), plus cache slab file number and
//    offset in file.
// 3) On node restart, we read all slab files to repopulate in-memory map.
//
// On eviction from primary block cache:
// 1) Check if file is not deleted, and has UsesSharedFS = true. If yes, file
//    blocks are eligible for secondary cache.
//    1.5) Check if block is already in the cache (possible if it was promoted
//         to primary cache but not evicted from secondary yet). If it is, take
//         it out of block eviction queue.
// 2) If cache usage is below capacity (fast path), append to end of current
//    slab file. If current slab file has no room, preallocate new slab file.
// 3) If cache usage is above capacity, we have two options:
//    a) Look at head of block eviction queue. If size of that block is higher
//       than that of new block, replace that block with this block.
//    b) Failing that, delete least recently accessed slab file, remove relevant
//       entries from it from block eviction queue (this can happen async),
//       and do 2.
//
// When a file is marked as deleted in the primary cache:
// 1) Enqueue all blocks from it into the block eviction queue.
//
// On a "Get":
// 1) Check in-memory map for block, if it exists, do a random read from that
//    slab file and return block contents.
// 2) Move up slab file in LRU queue.
// 3) Add to block eviction queue
//
// On block eviction for any reason:
// 1) Remove self from original file's list
// 2) Remove self from slab file's list (not necessary if part of slab file
//    deletion).
//
// Doubly linked lists:
// 1) LRU of slab files (doubly linked list with ability to move things around)
// 2) Block eviction queue (can come later)
// 3) List of all cached blocks from a given original sst file
//    operations: add to tail, remove at any point
// 4) List of all cached blocks from a given slab file
//    operations: add to tail, remove at any point

type blockKey struct {
	fileKey

	offset uint64
}

type lruFileList struct {
	head, tail *secondaryCacheFile
}

func (l *lruFileList) moveToFront(h *secondaryCacheFile) {
	if l.tail == h && l.head != h {
		l.tail = h.prev
	}
	if h.prev != nil {
		h.prev.next = h.next
	}
	if h.next != nil && l.head != h {
		h.next.prev = h.prev
	}
	h.prev = nil
	if l.head != h {
		h.next = l.head
		l.head.prev = h
		l.head = h
	}
}

func (l *lruFileList) add(h *secondaryCacheFile) {
	h.prev = nil
	h.next = l.head
	l.head = h
	if l.tail == nil {
		l.tail = h
	} else {
		h.next.prev = h
	}
}

func (l *lruFileList) evict() *secondaryCacheFile {
	if l.tail == nil {
		return nil
	}
	h := l.tail
	l.tail = h.prev
	if l.head != h {
		h.prev.next = nil
	} else {
		l.head = nil
	}
	h.next = nil
	h.prev = nil
	return h
}

type blockListType int

const (
	blockListOrigFile blockListType = iota
	blockListSlabFile
)

type blockLinkedList struct {
	head, tail *secondaryCacheValue
}

type blockListLinks struct {
	prev, next *secondaryCacheValue
}

func (b *blockLinkedList) add(s *secondaryCacheValue, listType blockListType) {
	switch listType {
	case blockListOrigFile:
		s.origFileLink.prev = b.tail
		s.origFileLink.next = nil
	case blockListSlabFile:
		s.slabFileLink.prev = b.tail
		s.slabFileLink.next = nil
	}
	if b.tail == nil {
		b.head = s
	} else {
		switch listType {
		case blockListOrigFile:
			b.tail.origFileLink.next = s
		case blockListSlabFile:
			b.tail.slabFileLink.next = s
		}
	}
	b.tail = s
}

func (b *blockLinkedList) remove(s *secondaryCacheValue, listType blockListType) {
	var leftNode, rightNode *secondaryCacheValue
	switch listType {
	case blockListOrigFile:
		leftNode = s.origFileLink.prev
		rightNode = s.origFileLink.next
		s.origFileLink.prev = nil
		s.origFileLink.next = nil
	case blockListSlabFile:
		leftNode = s.slabFileLink.prev
		rightNode = s.slabFileLink.next
		s.slabFileLink.prev = nil
		s.slabFileLink.next = nil
	}

	if leftNode == nil {
		b.head = rightNode
	} else {
		switch listType {
		case blockListOrigFile:
			leftNode.origFileLink.next = rightNode
		case blockListSlabFile:
			leftNode.slabFileLink.next = rightNode
		}
	}
	if rightNode == nil {
		b.tail = leftNode
	} else {
		switch listType {
		case blockListOrigFile:
			rightNode.origFileLink.prev = leftNode
		case blockListSlabFile:
			rightNode.slabFileLink.prev = leftNode
		}
	}
}

const secondaryCacheHeaderSize = 5 * 8 // 5 uint64s
const targetSlabFileSize = 16 << 20    // 16MB

type secondaryCache struct {
	mu struct {
		sync.Mutex

		blocks       map[blockKey]*secondaryCacheValue
		files        map[int]*secondaryCacheFile
		origFiles    map[fileKey]*secondaryCacheOrigFile
		list         lruFileList
		usedCapacity uint64

		currentFileNum int
		currentFile    *secondaryCacheFile
	}

	capacity uint64
	wg       sync.WaitGroup
	cacheDir string
	fs       vfs.FSWithOpenForWrites
}

type secondaryCacheFile struct {
	name     int
	handle   vfs.RandomWriteFile
	writerMu struct {
		sync.Mutex
		usedSize uint64
	}

	// protected by secondaryCache.mu
	size       uint64
	prev, next *secondaryCacheFile
	blocks     blockLinkedList

	refs refcnt
}

func (f *secondaryCacheFile) writeBlock(
	val *secondaryCacheValue, block []byte, fs vfs.FSWithOpenForWrites, dir string,
) {
	f.writerMu.Lock()
	defer f.writerMu.Unlock()

	if f.handle == nil {
		var err error
		f.handle, err = fs.OpenForWrites(fs.PathJoin(dir, fmt.Sprintf("%d.slab", f.name)))
		vfs.Preallocate(f.handle, 0, targetSlabFileSize)
		if err != nil {
			panic(err.Error())
		}
	}

	buf := make([]byte, 0, secondaryCacheHeaderSize+len(block))
	val.offset = f.writerMu.usedSize + secondaryCacheHeaderSize
	buf = val.Encode(buf, block)
	n, err := f.handle.WriteAt(buf, int64(f.writerMu.usedSize))
	if err != nil {
		panic(err.Error())
	}
	f.writerMu.usedSize += uint64(n)
	return
}

type secondaryCacheOrigFile struct {
	filenum base.FileNum
	blocks  blockLinkedList
}

type secondaryCacheValue struct {
	atomic struct {
		ready int64
	}
	cacheID    uint64
	origFile   base.FileNum
	origOffset uint64
	cacheFile  *secondaryCacheFile
	offset     uint64
	size       uint64
	unused     uint64

	// Protected by secondaryCache.mu.
	origFileLink blockListLinks
	slabFileLink blockListLinks
}

func (s *secondaryCacheValue) Encode(dst []byte, blockData []byte) []byte {
	newCap := cap(dst)
	if newCap == 0 {
		newCap = 1
	}
	for newCap < len(dst)+secondaryCacheHeaderSize+len(blockData) {
		newCap *= 2
	}
	if newCap > cap(dst) {
		oldDst := dst
		dst = make([]byte, len(dst), newCap)
		copy(dst, oldDst)
	}
	origLen := len(dst)
	dst = dst[:len(dst)+secondaryCacheHeaderSize+len(blockData)]
	binary.LittleEndian.PutUint64(dst[origLen:], s.cacheID)
	binary.LittleEndian.PutUint64(dst[origLen+8:], uint64(s.origFile))
	binary.LittleEndian.PutUint64(dst[origLen+16:], s.origOffset)
	binary.LittleEndian.PutUint64(dst[origLen+24:], s.size)
	binary.LittleEndian.PutUint64(dst[origLen+32:], s.unused)
	copy(dst[origLen+secondaryCacheHeaderSize:], blockData)
	return dst
}

func (s *secondaryCacheValue) Decode(src []byte) (rem []byte, block []byte, err error) {
	if len(src) < secondaryCacheHeaderSize {
		return nil, nil, errors.New("source slice too small")
	}
	s.cacheID = binary.LittleEndian.Uint64(src[:8])
	s.origFile = base.FileNum(binary.LittleEndian.Uint64(src[8:16]))
	s.origOffset = binary.LittleEndian.Uint64(src[16:24])
	s.size = binary.LittleEndian.Uint64(src[24:32])
	s.unused = binary.LittleEndian.Uint64(src[24:32])
	block = src[secondaryCacheHeaderSize : secondaryCacheHeaderSize+s.size]
	return src[secondaryCacheHeaderSize+s.size+s.unused:], block, nil
}

func NewPersistentCache(dir string, fs vfs.FSWithOpenForWrites, capacity uint64) SecondaryCache {
	psc := &secondaryCache{
		capacity: capacity,
		cacheDir: dir,
		fs:       fs,
	}
	psc.mu.files = make(map[int]*secondaryCacheFile)
	psc.mu.origFiles = make(map[fileKey]*secondaryCacheOrigFile)
	psc.mu.blocks = make(map[blockKey]*secondaryCacheValue)
	return psc
}

func (l *secondaryCache) GetAndEvict(id uint64, fileNum base.FileNum, offset uint64) []byte {
	key := blockKey{fileKey{id, fileNum}, offset}
	l.mu.Lock()
	val, ok := l.mu.blocks[key]
	if !ok {
		l.mu.Unlock()
		return nil
	}
	// TODO(bilal): Add block to eviction queue.
	f := val.cacheFile
	f.refs.acquire()
	defer f.refs.release()
	l.mu.list.moveToFront(f)
	l.mu.Unlock()

	buf := make([]byte, val.size)
	for atomic.LoadInt64(&val.atomic.ready) == 0 {
		// spin
	}
	n, err := f.handle.ReadAt(buf, int64(val.offset))
	if err != nil || n != int(val.size) {
		return nil
	}

	return buf
}

// mu must be held while calling this function.
func (l *secondaryCache) rotateFile() {
	l.mu.currentFileNum++
	l.mu.currentFile = &secondaryCacheFile{
		name:   l.mu.currentFileNum,
		handle: nil,
		size:   0,
		prev:   nil,
		next:   nil,
		blocks: blockLinkedList{},
	}
	l.mu.currentFile.refs.init(1)
	l.mu.list.add(l.mu.currentFile)
}

// Set pseudocode:
// 1) Check if file is not deleted, and has UsesSharedFS = true. If yes, file
//    blocks are eligible for secondary cache.
//    1.5) Check if block is already in the cache (possible if it was promoted
//         to primary cache but not evicted from secondary yet). If it is, take
//         it out of block eviction queue.
// 2) If cache usage is below capacity (fast path), append to end of current
//    slab file. If current slab file has no room, preallocate new slab file.
// 3) If cache usage is above capacity, we have two options:
//    a) Look at head of block eviction queue. If size of that block is higher
//       than that of new block, replace that block with this block.
//    b) Failing that, delete least recently accessed slab file, remove relevant
//       entries from it from block eviction queue (this can happen async),
//       and do 2.
func (l *secondaryCache) Set(id uint64, fileNum base.FileNum, offset uint64, block []byte) {
	key := blockKey{fileKey{id, fileNum}, offset}
	l.mu.Lock()
	val, ok := l.mu.blocks[key]
	if ok && val != nil {
		l.mu.Unlock()
		return
	}
	// TODO(bilal): Once block eviction queue is in, check if val is in it, and if
	// it is, take it out of it.

	l.mu.usedCapacity += uint64(len(block) + secondaryCacheHeaderSize)
	filesToEvict := make([]*secondaryCacheFile, 0, 1)
	for l.mu.usedCapacity > l.capacity {
		f := l.mu.list.evict()
		l.mu.usedCapacity -= f.size
		for ptr := f.blocks.head; ptr != nil; ptr = ptr.slabFileLink.next {
			fk := fileKey{ptr.cacheID, ptr.origFile}
			delete(l.mu.blocks, blockKey{fk, ptr.origOffset})
			l.mu.origFiles[fk].blocks.remove(ptr, blockListOrigFile)
		}
		delete(l.mu.files, f.name)
		filesToEvict = append(filesToEvict, f)
	}
	if l.mu.currentFile == nil || l.mu.currentFile.size > targetSlabFileSize {
		l.rotateFile()
	}
	currentFile := l.mu.currentFile
	currentFile.refs.acquire()
	currentFile.size += uint64(secondaryCacheHeaderSize + len(block))

	val = &secondaryCacheValue{
		cacheID:    id,
		origFile:   fileNum,
		origOffset: offset,
		cacheFile:  currentFile,
		size:       uint64(len(block)),
	}
	l.mu.blocks[key] = val
	currentFile.blocks.add(val, blockListSlabFile)
	origFile := l.mu.origFiles[key.fileKey]
	if origFile == nil {
		origFile = &secondaryCacheOrigFile{
			filenum: fileNum,
			blocks:  blockLinkedList{},
		}
		l.mu.origFiles[key.fileKey] = origFile
	}
	origFile.blocks.add(val, blockListOrigFile)
	l.mu.Unlock()

	currentFile.writeBlock(val, block, l.fs, l.cacheDir)
	atomic.StoreInt64(&val.atomic.ready, 1)
	currentFile.refs.release()

	// Delete evicted files.
	for _, f := range filesToEvict {
		refsClosed := f.refs.release()
		for !refsClosed && f.refs.refs() != 0 {
			// spin
		}
		f.handle.Close()
		l.fs.Remove(l.fs.PathJoin(l.cacheDir, fmt.Sprintf("%d.slab", f.name)))
	}
}

func (l *secondaryCache) DeleteFile(id uint64, fileNum base.FileNum) {
	// TODO(bilal) Once block eviction queue is in, add these blocks there instead.
	l.mu.Lock()
	defer l.mu.Unlock()
	fk := fileKey{id, fileNum}
	origFile, ok := l.mu.origFiles[fk]
	if !ok {
		return
	}

	for ptr := origFile.blocks.head; ptr != nil; ptr = ptr.origFileLink.next {
		ptr.cacheFile.blocks.remove(ptr, blockListSlabFile)
		delete(l.mu.blocks, blockKey{fk, ptr.origOffset})
	}
	delete(l.mu.origFiles, fk)
}
