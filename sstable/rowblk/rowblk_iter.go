// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rowblk

import (
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"unsafe"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manual"
)

// RawIter is an iterator over a single block of data. Unlike blockIter,
// keys are stored in "raw" format (i.e. not as internal keys). Note that there
// is significant similarity between this code and the code in blockIter. Yet
// reducing duplication is difficult due to the blockIter being performance
// critical. RawIter must only be used for blocks where the value is
// stored together with the key.
type RawIter struct {
	cmp         base.Compare
	offset      int32
	nextOffset  int32
	restarts    int32
	numRestarts int32
	ptr         unsafe.Pointer
	data        []byte
	key, val    []byte
	ikey        base.InternalKey
	cached      []blockEntry
	cachedBuf   []byte
}

type blockEntry struct {
	offset   int32
	keyStart int32
	keyEnd   int32
	valStart int32
	valSize  int32
}

// NewRawIter constructs a new raw block iterator.
func NewRawIter(cmp base.Compare, block []byte) (*RawIter, error) {
	i := &RawIter{}
	return i, i.Init(cmp, block)
}

// Init initializes the raw block iterator.
func (i *RawIter) Init(cmp base.Compare, blk []byte) error {
	numRestarts := int32(binary.LittleEndian.Uint32(blk[len(blk)-4:]))
	if numRestarts == 0 {
		return base.CorruptionErrorf("pebble/table: invalid table (block has no restart points)")
	}
	i.cmp = cmp
	i.restarts = int32(len(blk)) - 4*(1+numRestarts)
	i.numRestarts = numRestarts
	i.ptr = unsafe.Pointer(&blk[0])
	i.data = blk
	if i.key == nil {
		i.key = make([]byte, 0, 256)
	} else {
		i.key = i.key[:0]
	}
	i.val = nil
	i.clearCache()
	return nil
}

func (i *RawIter) readEntry() {
	ptr := unsafe.Pointer(uintptr(i.ptr) + uintptr(i.offset))
	shared, ptr := decodeVarint(ptr)
	unshared, ptr := decodeVarint(ptr)
	value, ptr := decodeVarint(ptr)
	i.key = append(i.key[:shared], getBytes(ptr, int(unshared))...)
	i.key = i.key[:len(i.key):len(i.key)]
	ptr = unsafe.Pointer(uintptr(ptr) + uintptr(unshared))
	i.val = getBytes(ptr, int(value))
	i.nextOffset = int32(uintptr(ptr)-uintptr(i.ptr)) + int32(value)
}

func (i *RawIter) loadEntry() {
	i.readEntry()
	i.ikey.UserKey = i.key
}

func (i *RawIter) clearCache() {
	i.cached = i.cached[:0]
	i.cachedBuf = i.cachedBuf[:0]
}

func (i *RawIter) cacheEntry() {
	var valStart int32
	valSize := int32(len(i.val))
	if valSize > 0 {
		valStart = int32(uintptr(unsafe.Pointer(&i.val[0])) - uintptr(i.ptr))
	}

	i.cached = append(i.cached, blockEntry{
		offset:   i.offset,
		keyStart: int32(len(i.cachedBuf)),
		keyEnd:   int32(len(i.cachedBuf) + len(i.key)),
		valStart: valStart,
		valSize:  valSize,
	})
	i.cachedBuf = append(i.cachedBuf, i.key...)
}

// SeekGE implements internalIterator.SeekGE, as documented in the pebble
// package.
func (i *RawIter) SeekGE(key []byte) bool {
	// Find the index of the smallest restart point whose key is > the key
	// sought; index will be numRestarts if there is no such restart point.
	i.offset = 0
	index := sort.Search(int(i.numRestarts), func(j int) bool {
		offset := int32(binary.LittleEndian.Uint32(i.data[int(i.restarts)+4*j:]))
		// For a restart point, there are 0 bytes shared with the previous key.
		// The varint encoding of 0 occupies 1 byte.
		ptr := unsafe.Pointer(uintptr(i.ptr) + uintptr(offset+1))
		// Decode the key at that restart point, and compare it to the key sought.
		v1, ptr := decodeVarint(ptr)
		_, ptr = decodeVarint(ptr)
		s := getBytes(ptr, int(v1))
		return i.cmp(key, s) < 0
	})

	// Since keys are strictly increasing, if index > 0 then the restart point at
	// index-1 will be the largest whose key is <= the key sought.  If index ==
	// 0, then all keys in this block are larger than the key sought, and offset
	// remains at zero.
	if index > 0 {
		i.offset = int32(binary.LittleEndian.Uint32(i.data[int(i.restarts)+4*(index-1):]))
	}
	i.loadEntry()

	// Iterate from that restart point to somewhere >= the key sought.
	for valid := i.Valid(); valid; valid = i.Next() {
		if i.cmp(key, i.key) <= 0 {
			break
		}
	}
	return i.Valid()
}

// First implements internalIterator.First, as documented in the pebble
// package.
func (i *RawIter) First() bool {
	i.offset = 0
	i.loadEntry()
	return i.Valid()
}

// Last implements internalIterator.Last, as documented in the pebble package.
func (i *RawIter) Last() bool {
	// Seek forward from the last restart point.
	i.offset = int32(binary.LittleEndian.Uint32(i.data[i.restarts+4*(i.numRestarts-1):]))

	i.readEntry()
	i.clearCache()
	i.cacheEntry()

	for i.nextOffset < i.restarts {
		i.offset = i.nextOffset
		i.readEntry()
		i.cacheEntry()
	}

	i.ikey.UserKey = i.key
	return i.Valid()
}

// Next implements internalIterator.Next, as documented in the pebble
// package.
func (i *RawIter) Next() bool {
	i.offset = i.nextOffset
	if !i.Valid() {
		return false
	}
	i.loadEntry()
	return true
}

// Prev implements internalIterator.Prev, as documented in the pebble
// package.
func (i *RawIter) Prev() bool {
	if n := len(i.cached) - 1; n > 0 && i.cached[n].offset == i.offset {
		i.nextOffset = i.offset
		e := &i.cached[n-1]
		i.offset = e.offset
		i.val = getBytes(unsafe.Pointer(uintptr(i.ptr)+uintptr(e.valStart)), int(e.valSize))
		i.ikey.UserKey = i.cachedBuf[e.keyStart:e.keyEnd]
		i.cached = i.cached[:n]
		return true
	}

	if i.offset == 0 {
		i.offset = -1
		i.nextOffset = 0
		return false
	}

	targetOffset := i.offset
	index := sort.Search(int(i.numRestarts), func(j int) bool {
		offset := int32(binary.LittleEndian.Uint32(i.data[int(i.restarts)+4*j:]))
		return offset >= targetOffset
	})
	i.offset = 0
	if index > 0 {
		i.offset = int32(binary.LittleEndian.Uint32(i.data[int(i.restarts)+4*(index-1):]))
	}

	i.readEntry()
	i.clearCache()
	i.cacheEntry()

	for i.nextOffset < targetOffset {
		i.offset = i.nextOffset
		i.readEntry()
		i.cacheEntry()
	}

	i.ikey.UserKey = i.key
	return true
}

// Key implements internalIterator.Key, as documented in the pebble package.
func (i *RawIter) Key() base.InternalKey {
	return i.ikey
}

// Value implements internalIterator.Value, as documented in the pebble
// package.
func (i *RawIter) Value() []byte {
	return i.val
}

// Valid implements internalIterator.Valid, as documented in the pebble
// package.
func (i *RawIter) Valid() bool {
	return i.offset >= 0 && i.offset < i.restarts
}

// Error implements internalIterator.Error, as documented in the pebble
// package.
func (i *RawIter) Error() error {
	return nil
}

// Close implements internalIterator.Close, as documented in the pebble
// package.
func (i *RawIter) Close() error {
	i.val = nil
	return nil
}

func (i *RawIter) getRestart(idx int) int32 {
	return int32(binary.LittleEndian.Uint32(i.data[i.restarts+4*int32(idx):]))
}

func (i *RawIter) isRestartPoint() bool {
	j := sort.Search(int(i.numRestarts), func(j int) bool {
		return i.getRestart(j) >= i.offset
	})
	return j < int(i.numRestarts) && i.getRestart(j) == i.offset
}

// DescribeKV is a function that formats a key-value pair, writing the
// description to w.
type DescribeKV func(w io.Writer, key *base.InternalKey, val []byte, enc KVEncoding)

// KVEncoding describes the encoding of a key-value pair within the block.
type KVEncoding struct {
	// Offset is the position within the block at which the key-value pair is
	// encoded.
	Offset int32
	// Length is the total length of the KV pair as it is encoded in the block
	// format.
	Length int32
}

// DescribeRaw describes the contents of a block, writing the description to w.
// It invokes fmtKV to describe each key-value pair.
func DescribeRaw(w io.Writer, it *RawIter, blkOffset uint64, fmtKV DescribeKV) {
	for valid := it.First(); valid; valid = it.Next() {
		enc := KVEncoding{
			Offset: it.offset,
			Length: int32(it.nextOffset - it.offset),
		}
		fmtKV(w, &it.ikey, it.val, enc)
		if it.isRestartPoint() {
			fmt.Fprintf(w, " [restart]\n")
		} else {
			fmt.Fprintf(w, "\n")
		}
	}
	// Format the restart points.
	for j := 0; j < int(it.numRestarts); j++ {
		offset := it.getRestart(j)
		// TODO(jackson): This formatting seems bizarre. We're taking blkOffset
		// which is an offset in the physical, compressed file, and adding the
		// offset of the KV pair within the uncompressed block. We should just
		// print offsets relative to the block start.
		fmt.Fprintf(w, "%10d    [restart %d]\n",
			blkOffset+uint64(it.restarts+4*int32(j)), blkOffset+uint64(offset))
	}
}

func getBytes(ptr unsafe.Pointer, length int) []byte {
	return (*[manual.MaxArrayLen]byte)(ptr)[:length:length]
}

func decodeVarint(ptr unsafe.Pointer) (uint32, unsafe.Pointer) {
	if a := *((*uint8)(ptr)); a < 128 {
		return uint32(a),
			unsafe.Pointer(uintptr(ptr) + 1)
	} else if a, b := a&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 1))); b < 128 {
		return uint32(b)<<7 | uint32(a),
			unsafe.Pointer(uintptr(ptr) + 2)
	} else if b, c := b&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 2))); c < 128 {
		return uint32(c)<<14 | uint32(b)<<7 | uint32(a),
			unsafe.Pointer(uintptr(ptr) + 3)
	} else if c, d := c&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 3))); d < 128 {
		return uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a),
			unsafe.Pointer(uintptr(ptr) + 4)
	} else {
		d, e := d&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 4)))
		return uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a),
			unsafe.Pointer(uintptr(ptr) + 5)
	}
}
