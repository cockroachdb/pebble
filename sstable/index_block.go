// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"encoding/binary"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/invariants"
)

const indexBlockTrailer = (base.InternalKeySeqNumMax << 8) | uint64(base.InternalKeyKindSeparator)

var encodedIndexBlockTrailer = func() []byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], indexBlockTrailer)
	return buf[:]
}()

type indexBlockFormat int8

const (
	indexBlockFormatDefault indexBlockFormat = iota
	indexBlockFormatCondensed
)

type indexBlockWriter struct {
	buf      []byte
	restarts []uint32
	curKey   base.InternalKey
	tmp      [4]byte
	format   indexBlockFormat
}

func (w *indexBlockWriter) numEntries() int {
	return len(w.restarts)
}

func (w *indexBlockWriter) clear() {
	*w = indexBlockWriter{
		buf:      w.buf[:0],
		restarts: w.restarts[:0],
		curKey:   InternalKey{UserKey: w.curKey.UserKey[:0]},
	}
}

func (w *indexBlockWriter) add(ik base.InternalKey, value []byte) {
	if cap(w.curKey.UserKey) < len(ik.UserKey) {
		w.curKey.UserKey = make([]byte, 0, len(ik.UserKey)*2)
	}
	w.curKey.UserKey = append(w.curKey.UserKey[:0], ik.UserKey...)
	w.curKey.Trailer = ik.Trailer
	w.restarts = append(w.restarts, uint32(len(w.buf)))

	// +1 varint for the shared key len
	// +1 varint for the unshared key len
	// +1 varint for the value len
	// TODO(jackson): This is an overestimate for the indexBlockFormatCondensed
	// format, which never writes the shared key len or the trailer. The 12
	// extra bytes should be harmless, but if we drop support for
	// indexBlockFormatDefault we can reduce this.
	needed := 3*binary.MaxVarintLen32 + len(w.curKey.UserKey) + len(value) + base.InternalTrailerLen

	n := len(w.buf)
	if cap(w.buf) < n+needed {
		newCap := 2 * cap(w.buf)
		if newCap == 0 {
			newCap = 1024
		}
		for newCap < n+needed {
			newCap *= 2
		}
		newBuf := make([]byte, n, newCap)
		copy(newBuf, w.buf)
		w.buf = newBuf
	}
	w.buf = w.buf[:n+needed]

	// TODO(peter): Manually inlined versions of binary.PutUvarint(). This is 15%
	// faster on BenchmarkWriter on go1.13. Remove if go1.14 or future versions
	// show this to not be a performance win.
	switch w.format {
	case indexBlockFormatDefault:
		// Write the count of shared keys, which is always zero. The varint
		// encoding of zero is a single zero byte.
		w.buf[n] = 0
		n++

		internalKeySize := ik.Size()

		{
			x := uint32(internalKeySize)
			for x >= 0x80 {
				w.buf[n] = byte(x) | 0x80
				x >>= 7
				n++
			}
			w.buf[n] = byte(x)
			n++
		}
		{
			x := uint32(len(value))
			for x >= 0x80 {
				w.buf[n] = byte(x) | 0x80
				x >>= 7
				n++
			}
			w.buf[n] = byte(x)
			n++
		}
		ik.Encode(w.buf[n:])
		n += internalKeySize
		n += copy(w.buf[n:], value)
	case indexBlockFormatCondensed:
		// uvarint(len(<userkey>)) <userkey> uvarint(len(<value>)) <value>
		{
			x := uint32(len(w.curKey.UserKey))
			for x >= 0x80 {
				w.buf[n] = byte(x) | 0x80
				x >>= 7
				n++
			}
			w.buf[n] = byte(x)
			n++
		}
		n += copy(w.buf[n:], w.curKey.UserKey)
		{
			x := uint32(len(value))
			for x >= 0x80 {
				w.buf[n] = byte(x) | 0x80
				x >>= 7
				n++
			}
			w.buf[n] = byte(x)
			n++
		}
		n += copy(w.buf[n:], value)
	default:
		panic("unknown index block format")
	}
	w.buf = w.buf[:n]
}

func (w *indexBlockWriter) finish() []byte {
	// Write the restart points to the buffer.
	tmp4 := w.tmp[:4]
	for _, x := range w.restarts {
		binary.LittleEndian.PutUint32(tmp4, x)
		w.buf = append(w.buf, tmp4...)
	}
	binary.LittleEndian.PutUint32(tmp4, uint32(len(w.restarts)))
	w.buf = append(w.buf, tmp4...)
	result := w.buf

	// Reset the block state.
	w.buf = w.buf[:0]
	w.restarts = w.restarts[:0]
	return result
}

func (w *indexBlockWriter) estimatedSize() int {
	return len(w.buf) + 4*len(w.restarts) + emptyBlockSize
}

func shouldFlushIndexBlock(
	key InternalKey,
	valueLen int,
	estimatedBlockSize, numEntries, targetBlockSize, sizeThreshold int,
	format indexBlockFormat,
) bool {
	if numEntries == 0 {
		return false
	}

	if estimatedBlockSize >= targetBlockSize {
		return true
	}

	// The block is currently smaller than the target size.
	if estimatedBlockSize <= sizeThreshold {
		// The block is smaller than the threshold size at which we'll consider
		// flushing it.
		return false
	}

	newSize := estimatedBlockSize + len(key.UserKey) + valueLen
	newSize += 4                            // uint32 restart point
	newSize += uvarintLen(uint32(valueLen)) // varint for value size
	switch format {
	case indexBlockFormatDefault:
		newSize += 4                              // varint for shared prefix length
		newSize += base.InternalTrailerLen        // InternalKey trailer
		newSize += uvarintLen(uint32(key.Size())) // varint for unshared key bytes
	case indexBlockFormatCondensed:
		newSize += uvarintLen(uint32(key.Size())) // varint for unshared key bytes
		// The condensed index block format does not have trailers or
		// shared-prefixes.
	default:
		panic("unreachable")
	}
	// Flush if the block plus the new entry is larger than the target size.
	return newSize > targetBlockSize
}

// indexBlockIter is an iterator over a single index block.
//
// An indexBlockIter always provides a guarantee of stable keys, because index
// blocks are always written with a start interval of 1.
type indexBlockIter struct {
	// offset is the byte index that marks where the current key/value is
	// encoded in the block.
	offset int32
	// The index [0, `numRestarts`] of the key at the current `offset`.
	currRestartIndex int32
	// nextOffset is the byte index where the next key/value is encoded in the
	// block.
	nextOffset int32
	// A "restart point" in a block is an offset where a key may be found. Index
	// blocks never use prefix compression, so every key within an index block
	// is a restart point and encodes the entirety of its key.
	//
	// All restart offsets are listed in increasing order in
	// i.ptr[i.restarts:len(block)-4], while numRestarts is encoded in the last
	// 4 bytes of the block as a uint32 (i.ptr[len(block)-4:]). i.restarts can
	// therefore be seen as the point where data in the block ends, and a list
	// of offsets of all restart points begins.
	restarts int32
	// Number of restart points in this block. Encoded at the end of the block
	// as a uint32.
	numRestarts int32
	ptr         unsafe.Pointer
	data        []byte
	cmp         Compare
	// key contains the raw key the iterator is currently pointed at. If
	// non-nil, this points to a slice of the block data.
	key []byte
	// val contains the value the iterator is currently pointed at. If non-nil,
	// this points to a slice of the block data.
	val         []byte
	cacheHandle cache.Handle
	readEntry   func()
	seekGE      func([]byte) []byte
}

func newIndexBlockIter(cmp Compare, block block, format indexBlockFormat) (*indexBlockIter, error) {
	i := &indexBlockIter{}
	return i, i.init(cmp, block, format)
}

func (i *indexBlockIter) init(cmp Compare, block block, format indexBlockFormat) error {
	numRestarts := int32(binary.LittleEndian.Uint32(block[len(block)-4:]))
	if numRestarts == 0 {
		return base.CorruptionErrorf("pebble/table: invalid table (block has no restart points)")
	}
	i.cmp = cmp
	i.restarts = int32(len(block)) - 4*(1+numRestarts)
	i.numRestarts = numRestarts
	i.ptr = unsafe.Pointer(&block[0])
	i.data = block
	i.val = nil
	if format == indexBlockFormatDefault {
		i.readEntry = i.readEntryDefault
		i.seekGE = i.seekGEDefault
	} else {
		i.readEntry = i.readEntryCondensed
		i.seekGE = i.seekGECondensed
	}
	return nil
}

func (i *indexBlockIter) initHandle(
	cmp Compare, block cache.Handle, format indexBlockFormat,
) error {
	i.cacheHandle.Release()
	i.cacheHandle = block
	return i.init(cmp, block.Get(), format)
}

func (i *indexBlockIter) invalidate() {
	*i = indexBlockIter{}
}

// isDataInvalidated returns true when the blockIter has been invalidated
// using an invalidate call. NB: this is different from blockIter.Valid
// which is part of the InternalIterator implementation.
func (i *indexBlockIter) isDataInvalidated() bool {
	return i.data == nil
}

func (i *indexBlockIter) resetForReuse() indexBlockIter {
	return indexBlockIter{}
}

func (i *indexBlockIter) readEntryDefault() {
	// NB: The +1 is skipping the uvarint encoding the shared prefix length.
	// Index blocks always use a restart interval of 1, so the shared prefix
	// length must be zero.
	ptr := unsafe.Pointer(uintptr(i.ptr) + uintptr(i.offset) + 1)
	if invariants.Enabled && *((*uint8)(unsafe.Pointer(uintptr(ptr) - 1))) != 0x00 {
		panic(errors.AssertionFailedf("index block entry uses prefix compression"))
	}

	// This is an ugly performance hack. Reading entries from blocks is one of
	// the inner-most routines and decoding the varints per-entry takes
	// significant time. Neither go1.11 or go1.12 will inline decodeVarint for
	// us, so we do it manually. This provides a 10-15% performance improvement
	// on blockIter benchmarks on both go1.11 and go1.12.
	//
	// TODO(peter): remove this hack if go:inline is ever supported.

	var unshared uint32
	if a := *((*uint8)(ptr)); a < 128 {
		unshared = uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 1)
	} else if a, b := a&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 1))); b < 128 {
		unshared = uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 2)
	} else if b, c := b&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 2))); c < 128 {
		unshared = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 3)
	} else if c, d := c&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 3))); d < 128 {
		unshared = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 4)
	} else {
		d, e := d&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 4)))
		unshared = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 5)
	}

	var value uint32
	if a := *((*uint8)(ptr)); a < 128 {
		value = uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 1)
	} else if a, b := a&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 1))); b < 128 {
		value = uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 2)
	} else if b, c := b&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 2))); c < 128 {
		value = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 3)
	} else if c, d := c&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 3))); d < 128 {
		value = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 4)
	} else {
		d, e := d&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 4)))
		value = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 5)
	}

	i.key = getBytes(ptr, int(unshared)-8)
	ptr = unsafe.Pointer(uintptr(ptr) + uintptr(unshared))
	i.val = getBytes(ptr, int(value))
	i.nextOffset = int32(uintptr(ptr)-uintptr(i.ptr)) + int32(value)
}

func (i *indexBlockIter) readEntryCondensed() {
	ptr := unsafe.Pointer(uintptr(i.ptr) + uintptr(i.offset))

	// This is an ugly performance hack. Reading entries from blocks is one of
	// the inner-most routines and decoding the varints per-entry takes
	// significant time. Neither go1.11 or go1.12 will inline decodeVarint for
	// us, so we do it manually. This provides a 10-15% performance improvement
	// on blockIter benchmarks on both go1.11 and go1.12.
	//
	// TODO(peter): remove this hack if go:inline is ever supported.

	var unshared uint32
	if a := *((*uint8)(ptr)); a < 128 {
		unshared = uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 1)
	} else if a, b := a&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 1))); b < 128 {
		unshared = uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 2)
	} else if b, c := b&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 2))); c < 128 {
		unshared = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 3)
	} else if c, d := c&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 3))); d < 128 {
		unshared = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 4)
	} else {
		d, e := d&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 4)))
		unshared = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 5)
	}
	i.key = getBytes(ptr, int(unshared))
	ptr = unsafe.Pointer(uintptr(ptr) + uintptr(unshared))

	var value uint32
	if a := *((*uint8)(ptr)); a < 128 {
		value = uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 1)
	} else if a, b := a&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 1))); b < 128 {
		value = uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 2)
	} else if b, c := b&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 2))); c < 128 {
		value = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 3)
	} else if c, d := c&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 3))); d < 128 {
		value = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 4)
	} else {
		d, e := d&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 4)))
		value = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 5)
	}
	i.val = getBytes(ptr, int(value))
	i.nextOffset = int32(uintptr(ptr)-uintptr(i.ptr)) + int32(value)
}

// seekGE seeks the iterator to the first entry containing a key >= `key`.
func (i *indexBlockIter) seekGEDefault(key []byte) (entryKey []byte) {
	// Find the index of the smallest restart point whose key is > the key
	// sought; index will be numRestarts if there is no such restart point.
	i.offset = 0
	i.currRestartIndex = 0
	var index int32

	{
		// NB: manually inlined sort.Seach is ~5% faster.
		//
		// Define f(-1) == false and f(n) == true.
		// Invariant: f(index-1) == false, f(upper) == true.
		upper := i.numRestarts
		for index < upper {
			h := int32(uint(index+upper) >> 1) // avoid overflow when computing h
			// index ≤ h < upper
			offset := decodeIndexBlockRestart(i.data[i.restarts+4*h:])
			// For a restart point, there are 0 bytes shared with the previous key.
			// The varint encoding of 0 occupies 1 byte.
			ptr := unsafe.Pointer(uintptr(i.ptr) + uintptr(offset+1))

			// Decode the key at that restart point, and compare it to the key
			// sought. See the comment in readEntry for why we manually inline the
			// varint decoding.
			var v1 uint32
			if a := *((*uint8)(ptr)); a < 128 {
				v1 = uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 1)
			} else if a, b := a&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 1))); b < 128 {
				v1 = uint32(b)<<7 | uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 2)
			} else if b, c := b&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 2))); c < 128 {
				v1 = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 3)
			} else if c, d := c&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 3))); d < 128 {
				v1 = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 4)
			} else {
				d, e := d&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 4)))
				v1 = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 5)
			}

			if *((*uint8)(ptr)) < 128 {
				ptr = unsafe.Pointer(uintptr(ptr) + 1)
			} else if *((*uint8)(unsafe.Pointer(uintptr(ptr) + 1))) < 128 {
				ptr = unsafe.Pointer(uintptr(ptr) + 2)
			} else if *((*uint8)(unsafe.Pointer(uintptr(ptr) + 2))) < 128 {
				ptr = unsafe.Pointer(uintptr(ptr) + 3)
			} else if *((*uint8)(unsafe.Pointer(uintptr(ptr) + 3))) < 128 {
				ptr = unsafe.Pointer(uintptr(ptr) + 4)
			} else {
				ptr = unsafe.Pointer(uintptr(ptr) + 5)
			}

			// Manually inlining part of base.DecodeInternalKey provides a 5-10%
			// speedup on BlockIter benchmarks.
			k := getBytes(ptr, int(v1-8))

			if i.cmp(key, k) > 0 {
				// The search key is greater than the user key at this restart point.
				// Search beyond this restart point, since we are trying to find the
				// first restart point with a user key >= the search key.
				index = h + 1 // preserves f(i-1) == false
			} else {
				// k >= search key, so prune everything after index (since index
				// satisfies the property we are looking for).
				upper = h // preserves f(j) == true
			}
		}
		// index == upper, f(index-1) == false, and f(upper) (= f(index)) == true
		// => answer is index.
	}

	// index is the first restart point with key >= search key. Define the keys
	// between a restart point and the next restart point as belonging to that
	// restart point.
	//
	// Since keys are strictly increasing, if index > 0 then the restart point
	// at index-1 will be the first one that has some keys belonging to it that
	// could be equal to the search key.  If index == 0, then all keys in this
	// block are larger than the key sought, and offset remains at zero.
	if index > 0 {
		i.currRestartIndex = index - 1
		i.offset = decodeIndexBlockRestart(i.data[i.restarts+4*(index-1):])
	}
	i.readEntry()

	// Iterate from that restart point to somewhere >= the key sought.
	if !i.valid() {
		return nil
	}
	if i.cmp(i.key, key) >= 0 {
		return i.key
	}
	for i.next(); i.valid(); i.next() {
		if i.cmp(i.key, key) >= 0 {
			return i.key
		}
	}
	return nil
}

// seekGECondensed seeks the iterator to the first entry containing a key >= `key`.
func (i *indexBlockIter) seekGECondensed(key []byte) (entryKey []byte) {
	// Find the index of the smallest restart point whose key is > the key
	// sought; index will be numRestarts if there is no such restart point.
	i.offset = 0
	i.currRestartIndex = 0
	var index int32

	{
		// NB: manually inlined sort.Seach is ~5% faster.
		//
		// Define f(-1) == false and f(n) == true.
		// Invariant: f(index-1) == false, f(upper) == true.
		upper := i.numRestarts
		for index < upper {
			h := int32(uint(index+upper) >> 1) // avoid overflow when computing h
			// index ≤ h < upper
			offset := decodeIndexBlockRestart(i.data[i.restarts+4*h:])
			ptr := unsafe.Pointer(uintptr(i.ptr) + uintptr(offset))

			// Decode the key at that restart point, and compare it to the key
			// sought. See the comment in readEntry for why we manually inline the
			// varint decoding.
			var v1 uint32
			if a := *((*uint8)(ptr)); a < 128 {
				v1 = uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 1)
			} else if a, b := a&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 1))); b < 128 {
				v1 = uint32(b)<<7 | uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 2)
			} else if b, c := b&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 2))); c < 128 {
				v1 = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 3)
			} else if c, d := c&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 3))); d < 128 {
				v1 = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 4)
			} else {
				d, e := d&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 4)))
				v1 = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 5)
			}

			if i.cmp(key, getBytes(ptr, int(v1))) > 0 {
				// The search key is greater than the user key at this restart point.
				// Search beyond this restart point, since we are trying to find the
				// first restart point with a user key >= the search key.
				index = h + 1 // preserves f(i-1) == false
			} else {
				// k >= search key, so prune everything after index (since index
				// satisfies the property we are looking for).
				upper = h // preserves f(j) == true
			}
		}
		// index == upper, f(index-1) == false, and f(upper) (= f(index)) == true
		// => answer is index.
	}

	// index is the first restart point with key >= search key. Define the keys
	// between a restart point and the next restart point as belonging to that
	// restart point.
	//
	// Since keys are strictly increasing, if index > 0 then the restart point
	// at index-1 will be the first one that has some keys belonging to it that
	// could be equal to the search key.  If index == 0, then all keys in this
	// block are larger than the key sought, and offset remains at zero.
	if index > 0 {
		i.currRestartIndex = index - 1
		i.offset = decodeIndexBlockRestart(i.data[i.restarts+4*(index-1):])
	}
	i.readEntry()

	// Iterate from that restart point to somewhere >= the key sought.
	if !i.valid() {
		return nil
	}
	if i.cmp(i.key, key) >= 0 {
		return i.key
	}
	for i.next(); i.valid(); i.next() {
		if i.cmp(i.key, key) >= 0 {
			return i.key
		}
	}
	return nil
}

func (i *indexBlockIter) first() (key []byte) {
	i.offset = 0
	i.currRestartIndex = 0
	if !i.valid() {
		return nil
	}
	i.readEntry()
	return i.key
}

func (i *indexBlockIter) last() (key []byte) {
	// Seek forward from the last restart point.
	i.currRestartIndex = i.numRestarts - 1
	i.offset = decodeIndexBlockRestart(i.data[i.restarts+4*(i.currRestartIndex):])
	if !i.valid() {
		return nil
	}
	i.readEntry()
	if invariants.Enabled && i.nextOffset < i.restarts {
		panic(errors.AssertionFailedf("pebble: index block's last restart offset is not the last key"))
	}
	return i.key
}

func (i *indexBlockIter) next() (key []byte) {
	i.offset = i.nextOffset
	i.currRestartIndex++
	if !i.valid() {
		return nil
	}
	i.readEntry()
	return i.key
}

func (i *indexBlockIter) prev() (key []byte) {
	i.currRestartIndex--
	if i.currRestartIndex < 0 || i.numRestarts == 0 {
		i.offset = -1
		i.nextOffset = 0
		return nil
	}
	i.offset = decodeIndexBlockRestart(i.data[i.restarts+4*(i.currRestartIndex):])
	i.readEntry()
	return i.key
}

func (i *indexBlockIter) peekPrev() (key []byte) {
	if i.currRestartIndex <= 0 {
		return nil
	}
	// Save our current position.
	currKey, currVal, nextOffset := i.key, i.val, i.nextOffset
	i.offset = decodeIndexBlockRestart(i.data[i.restarts+4*(i.currRestartIndex-1):])
	i.readEntry()
	key = i.key
	// Restore the position.
	i.key, i.val, i.nextOffset = currKey, currVal, nextOffset
	return key
}

func (i *indexBlockIter) Close() error {
	i.cacheHandle.Release()
	i.cacheHandle = cache.Handle{}
	i.val = nil
	return nil
}

func (i *indexBlockIter) valid() bool {
	return i.offset >= 0 && i.offset < i.restarts
}

// decodeIndexBlockRestart is equivalent to decodeRestart except it avoids
// masking the `restartMaskLittleEndianHighByteWithoutSetHasSamePrefix` bit
// which is never set within index blocks.
func decodeIndexBlockRestart(b []byte) int32 {
	_ = b[3] // bounds check hint to compiler; see golang.org/issue/14808
	return int32(uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24)
}
