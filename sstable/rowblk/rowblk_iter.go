// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rowblk

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"iter"
	"slices"
	"sort"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2/internal/base"
	"github.com/cockroachdb/pebble/v2/internal/invariants"
	"github.com/cockroachdb/pebble/v2/internal/treeprinter"
	"github.com/cockroachdb/pebble/v2/sstable/block"
)

// Iter is an iterator over a single block of data.
//
// An Iter provides an additional guarantee around key stability when a block
// has a restart interval of 1 (i.e. when there is no prefix compression). Key
// stability refers to whether the InternalKey.UserKey bytes returned by a
// positioning call will remain stable after a subsequent positioning call. The
// normal case is that a positioning call will invalidate any previously
// returned InternalKey.UserKey. If a block has a restart interval of 1 (no
// prefix compression), Iter guarantees that InternalKey.UserKey will point to
// the key as stored in the block itself which will remain valid until the Iter
// is closed. The key stability guarantee is used by the range tombstone and
// range key code, which knows that the respective blocks are always encoded
// with a restart interval of 1. This per-block key stability guarantee is
// sufficient for range tombstones and range deletes as they are always encoded
// in a single block. Note: this stability guarantee no longer holds for a block
// iter with synthetic prefix/suffix replacement, but we don't use the synthetic
// suffix/prefix functionality of Iter for range keys.
//
// An Iter also provides a value stability guarantee for range deletions and
// range keys since there is only a single range deletion and range key block
// per sstable and the Iter will not release the bytes for the block until it is
// closed.
//
// Note on why Iter knows about lazyValueHandling:
//
// Iter's positioning functions (that return a LazyValue), are too
// complex to inline even prior to lazyValueHandling. Iter.Next and
// Iter.First were by far the cheapest and had costs 195 and 180
// respectively, which exceeds the budget of 80. We initially tried to keep
// the lazyValueHandling logic out of Iter by wrapping it with a
// lazyValueDataBlockIter. singleLevelIter and twoLevelIter would use this
// wrapped iter. The functions in lazyValueDataBlockIter were simple, in that
// they called the corresponding Iter func and then decided whether the
// value was in fact in-place (so return immediately) or needed further
// handling. But these also turned out too costly for mid-stack inlining since
// simple calls like the following have a high cost that is barely under the
// budget of 80
//
//	k, v := i.data.SeekGE(key, flags)  // cost 74
//	k, v := i.data.Next()              // cost 72
//
// We have 2 options for minimizing performance regressions:
//   - Include the lazyValueHandling logic in the already non-inlineable
//     Iter functions: Since most of the time is spent in data block iters,
//     it is acceptable to take the small hit of unnecessary branching (which
//     hopefully branch prediction will predict correctly) for other kinds of
//     blocks.
//   - Duplicate the logic of singleLevelIterator and twoLevelIterator for the
//     v3 sstable and only use the aforementioned lazyValueDataBlockIter for a
//     v3 sstable. We would want to manage these copies via code generation.
//
// We have picked the first option here.
type Iter struct {
	cmp   base.Compare
	split base.Split

	// Iterator transforms.
	//
	// SyntheticSuffix, if set, will replace the decoded ikey.UserKey suffix
	// before the key is returned to the user. A sequence of iter operations on a
	// block with a syntheticSuffix rule should return keys as if those operations
	// ran on a block with keys that all had the syntheticSuffix. As an example:
	// any sequence of block iter cmds should return the same keys for the
	// following two blocks:
	//
	// blockA: a@3,b@3,c@3
	// blockB: a@1,b@2,c@1 with syntheticSuffix=3
	//
	// To ensure this, Suffix replacement will not change the ordering of keys in
	// the block because the iter assumes that no two keys in the block share the
	// same prefix. Furthermore, during SeekGE and SeekLT operations, the block
	// iterator handles "off by one" errors (explained in more detail in those
	// functions) when, for a given key, originalSuffix < searchSuffix <
	// replacementSuffix, with integer comparison. To handle these cases, the
	// iterator assumes:
	//
	//  pebble.Compare(keyPrefix{replacementSuffix},keyPrefix{originalSuffix}) < 0
	//  for keys with a suffix.
	//
	//  NB: it is possible for a block iter to add a synthetic suffix on a key
	//  without a suffix, which implies
	//  pebble.Compare(keyPrefix{replacementSuffix},keyPrefix{noSuffix}) > 0 ,
	//  however, the iterator would never need to handle an off by one error in
	//  this case since originalSuffix (empty) > searchSuffix (non empty), with
	//  integer comparison.
	//
	//
	// In addition, we also assume that any block with rangekeys will not contain
	// a synthetic suffix.
	transforms block.IterTransforms

	// offset is the byte index that marks where the current key/value is
	// encoded in the block.
	offset offsetInBlock
	// nextOffset is the byte index where the next key/value is encoded in the
	// block.
	nextOffset offsetInBlock
	// A "restart point" in a block is a point where the full key is encoded,
	// instead of just having a suffix of the key encoded. See readEntry() for
	// how prefix compression of keys works. Keys in between two restart points
	// only have a suffix encoded in the block. When restart interval is 1, no
	// prefix compression of keys happens. This is the case with range tombstone
	// blocks.
	//
	// All restart offsets are listed in increasing order in
	// i.ptr[i.restarts:len(block)-4], while numRestarts is encoded in the last
	// 4 bytes of the block as a uint32 (i.ptr[len(block)-4:]). i.restarts can
	// therefore be seen as the point where data in the block ends, and a list
	// of offsets of all restart points begins.
	//
	// int64 is used to prevent overflow and preserve signedness for binary
	// search invariants.
	restarts offsetInBlock
	// Number of restart points in this block. Encoded at the end of the block
	// as a uint32.
	numRestarts int32
	ptr         unsafe.Pointer
	data        []byte
	// key contains the raw key the iterator is currently pointed at. This may
	// point directly to data stored in the block (for a key which has no prefix
	// compression), to fullKey (for a prefix compressed key), or to a slice of
	// data stored in cachedBuf (during reverse iteration).
	//
	// NB: In general, key contains the same logical content as ikey
	// (i.e. ikey = decode(key)), but if the iterator contains a synthetic suffix
	// replacement rule, this will not be the case. Therefore, key should never
	// be used after ikey is set.
	key []byte
	// fullKey is a buffer used for key prefix decompression. Note that if
	// transforms.SyntheticPrifix is not nil, fullKey always starts with that
	// prefix.
	fullKey []byte
	// val contains the value the iterator is currently pointed at. If non-nil,
	// this points to a slice of the block data.
	val []byte
	// ikv contains the decoded internal KV the iterator is currently positioned
	// at.
	//
	// ikv.InternalKey contains the decoded InternalKey the iterator is
	// currently pointed at. Note that the memory backing ikv.UserKey is either
	// data stored directly in the block, fullKey, or cachedBuf. The key
	// stability guarantee for blocks built with a restart interval of 1 is
	// achieved by having ikv.UserKey always point to data stored directly in
	// the block.
	//
	// ikv.LazyValue is val turned into a LazyValue, whenever a positioning
	// method returns a non-nil key-value pair.
	ikv base.InternalKV
	// cached and cachedBuf are used during reverse iteration. They are needed
	// because we can't perform prefix decoding in reverse, only in the forward
	// direction. In order to iterate in reverse, we decode and cache the entries
	// between two restart points.
	//
	// Note that cached[len(cached)-1] contains the previous entry to the one the
	// blockIter is currently pointed at. As usual, nextOffset will contain the
	// offset of the next entry. During reverse iteration, nextOffset will be
	// updated to point to offset, and we'll set the blockIter to point at the
	// entry cached[len(cached)-1]. See Prev() for more details.
	//
	// For a block encoded with a restart interval of 1, cached and cachedBuf
	// will not be used as there are no prefix compressed entries between the
	// restart points.
	cached    []blockEntry
	cachedBuf []byte
	handle    block.BufferHandle
	// for block iteration for already loaded blocks.
	firstUserKey      []byte
	lazyValueHandling struct {
		getValue       block.GetInternalValueForPrefixAndValueHandler
		hasValuePrefix bool
	}
	synthSuffixBuf            []byte
	firstUserKeyWithPrefixBuf []byte
}

// offsetInBlock represents an offset in a block
//
// While restart points are serialized as uint32's, it is possible for offsets to
// be greater than math.MaxUint32 since they may point to an offset after the KVs.
//
// Previously, offsets were represented as int32, which causes problems with
// integer overflows while indexing into blocks (i.data) with large KVs in SeekGE()
// and SeekLT(). Using an int64 solves the problem of overflows as wraparounds will
// be prevented. Additionally, the signedness of int64 allows repsentation of
// iterators that have conducted backward interation and allows for binary search
// invariants in SeekGE() and SeekLT() to be preserved.
type offsetInBlock int64

type blockEntry struct {
	offset   offsetInBlock
	keyStart offsetInBlock
	keyEnd   offsetInBlock
	valStart offsetInBlock
	valSize  uint32
}

// *Iter implements the block.DataBlockIterator interface.
var _ block.DataBlockIterator = (*Iter)(nil)

// NewIter constructs a new row-oriented block iterator over the provided serialized block.
func NewIter(
	cmp base.Compare,
	suffixCmp base.ComparePointSuffixes,
	split base.Split,
	block []byte,
	transforms block.IterTransforms,
) (*Iter, error) {
	i := &Iter{}
	return i, i.Init(cmp, suffixCmp, split, block, transforms)
}

// String implements fmt.Stringer.
func (i *Iter) String() string {
	return "block"
}

// Init initializes the block iterator from the provided block.
func (i *Iter) Init(
	cmp base.Compare,
	suffixCmp base.ComparePointSuffixes,
	split base.Split,
	blk []byte,
	transforms block.IterTransforms,
) error {
	numRestarts := int32(binary.LittleEndian.Uint32(blk[len(blk)-4:]))
	if numRestarts == 0 {
		return base.CorruptionErrorf("pebble/table: invalid table (block has no restart points)")
	}
	i.transforms = transforms
	i.synthSuffixBuf = i.synthSuffixBuf[:0]
	i.split = split
	i.cmp = cmp
	i.restarts = offsetInBlock(len(blk)) - 4*(1+offsetInBlock(numRestarts))
	i.numRestarts = numRestarts
	i.ptr = unsafe.Pointer(&blk[0])
	i.data = blk
	if i.transforms.HasSyntheticPrefix() {
		i.fullKey = append(i.fullKey[:0], i.transforms.SyntheticPrefix()...)
	} else {
		i.fullKey = i.fullKey[:0]
	}
	i.val = nil
	i.clearCache()
	if i.restarts > 0 {
		if err := i.readFirstKey(); err != nil {
			return err
		}
	} else {
		// Block is empty.
		i.firstUserKey = nil
	}
	return nil
}

// InitHandle initializes an iterator from the provided block handle.
// NB: two cases of hideObsoletePoints:
//   - Local sstable iteration: syntheticSeqNum will be set iff the sstable was
//     ingested.
//   - Foreign sstable iteration: syntheticSeqNum is always set.
func (i *Iter) InitHandle(
	comparer *base.Comparer, block block.BufferHandle, transforms block.IterTransforms,
) error {
	i.handle.Release()
	i.handle = block
	return i.Init(
		comparer.Compare,
		comparer.ComparePointSuffixes,
		comparer.Split,
		block.BlockData(),
		transforms)
}

// SetHasValuePrefix sets whether or not the block iterator should expect values
// corresponding to Set keys to have a prefix byte.
func (i *Iter) SetHasValuePrefix(hasValuePrefix bool) {
	i.lazyValueHandling.hasValuePrefix = hasValuePrefix
}

// SetGetLazyValuer sets the value block reader the iterator should use to get
// lazy values when the value encodes a value prefix.
func (i *Iter) SetGetLazyValuer(g block.GetInternalValueForPrefixAndValueHandler) {
	i.lazyValueHandling.getValue = g

}

// Handle returns the underlying block buffer handle, if the iterator was
// initialized with one.
func (i *Iter) Handle() block.BufferHandle {
	return i.handle
}

// Invalidate invalidates the block iterator, removing references to the block
// it was initialized with.
func (i *Iter) Invalidate() {
	i.clearCache()
	i.offset = 0
	i.nextOffset = 0
	i.restarts = 0
	i.numRestarts = 0
	i.data = nil
}

// IsDataInvalidated returns true when the blockIter has been invalidated
// using an invalidate call. NB: this is different from blockIter.Valid
// which is part of the InternalIterator implementation.
func (i *Iter) IsDataInvalidated() bool {
	return i.data == nil
}

func (i *Iter) readEntry() {
	ptr := unsafe.Pointer(uintptr(i.ptr) + uintptr(i.offset))

	// This is an ugly performance hack. Reading entries from blocks is one of
	// the inner-most routines and decoding the 3 varints per-entry takes
	// significant time. Neither go1.11 or go1.12 will inline decodeVarint for
	// us, so we do it manually. This provides a 10-15% performance improvement
	// on blockIter benchmarks on both go1.11 and go1.12.
	//
	// TODO(peter): remove this hack if go:inline is ever supported.

	var shared uint32
	if a := *((*uint8)(ptr)); a < 128 {
		shared = uint32(a)
		ptr = unsafe.Add(ptr, 1)
	} else if a, b := a&0x7f, *((*uint8)(unsafe.Add(ptr, 1))); b < 128 {
		shared = uint32(b)<<7 | uint32(a)
		ptr = unsafe.Add(ptr, 2)
	} else if b, c := b&0x7f, *((*uint8)(unsafe.Add(ptr, 2))); c < 128 {
		shared = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Add(ptr, 3)
	} else if c, d := c&0x7f, *((*uint8)(unsafe.Add(ptr, 3))); d < 128 {
		shared = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Add(ptr, 4)
	} else {
		d, e := d&0x7f, *((*uint8)(unsafe.Add(ptr, 4)))
		shared = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Add(ptr, 5)
	}

	var unshared uint32
	if a := *((*uint8)(ptr)); a < 128 {
		unshared = uint32(a)
		ptr = unsafe.Add(ptr, 1)
	} else if a, b := a&0x7f, *((*uint8)(unsafe.Add(ptr, 1))); b < 128 {
		unshared = uint32(b)<<7 | uint32(a)
		ptr = unsafe.Add(ptr, 2)
	} else if b, c := b&0x7f, *((*uint8)(unsafe.Add(ptr, 2))); c < 128 {
		unshared = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Add(ptr, 3)
	} else if c, d := c&0x7f, *((*uint8)(unsafe.Add(ptr, 3))); d < 128 {
		unshared = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Add(ptr, 4)
	} else {
		d, e := d&0x7f, *((*uint8)(unsafe.Add(ptr, 4)))
		unshared = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Add(ptr, 5)
	}

	var value uint32
	if a := *((*uint8)(ptr)); a < 128 {
		value = uint32(a)
		ptr = unsafe.Add(ptr, 1)
	} else if a, b := a&0x7f, *((*uint8)(unsafe.Add(ptr, 1))); b < 128 {
		value = uint32(b)<<7 | uint32(a)
		ptr = unsafe.Add(ptr, 2)
	} else if b, c := b&0x7f, *((*uint8)(unsafe.Add(ptr, 2))); c < 128 {
		value = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Add(ptr, 3)
	} else if c, d := c&0x7f, *((*uint8)(unsafe.Add(ptr, 3))); d < 128 {
		value = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Add(ptr, 4)
	} else {
		d, e := d&0x7f, *((*uint8)(unsafe.Add(ptr, 4)))
		value = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Add(ptr, 5)
	}
	shared += i.transforms.SyntheticPrefixAndSuffix.PrefixLen()
	unsharedKey := unsafe.Slice((*byte)(ptr), int(unshared))
	// TODO(sumeer): move this into the else block below.
	i.fullKey = append(i.fullKey[:shared], unsharedKey...)
	if shared == 0 {
		// Provide stability for the key across positioning calls if the key
		// doesn't share a prefix with the previous key. This removes requiring the
		// key to be copied if the caller knows the block has a restart interval of
		// 1. An important example of this is range-del blocks.
		i.key = unsharedKey
	} else {
		i.key = i.fullKey
	}
	ptr = unsafe.Add(ptr, unshared)
	i.val = unsafe.Slice((*byte)(ptr), int(value))
	i.nextOffset = offsetInBlock(uintptr(ptr)-uintptr(i.ptr)) + offsetInBlock(value)
}

func (i *Iter) readFirstKey() error {
	ptr := i.ptr

	// This is an ugly performance hack. Reading entries from blocks is one of
	// the inner-most routines and decoding the 3 varints per-entry takes
	// significant time. Neither go1.11 or go1.12 will inline decodeVarint for
	// us, so we do it manually. This provides a 10-15% performance improvement
	// on blockIter benchmarks on both go1.11 and go1.12.
	//
	// TODO(peter): remove this hack if go:inline is ever supported.

	if shared := *((*uint8)(ptr)); shared == 0 {
		ptr = unsafe.Add(ptr, 1)
	} else {
		// The shared length is != 0, which is invalid.
		panic("first key in block must have zero shared length")
	}

	var unshared uint32
	if a := *((*uint8)(ptr)); a < 128 {
		unshared = uint32(a)
		ptr = unsafe.Add(ptr, 1)
	} else if a, b := a&0x7f, *((*uint8)(unsafe.Add(ptr, 1))); b < 128 {
		unshared = uint32(b)<<7 | uint32(a)
		ptr = unsafe.Add(ptr, 2)
	} else if b, c := b&0x7f, *((*uint8)(unsafe.Add(ptr, 2))); c < 128 {
		unshared = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Add(ptr, 3)
	} else if c, d := c&0x7f, *((*uint8)(unsafe.Add(ptr, 3))); d < 128 {
		unshared = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Add(ptr, 4)
	} else {
		d, e := d&0x7f, *((*uint8)(unsafe.Add(ptr, 4)))
		unshared = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Add(ptr, 5)
	}

	// Skip the value length.
	if a := *((*uint8)(ptr)); a < 128 {
		ptr = unsafe.Add(ptr, 1)
	} else if a := *((*uint8)(unsafe.Add(ptr, 1))); a < 128 {
		ptr = unsafe.Add(ptr, 2)
	} else if a := *((*uint8)(unsafe.Add(ptr, 2))); a < 128 {
		ptr = unsafe.Add(ptr, 3)
	} else if a := *((*uint8)(unsafe.Add(ptr, 3))); a < 128 {
		ptr = unsafe.Add(ptr, 4)
	} else {
		ptr = unsafe.Add(ptr, 5)
	}

	firstKey := unsafe.Slice((*byte)(ptr), int(unshared))
	// Manually inlining base.DecodeInternalKey provides a 5-10% speedup on
	// BlockIter benchmarks.
	if n := len(firstKey) - 8; n >= 0 {
		i.firstUserKey = firstKey[:n:n]
	} else {
		i.firstUserKey = nil
		return base.CorruptionErrorf("pebble/table: invalid firstKey in block")
	}
	if i.transforms.HasSyntheticPrefix() {
		syntheticPrefix := i.transforms.SyntheticPrefix()
		i.firstUserKeyWithPrefixBuf = slices.Grow(i.firstUserKeyWithPrefixBuf[:0], len(syntheticPrefix)+len(i.firstUserKey))
		i.firstUserKeyWithPrefixBuf = append(i.firstUserKeyWithPrefixBuf, syntheticPrefix...)
		i.firstUserKeyWithPrefixBuf = append(i.firstUserKeyWithPrefixBuf, i.firstUserKey...)
		i.firstUserKey = i.firstUserKeyWithPrefixBuf
	}
	return nil
}

func (i *Iter) decodeInternalKey(key []byte) (hiddenPoint bool) {
	// Manually inlining base.DecodeInternalKey provides a 5-10% speedup on
	// BlockIter benchmarks.
	if n := len(key) - 8; n >= 0 {
		trailer := base.InternalKeyTrailer(binary.LittleEndian.Uint64(key[n:]))
		hiddenPoint = i.transforms.HideObsoletePoints &&
			(trailer&TrailerObsoleteBit != 0)
		i.ikv.K.Trailer = trailer & TrailerObsoleteMask
		i.ikv.K.UserKey = key[:n:n]
		if n := i.transforms.SyntheticSeqNum; n != 0 {
			i.ikv.K.SetSeqNum(base.SeqNum(n))
		}
	} else {
		i.ikv.K.Trailer = base.InternalKeyTrailer(base.InternalKeyKindInvalid)
		i.ikv.K.UserKey = nil
	}
	return hiddenPoint
}

// maybeReplaceSuffix replaces the suffix in i.ikey.UserKey with
// i.transforms.syntheticSuffix.
func (i *Iter) maybeReplaceSuffix() {
	if i.transforms.HasSyntheticSuffix() && i.ikv.K.UserKey != nil {
		prefixLen := i.split(i.ikv.K.UserKey)
		// If ikey is cached or may get cached, we must copy
		// UserKey to a new buffer before suffix replacement.
		i.synthSuffixBuf = append(i.synthSuffixBuf[:0], i.ikv.K.UserKey[:prefixLen]...)
		i.synthSuffixBuf = append(i.synthSuffixBuf, i.transforms.SyntheticSuffix()...)
		i.ikv.K.UserKey = i.synthSuffixBuf
	}
}

func (i *Iter) clearCache() {
	i.cached = i.cached[:0]
	i.cachedBuf = i.cachedBuf[:0]
}

func (i *Iter) cacheEntry() {
	var valStart offsetInBlock
	valSize := uint32(len(i.val))
	if valSize > 0 {
		valStart = offsetInBlock(uintptr(unsafe.Pointer(&i.val[0])) - uintptr(i.ptr))
	}

	i.cached = append(i.cached, blockEntry{
		offset:   i.offset,
		keyStart: offsetInBlock(len(i.cachedBuf)),
		keyEnd:   offsetInBlock(len(i.cachedBuf) + len(i.key)),
		valStart: valStart,
		valSize:  valSize,
	})
	i.cachedBuf = append(i.cachedBuf, i.key...)
}

// IsLowerBound implements the block.DataBlockIterator interface.
func (i *Iter) IsLowerBound(k []byte) bool {
	// Note: we ignore HideObsoletePoints, but false negatives are allowed.
	return i.cmp(i.firstUserKey, k) >= 0
}

// SeekGE implements internalIterator.SeekGE, as documented in the pebble
// package.
func (i *Iter) SeekGE(key []byte, flags base.SeekGEFlags) *base.InternalKV {
	if invariants.Enabled && i.IsDataInvalidated() {
		panic(errors.AssertionFailedf("invalidated blockIter used"))
	}
	searchKey := key
	if i.transforms.HasSyntheticPrefix() {
		syntheticPrefix := i.transforms.SyntheticPrefix()
		if !bytes.HasPrefix(key, syntheticPrefix) {
			// The seek key is before or after the entire block of keys that start
			// with SyntheticPrefix. To determine which, we need to compare against a
			// valid key in the block. We use firstUserKey which has the synthetic
			// prefix.
			if i.cmp(i.firstUserKey, key) >= 0 {
				return i.First()
			}
			// Set the offset to the end of the block to mimic the offset of an
			// invalid iterator. This ensures a subsequent i.Prev() returns a valid
			// result.
			i.offset = i.restarts
			i.nextOffset = i.restarts
			return nil
		}
		searchKey = key[len(syntheticPrefix):]
	}

	i.clearCache()
	// Find the index of the smallest restart point whose key is > the key
	// sought; index will be numRestarts if there is no such restart point.
	i.offset = 0
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
			offset := decodeRestart(i.data[i.restarts+4*offsetInBlock(h):])
			// For a restart point, there are 0 bytes shared with the previous key.
			// The varint encoding of 0 occupies 1 byte.
			ptr := unsafe.Pointer(uintptr(i.ptr) + uintptr(offset+1))

			// Decode the key at that restart point, and compare it to the key
			// sought. See the comment in readEntry for why we manually inline the
			// varint decoding.
			var v1 uint32
			if a := *((*uint8)(ptr)); a < 128 {
				v1 = uint32(a)
				ptr = unsafe.Add(ptr, 1)
			} else if a, b := a&0x7f, *((*uint8)(unsafe.Add(ptr, 1))); b < 128 {
				v1 = uint32(b)<<7 | uint32(a)
				ptr = unsafe.Add(ptr, 2)
			} else if b, c := b&0x7f, *((*uint8)(unsafe.Add(ptr, 2))); c < 128 {
				v1 = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
				ptr = unsafe.Add(ptr, 3)
			} else if c, d := c&0x7f, *((*uint8)(unsafe.Add(ptr, 3))); d < 128 {
				v1 = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
				ptr = unsafe.Add(ptr, 4)
			} else {
				d, e := d&0x7f, *((*uint8)(unsafe.Add(ptr, 4)))
				v1 = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
				ptr = unsafe.Add(ptr, 5)
			}

			if *((*uint8)(ptr)) < 128 {
				ptr = unsafe.Add(ptr, 1)
			} else if *((*uint8)(unsafe.Add(ptr, 1))) < 128 {
				ptr = unsafe.Add(ptr, 2)
			} else if *((*uint8)(unsafe.Add(ptr, 2))) < 128 {
				ptr = unsafe.Add(ptr, 3)
			} else if *((*uint8)(unsafe.Add(ptr, 3))) < 128 {
				ptr = unsafe.Add(ptr, 4)
			} else {
				ptr = unsafe.Add(ptr, 5)
			}

			// Manually inlining part of base.DecodeInternalKey provides a 5-10%
			// speedup on BlockIter benchmarks.
			s := unsafe.Slice((*byte)(ptr), int(v1))
			var k []byte
			if n := len(s) - 8; n >= 0 {
				k = s[:n:n]
			}
			// Else k is invalid, and left as nil

			if i.cmp(searchKey, k) > 0 {
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
		i.offset = decodeRestart(i.data[i.restarts+4*offsetInBlock(index-1):])
	}
	i.readEntry()
	hiddenPoint := i.decodeInternalKey(i.key)

	// Iterate from that restart point to somewhere >= the key sought.
	if !i.Valid() {
		return nil
	}

	// A note on seeking in a block with a suffix replacement rule: even though
	// the binary search above was conducted on keys without suffix replacement,
	// Seek will still return the correct suffix replaced key. A binary
	// search without suffix replacement will land on a key that is _less_ than
	// the key the search would have landed on if all keys were already suffix
	// replaced. Since Seek then conducts forward iteration to the first suffix
	// replaced user key that is greater than or equal to the search key, the
	// correct key is still returned.
	//
	// As an example, consider the following block with a restart interval of 1,
	// with a replacement suffix of "4":
	// - Pre-suffix replacement: apple@1, banana@3
	// - Post-suffix replacement: apple@4, banana@4
	//
	// Suppose the client seeks with apple@3. Assuming suffixes sort in reverse
	// chronological order (i.e. apple@1>apple@3), the binary search without
	// suffix replacement would return apple@1. A binary search with suffix
	// replacement would return banana@4. After beginning forward iteration from
	// either returned restart point, forward iteration would
	// always return the correct key, banana@4.
	//
	// Further, if the user searched with apple@0 (i.e. a suffix less than the
	// pre replacement suffix) or with apple@5 (a suffix larger than the post
	// replacement suffix), the binary search with or without suffix replacement
	// would land on the same key, as we assume the following:
	// (1) no two keys in the sst share the same prefix.
	// (2) pebble.Compare(replacementSuffix,originalSuffix) > 0

	i.maybeReplaceSuffix()

	if !hiddenPoint && i.cmp(i.ikv.K.UserKey, key) >= 0 {
		// Initialize i.lazyValue
		if !i.lazyValueHandling.hasValuePrefix ||
			i.ikv.K.Kind() != base.InternalKeyKindSet {
			i.ikv.V = base.MakeInPlaceValue(i.val)
		} else if i.lazyValueHandling.getValue == nil || block.ValuePrefix(i.val[0]).IsInPlaceValue() {
			i.ikv.V = base.MakeInPlaceValue(i.val[1:])
		} else {
			i.ikv.V = i.lazyValueHandling.getValue.GetInternalValueForPrefixAndValueHandle(i.val)
		}
		return &i.ikv
	}
	for i.Next(); i.Valid(); i.Next() {
		if i.cmp(i.ikv.K.UserKey, key) >= 0 {
			// i.Next() has already initialized i.ikv.LazyValue.
			return &i.ikv
		}
	}
	return nil
}

// SeekPrefixGE implements internalIterator.SeekPrefixGE, as documented in the
// pebble package.
func (i *Iter) SeekPrefixGE(prefix, key []byte, flags base.SeekGEFlags) *base.InternalKV {
	// This should never be called as prefix iteration is handled by sstable.Iterator.
	panic("pebble: SeekPrefixGE unimplemented")
}

// SeekLT implements internalIterator.SeekLT, as documented in the pebble
// package.
func (i *Iter) SeekLT(key []byte, flags base.SeekLTFlags) *base.InternalKV {
	if invariants.Enabled && i.IsDataInvalidated() {
		panic(errors.AssertionFailedf("invalidated blockIter used"))
	}
	searchKey := key
	if i.transforms.HasSyntheticPrefix() {
		syntheticPrefix := i.transforms.SyntheticPrefix()
		if !bytes.HasPrefix(key, syntheticPrefix) {
			// The seek key is before or after the entire block of keys that start
			// with SyntheticPrefix. To determine which, we need to compare against a
			// valid key in the block. We use firstUserKey which has the synthetic
			// prefix.
			if i.cmp(i.firstUserKey, key) < 0 {
				return i.Last()
			}
			// Set the offset to the beginning of the block to mimic an exhausted
			// iterator that has conducted backward interation. This ensures a
			// subsequent Next() call returns the first key in the block.
			i.offset = -1
			i.nextOffset = 0
			return nil
		}
		searchKey = key[len(syntheticPrefix):]
	}

	i.clearCache()
	// Find the index of the smallest restart point whose key is >= the key
	// sought; index will be numRestarts if there is no such restart point.
	i.offset = 0
	var index int32

	{
		// NB: manually inlined sort.Search is ~5% faster.
		//
		// Define f(-1) == false and f(n) == true.
		// Invariant: f(index-1) == false, f(upper) == true.
		upper := i.numRestarts
		for index < upper {
			h := int32(uint(index+upper) >> 1) // avoid overflow when computing h

			// index ≤ h < upper
			offset := decodeRestart(i.data[i.restarts+4*offsetInBlock(h):])
			// For a restart point, there are 0 bytes shared with the previous key.
			// The varint encoding of 0 occupies 1 byte.
			ptr := unsafe.Pointer(uintptr(i.ptr) + uintptr(offset+1))

			// Decode the key at that restart point, and compare it to the key
			// sought. See the comment in readEntry for why we manually inline the
			// varint decoding.
			var v1 uint32
			if a := *((*uint8)(ptr)); a < 128 {
				v1 = uint32(a)
				ptr = unsafe.Add(ptr, 1)
			} else if a, b := a&0x7f, *((*uint8)(unsafe.Add(ptr, 1))); b < 128 {
				v1 = uint32(b)<<7 | uint32(a)
				ptr = unsafe.Add(ptr, 2)
			} else if b, c := b&0x7f, *((*uint8)(unsafe.Add(ptr, 2))); c < 128 {
				v1 = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
				ptr = unsafe.Add(ptr, 3)
			} else if c, d := c&0x7f, *((*uint8)(unsafe.Add(ptr, 3))); d < 128 {
				v1 = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
				ptr = unsafe.Add(ptr, 4)
			} else {
				d, e := d&0x7f, *((*uint8)(unsafe.Add(ptr, 4)))
				v1 = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
				ptr = unsafe.Add(ptr, 5)
			}

			if *((*uint8)(ptr)) < 128 {
				ptr = unsafe.Add(ptr, 1)
			} else if *((*uint8)(unsafe.Add(ptr, 1))) < 128 {
				ptr = unsafe.Add(ptr, 2)
			} else if *((*uint8)(unsafe.Add(ptr, 2))) < 128 {
				ptr = unsafe.Add(ptr, 3)
			} else if *((*uint8)(unsafe.Add(ptr, 3))) < 128 {
				ptr = unsafe.Add(ptr, 4)
			} else {
				ptr = unsafe.Add(ptr, 5)
			}

			// Manually inlining part of base.DecodeInternalKey provides a 5-10%
			// speedup on BlockIter benchmarks.
			s := unsafe.Slice((*byte)(ptr), int(v1))
			var k []byte
			if n := len(s) - 8; n >= 0 {
				k = s[:n:n]
			}
			// Else k is invalid, and left as nil

			if i.cmp(searchKey, k) > 0 {
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

	if index == 0 {
		if i.transforms.HasSyntheticSuffix() {
			// The binary search was conducted on keys without suffix replacement,
			// implying the first key in the block may be less than the search key. To
			// double check, get the first key in the block with suffix replacement
			// and compare to the search key. Consider the following example: suppose
			// the user searches with a@3, the first key in the block is a@2 and the
			// block contains a suffix replacement rule of 4. Since a@3 sorts before
			// a@2, the binary search would return index==0. Without conducting the
			// suffix replacement, the SeekLT would incorrectly return nil. With
			// suffix replacement though, a@4 should be returned as a@4 sorts before
			// a@3.
			ikv := i.First()
			if i.cmp(ikv.K.UserKey, key) < 0 {
				return ikv
			}
		}
		// If index == 0 then all keys in this block are larger than the key
		// sought, so there is no match.
		i.offset = -1
		i.nextOffset = 0
		return nil
	}

	// INVARIANT: index > 0

	// Ignoring suffix replacement, index is the first restart point with key >=
	// search key. Define the keys between a restart point and the next restart
	// point as belonging to that restart point. Note that index could be equal to
	// i.numRestarts, i.e., we are past the last restart.  Since keys are strictly
	// increasing, then the restart point at index-1 will be the first one that
	// has some keys belonging to it that are less than the search key.
	//
	// Next, we will search between the restart at index-1 and the restart point
	// at index, for the first key >= key, and then on finding it, return
	// i.Prev(). We need to know when we have hit the offset for index, since then
	// we can stop searching. targetOffset encodes that offset for index.
	targetOffset := i.restarts
	i.offset = decodeRestart(i.data[i.restarts+4*offsetInBlock(index-1):])
	if index < i.numRestarts {
		targetOffset = decodeRestart(i.data[i.restarts+4*offsetInBlock(index):])

		if i.transforms.HasSyntheticSuffix() {
			// The binary search was conducted on keys without suffix replacement,
			// implying the returned restart point (index) may be less than the search
			// key, breaking the assumption described above.
			//
			// For example: consider this block with a replacement ts of 4, and
			// restart interval of 1: - pre replacement: a@3,b@2,c@3 - post
			// replacement: a@4,b@4,c@4
			//
			// Suppose the client calls SeekLT(b@3), SeekLT must return b@4.
			//
			// If the client calls  SeekLT(b@3), the binary search would return b@2,
			// the lowest key geq to b@3, pre-suffix replacement. Then, SeekLT will
			// begin forward iteration from a@3, the previous restart point, to
			// b{suffix}. The iteration stops when it encounters a key geq to the
			// search key or if it reaches the upper bound. Without suffix
			// replacement, we can assume that the upper bound of this forward
			// iteration, b{suffix}, is greater than the search key, as implied by the
			// binary search.
			//
			// If we naively hold this assumption with suffix replacement, the
			// iteration would terminate at the upper bound, b@4, call i.Prev, and
			// incorrectly return a@4. To correct for this, if the original returned
			// index is less than the search key, shift our forward iteration to begin
			// at index instead of index -1. With suffix replacement the key at index
			// is guaranteed to be the highest restart point less than the seach key
			// (i.e. the same property of index-1 for a block without suffix
			// replacement). This property holds because of the invariant that a block
			// with suffix replacement will not have two keys that share the same
			// prefix. To consider the above example, binary searching with b@3 landed
			// naively at a@3, but since b@4<b@3, we shift our forward iteration to
			// begin at b@4. We never need to shift by more than one restart point
			// (i.e. to c@4) because it's impossible for the search key to be greater
			// than the key at the next restart point in the block because that
			// key will always have a different prefix. Put another way, because no
			// key in the block shares the same prefix, naive binary search should
			// always land at most 1 restart point off the correct one.

			naiveOffset := i.offset
			// Shift up to the original binary search result and decode the key.
			i.offset = targetOffset
			i.readEntry()
			i.decodeInternalKey(i.key)
			i.maybeReplaceSuffix()

			// If the binary search point is actually less than the search key, post
			// replacement, bump the target offset.
			if i.cmp(i.ikv.K.UserKey, key) < 0 {
				i.offset = targetOffset
				if index+1 < i.numRestarts {
					// if index+1 is within the i.data bounds, use it to find the target
					// offset.
					targetOffset = decodeRestart(i.data[i.restarts+4*offsetInBlock(index+1):])
				} else {
					targetOffset = i.restarts
				}
			} else {
				i.offset = naiveOffset
			}
		}
	}

	// Init nextOffset for the forward iteration below.
	i.nextOffset = i.offset

	for {
		i.offset = i.nextOffset
		i.readEntry()
		// When hidden keys are common, there is additional optimization possible
		// by not caching entries that are hidden (note that some calls to
		// cacheEntry don't decode the internal key before caching, but checking
		// whether a key is hidden does not require full decoding). However, we do
		// need to use the blockEntry.offset in the cache for the first entry at
		// the reset point to do the binary search when the cache is empty -- so
		// we would need to cache that first entry (though not the key) even if
		// was hidden. Our current assumption is that if there are large numbers
		// of hidden keys we will be able to skip whole blocks (using block
		// property filters) so we don't bother optimizing.
		hiddenPoint := i.decodeInternalKey(i.key)
		i.maybeReplaceSuffix()

		// NB: we don't use the hiddenPoint return value of decodeInternalKey
		// since we want to stop as soon as we reach a key >= ikey.UserKey, so
		// that we can reverse.
		if i.cmp(i.ikv.K.UserKey, key) >= 0 {
			// The current key is greater than or equal to our search key. Back up to
			// the previous key which was less than our search key. Note that this for
			// loop will execute at least once with this if-block not being true, so
			// the key we are backing up to is the last one this loop cached.
			return i.Prev()
		}

		if i.nextOffset >= targetOffset {
			// We've reached the end of the current restart block. Return the
			// current key if not hidden, else call Prev().
			//
			// When the restart interval is 1, the first iteration of the for loop
			// will bring us here. In that case ikey is backed by the block so we
			// get the desired key stability guarantee for the lifetime of the
			// blockIter. That is, we never cache anything and therefore never
			// return a key backed by cachedBuf.
			if hiddenPoint {
				return i.Prev()
			}
			break
		}
		i.cacheEntry()
	}

	if !i.Valid() {
		return nil
	}
	if !i.lazyValueHandling.hasValuePrefix ||
		i.ikv.K.Kind() != base.InternalKeyKindSet {
		i.ikv.V = base.MakeInPlaceValue(i.val)
	} else if i.lazyValueHandling.getValue == nil || block.ValuePrefix(i.val[0]).IsInPlaceValue() {
		i.ikv.V = base.MakeInPlaceValue(i.val[1:])
	} else {
		i.ikv.V = i.lazyValueHandling.getValue.GetInternalValueForPrefixAndValueHandle(i.val)
	}
	return &i.ikv
}

// First implements internalIterator.First, as documented in the pebble
// package.
func (i *Iter) First() *base.InternalKV {
	if invariants.Enabled && i.IsDataInvalidated() {
		panic(errors.AssertionFailedf("invalidated blockIter used"))
	}

	i.offset = 0
	if !i.Valid() {
		return nil
	}
	i.clearCache()
	i.readEntry()
	hiddenPoint := i.decodeInternalKey(i.key)
	if hiddenPoint {
		return i.Next()
	}
	i.maybeReplaceSuffix()
	if !i.lazyValueHandling.hasValuePrefix ||
		i.ikv.K.Kind() != base.InternalKeyKindSet {
		i.ikv.V = base.MakeInPlaceValue(i.val)
	} else if i.lazyValueHandling.getValue == nil || block.ValuePrefix(i.val[0]).IsInPlaceValue() {
		i.ikv.V = base.MakeInPlaceValue(i.val[1:])
	} else {
		i.ikv.V = i.lazyValueHandling.getValue.GetInternalValueForPrefixAndValueHandle(i.val)
	}
	return &i.ikv
}

const restartMaskLittleEndianHighByteWithoutSetHasSamePrefix byte = 0b0111_1111
const restartMaskLittleEndianHighByteOnlySetHasSamePrefix byte = 0b1000_0000

func decodeRestart(b []byte) offsetInBlock {
	_ = b[3] // bounds check hint to compiler; see golang.org/issue/14808
	return offsetInBlock(uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 |
		uint32(b[3]&restartMaskLittleEndianHighByteWithoutSetHasSamePrefix)<<24)
}

// Last implements internalIterator.Last, as documented in the pebble package.
func (i *Iter) Last() *base.InternalKV {
	if invariants.Enabled && i.IsDataInvalidated() {
		panic(errors.AssertionFailedf("invalidated blockIter used"))
	}

	// Seek forward from the last restart point.
	i.offset = decodeRestart(i.data[i.restarts+4*offsetInBlock(i.numRestarts-1):])
	if !i.Valid() {
		return nil
	}

	i.readEntry()
	i.clearCache()

	for i.nextOffset < i.restarts {
		i.cacheEntry()
		i.offset = i.nextOffset
		i.readEntry()
	}

	hiddenPoint := i.decodeInternalKey(i.key)
	if hiddenPoint {
		return i.Prev()
	}
	i.maybeReplaceSuffix()
	if !i.lazyValueHandling.hasValuePrefix ||
		i.ikv.K.Kind() != base.InternalKeyKindSet {
		i.ikv.V = base.MakeInPlaceValue(i.val)
	} else if i.lazyValueHandling.getValue == nil || block.ValuePrefix(i.val[0]).IsInPlaceValue() {
		i.ikv.V = base.MakeInPlaceValue(i.val[1:])
	} else {
		i.ikv.V = i.lazyValueHandling.getValue.GetInternalValueForPrefixAndValueHandle(i.val)
	}
	return &i.ikv
}

// Next implements internalIterator.Next, as documented in the pebble
// package.
func (i *Iter) Next() *base.InternalKV {
	if len(i.cachedBuf) > 0 {
		// We're switching from reverse iteration to forward iteration. We need to
		// populate i.fullKey with the current key we're positioned at so that
		// readEntry() can use i.fullKey for key prefix decompression. Note that we
		// don't know whether i.key is backed by i.cachedBuf or i.fullKey (if
		// SeekLT was the previous call, i.key may be backed by i.fullKey), but
		// copying into i.fullKey works for both cases.
		//
		// TODO(peter): Rather than clearing the cache, we could instead use the
		// cache until it is exhausted. This would likely be faster than falling
		// through to the normal forward iteration code below.
		i.fullKey = append(i.fullKey[:0], i.key...)
		i.clearCache()
	}

start:
	i.offset = i.nextOffset
	if !i.Valid() {
		return nil
	}
	i.readEntry()
	// Manually inlined version of i.decodeInternalKey(i.key).
	if n := len(i.key) - 8; n >= 0 {
		trailer := base.InternalKeyTrailer(binary.LittleEndian.Uint64(i.key[n:]))
		hiddenPoint := i.transforms.HideObsoletePoints &&
			(trailer&TrailerObsoleteBit != 0)
		i.ikv.K.Trailer = trailer & TrailerObsoleteMask
		i.ikv.K.UserKey = i.key[:n:n]
		if n := i.transforms.SyntheticSeqNum; n != 0 {
			i.ikv.K.SetSeqNum(base.SeqNum(n))
		}
		if hiddenPoint {
			goto start
		}
		if i.transforms.HasSyntheticSuffix() {
			// Inlined version of i.maybeReplaceSuffix()
			prefixLen := i.split(i.ikv.K.UserKey)
			i.synthSuffixBuf = append(i.synthSuffixBuf[:0], i.ikv.K.UserKey[:prefixLen]...)
			i.synthSuffixBuf = append(i.synthSuffixBuf, i.transforms.SyntheticSuffix()...)
			i.ikv.K.UserKey = i.synthSuffixBuf
		}
	} else {
		i.ikv.K.Trailer = base.InternalKeyTrailer(base.InternalKeyKindInvalid)
		i.ikv.K.UserKey = nil
	}
	if !i.lazyValueHandling.hasValuePrefix ||
		i.ikv.K.Kind() != base.InternalKeyKindSet {
		i.ikv.V = base.MakeInPlaceValue(i.val)
	} else if i.lazyValueHandling.getValue == nil || block.ValuePrefix(i.val[0]).IsInPlaceValue() {
		i.ikv.V = base.MakeInPlaceValue(i.val[1:])
	} else {
		i.ikv.V = i.lazyValueHandling.getValue.GetInternalValueForPrefixAndValueHandle(i.val)
	}
	return &i.ikv
}

// NextPrefix implements (base.InternalIterator).NextPrefix.
func (i *Iter) NextPrefix(succKey []byte) *base.InternalKV {
	if i.lazyValueHandling.hasValuePrefix {
		return i.nextPrefixV3(succKey)
	}
	const nextsBeforeSeek = 3
	kv := i.Next()
	for j := 1; kv != nil && i.cmp(kv.K.UserKey, succKey) < 0; j++ {
		if j >= nextsBeforeSeek {
			return i.SeekGE(succKey, base.SeekGEFlagsNone)
		}
		kv = i.Next()
	}
	return kv
}

func (i *Iter) nextPrefixV3(succKey []byte) *base.InternalKV {
	// Doing nexts that involve a key comparison can be expensive (and the cost
	// depends on the key length), so we use the same threshold of 3 that we use
	// for TableFormatPebblev2 in blockIter.nextPrefix above. The next fast path
	// that looks at setHasSamePrefix takes ~5ns per key, which is ~150x faster
	// than doing a SeekGE within the block, so we do this 16 times
	// (~5ns*16=80ns), and then switch to looking at restarts. Doing the binary
	// search for the restart consumes > 100ns. If the number of versions is >
	// 17, we will increment nextFastCount to 17, then do a binary search, and
	// on average need to find a key between two restarts, so another 8 steps
	// corresponding to nextFastCount, for a mean total of 17 + 8 = 25 such
	// steps.
	//
	// TODO(sumeer): use the configured restartInterval for the sstable when it
	// was written (which we don't currently store) instead of the default value
	// of 16.
	const nextCmpThresholdBeforeSeek = 3
	const nextFastThresholdBeforeRestarts = 16
	nextCmpCount := 0
	nextFastCount := 0
	usedRestarts := false
	// INVARIANT: blockIter is valid.
	if invariants.Enabled && !i.Valid() {
		panic(errors.AssertionFailedf("nextPrefixV3 called on invalid blockIter"))
	}
	prevKeyIsSet := i.ikv.Kind() == base.InternalKeyKindSet
	for {
		i.offset = i.nextOffset
		if !i.Valid() {
			return nil
		}
		// Need to decode the length integers, so we can compute nextOffset.
		ptr := unsafe.Pointer(uintptr(i.ptr) + uintptr(i.offset))
		// This is an ugly performance hack. Reading entries from blocks is one of
		// the inner-most routines and decoding the 3 varints per-entry takes
		// significant time. Neither go1.11 or go1.12 will inline decodeVarint for
		// us, so we do it manually. This provides a 10-15% performance improvement
		// on blockIter benchmarks on both go1.11 and go1.12.
		//
		// TODO(peter): remove this hack if go:inline is ever supported.

		// Decode the shared key length integer.
		var shared uint32
		if a := *((*uint8)(ptr)); a < 128 {
			shared = uint32(a)
			ptr = unsafe.Add(ptr, 1)
		} else if a, b := a&0x7f, *((*uint8)(unsafe.Add(ptr, 1))); b < 128 {
			shared = uint32(b)<<7 | uint32(a)
			ptr = unsafe.Add(ptr, 2)
		} else if b, c := b&0x7f, *((*uint8)(unsafe.Add(ptr, 2))); c < 128 {
			shared = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
			ptr = unsafe.Add(ptr, 3)
		} else if c, d := c&0x7f, *((*uint8)(unsafe.Add(ptr, 3))); d < 128 {
			shared = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
			ptr = unsafe.Add(ptr, 4)
		} else {
			d, e := d&0x7f, *((*uint8)(unsafe.Add(ptr, 4)))
			shared = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
			ptr = unsafe.Add(ptr, 5)
		}
		// Decode the unshared key length integer.
		var unshared uint32
		if a := *((*uint8)(ptr)); a < 128 {
			unshared = uint32(a)
			ptr = unsafe.Add(ptr, 1)
		} else if a, b := a&0x7f, *((*uint8)(unsafe.Add(ptr, 1))); b < 128 {
			unshared = uint32(b)<<7 | uint32(a)
			ptr = unsafe.Add(ptr, 2)
		} else if b, c := b&0x7f, *((*uint8)(unsafe.Add(ptr, 2))); c < 128 {
			unshared = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
			ptr = unsafe.Add(ptr, 3)
		} else if c, d := c&0x7f, *((*uint8)(unsafe.Add(ptr, 3))); d < 128 {
			unshared = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
			ptr = unsafe.Add(ptr, 4)
		} else {
			d, e := d&0x7f, *((*uint8)(unsafe.Add(ptr, 4)))
			unshared = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
			ptr = unsafe.Add(ptr, 5)
		}
		// Decode the value length integer.
		var value uint32
		if a := *((*uint8)(ptr)); a < 128 {
			value = uint32(a)
			ptr = unsafe.Add(ptr, 1)
		} else if a, b := a&0x7f, *((*uint8)(unsafe.Add(ptr, 1))); b < 128 {
			value = uint32(b)<<7 | uint32(a)
			ptr = unsafe.Add(ptr, 2)
		} else if b, c := b&0x7f, *((*uint8)(unsafe.Add(ptr, 2))); c < 128 {
			value = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
			ptr = unsafe.Add(ptr, 3)
		} else if c, d := c&0x7f, *((*uint8)(unsafe.Add(ptr, 3))); d < 128 {
			value = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
			ptr = unsafe.Add(ptr, 4)
		} else {
			d, e := d&0x7f, *((*uint8)(unsafe.Add(ptr, 4)))
			value = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
			ptr = unsafe.Add(ptr, 5)
		}
		shared += i.transforms.SyntheticPrefixAndSuffix.PrefixLen()
		// The starting position of the value.
		valuePtr := unsafe.Add(ptr, unshared)
		i.nextOffset = offsetInBlock(uintptr(valuePtr)-uintptr(i.ptr)) + offsetInBlock(value)
		if invariants.Enabled && unshared < 8 {
			// This should not happen since only the key prefix is shared, so even
			// if the prefix length is the same as the user key length, the unshared
			// will include the trailer.
			panic(errors.AssertionFailedf("unshared %d is too small", unshared))
		}
		// The trailer is written in little endian, so the key kind is the first
		// byte in the trailer that is encoded in the slice [unshared-8:unshared].
		keyKind := base.InternalKeyKind(*(*byte)(unsafe.Add(ptr, unshared-8)))
		keyKind = keyKind & base.InternalKeyKindSSTableInternalObsoleteMask
		prefixChanged := false
		if keyKind == base.InternalKeyKindSet {
			if invariants.Enabled && value == 0 {
				panic(errors.AssertionFailedf("value is of length 0, but we expect a valuePrefix"))
			}
			valPrefix := *((*block.ValuePrefix)(valuePtr))
			if valPrefix.SetHasSamePrefix() {
				// Fast-path. No need to assemble i.fullKey, or update i.key. We know
				// that subsequent keys will not have a shared length that is greater
				// than the prefix of the current key, which is also the prefix of
				// i.key. Since we are continuing to iterate, we don't need to
				// initialize i.ikey and i.lazyValue (these are initialized before
				// returning).
				nextFastCount++
				if nextFastCount > nextFastThresholdBeforeRestarts {
					if usedRestarts {
						// Exhausted iteration budget. This will never happen unless
						// someone is using a restart interval > 16. It is just to guard
						// against long restart intervals causing too much iteration.
						break
					}
					// Haven't used restarts yet, so find the first restart at or beyond
					// the current offset.
					targetOffset := i.offset
					var index int32
					{
						// NB: manually inlined sort.Sort is ~5% faster.
						//
						// f defined for a restart point is true iff the offset >=
						// targetOffset.
						// Define f(-1) == false and f(i.numRestarts) == true.
						// Invariant: f(index-1) == false, f(upper) == true.
						upper := i.numRestarts
						for index < upper {
							h := int32(uint(index+upper) >> 1) // avoid overflow when computing h

							// index ≤ h < upper
							offset := decodeRestart(i.data[i.restarts+4*offsetInBlock(h):])
							if offset < targetOffset {
								index = h + 1 // preserves f(index-1) == false
							} else {
								upper = h // preserves f(upper) == true
							}
						}
						// index == upper, f(index-1) == false, and f(upper) (= f(index)) == true
						// => answer is index.
					}
					usedRestarts = true
					nextFastCount = 0
					if index == i.numRestarts {
						// Already past the last real restart, so iterate a bit more until
						// we are done with the block.
						continue
					}
					// Have some real restarts after index. NB: index is the first
					// restart at or beyond the current offset.
					startingIndex := index
					for index != i.numRestarts &&
						// The restart at index is 4 bytes written in little endian format
						// starting at i.restart+4*index. The 0th byte is the least
						// significant and the 3rd byte is the most significant. Since the
						// most significant bit of the 3rd byte is what we use for
						// encoding the set-has-same-prefix information, the indexing
						// below has +3.
						i.data[i.restarts+4*offsetInBlock(index)+3]&restartMaskLittleEndianHighByteOnlySetHasSamePrefix != 0 {
						// We still have the same prefix, so move to the next restart.
						index++
					}
					// index is the first restart that did not have the same prefix.
					if index != startingIndex {
						// Managed to skip past at least one restart. Resume iteration
						// from index-1. Since nextFastCount has been reset to 0, we
						// should be able to iterate to the next prefix.
						i.offset = decodeRestart(i.data[i.restarts+4*offsetInBlock(index-1):])
						i.readEntry()
					}
					// Else, unable to skip past any restart. Resume iteration. Since
					// nextFastCount has been reset to 0, we should be able to iterate
					// to the next prefix.
					continue
				}
				continue
			} else if prevKeyIsSet {
				prefixChanged = true
			}
		} else {
			prevKeyIsSet = false
		}
		// Slow-path cases:
		// - (Likely) The prefix has changed.
		// - (Unlikely) The prefix has not changed.
		// We assemble the key etc. under the assumption that it is the likely
		// case.
		unsharedKey := unsafe.Slice((*byte)(ptr), int(unshared))
		// TODO(sumeer): move this into the else block below. This is a bit tricky
		// since the current logic assumes we have always copied the latest key
		// into fullKey, which is why when we get to the next key we can (a)
		// access i.fullKey[:shared], (b) append only the unsharedKey to
		// i.fullKey. For (a), we can access i.key[:shared] since that memory is
		// valid (even if unshared). For (b), we will need to remember whether
		// i.key refers to i.fullKey or not, and can append the unsharedKey only
		// in the former case and for the latter case need to copy the shared part
		// too. This same comment applies to the other place where we can do this
		// optimization, in readEntry().
		i.fullKey = append(i.fullKey[:shared], unsharedKey...)
		i.val = unsafe.Slice((*byte)(valuePtr), int(value))
		if shared == 0 {
			// Provide stability for the key across positioning calls if the key
			// doesn't share a prefix with the previous key. This removes requiring the
			// key to be copied if the caller knows the block has a restart interval of
			// 1. An important example of this is range-del blocks.
			i.key = unsharedKey
		} else {
			i.key = i.fullKey
		}
		// Manually inlined version of i.decodeInternalKey(i.key).
		hiddenPoint := false
		if n := len(i.key) - 8; n >= 0 {
			trailer := base.InternalKeyTrailer(binary.LittleEndian.Uint64(i.key[n:]))
			hiddenPoint = i.transforms.HideObsoletePoints &&
				(trailer&TrailerObsoleteBit != 0)
			i.ikv.K = base.InternalKey{
				Trailer: trailer & TrailerObsoleteMask,
				UserKey: i.key[:n:n],
			}
			if n := i.transforms.SyntheticSeqNum; n != 0 {
				i.ikv.K.SetSeqNum(base.SeqNum(n))
			}
			if i.transforms.HasSyntheticSuffix() {
				// Inlined version of i.maybeReplaceSuffix()
				prefixLen := i.split(i.ikv.K.UserKey)
				i.synthSuffixBuf = append(i.synthSuffixBuf[:0], i.ikv.K.UserKey[:prefixLen]...)
				i.synthSuffixBuf = append(i.synthSuffixBuf, i.transforms.SyntheticSuffix()...)
				i.ikv.K.UserKey = i.synthSuffixBuf
			}
		} else {
			i.ikv.K.Trailer = base.InternalKeyTrailer(base.InternalKeyKindInvalid)
			i.ikv.K.UserKey = nil
		}
		nextCmpCount++
		if invariants.Enabled && prefixChanged && i.cmp(i.ikv.K.UserKey, succKey) < 0 {
			panic(errors.AssertionFailedf("prefix should have changed but %x < %x",
				i.ikv.K.UserKey, succKey))
		}
		if prefixChanged || i.cmp(i.ikv.K.UserKey, succKey) >= 0 {
			// Prefix has changed.
			if hiddenPoint {
				return i.Next()
			}
			if invariants.Enabled && !i.lazyValueHandling.hasValuePrefix {
				panic(errors.AssertionFailedf("nextPrefixV3 being run for non-v3 sstable"))
			}
			if i.ikv.K.Kind() != base.InternalKeyKindSet {
				i.ikv.V = base.MakeInPlaceValue(i.val)
			} else if i.lazyValueHandling.getValue == nil || block.ValuePrefix(i.val[0]).IsInPlaceValue() {
				i.ikv.V = base.MakeInPlaceValue(i.val[1:])
			} else {
				i.ikv.V = i.lazyValueHandling.getValue.GetInternalValueForPrefixAndValueHandle(i.val)
			}
			return &i.ikv
		}
		// Else prefix has not changed.

		if nextCmpCount >= nextCmpThresholdBeforeSeek {
			break
		}
	}
	return i.SeekGE(succKey, base.SeekGEFlagsNone)
}

// Prev implements internalIterator.Prev, as documented in the pebble
// package.
func (i *Iter) Prev() *base.InternalKV {
start:
	for n := len(i.cached) - 1; n >= 0; n-- {
		i.nextOffset = i.offset
		e := &i.cached[n]
		i.offset = e.offset
		i.val = unsafe.Slice((*byte)(unsafe.Add(i.ptr, e.valStart)), int(e.valSize))
		// Manually inlined version of i.decodeInternalKey(i.key).
		i.key = i.cachedBuf[e.keyStart:e.keyEnd]
		if n := len(i.key) - 8; n >= 0 {
			trailer := base.InternalKeyTrailer(binary.LittleEndian.Uint64(i.key[n:]))
			hiddenPoint := i.transforms.HideObsoletePoints &&
				(trailer&TrailerObsoleteBit != 0)
			if hiddenPoint {
				continue
			}
			i.ikv.K = base.InternalKey{
				Trailer: trailer & TrailerObsoleteMask,
				UserKey: i.key[:n:n],
			}
			if n := i.transforms.SyntheticSeqNum; n != 0 {
				i.ikv.K.SetSeqNum(base.SeqNum(n))
			}
			if i.transforms.HasSyntheticSuffix() {
				// Inlined version of i.maybeReplaceSuffix()
				prefixLen := i.split(i.ikv.K.UserKey)
				// If ikey is cached or may get cached, we must de-reference
				// UserKey before suffix replacement.
				i.synthSuffixBuf = append(i.synthSuffixBuf[:0], i.ikv.K.UserKey[:prefixLen]...)
				i.synthSuffixBuf = append(i.synthSuffixBuf, i.transforms.SyntheticSuffix()...)
				i.ikv.K.UserKey = i.synthSuffixBuf
			}
		} else {
			i.ikv.K.Trailer = base.InternalKeyTrailer(base.InternalKeyKindInvalid)
			i.ikv.K.UserKey = nil
		}
		i.cached = i.cached[:n]
		if !i.lazyValueHandling.hasValuePrefix ||
			i.ikv.K.Kind() != base.InternalKeyKindSet {
			i.ikv.V = base.MakeInPlaceValue(i.val)
		} else if i.lazyValueHandling.getValue == nil || block.ValuePrefix(i.val[0]).IsInPlaceValue() {
			i.ikv.V = base.MakeInPlaceValue(i.val[1:])
		} else {
			i.ikv.V = i.lazyValueHandling.getValue.GetInternalValueForPrefixAndValueHandle(i.val)
		}
		return &i.ikv
	}

	i.clearCache()
	if i.offset <= 0 {
		i.offset = -1
		i.nextOffset = 0
		return nil
	}

	targetOffset := i.offset
	var index int32

	{
		// NB: manually inlined sort.Sort is ~5% faster.
		//
		// Define f(-1) == false and f(n) == true.
		// Invariant: f(index-1) == false, f(upper) == true.
		upper := i.numRestarts
		for index < upper {
			h := int32(uint(index+upper) >> 1) // avoid overflow when computing h

			// index ≤ h < upper
			offset := decodeRestart(i.data[i.restarts+4*offsetInBlock(h):])
			if offset < targetOffset {
				// Looking for the first restart that has offset >= targetOffset, so
				// ignore h and earlier.
				index = h + 1 // preserves f(i-1) == false
			} else {
				upper = h // preserves f(j) == true
			}
		}
		// index == upper, f(index-1) == false, and f(upper) (= f(index)) == true
		// => answer is index.
	}

	// index is first restart with offset >= targetOffset. Note that
	// targetOffset may not be at a restart point since one can call Prev()
	// after Next() (so the cache was not populated) and targetOffset refers to
	// the current entry. index-1 must have an offset < targetOffset (it can't
	// be equal to targetOffset since the binary search would have selected that
	// as the index).
	i.offset = 0
	if index > 0 {
		i.offset = decodeRestart(i.data[i.restarts+4*offsetInBlock(index-1):])
	}
	// TODO(sumeer): why is the else case not an error given targetOffset is a
	// valid offset.

	i.readEntry()

	// We stop when i.nextOffset == targetOffset since the targetOffset is the
	// entry we are stepping back from, and we don't need to cache the entry
	// before it, since it is the candidate to return.
	for i.nextOffset < targetOffset {
		i.cacheEntry()
		i.offset = i.nextOffset
		i.readEntry()
	}

	hiddenPoint := i.decodeInternalKey(i.key)
	if hiddenPoint {
		// Use the cache.
		goto start
	}
	if i.transforms.HasSyntheticSuffix() {
		// Inlined version of i.maybeReplaceSuffix()
		prefixLen := i.split(i.ikv.K.UserKey)
		// If ikey is cached or may get cached, we must de-reference
		// UserKey before suffix replacement.
		i.synthSuffixBuf = append(i.synthSuffixBuf[:0], i.ikv.K.UserKey[:prefixLen]...)
		i.synthSuffixBuf = append(i.synthSuffixBuf, i.transforms.SyntheticSuffix()...)
		i.ikv.K.UserKey = i.synthSuffixBuf
	}
	if !i.lazyValueHandling.hasValuePrefix ||
		i.ikv.K.Kind() != base.InternalKeyKindSet {
		i.ikv.V = base.MakeInPlaceValue(i.val)
	} else if i.lazyValueHandling.getValue == nil || block.ValuePrefix(i.val[0]).IsInPlaceValue() {
		i.ikv.V = base.MakeInPlaceValue(i.val[1:])
	} else {
		i.ikv.V = i.lazyValueHandling.getValue.GetInternalValueForPrefixAndValueHandle(i.val)
	}
	return &i.ikv
}

// KV returns the internal KV at the current iterator position.
func (i *Iter) KV() *base.InternalKV {
	return &i.ikv
}

// Error implements internalIterator.Error, as documented in the pebble
// package.
func (i *Iter) Error() error {
	return nil // infallible
}

// Close implements internalIterator.Close, as documented in the pebble
// package.
func (i *Iter) Close() error {
	i.handle.Release()
	fullKey := i.fullKey[:0]
	cached := i.cached[:0]
	cachedBuf := i.cachedBuf[:0]
	firstUserKeyWithPrefixBuf := i.firstUserKeyWithPrefixBuf[:0]
	*i = Iter{
		fullKey:                   fullKey,
		cached:                    cached,
		cachedBuf:                 cachedBuf,
		firstUserKeyWithPrefixBuf: firstUserKeyWithPrefixBuf,
	}
	return nil
}

// SetBounds implements base.InternalIterator. It panics, as bounds should
// always be handled the by the parent sstable iterator.
func (i *Iter) SetBounds(lower, upper []byte) {
	// This should never be called as bounds are handled by sstable.Iterator.
	panic("pebble: SetBounds unimplemented")
}

// SetContext implements base.InternalIterator.
func (i *Iter) SetContext(_ context.Context) {}

// Valid returns true if the iterator is currently positioned at a valid KV.
func (i *Iter) Valid() bool {
	return i.offset >= 0 && i.offset < i.restarts
}

// DebugTree is part of the InternalIterator interface.
func (i *Iter) DebugTree(tp treeprinter.Node) {
	tp.Childf("%T(%p)", i, i)
}

func (i *Iter) getRestart(idx int) offsetInBlock {
	return offsetInBlock(binary.LittleEndian.Uint32(i.data[i.restarts+4*offsetInBlock(idx):]))
}

func (i *Iter) isRestartPoint() bool {
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
	// IsRestart is true if the key is a restart point.
	IsRestart bool
	// Offset is the position within the block at which the key-value pair is
	// encoded.
	Offset offsetInBlock
	// Length is the total length of the KV pair as it is encoded in the block
	// format.
	Length int32
	// KeyShared is the number of bytes this KV's user key shared with its predecessor.
	KeyShared uint32
	// KeyUnshared is the number of bytes this KV's user key did not share with
	// its predecessor.
	KeyUnshared uint32
	// ValueLen is the length of the internal value.
	ValueLen uint32
}

// Describe describes the contents of a block, writing the description to w.
// It invokes fmtKV to describe each key-value pair.
func (i *Iter) Describe(tp treeprinter.Node, fmtKV DescribeKV) {
	var buf bytes.Buffer
	for kv := i.First(); kv != nil; kv = i.Next() {
		enc := KVEncoding{
			IsRestart: i.isRestartPoint(),
			Offset:    i.offset,
			Length:    int32(i.nextOffset - i.offset),
		}
		ptr := unsafe.Add(i.ptr, i.offset)
		enc.KeyShared, ptr = decodeVarint(ptr)
		enc.KeyUnshared, ptr = decodeVarint(ptr)
		enc.ValueLen, _ = decodeVarint(ptr)
		buf.Reset()
		lv := kv.V.LazyValue()
		fmtKV(&buf, &kv.K, lv.ValueOrHandle, enc)
		tp.Child(buf.String())
	}
	// Format the restart points.
	n := tp.Child("restart points")
	// Format the restart points.
	for j := 0; j < int(i.numRestarts); j++ {
		offset := i.getRestart(j)
		n.Childf("%05d [restart %d]", uint64(i.restarts+4*offsetInBlock(j)), offset)
	}
}

// RawIter is an iterator over a single block of data. Unlike blockIter,
// keys are stored in "raw" format (i.e. not as internal keys). Note that there
// is significant similarity between this code and the code in blockIter. Yet
// reducing duplication is difficult due to the blockIter being performance
// critical. RawIter must only be used for blocks where the value is
// stored together with the key.
type RawIter struct {
	cmp         base.Compare
	offset      offsetInBlock
	nextOffset  offsetInBlock
	restarts    offsetInBlock
	numRestarts int32
	ptr         unsafe.Pointer
	data        []byte
	key, val    []byte
	ikey        base.InternalKey
	cached      []blockEntry
	cachedBuf   []byte
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
	i.restarts = offsetInBlock(len(blk)) - 4*(1+offsetInBlock(numRestarts))
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
	i.key = append(i.key[:shared], unsafe.Slice((*byte)(ptr), int(unshared))...)
	i.key = i.key[:len(i.key):len(i.key)]
	ptr = unsafe.Add(ptr, unshared)
	i.val = unsafe.Slice((*byte)(ptr), int(value))
	i.nextOffset = offsetInBlock(uintptr(ptr)-uintptr(i.ptr)) + offsetInBlock(value)
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
	var valStart offsetInBlock
	valSize := uint32(len(i.val))
	if valSize > 0 {
		valStart = offsetInBlock(uintptr(unsafe.Pointer(&i.val[0])) - uintptr(i.ptr))
	}

	i.cached = append(i.cached, blockEntry{
		offset:   i.offset,
		keyStart: offsetInBlock(len(i.cachedBuf)),
		keyEnd:   offsetInBlock(len(i.cachedBuf) + len(i.key)),
		valStart: valStart,
		valSize:  valSize,
	})
	i.cachedBuf = append(i.cachedBuf, i.key...)
}

// SeekGE repositions the iterator to point to the first KV with a user key
// greater than or equal to key. It returns whether the iterator is valid.
func (i *RawIter) SeekGE(key []byte) bool {
	// Find the index of the smallest restart point whose key is > the key
	// sought; index will be numRestarts if there is no such restart point.
	i.offset = 0
	index := sort.Search(int(i.numRestarts), func(j int) bool {
		offset := int32(binary.LittleEndian.Uint32(i.data[int(i.restarts)+4*j:]))
		// For a restart point, there are 0 bytes shared with the previous key.
		// The varint encoding of 0 occupies 1 byte.
		ptr := unsafe.Add(i.ptr, offset+1)
		// Decode the key at that restart point, and compare it to the key sought.
		v1, ptr := decodeVarint(ptr)
		_, ptr = decodeVarint(ptr)
		s := unsafe.Slice((*byte)(ptr), int(v1))
		return i.cmp(key, s) < 0
	})

	// Since keys are strictly increasing, if index > 0 then the restart point at
	// index-1 will be the largest whose key is <= the key sought.  If index ==
	// 0, then all keys in this block are larger than the key sought, and offset
	// remains at zero.
	if index > 0 {
		i.offset = offsetInBlock(binary.LittleEndian.Uint32(i.data[int(i.restarts)+4*(index-1):]))
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

// First repositions the iterator so that it is pointing to the first key in the
// block, returning whether or not the iterator is still valid.
func (i *RawIter) First() bool {
	i.offset = 0
	i.loadEntry()
	return i.Valid()
}

// Last repositions the iterator so that it is pointing to the last key in the
// block, returning whether or not the iterator is still valid.
func (i *RawIter) Last() bool {
	// Seek forward from the last restart point.
	i.offset = offsetInBlock(binary.LittleEndian.Uint32(i.data[i.restarts+4*offsetInBlock(i.numRestarts-1):]))

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

// Next repositions the iterator one step forward, returning whether or not the
// iterator is still valid.
func (i *RawIter) Next() bool {
	i.offset = i.nextOffset
	if !i.Valid() {
		return false
	}
	i.loadEntry()
	return true
}

// Prev repositions the iterator one step backwards, returning whether or not
// the iterator is still valid.
func (i *RawIter) Prev() bool {
	if n := len(i.cached) - 1; n > 0 && i.cached[n].offset == i.offset {
		i.nextOffset = i.offset
		e := &i.cached[n-1]
		i.offset = e.offset
		i.val = unsafe.Slice((*byte)(unsafe.Add(i.ptr, e.valStart)), int(e.valSize))
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
		offset := offsetInBlock(binary.LittleEndian.Uint32(i.data[int(i.restarts)+4*j:]))
		return offset >= targetOffset
	})
	i.offset = 0
	if index > 0 {
		i.offset = offsetInBlock(binary.LittleEndian.Uint32(i.data[int(i.restarts)+4*(index-1):]))
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

// Key returns the key at the current iterator position. The iterator must be
// Valid().
func (i *RawIter) Key() base.InternalKey {
	return i.ikey
}

// Value returns the value at the current iterator position. The iterator must
// be Valid().
func (i *RawIter) Value() []byte {
	return i.val
}

// Valid returns true if the iterator is currently positioned at a valid KV.
func (i *RawIter) Valid() bool {
	return i.offset >= 0 && i.offset < i.restarts
}

// Close implements closes the iterator.
func (i *RawIter) Close() error {
	i.val = nil
	return nil
}

// DebugTree is part of the InternalIterator interface.
func (i *RawIter) DebugTree(tp treeprinter.Node) {
	tp.Childf("%T(%p)", i, i)
}

func (i *RawIter) getRestart(idx int) offsetInBlock {
	return offsetInBlock(binary.LittleEndian.Uint32(i.data[i.restarts+4*offsetInBlock(idx):]))
}

func (i *RawIter) isRestartPoint() bool {
	j := sort.Search(int(i.numRestarts), func(j int) bool {
		return i.getRestart(j) >= i.offset
	})
	return j < int(i.numRestarts) && i.getRestart(j) == i.offset
}

// Describe describes the contents of a block, writing the description to w.
// It invokes fmtKV to describe each key-value pair.
func (i *RawIter) Describe(tp treeprinter.Node, fmtKV DescribeKV) {
	var buf bytes.Buffer
	for valid := i.First(); valid; valid = i.Next() {
		enc := KVEncoding{
			IsRestart: i.isRestartPoint(),
			Offset:    i.offset,
			Length:    int32(i.nextOffset - i.offset),
		}
		ptr := unsafe.Add(i.ptr, i.offset)
		enc.KeyShared, ptr = decodeVarint(ptr)
		enc.KeyUnshared, ptr = decodeVarint(ptr)
		enc.ValueLen, _ = decodeVarint(ptr)
		buf.Reset()
		fmtKV(&buf, &i.ikey, i.val, enc)
		if i.isRestartPoint() {
			buf.WriteString(" [restart]")
		}
		tp.Child(buf.String())
	}
	n := tp.Child("restart points")
	// Format the restart points.
	for j := 0; j < int(i.numRestarts); j++ {
		offset := i.getRestart(j)
		n.Childf("%05d [restart %d]", uint64(i.restarts+4*offsetInBlock(j)), offset)
	}
}

// All returns an iterator that ranges over all key-value pairs in the block.
func (i *RawIter) All() iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		for valid := i.First(); valid; valid = i.Next() {
			if !yield(i.Key().UserKey, i.Value()) {
				return
			}
		}
	}
}

func decodeVarint(ptr unsafe.Pointer) (uint32, unsafe.Pointer) {
	if a := *((*uint8)(ptr)); a < 128 {
		return uint32(a),
			unsafe.Add(ptr, 1)
	} else if a, b := a&0x7f, *((*uint8)(unsafe.Add(ptr, 1))); b < 128 {
		return uint32(b)<<7 | uint32(a),
			unsafe.Add(ptr, 2)
	} else if b, c := b&0x7f, *((*uint8)(unsafe.Add(ptr, 2))); c < 128 {
		return uint32(c)<<14 | uint32(b)<<7 | uint32(a),
			unsafe.Add(ptr, 3)
	} else if c, d := c&0x7f, *((*uint8)(unsafe.Add(ptr, 3))); d < 128 {
		return uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a),
			unsafe.Add(ptr, 4)
	} else {
		d, e := d&0x7f, *((*uint8)(unsafe.Add(ptr, 4)))
		return uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a),
			unsafe.Add(ptr, 5)
	}
}
