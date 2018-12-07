// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"bytes"

	"github.com/petermattis/pebble"
	"github.com/petermattis/pebble/db"
)

// MVCC encoding and decoding routines adapted from CockroachDB sources. Used
// to perform apples-to-apples benchmarking for CockroachDB's usage of RocksDB.

var mvccComparer = &db.Comparer{
	Compare: mvccCompare,

	InlineKey: func(k []byte) uint64 {
		key, _, ok := mvccSplitKey(k)
		if !ok {
			return 0
		}
		var v uint64
		n := 8
		if n > len(key) {
			n = len(key)
		}
		for _, b := range key[:n] {
			v <<= 8
			v |= uint64(b)
		}
		return v
	},

	Separator: func(dst, a, b []byte) []byte {
		return append(dst, a...)
	},

	Successor: func(dst, a []byte) []byte {
		return append(dst, a...)
	},

	Name: "cockroach_comparator",
}

func mvccSplitKey(mvccKey []byte) (key []byte, ts []byte, ok bool) {
	if len(mvccKey) == 0 {
		return nil, nil, false
	}
	n := len(mvccKey) - 1
	tsLen := int(mvccKey[n])
	if n < tsLen {
		return nil, nil, false
	}
	key = mvccKey[:n-tsLen]
	if tsLen > 0 {
		ts = mvccKey[n-tsLen+1 : len(mvccKey)-1]
	}
	return key, ts, true
}

func mvccCompare(a, b []byte) int {
	aKey, aTS, aOK := mvccSplitKey(a)
	bKey, bTS, bOK := mvccSplitKey(b)
	if !aOK || !bOK {
		// This should never happen unless there is some sort of corruption of
		// the keys.
		return bytes.Compare(a, b)
	}
	if c := bytes.Compare(aKey, bKey); c != 0 {
		return c
	}
	if len(aTS) == 0 {
		if len(bTS) == 0 {
			return 0
		}
		return -1
	} else if len(bTS) == 0 {
		return +1
	}
	return bytes.Compare(bTS, aTS)
}

// <key>\x00[<wall_time>[<logical>]]<#timestamp-bytes>
func mvccEncode(dst, key []byte, walltime uint64, logical uint32) []byte {
	dst = append(dst, key...)
	dst = append(dst, 0)
	if walltime != 0 || logical != 0 {
		extra := byte(1 + 8)
		dst = encodeUint64Ascending(dst, walltime)
		if logical != 0 {
			dst = encodeUint32Ascending(dst, logical)
			extra += 4
		}
		dst = append(dst, extra)
	}
	return dst
}

func mvccForwardScan(d *pebble.DB, start, end, ts []byte) int {
	it := d.NewIter(&db.IterOptions{
		LowerBound: mvccEncode(nil, start, 0, 0),
		UpperBound: mvccEncode(nil, end, 0, 0),
	})
	defer it.Close()

	var data byteAllocator
	var count int

	for it.First(); it.Valid(); it.Next() {
		key, keyTS, _ := mvccSplitKey(it.Key())
		if bytes.Compare(keyTS, ts) <= 0 {
			data, _ = data.Copy(key)
			data, _ = data.Copy(it.Value())
		}
		count++
	}
	return count
}

func mvccReverseScan(d *pebble.DB, start, end, ts []byte) int {
	it := d.NewIter(&db.IterOptions{
		LowerBound: mvccEncode(nil, start, 0, 0),
		UpperBound: mvccEncode(nil, end, 0, 0),
	})
	defer it.Close()

	var data byteAllocator
	var count int

	for it.Last(); it.Valid(); it.Prev() {
		key, keyTS, _ := mvccSplitKey(it.Key())
		if bytes.Compare(keyTS, ts) <= 0 {
			data, _ = data.Copy(key)
			data, _ = data.Copy(it.Value())
		}
		count++
	}
	return count
}

// byteAllocator provides chunk allocation of []byte, amortizing the overhead
// of each allocation. Because the underlying storage for the slices is shared,
// they should share a similar lifetime in order to avoid pinning large amounts
// of memory unnecessarily. The allocator itself is a []byte where cap()
// indicates the total amount of memory and len() is the amount already
// allocated. The size of the buffer to allocate from is grown exponentially
// when it runs out of room up to a maximum size (chunkAllocMaxSize).
type byteAllocator []byte

const chunkAllocMinSize = 512
const chunkAllocMaxSize = 16384

func (a byteAllocator) reserve(n int) byteAllocator {
	allocSize := cap(a) * 2
	if allocSize < chunkAllocMinSize {
		allocSize = chunkAllocMinSize
	} else if allocSize > chunkAllocMaxSize {
		allocSize = chunkAllocMaxSize
	}
	if allocSize < n {
		allocSize = n
	}
	return make([]byte, 0, allocSize)
}

// Alloc allocates a new chunk of memory with the specified length.
func (a byteAllocator) Alloc(n int) (byteAllocator, []byte) {
	if cap(a)-len(a) < n {
		a = a.reserve(n)
	}
	p := len(a)
	r := a[p : p+n : p+n]
	a = a[:p+n]
	return a, r
}

// Copy allocates a new chunk of memory, initializing it from src.
func (a byteAllocator) Copy(src []byte) (byteAllocator, []byte) {
	var alloc []byte
	a, alloc = a.Alloc(len(src))
	copy(alloc, src)
	return a, alloc
}
