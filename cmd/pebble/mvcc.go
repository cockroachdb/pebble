// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"bytes"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/bytealloc"
)

// MVCC encoding and decoding routines adapted from CockroachDB sources. Used
// to perform apples-to-apples benchmarking for CockroachDB's usage of RocksDB.

var mvccComparer = &pebble.Comparer{
	Compare: mvccCompare,

	AbbreviatedKey: func(k []byte) uint64 {
		key, _, ok := mvccSplitKey(k)
		if !ok {
			return 0
		}
		return pebble.DefaultComparer.AbbreviatedKey(key)
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

func mvccForwardScan(d DB, start, end, ts []byte) (int, int64) {
	it := d.NewIter(&pebble.IterOptions{
		LowerBound: mvccEncode(nil, start, 0, 0),
		UpperBound: mvccEncode(nil, end, 0, 0),
	})
	defer it.Close()

	var data bytealloc.A
	var count int
	var nbytes int64

	for valid := it.First(); valid; valid = it.Next() {
		key, keyTS, _ := mvccSplitKey(it.Key())
		if bytes.Compare(keyTS, ts) <= 0 {
			data, _ = data.Copy(key)
			data, _ = data.Copy(it.Value())
		}
		count++
		nbytes += int64(len(it.Key()) + len(it.Value()))
	}
	return count, nbytes
}

func mvccReverseScan(d DB, start, end, ts []byte) (int, int64) {
	it := d.NewIter(&pebble.IterOptions{
		LowerBound: mvccEncode(nil, start, 0, 0),
		UpperBound: mvccEncode(nil, end, 0, 0),
	})
	defer it.Close()

	var data bytealloc.A
	var count int
	var nbytes int64

	for valid := it.Last(); valid; valid = it.Prev() {
		key, keyTS, _ := mvccSplitKey(it.Key())
		if bytes.Compare(keyTS, ts) <= 0 {
			data, _ = data.Copy(key)
			data, _ = data.Copy(it.Value())
		}
		count++
		nbytes += int64(len(it.Key()) + len(it.Value()))
	}
	return count, nbytes
}
