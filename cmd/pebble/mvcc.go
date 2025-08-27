// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"bytes"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/cockroachkvs"
	"github.com/cockroachdb/pebble/v2/internal/bytealloc"
)

// MVCC routines adapted from CockroachDB sources. Used to perform
// apples-to-apples benchmarking for CockroachDB's usage of RocksDB.

func mvccForwardScan(d DB, start, end, ts []byte) (int, int64) {
	it := d.NewIter(&pebble.IterOptions{
		LowerBound: cockroachkvs.EncodeMVCCKey(nil, start, 0, 0),
		UpperBound: cockroachkvs.EncodeMVCCKey(nil, end, 0, 0),
	})
	defer func() { _ = it.Close() }()

	var data bytealloc.A
	var count int
	var nbytes int64

	for valid := it.First(); valid; valid = it.Next() {
		k := it.Key()
		si := cockroachkvs.Split(k)
		if bytes.Compare(k[si:], ts) <= 0 {
			data, _ = data.Copy(k[:si])
			data, _ = data.Copy(it.Value())
		}
		count++
		nbytes += int64(len(it.Key()) + len(it.Value()))
	}
	return count, nbytes
}

func mvccReverseScan(d DB, start, end, ts []byte) (int, int64) {
	it := d.NewIter(&pebble.IterOptions{
		LowerBound: cockroachkvs.EncodeMVCCKey(nil, start, 0, 0),
		UpperBound: cockroachkvs.EncodeMVCCKey(nil, end, 0, 0),
	})
	defer func() { _ = it.Close() }()

	var data bytealloc.A
	var count int
	var nbytes int64

	for valid := it.Last(); valid; valid = it.Prev() {
		k := it.Key()
		si := cockroachkvs.Split(k)
		if bytes.Compare(k[si:], ts) <= 0 {
			data, _ = data.Copy(k[:si])
			data, _ = data.Copy(it.Value())
		}
		count++
		nbytes += int64(len(it.Key()) + len(it.Value()))
	}
	return count, nbytes
}

var fauxMVCCMerger = &pebble.Merger{
	Name: "cockroach_merge_operator",
	Merge: func(key, value []byte) (pebble.ValueMerger, error) {
		// This merger is used by the compact benchmark and use the
		// pebble default value merger to concatenate values.
		// It shouldn't materially affect the benchmarks.
		return pebble.DefaultMerger.Merge(key, value)
	},
}
