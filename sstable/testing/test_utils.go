// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package testing

import (
	"context"

	"github.com/cockroachdb/pebble/v2/internal/base"
	"github.com/cockroachdb/pebble/v2/internal/keyspan"
	"github.com/cockroachdb/pebble/v2/internal/testutils"
	"github.com/cockroachdb/pebble/v2/objstorage"
	"github.com/cockroachdb/pebble/v2/sstable"
)

// ReadAll returns all point keys, range del spans, and range key spans from an
// sstable. Closes the Readable. Panics on errors.
func ReadAll(
	r objstorage.Readable, ro sstable.ReaderOptions,
) (points []base.InternalKV, rangeDels, rangeKeys []keyspan.Span) {
	reader := testutils.CheckErr(sstable.NewReader(context.Background(), r, ro))
	defer reader.Close()
	pointIter := testutils.CheckErr(reader.NewIter(sstable.NoTransforms, nil /* lower */, nil /* upper */))
	defer pointIter.Close()

	for kv := pointIter.First(); kv != nil; kv = pointIter.Next() {
		val, _ := testutils.CheckErr2(kv.Value(nil))
		points = append(points, base.InternalKV{
			K: kv.K.Clone(),
			V: base.MakeInPlaceValue(val),
		})
	}

	ctx := context.Background()
	if rangeDelIter := testutils.CheckErr(reader.NewRawRangeDelIter(ctx, sstable.NoFragmentTransforms)); rangeDelIter != nil {
		defer rangeDelIter.Close()
		for s := testutils.CheckErr(rangeDelIter.First()); s != nil; s = testutils.CheckErr(rangeDelIter.Next()) {
			rangeDels = append(rangeDels, s.Clone())
		}
	}

	if rangeKeyIter := testutils.CheckErr(reader.NewRawRangeKeyIter(ctx, sstable.NoFragmentTransforms)); rangeKeyIter != nil {
		defer rangeKeyIter.Close()
		for s := testutils.CheckErr(rangeKeyIter.First()); s != nil; s = testutils.CheckErr(rangeKeyIter.Next()) {
			rangeKeys = append(rangeKeys, s.Clone())
		}
	}
	return points, rangeDels, rangeKeys
}
