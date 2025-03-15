// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"context"

	"github.com/cockroachdb/pebble/v2/internal/base"
	"github.com/cockroachdb/pebble/v2/internal/keyspan"
	"github.com/cockroachdb/pebble/v2/objstorage"
)

// ReadAll returns all point keys, range del spans, and range key spans from an
// sstable. Closes the Readable. Panics on errors.
func ReadAll(
	r objstorage.Readable, ro ReaderOptions,
) (points []base.InternalKV, rangeDels, rangeKeys []keyspan.Span, err error) {
	reader, err := NewReader(context.Background(), r, ro)
	if err != nil {
		return nil, nil, nil, err
	}
	defer reader.Close()
	pointIter, err := reader.NewIter(NoTransforms, nil /* lower */, nil /* upper */)
	if err != nil {
		return nil, nil, nil, err
	}
	defer pointIter.Close()

	for kv := pointIter.First(); kv != nil; kv = pointIter.Next() {
		val, _, err := kv.Value(nil)
		if err != nil {
			return nil, nil, nil, err
		}
		points = append(points, base.InternalKV{
			K: kv.K.Clone(),
			V: base.MakeInPlaceValue(val),
		})
	}

	ctx := context.Background()
	rangeDelIter, err := reader.NewRawRangeDelIter(ctx, NoFragmentTransforms)
	if err != nil {
		return nil, nil, nil, err
	}
	if rangeDelIter != nil {
		defer rangeDelIter.Close()
		s, err := rangeDelIter.First()
		if err != nil {
			return nil, nil, nil, err
		}
		for s != nil {
			rangeDels = append(rangeDels, s.Clone())
			s, err = rangeDelIter.Next()
			if err != nil {
				return nil, nil, nil, err
			}
		}
	}

	rangeKeyIter, err := reader.NewRawRangeKeyIter(ctx, NoFragmentTransforms)
	if err != nil {
		return nil, nil, nil, err
	}
	if rangeKeyIter != nil {
		defer rangeKeyIter.Close()
		s, err := rangeKeyIter.First()
		if err != nil {
			return nil, nil, nil, err
		}
		for s != nil {
			rangeKeys = append(rangeKeys, s.Clone())
			s, err = rangeKeyIter.Next()
			if err != nil {
				return nil, nil, nil, err
			}
		}
	}
	return points, rangeDels, rangeKeys, nil
}
