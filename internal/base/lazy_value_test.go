// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import (
	"bytes"
	"context"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

type valueFetcherFunc func(
	handle []byte, blobFileID BlobFileID, valLen uint32, buf []byte) (val []byte, callerOwned bool, err error)

func (v valueFetcherFunc) FetchHandle(
	ctx context.Context, handle []byte, blobFileID BlobFileID, valLen uint32, buf []byte,
) (val []byte, callerOwned bool, err error) {
	return v(handle, blobFileID, valLen, buf)
}

func TestLazyValue(t *testing.T) {
	// Both 40 and 48 bytes makes iteration benchmarks like
	// BenchmarkIteratorScan/keys=1000,r-amp=1,key-types=points-only 75%
	// slower.
	require.True(t, unsafe.Sizeof(LazyValue{}) <= 32)

	fooBytes1 := []byte("foo")
	fooLV1 := LazyValue{ValueOrHandle: fooBytes1}
	require.Equal(t, 3, fooLV1.Len())
	_, hasAttr := fooLV1.TryGetShortAttribute()
	require.False(t, hasAttr)
	fooLV2, fooBytes2 := fooLV1.Clone(nil, &LazyFetcher{})
	require.Equal(t, 3, fooLV2.Len())
	_, hasAttr = fooLV2.TryGetShortAttribute()
	require.False(t, hasAttr)
	require.Equal(t, fooLV1.ValueOrHandle, fooLV2.ValueOrHandle)
	getValue := func(lv LazyValue, expectedCallerOwned bool) []byte {
		v, callerOwned, err := lv.Value(nil)
		require.NoError(t, err)
		require.Equal(t, expectedCallerOwned, callerOwned)
		return v
	}
	require.Equal(t, getValue(fooLV1, false), getValue(fooLV2, false))
	fooBytes2[0] = 'b'
	require.False(t, bytes.Equal(fooLV1.ValueOrHandle, fooLV2.ValueOrHandle))

	for _, callerOwned := range []bool{false, true} {
		numCalls := 0
		fooLV3 := LazyValue{
			ValueOrHandle: []byte("foo-handle"),
			Fetcher: &LazyFetcher{
				Fetcher: valueFetcherFunc(
					func(handle []byte, blobFileID BlobFileID, valLen uint32, buf []byte) ([]byte, bool, error) {
						numCalls++
						require.Equal(t, []byte("foo-handle"), handle)
						require.Equal(t, uint32(3), valLen)
						require.Equal(t, BlobFileID(90), blobFileID)
						return fooBytes1, callerOwned, nil
					}),
				Attribute:  AttributeAndLen{ValueLen: 3, ShortAttribute: 7},
				BlobFileID: 90,
			},
		}
		require.Equal(t, []byte("foo"), getValue(fooLV3, callerOwned))
		require.Equal(t, 1, numCalls)
		require.Equal(t, []byte("foo"), getValue(fooLV3, callerOwned))
		require.Equal(t, 2, numCalls)
		require.Equal(t, 3, fooLV3.Len())
		attr, hasAttr := fooLV3.TryGetShortAttribute()
		require.True(t, hasAttr)
		require.Equal(t, ShortAttribute(7), attr)
	}
}
