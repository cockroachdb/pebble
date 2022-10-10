// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import (
	"bytes"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestLazyValue(t *testing.T) {
	// Both 40 and 48 bytes makes iteration benchmarks like
	// BenchmarkIteratorScan/keys=1000,r-amp=1,key-types=points-only 75%
	// slower.
	require.Equal(t, 32, int(unsafe.Sizeof(LazyValue{})))

	fooBytes1 := []byte("foo")
	fooLV1 := MakeInPlaceValue(fooBytes1)
	require.Equal(t, 3, fooLV1.Len())
	fooLV2, fooBytes2 := fooLV1.Clone(nil)
	require.Equal(t, 3, fooLV2.Len())
	require.Equal(t, fooLV1.InPlaceValue(), fooLV2.InPlaceValue())
	getValue := func(lv LazyValue, expectedCallerOwned bool) []byte {
		v, callerOwned, err := lv.Value(nil)
		require.NoError(t, err)
		require.Equal(t, expectedCallerOwned, callerOwned)
		return v
	}
	require.Equal(t, getValue(fooLV1, false), getValue(fooLV2, false))
	fooBytes2[0] = 'b'
	require.False(t, bytes.Equal(fooLV1.InPlaceValue(), fooLV2.InPlaceValue()))

	for _, callerOwned := range []bool{false, true} {
		numCalls := 0
		fooLV3 := LazyValue{
			ValueOrHandle: []byte("foo-handle"),
			Fetcher: &LazyFetcher{
				ValueFetcher: func(handle []byte, buf []byte) ([]byte, bool, error) {
					numCalls++
					require.Equal(t, []byte("foo-handle"), handle)
					return fooBytes1, callerOwned, nil
				},
				Attribute: AttributeAndLen{ValueLen: 3, ShortAttribute: 7},
			},
		}
		require.Equal(t, []byte("foo"), getValue(fooLV3, callerOwned))
		require.Equal(t, 1, numCalls)
		require.Equal(t, []byte("foo"), getValue(fooLV3, callerOwned))
		require.Equal(t, 1, numCalls)
		require.Equal(t, 3, fooLV3.Len())
	}
}
