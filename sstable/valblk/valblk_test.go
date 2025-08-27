// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package valblk

import (
	"fmt"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/pebble/v2/sstable/block"
	"github.com/stretchr/testify/require"
)

func TestHandleEncodeDecode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []Handle{
		{ValueLen: 23, BlockNum: 100003, OffsetInBlock: 2300},
		{ValueLen: math.MaxUint32 - 1, BlockNum: math.MaxUint32 / 2, OffsetInBlock: math.MaxUint32 - 2},
	}
	var buf [HandleMaxLen]byte
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%+v", tc), func(t *testing.T) {
			n := EncodeHandle(buf[:], tc)
			vh := DecodeHandle(buf[:n])
			require.Equal(t, tc, vh)
		})
	}
}

func TestValueBlocksIndexHandleEncodeDecode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []IndexHandle{
		{
			Handle: block.Handle{
				Offset: math.MaxUint64 / 2,
				Length: math.MaxUint64 / 4,
			},
			BlockNumByteLength:    53,
			BlockOffsetByteLength: math.MaxUint8,
			BlockLengthByteLength: math.MaxUint8 / 2,
		},
	}
	var buf [100]byte
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%+v", tc), func(t *testing.T) {
			n := EncodeIndexHandle(buf[:], tc)
			vbih, n2, err := DecodeIndexHandle(buf[:n])
			require.NoError(t, err)
			require.Equal(t, n, n2)
			require.Equal(t, tc, vbih)
		})
	}
}

func TestLittleEndianGetPut(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []uint64{
		0, (1 << 10) - 1, (1 << 25) + 1, math.MaxUint32, math.MaxUint64, rand.Uint64()}
	var buf [8]byte
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%d", tc), func(t *testing.T) {
			length := lenLittleEndian(tc)
			b := buf[:length:length]
			littleEndianPut(tc, b, length)
			v := littleEndianGet(b, length)
			require.Equal(t, tc, v)
		})
	}
}
