// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/stretchr/testify/require"
)

func TestValueHandleEncodeDecode(t *testing.T) {
	testCases := []valueHandle{
		{valueLen: 23, blockNum: 100003, offsetInBlock: 2300},
		{valueLen: math.MaxUint32 - 1, blockNum: math.MaxUint32 / 2, offsetInBlock: math.MaxUint32 - 2},
	}
	var buf [valueHandleMaxLen]byte
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%+v", tc), func(t *testing.T) {
			n := encodeValueHandle(buf[:], tc)
			vh := decodeValueHandle(buf[:n])
			require.Equal(t, tc, vh)
		})
	}
}

func TestValueBlocksIndexHandleEncodeDecode(t *testing.T) {
	testCases := []valueBlocksIndexHandle{
		{
			h: block.Handle{
				Offset: math.MaxUint64 / 2,
				Length: math.MaxUint64 / 4,
			},
			blockNumByteLength:    53,
			blockOffsetByteLength: math.MaxUint8,
			blockLengthByteLength: math.MaxUint8 / 2,
		},
	}
	var buf [valueBlocksIndexHandleMaxLen]byte
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%+v", tc), func(t *testing.T) {
			n := encodeValueBlocksIndexHandle(buf[:], tc)
			vbih, n2, err := decodeValueBlocksIndexHandle(buf[:n])
			require.NoError(t, err)
			require.Equal(t, n, n2)
			require.Equal(t, tc, vbih)
		})
	}
}

func TestLittleEndianGetPut(t *testing.T) {
	testCases := []uint64{
		0, (1 << 10) - 1, (1 << 25) + 1, math.MaxUint32, math.MaxUint64, uint64(rand.Int63())}
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
