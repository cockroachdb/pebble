// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package cockroachkvs

import (
	"encoding/binary"
	"testing"
	"unsafe"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/crlib/testutils/require"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/colblk"
)

func testEncodeMVCCSuffix(wallTime uint64, logical uint32) []byte {
	if wallTime == 0 && logical == 0 {
		return nil
	}
	if logical == 0 {
		buf := make([]byte, suffixLenWithWall)
		binary.BigEndian.PutUint64(buf, wallTime)
		buf[len(buf)-1] = suffixLenWithWall
		return buf
	}
	buf := make([]byte, suffixLenWithLogical)
	binary.BigEndian.PutUint64(buf, wallTime)
	binary.BigEndian.PutUint32(buf[8:], logical)
	buf[len(buf)-1] = suffixLenWithLogical
	return buf
}

func TestSuffixMaskBlockPropertyFilter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	bound := testEncodeMVCCSuffix(100, 0)
	filter := MakeSuffixMaskBlockPropertyFilter(bound)
	require.True(t, filter != nil)

	// Empty bound should return nil filter.
	require.True(t, MakeSuffixMaskBlockPropertyFilter(nil) == nil)
}

// initKeySeekerWithRow builds a data block containing a single key with the
// given roach key, wall time, and logical time, then initializes and returns a
// cockroachKeySeeker over that block.
func initKeySeekerWithRow(
	t *testing.T, roachKey []byte, wallTime uint64, logical uint32,
) *cockroachKeySeeker {
	t.Helper()
	var enc colblk.DataBlockEncoder
	enc.Init(&KeySchema, colblk.NoTieringColumns())
	k := makeMVCCKey(roachKey, wallTime, logical)
	kcmp := enc.KeyWriter.ComparePrev(k)
	ikey := base.MakeInternalKey(k, 0, base.InternalKeyKindSet)
	enc.Add(ikey, k, block.InPlaceValuePrefix(false), kcmp, false /* isObsolete */, base.KVMeta{})
	blk, _ := enc.Finish(1, enc.Size())

	var dec colblk.DataBlockDecoder
	bd := dec.Init(&KeySchema, blk)
	ks := &cockroachKeySeeker{}
	KeySchema.InitKeySeekerMetadata(
		(*colblk.KeySeekerMetadata)(unsafe.Pointer(ks)), &dec, bd,
	)
	return ks
}

func TestIsMaskedBySuffixMask(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name     string
		rowWall  uint64
		rowLogic uint32
		lower    []byte
		upper    []byte
		want     bool
	}{
		{
			name:    "row in mask range",
			rowWall: 50, rowLogic: 0,
			lower: testEncodeMVCCSuffix(100, 0), // inclusive
			upper: testEncodeMVCCSuffix(10, 0),  // exclusive
			want:  true,
		},
		{
			name:    "row at upper (exclusive, not masked)",
			rowWall: 10, rowLogic: 0,
			lower: testEncodeMVCCSuffix(100, 0),
			upper: testEncodeMVCCSuffix(10, 0),
			want:  false,
		},
		{
			name:    "row at lower (inclusive, masked)",
			rowWall: 100, rowLogic: 0,
			lower: testEncodeMVCCSuffix(100, 0),
			upper: testEncodeMVCCSuffix(10, 0),
			want:  true,
		},
		{
			name:    "row older than upper",
			rowWall: 5, rowLogic: 0,
			lower: testEncodeMVCCSuffix(100, 0),
			upper: testEncodeMVCCSuffix(10, 0),
			want:  false,
		},
		{
			name:    "row newer than lower",
			rowWall: 200, rowLogic: 0,
			lower: testEncodeMVCCSuffix(100, 0),
			upper: testEncodeMVCCSuffix(10, 0),
			want:  false,
		},
		{
			name:    "suffixless row is never masked",
			rowWall: 0, rowLogic: 0,
			lower: testEncodeMVCCSuffix(100, 0),
			upper: testEncodeMVCCSuffix(10, 0),
			want:  false,
		},
		{
			name:    "wall-only bounds, 9-byte suffix",
			rowWall: 50, rowLogic: 0,
			lower: testEncodeMVCCSuffix(100, 0),
			upper: testEncodeMVCCSuffix(10, 0),
			want:  true,
		},
		{
			name:    "wall+logical bounds, 13-byte suffix",
			rowWall: 50, rowLogic: 5,
			lower: testEncodeMVCCSuffix(100, 1),
			upper: testEncodeMVCCSuffix(10, 1),
			want:  true,
		},
		{
			name:    "equal wall at upper, row logical > upper logical",
			rowWall: 10, rowLogic: 5,
			lower: testEncodeMVCCSuffix(100, 0),
			upper: testEncodeMVCCSuffix(10, 3),
			want:  true, // row logical 5 > upper logical 3, so row > upper → masked
		},
		{
			name:    "equal wall at upper, row logical == upper logical",
			rowWall: 10, rowLogic: 3,
			lower: testEncodeMVCCSuffix(100, 0),
			upper: testEncodeMVCCSuffix(10, 3),
			want:  false, // upper is exclusive
		},
		{
			name:    "equal wall at upper, row logical < upper logical",
			rowWall: 10, rowLogic: 1,
			lower: testEncodeMVCCSuffix(100, 0),
			upper: testEncodeMVCCSuffix(10, 3),
			want:  false,
		},
		{
			name:    "short lower bound fails open",
			rowWall: 50, rowLogic: 0,
			lower: []byte{0x01, 0x02},
			upper: testEncodeMVCCSuffix(10, 0),
			want:  false,
		},
		{
			name:    "short upper bound fails open",
			rowWall: 50, rowLogic: 0,
			lower: testEncodeMVCCSuffix(100, 0),
			upper: []byte{0x01, 0x02},
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ks := initKeySeekerWithRow(t, []byte("key"), tt.rowWall, tt.rowLogic)
			got := ks.IsMaskedBySuffixMask(0, tt.lower, tt.upper)
			require.Equal(t, tt.want, got)
		})
	}
}
