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

	// MakeSuffixMaskBlockPropertyFilter creates a filter that skips blocks
	// where all MVCC wall times are strictly greater than the bound's wall
	// time. Internally it calls NewMVCCTimeIntervalFilter(0, wall).

	bound := testEncodeMVCCSuffix(100, 0)
	filter := MakeSuffixMaskBlockPropertyFilter(bound)
	require.True(t, filter != nil)

	// The filter should have the MVCCTimeInterval collector name.
	require.Equal(t, "MVCCTimeInterval", filter.Name())

	// Encode a block property interval representing a block with wall times
	// in [50, 80). This should intersect a filter with range [0, 101).
	prop := encodeTestBlockInterval(50, 80)
	intersects, err := filter.Intersects(prop)
	require.NoError(t, err)
	require.True(t, intersects)

	// A block with wall times in [150, 200) does not intersect the filter
	// range [0, 101) — the block is entirely above the bound.
	prop2 := encodeTestBlockInterval(150, 200)
	intersects2, err := filter.Intersects(prop2)
	require.NoError(t, err)
	require.False(t, intersects2)

	// A block with wall times in [90, 110) partially overlaps [0, 101).
	prop3 := encodeTestBlockInterval(90, 110)
	intersects3, err := filter.Intersects(prop3)
	require.NoError(t, err)
	require.True(t, intersects3)

	// Empty bound should return nil filter.
	require.True(t, MakeSuffixMaskBlockPropertyFilter(nil) == nil)

	// A suffix with wall=0 should return nil (no meaningful timestamp).
	require.True(t, MakeSuffixMaskBlockPropertyFilter(testEncodeMVCCSuffix(0, 0)) == nil)
}

// encodeTestBlockInterval encodes a block property interval [lower, upper) in
// the same format used by BlockIntervalCollector: two uvarints, the first
// being Lower and the second being (Upper - Lower).
func encodeTestBlockInterval(lower, upper uint64) []byte {
	buf := binary.AppendUvarint(nil, lower)
	buf = binary.AppendUvarint(buf, upper-lower)
	return buf
}

// makeUserProps wraps an encoded block interval in a single-entry user-
// properties map keyed by the MVCCTimeInterval collector name. The leading
// byte (the collector's shortID) is arbitrary; SuffixRangeIntersectsTable
// only skips it.
func makeUserProps(interval []byte) map[string]string {
	buf := make([]byte, 0, len(interval)+1)
	buf = append(buf, 0) // shortID
	buf = append(buf, interval...)
	return map[string]string{mvccWallTimeIntervalCollector: string(buf)}
}

func TestSuffixRangeIntersectsTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Helper: shorthand for the [lower, upper) inputs to
	// SuffixRangeIntersectsTable. Lower has the newer (larger) wall;
	// upper has the older (smaller) wall.
	mkRange := func(lowerWall, upperWall uint64) ([]byte, []byte) {
		return testEncodeMVCCSuffix(lowerWall, 0), testEncodeMVCCSuffix(upperWall, 0)
	}

	tests := []struct {
		name      string
		fileLower uint64
		fileUpper uint64
		lowerWall uint64
		upperWall uint64
		want      bool
	}{
		{
			// File walls [50, 80); mask (200, 400]. File band is entirely
			// older than the mask range. No intersection.
			name:      "file entirely below mask",
			fileLower: 50, fileUpper: 80,
			lowerWall: 400, upperWall: 200,
			want: false,
		},
		{
			// File walls [500, 600); mask (200, 400]. File is entirely
			// newer. No intersection.
			name:      "file entirely above mask",
			fileLower: 500, fileUpper: 600,
			lowerWall: 400, upperWall: 200,
			want: false,
		},
		{
			// File walls [150, 250); mask (200, 400]. Overlap at walls
			// 201..249. Intersection.
			name:      "file straddles mask lower",
			fileLower: 150, fileUpper: 250,
			lowerWall: 400, upperWall: 200,
			want: true,
		},
		{
			// File walls [350, 500); mask (200, 400]. Overlap at walls
			// 350..400. Intersection.
			name:      "file straddles mask upper",
			fileLower: 350, fileUpper: 500,
			lowerWall: 400, upperWall: 200,
			want: true,
		},
		{
			// File maxWall == upperWall exactly. fileUpper = upperWall+1.
			// Mask (upperWall, lowerWall] excludes upperWall. No
			// intersection.
			name:      "file ends exactly at mask exclusive lower",
			fileLower: 100, fileUpper: 201, // maxWall = 200
			lowerWall: 400, upperWall: 200,
			want: false,
		},
		{
			// File maxWall == upperWall+1. fileUpper = upperWall+2.
			// Mask includes upperWall+1. Intersection.
			name:      "file just above mask exclusive lower",
			fileLower: 100, fileUpper: 202, // maxWall = 201
			lowerWall: 400, upperWall: 200,
			want: true,
		},
		{
			// File minWall == lowerWall. Mask includes lowerWall. Intersection.
			name:      "file starts at mask inclusive upper",
			fileLower: 400, fileUpper: 500,
			lowerWall: 400, upperWall: 200,
			want: true,
		},
		{
			// File minWall == lowerWall+1. Mask excludes anything > lowerWall.
			// No intersection.
			name:      "file starts just above mask inclusive upper",
			fileLower: 401, fileUpper: 500,
			lowerWall: 400, upperWall: 200,
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lower, upper := mkRange(tt.lowerWall, tt.upperWall)
			interval := encodeTestBlockInterval(tt.fileLower, tt.fileUpper)
			props := makeUserProps(interval)
			got := SuffixRangeIntersectsTable(props, lower, upper)
			require.Equal(t, tt.want, got)
		})
	}

	t.Run("missing property fails open", func(t *testing.T) {
		lower, upper := mkRange(400, 200)
		got := SuffixRangeIntersectsTable(map[string]string{}, lower, upper)
		require.True(t, got)
	})

	t.Run("nil bounds fail open", func(t *testing.T) {
		props := makeUserProps(encodeTestBlockInterval(50, 80))
		require.True(t, SuffixRangeIntersectsTable(props, nil, testEncodeMVCCSuffix(200, 0)))
		require.True(t, SuffixRangeIntersectsTable(props, testEncodeMVCCSuffix(400, 0), nil))
	})

	t.Run("inverted bounds fail open", func(t *testing.T) {
		// lowerWall < upperWall — degenerate. Should fail open.
		lower, upper := mkRange(100, 400) // lowerWall=100, upperWall=400
		props := makeUserProps(encodeTestBlockInterval(50, 80))
		require.True(t, SuffixRangeIntersectsTable(props, lower, upper))
	})

	t.Run("empty file interval", func(t *testing.T) {
		// An empty file interval (e.g., a file with no MVCC keys) cannot
		// intersect any range.
		lower, upper := mkRange(400, 200)
		// Empty interval has Lower == Upper; encoded as two uvarints
		// where the second is 0. The aggregate is empty.
		props := makeUserProps(nil)
		got := SuffixRangeIntersectsTable(props, lower, upper)
		// Aggregate decodes as empty BlockInterval; we treat empty as no
		// intersection (the file has no MVCC keys whose wall time the
		// mask could match).
		require.False(t, got)
	})
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
