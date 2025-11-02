// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metrics

import (
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/stretchr/testify/require"
)

func TestByPlacement_GetPtr(t *testing.T) {
	var bp CountAndSizeByPlacement
	bp.Local = CountAndSize{Count: 10, Bytes: 1000}
	bp.Shared = CountAndSize{Count: 20, Bytes: 2000}
	bp.External = CountAndSize{Count: 30, Bytes: 3000}

	require.Equal(t, CountAndSize{Count: 10, Bytes: 1000}, bp.Get(base.Local))
	require.Equal(t, CountAndSize{Count: 20, Bytes: 2000}, bp.Get(base.Shared))
	require.Equal(t, CountAndSize{Count: 30, Bytes: 3000}, bp.Get(base.External))

	// Verify we get pointers to the actual fields
	require.Equal(t, &bp.Local, bp.Ptr(base.Local))
	require.Equal(t, &bp.Shared, bp.Ptr(base.Shared))
	require.Equal(t, &bp.External, bp.Ptr(base.External))

	// Verify modifications through pointer affect the original
	bp.Ptr(base.Local).Inc(500)
	require.Equal(t, CountAndSize{Count: 11, Bytes: 1500}, bp.Get(base.Local))
}

func TestByPlacement_Accumulate(t *testing.T) {
	var bp1, bp2 CountAndSizeByPlacement

	bp1.Local = CountAndSize{Count: 10, Bytes: 1000}
	bp1.Shared = CountAndSize{Count: 20, Bytes: 2000}
	bp1.External = CountAndSize{Count: 30, Bytes: 3000}

	bp2.Local = CountAndSize{Count: 5, Bytes: 500}
	bp2.Shared = CountAndSize{Count: 15, Bytes: 1500}
	bp2.External = CountAndSize{Count: 25, Bytes: 2500}

	bp1.Accumulate(bp2)

	require.Equal(t, CountAndSize{Count: 15, Bytes: 1500}, bp1.Local)
	require.Equal(t, CountAndSize{Count: 35, Bytes: 3500}, bp1.Shared)
	require.Equal(t, CountAndSize{Count: 55, Bytes: 5500}, bp1.External)
}

func TestByPlacement_Inc_Dec(t *testing.T) {
	var bp CountAndSizeByPlacement

	// Test Inc
	bp.Inc(1000, base.Local)
	bp.Inc(2000, base.Shared)
	bp.Inc(3000, base.External)

	require.Equal(t, CountAndSize{Count: 1, Bytes: 1000}, bp.Local)
	require.Equal(t, CountAndSize{Count: 1, Bytes: 2000}, bp.Shared)
	require.Equal(t, CountAndSize{Count: 1, Bytes: 3000}, bp.External)

	// Test Dec
	bp.Dec(500, base.Local)
	bp.Dec(1000, base.Shared)

	require.Equal(t, CountAndSize{Count: 0, Bytes: 500}, bp.Local)
	require.Equal(t, CountAndSize{Count: 0, Bytes: 1000}, bp.Shared)
	require.Equal(t, CountAndSize{Count: 1, Bytes: 3000}, bp.External)
}

func TestByPlacement_Deduct(t *testing.T) {
	bp1 := CountAndSizeByPlacement{
		ByPlacement: ByPlacement[CountAndSize]{
			Local:    CountAndSize{Count: 10, Bytes: 1000},
			Shared:   CountAndSize{Count: 20, Bytes: 2000},
			External: CountAndSize{Count: 30, Bytes: 3000},
		},
	}

	bp2 := CountAndSizeByPlacement{
		ByPlacement: ByPlacement[CountAndSize]{
			Local:    CountAndSize{Count: 3, Bytes: 300},
			Shared:   CountAndSize{Count: 5, Bytes: 500},
			External: CountAndSize{Count: 10, Bytes: 1000},
		},
	}

	bp1.Deduct(bp2)

	require.Equal(t, CountAndSize{Count: 7, Bytes: 700}, bp1.Local)
	require.Equal(t, CountAndSize{Count: 15, Bytes: 1500}, bp1.Shared)
	require.Equal(t, CountAndSize{Count: 20, Bytes: 2000}, bp1.External)
}

func TestByPlacement_Total(t *testing.T) {
	bp := CountAndSizeByPlacement{
		ByPlacement: ByPlacement[CountAndSize]{
			Local:    CountAndSize{Count: 10, Bytes: 1000},
			Shared:   CountAndSize{Count: 20, Bytes: 2000},
			External: CountAndSize{Count: 30, Bytes: 3000},
		},
	}

	total := bp.Total()
	require.Equal(t, CountAndSize{Count: 60, Bytes: 6000}, total)
}

func TestFileCountsAndSizes_Inc_Dec(t *testing.T) {
	var cs FileCountsAndSizes

	// Add files at different placements
	cs.Inc(base.FileTypeTable, 1000, base.Local)
	cs.Inc(base.FileTypeTable, 2000, base.Shared)
	cs.Inc(base.FileTypeBlob, 3000, base.External)
	cs.Inc(base.FileTypeLog, 500, base.Local)

	require.Equal(t, CountAndSize{Count: 1, Bytes: 1000}, cs.Tables.Local)
	require.Equal(t, CountAndSize{Count: 1, Bytes: 2000}, cs.Tables.Shared)
	require.Equal(t, CountAndSize{Count: 0, Bytes: 0}, cs.Tables.External)
	require.Equal(t, CountAndSize{Count: 0, Bytes: 0}, cs.BlobFiles.Local)
	require.Equal(t, CountAndSize{Count: 1, Bytes: 3000}, cs.BlobFiles.External)
	require.Equal(t, CountAndSize{Count: 1, Bytes: 500}, cs.Other)

	// Subtract files
	cs.Dec(base.FileTypeTable, 1000, base.Local)
	cs.Dec(base.FileTypeLog, 500, base.Local)

	require.Equal(t, CountAndSize{Count: 0, Bytes: 0}, cs.Tables.Local)
	require.Equal(t, CountAndSize{Count: 0, Bytes: 0}, cs.Other)
}

func TestFileCountsAndSizes_Accumulate_Deduct(t *testing.T) {
	cs1 := FileCountsAndSizes{
		Tables: CountAndSizeByPlacement{
			ByPlacement: ByPlacement[CountAndSize]{
				Local:  CountAndSize{Count: 10, Bytes: 2000},
				Shared: CountAndSize{Count: 5, Bytes: 1000},
			},
		},
		BlobFiles: CountAndSizeByPlacement{
			ByPlacement: ByPlacement[CountAndSize]{
				External: CountAndSize{Count: 3, Bytes: 600},
			},
		},
		Other: CountAndSize{Count: 2, Bytes: 300},
	}

	cs2 := FileCountsAndSizes{
		Tables: CountAndSizeByPlacement{
			ByPlacement: ByPlacement[CountAndSize]{
				Local:  CountAndSize{Count: 3, Bytes: 500},
				Shared: CountAndSize{Count: 2, Bytes: 400},
			},
		},
		BlobFiles: CountAndSizeByPlacement{
			ByPlacement: ByPlacement[CountAndSize]{
				External: CountAndSize{Count: 1, Bytes: 200},
			},
		},
		Other: CountAndSize{Count: 1, Bytes: 100},
	}

	// Test Accumulate
	sum := cs1
	sum.Accumulate(cs2)

	require.Equal(t, CountAndSize{Count: 13, Bytes: 2500}, sum.Tables.Local)
	require.Equal(t, CountAndSize{Count: 7, Bytes: 1400}, sum.Tables.Shared)
	require.Equal(t, CountAndSize{Count: 4, Bytes: 800}, sum.BlobFiles.External)
	require.Equal(t, CountAndSize{Count: 3, Bytes: 400}, sum.Other)

	// Test Deduct
	sum.Deduct(cs2)
	require.Equal(t, cs1, sum)
}

func TestFileCountsAndSizes_Total(t *testing.T) {
	cs := FileCountsAndSizes{
		Tables: CountAndSizeByPlacement{
			ByPlacement: ByPlacement[CountAndSize]{
				Local:  CountAndSize{Count: 10, Bytes: 2000},
				Shared: CountAndSize{Count: 5, Bytes: 1000},
			},
		},
		BlobFiles: CountAndSizeByPlacement{
			ByPlacement: ByPlacement[CountAndSize]{
				External: CountAndSize{Count: 3, Bytes: 600},
			},
		},
		Other: CountAndSize{Count: 2, Bytes: 300},
	}

	total := cs.Total()
	require.Equal(t, CountAndSize{Count: 20, Bytes: 3900}, total)
}

func TestFileCountsAndSizes_String(t *testing.T) {
	testCases := []struct {
		name     string
		cs       FileCountsAndSizes
		expected string
	}{
		{
			name:     "empty",
			cs:       FileCountsAndSizes{},
			expected: "no files",
		},
		{
			name: "only local tables",
			cs: FileCountsAndSizes{
				Tables: CountAndSizeByPlacement{
					ByPlacement: ByPlacement[CountAndSize]{
						Local: CountAndSize{Count: 5, Bytes: 1024},
					},
				},
			},
			expected: "tables: 5 (1KB)",
		},
		{
			name: "tables with shared placement",
			cs: FileCountsAndSizes{
				Tables: CountAndSizeByPlacement{
					ByPlacement: ByPlacement[CountAndSize]{
						Local:  CountAndSize{Count: 3, Bytes: 512},
						Shared: CountAndSize{Count: 2, Bytes: 512},
					},
				},
			},
			expected: "tables: 5 (1KB) [local: 3 (512B)]",
		},
		{
			name: "tables and blob files",
			cs: FileCountsAndSizes{
				Tables: CountAndSizeByPlacement{
					ByPlacement: ByPlacement[CountAndSize]{
						Local: CountAndSize{Count: 10, Bytes: 2000},
					},
				},
				BlobFiles: CountAndSizeByPlacement{
					ByPlacement: ByPlacement[CountAndSize]{
						External: CountAndSize{Count: 3, Bytes: 600},
					},
				},
			},
			expected: "tables: 10 (2KB); blob files: 3 (600B) [local: 0 (0B)]",
		},
		{
			name: "just blob files",
			cs: FileCountsAndSizes{
				BlobFiles: CountAndSizeByPlacement{
					ByPlacement: ByPlacement[CountAndSize]{
						External: CountAndSize{Count: 3, Bytes: 600},
					},
				},
			},
			expected: "blob files: 3 (600B) [local: 0 (0B)]",
		},
		{
			name: "blob files and other",
			cs: FileCountsAndSizes{
				BlobFiles: CountAndSizeByPlacement{
					ByPlacement: ByPlacement[CountAndSize]{
						External: CountAndSize{Count: 3, Bytes: 600},
					},
				},
				Other: CountAndSize{Count: 1, Bytes: 100},
			},
			expected: "blob files: 3 (600B) [local: 0 (0B)]; other: 1 (100B)",
		},
		{
			name: "all file types",
			cs: FileCountsAndSizes{
				Tables: CountAndSizeByPlacement{
					ByPlacement: ByPlacement[CountAndSize]{
						Local: CountAndSize{Count: 5, Bytes: 1000},
					},
				},
				BlobFiles: CountAndSizeByPlacement{
					ByPlacement: ByPlacement[CountAndSize]{
						Local: CountAndSize{Count: 2, Bytes: 400},
					},
				},
				Other: CountAndSize{Count: 1, Bytes: 100},
			},
			expected: "tables: 5 (1KB); blob files: 2 (400B); other: 1 (100B)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, tc.cs.String())
		})
	}
}
