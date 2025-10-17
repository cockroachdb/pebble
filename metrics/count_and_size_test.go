// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metrics

import (
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/stretchr/testify/require"
)

func expect(t *testing.T, cs CountAndSize, expCount uint64, expBytes uint64) {
	t.Helper()
	require.Equal(t, expCount, cs.Count)
	require.Equal(t, expBytes, cs.Bytes)
}

func TestTableCountsAndSizes(t *testing.T) {
	var cs TableCountsAndSizes

	// Add local table
	cs.Inc(1000, true)
	expect(t, cs.All, 1, 1000)
	expect(t, cs.Local, 1, 1000)

	// Add remote table
	cs.Inc(2000, false)
	expect(t, cs.All, 2, 3000)
	expect(t, cs.Local, 1, 1000)

	// Subtract local table
	cs.Dec(500, true)
	expect(t, cs.All, 1, 2500)
	expect(t, cs.Local, 0, 500)

	// Subtract remote table
	cs.Dec(1000, false)
	expect(t, cs.All, 0, 1500)
	expect(t, cs.Local, 0, 500)

	// Test Accumulate method
	cs2 := TableCountsAndSizes{
		All:   CountAndSize{Count: 5, Bytes: 5000},
		Local: CountAndSize{Count: 3, Bytes: 3000},
	}
	cs.Accumulate(cs2)
	expect(t, cs.All, 5, 6500)
	expect(t, cs.Local, 3, 3500)

	// Test Deduct method
	cs.Deduct(cs2)
	expect(t, cs.All, 0, 1500)
	expect(t, cs.Local, 0, 500)
}

func TestBlobFileCountsAndSizes(t *testing.T) {
	var cs BlobFileCountsAndSizes

	// Add local blob
	cs.Inc(5000, true)
	expect(t, cs.All, 1, 5000)
	expect(t, cs.Local, 1, 5000)

	// Add remote blob
	cs.Inc(3000, false)
	expect(t, cs.All, 2, 8000)
	expect(t, cs.Local, 1, 5000)

	// Subtract local blob
	cs.Dec(2000, true)
	expect(t, cs.All, 1, 6000)
	expect(t, cs.Local, 0, 3000)

	// Subtract remote blob
	cs.Dec(3000, false)
	expect(t, cs.All, 0, 3000)
	expect(t, cs.Local, 0, 3000)

	// Test Accumulate method
	cs2 := BlobFileCountsAndSizes{
		All:   CountAndSize{Count: 4, Bytes: 8000},
		Local: CountAndSize{Count: 2, Bytes: 4000},
	}
	cs.Accumulate(cs2)
	expect(t, cs.All, 4, 11000)
	expect(t, cs.Local, 2, 7000)

	// Test Deduct method
	cs.Deduct(cs2)
	expect(t, cs.All, 0, 3000)
	expect(t, cs.Local, 0, 3000)
}

func TestFileCountsAndSizes(t *testing.T) {
	cs1 := FileCountsAndSizes{}
	cs1.Tables.All = CountAndSize{Count: 5, Bytes: 1000}
	cs1.Tables.Local = CountAndSize{Count: 3, Bytes: 600}
	cs1.BlobFiles.All = CountAndSize{Count: 2, Bytes: 500}
	cs1.BlobFiles.Local = CountAndSize{Count: 1, Bytes: 250}

	cs2 := FileCountsAndSizes{}
	cs2.Tables.All = CountAndSize{Count: 3, Bytes: 400}
	cs2.Tables.Local = CountAndSize{Count: 2, Bytes: 300}
	cs2.BlobFiles.All = CountAndSize{Count: 1, Bytes: 200}
	cs2.BlobFiles.Local = CountAndSize{Count: 1, Bytes: 200}

	sum := cs1
	sum.Accumulate(cs2)

	x := sum
	x.Deduct(cs2)
	require.Equal(t, cs1, x)

	expect(t, sum.Tables.All, 8, 1400)
	expect(t, sum.Tables.Local, 5, 900)
	expect(t, sum.BlobFiles.All, 3, 700)
	expect(t, sum.BlobFiles.Local, 2, 450)

	sum.Inc(base.FileTypeTable, 10000, true /* isLocal */)
	sum.Dec(base.FileTypeBlob, 250, false)

	expect(t, sum.Tables.All, 9, 11400)
	expect(t, sum.Tables.Local, 6, 10900)
	expect(t, sum.BlobFiles.All, 2, 450)
	expect(t, sum.BlobFiles.Local, 2, 450)
}

func TestFileCountsAndSizes_Inc_OtherTypes(t *testing.T) {
	var cs FileCountsAndSizes

	// Add log file
	cs.Inc(base.FileTypeLog, 1000, true)
	expect(t, cs.Other, 1, 1000)
	expect(t, cs.Tables.All, 0, 0)
	expect(t, cs.BlobFiles.All, 0, 0)

	// Add manifest file
	cs.Inc(base.FileTypeManifest, 2000, false)
	expect(t, cs.Other, 2, 3000)

	// Add options file
	cs.Inc(base.FileTypeOptions, 100, true)
	expect(t, cs.Other, 3, 3100)

	// Subtract log file
	cs.Dec(base.FileTypeLog, 500, false)
	expect(t, cs.Other, 2, 2600)
}

func TestFileCountsAndSizes_Mixed(t *testing.T) {
	var cs FileCountsAndSizes

	// Add various files
	cs.Inc(base.FileTypeTable, 1000, true)    // local table
	cs.Inc(base.FileTypeTable, 2000, false)   // remote table
	cs.Inc(base.FileTypeBlob, 3000, true)     // local blob
	cs.Inc(base.FileTypeBlob, 4000, false)    // remote blob
	cs.Inc(base.FileTypeLog, 500, true)       // log file
	cs.Inc(base.FileTypeManifest, 200, false) // manifest file

	expect(t, cs.Tables.All, 2, 3000)
	expect(t, cs.Tables.Local, 1, 1000)
	expect(t, cs.BlobFiles.All, 2, 7000)
	expect(t, cs.BlobFiles.Local, 1, 3000)
	expect(t, cs.Other, 2, 700)

	// Subtract some files
	cs.Dec(base.FileTypeTable, 1000, true)
	cs.Dec(base.FileTypeBlob, 3000, true)
	cs.Dec(base.FileTypeLog, 500, true)

	expect(t, cs.Tables.All, 1, 2000)
	expect(t, cs.Tables.Local, 0, 0)
	expect(t, cs.BlobFiles.All, 1, 4000)
	expect(t, cs.BlobFiles.Local, 0, 0)
	expect(t, cs.Other, 1, 200)
}

func TestFileCountsAndSizes_AccumulateDeduct(t *testing.T) {
	cs1 := FileCountsAndSizes{
		Tables: TableCountsAndSizes{
			All:   CountAndSize{Count: 10, Bytes: 2000},
			Local: CountAndSize{Count: 6, Bytes: 1200},
		},
		BlobFiles: BlobFileCountsAndSizes{
			All:   CountAndSize{Count: 5, Bytes: 1000},
			Local: CountAndSize{Count: 3, Bytes: 600},
		},
		Other: CountAndSize{Count: 2, Bytes: 300},
	}

	cs2 := FileCountsAndSizes{
		Tables: TableCountsAndSizes{
			All:   CountAndSize{Count: 3, Bytes: 500},
			Local: CountAndSize{Count: 2, Bytes: 400},
		},
		BlobFiles: BlobFileCountsAndSizes{
			All:   CountAndSize{Count: 2, Bytes: 300},
			Local: CountAndSize{Count: 1, Bytes: 200},
		},
		Other: CountAndSize{Count: 1, Bytes: 100},
	}

	// Test Accumulate
	sum := cs1
	sum.Accumulate(cs2)

	expect(t, sum.Tables.All, 13, 2500)
	expect(t, sum.Tables.Local, 8, 1600)
	expect(t, sum.BlobFiles.All, 7, 1300)
	expect(t, sum.BlobFiles.Local, 4, 800)
	expect(t, sum.Other, 3, 400)

	// Test Deduct
	sum.Deduct(cs2)
	require.Equal(t, cs1, sum)
}
