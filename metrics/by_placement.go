// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metrics

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/redact"
)

// CountAndSizeByPlacement contains space usage information for a set of files
// (e.g. blob files, table backings), broken down by where they are stored.
type CountAndSizeByPlacement struct {
	ByPlacement[CountAndSize]
}

func (csp *CountAndSizeByPlacement) Inc(fileSize uint64, placement base.Placement) {
	csp.Ptr(placement).Inc(fileSize)
}

func (csp *CountAndSizeByPlacement) Dec(fileSize uint64, placement base.Placement) {
	csp.Ptr(placement).Dec(fileSize)
}

func (csp *CountAndSizeByPlacement) Accumulate(rhs CountAndSizeByPlacement) {
	csp.Local.Accumulate(rhs.Local)
	csp.Shared.Accumulate(rhs.Shared)
	csp.External.Accumulate(rhs.External)
}

func (csp *CountAndSizeByPlacement) Deduct(rhs CountAndSizeByPlacement) {
	csp.Local.Deduct(rhs.Local)
	csp.Shared.Deduct(rhs.Shared)
	csp.External.Deduct(rhs.External)
}

func (csp CountAndSizeByPlacement) Total() CountAndSize {
	return csp.Local.Sum(csp.Shared).Sum(csp.External)
}

func (csp CountAndSizeByPlacement) String() string {
	return redact.StringWithoutMarkers(csp)
}

// SafeFormat implements redact.SafeFormatter.
func (csp CountAndSizeByPlacement) SafeFormat(w redact.SafePrinter, verb rune) {
	if csp.Shared.IsZero() && csp.External.IsZero() {
		w.Printf("%s", csp.Local)
		return
	}
	w.Printf("%s [local: %s]", csp.Total(), csp.Local)
}

// ByPlacement contains multiple instances of a struct T, one for each possible
// Placement.
type ByPlacement[T any] struct {
	Local    T
	Shared   T
	External T
}

func (bp *ByPlacement[T]) Get(placement base.Placement) T {
	switch placement {
	case base.Local:
		return bp.Local
	case base.Shared:
		return bp.Shared
	case base.External:
		return bp.External
	default:
		if invariants.Enabled {
			panic(errors.AssertionFailedf("invalid placement %d", placement))
		}
		return bp.Local
	}
}

func (bp *ByPlacement[T]) Set(placement base.Placement, value T) {
	switch placement {
	case base.Local:
		bp.Local = value
	case base.Shared:
		bp.Shared = value
	case base.External:
		bp.External = value
	default:
		if invariants.Enabled {
			panic(errors.AssertionFailedf("invalid placement %d", placement))
		}
		bp.Local = value
	}
}

func (bp *ByPlacement[T]) Ptr(placement base.Placement) *T {
	switch placement {
	case base.Local:
		return &bp.Local
	case base.Shared:
		return &bp.Shared
	case base.External:
		return &bp.External
	default:
		if invariants.Enabled {
			panic(errors.AssertionFailedf("invalid placement %d", placement))
		}
		return &bp.Local
	}
}

// FileCountsAndSizes contains counts and sizes for all file types.
type FileCountsAndSizes struct {
	// Tables contains counts and sizes for tables.
	Tables CountAndSizeByPlacement

	// BlobFiles contains counts and sizes for blob files.
	BlobFiles CountAndSizeByPlacement

	// Other contains counts and sizes for other file types (log, manifest, etc).
	// These files are all local.
	Other CountAndSize
}

// Total returns the total count and size of files.
func (cs *FileCountsAndSizes) Total() CountAndSize {
	return cs.Tables.Total().Sum(cs.BlobFiles.Total()).Sum(cs.Other)
}

// Inc increases the relevant count and size for a single file.
func (cs *FileCountsAndSizes) Inc(
	fileType base.FileType, fileSize uint64, placement base.Placement,
) {
	switch fileType {
	case base.FileTypeTable:
		cs.Tables.Inc(fileSize, placement)
	case base.FileTypeBlob:
		cs.BlobFiles.Inc(fileSize, placement)
	default:
		cs.Other.Inc(fileSize)
	}
}

// Dec decreases the relevant count and size for a single file.
func (cs *FileCountsAndSizes) Dec(
	fileType base.FileType, fileSize uint64, placement base.Placement,
) {
	switch fileType {
	case base.FileTypeTable:
		cs.Tables.Dec(fileSize, placement)
	case base.FileTypeBlob:
		cs.BlobFiles.Dec(fileSize, placement)
	default:
		cs.Other.Dec(fileSize)
	}
}

// Accumulate increases the counts and sizes by the given amounts.
func (cs *FileCountsAndSizes) Accumulate(other FileCountsAndSizes) {
	cs.Tables.Accumulate(other.Tables)
	cs.BlobFiles.Accumulate(other.BlobFiles)
	cs.Other.Accumulate(other.Other)
}

// Deduct decreases the counts and sizes by the given amounts.
func (cs *FileCountsAndSizes) Deduct(other FileCountsAndSizes) {
	cs.Tables.Deduct(other.Tables)
	cs.BlobFiles.Deduct(other.BlobFiles)
	cs.Other.Deduct(other.Other)
}

func (cs FileCountsAndSizes) String() string {
	return redact.StringWithoutMarkers(cs)
}

// SafeFormat implements redact.SafeFormatter.
func (cs FileCountsAndSizes) SafeFormat(w redact.SafePrinter, verb rune) {
	if cs == (FileCountsAndSizes{}) {
		w.Printf("no files")
		return
	}
	first := true
	if cs.Tables != (CountAndSizeByPlacement{}) {
		first = false
		w.Printf("tables: %s", cs.Tables)
	}
	if cs.BlobFiles != (CountAndSizeByPlacement{}) {
		if !first {
			w.Printf("; ")
		}
		first = false
		w.Printf("blob files: %s", cs.BlobFiles)
	}
	if cs.Other != (CountAndSize{}) {
		if !first {
			w.Printf("; ")
		}
		first = false
		w.Printf("other: %s", cs.Other)
	}
}
