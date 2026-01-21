// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"cmp"
	"fmt"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
)

// lsmScanCursor represents a position in an LSM scan. The scan proceeds
// level-by-level, and within each level files are ordered by their smallest
// user key. For L0, we break ties using SeqNums.High since files can overlap.
//
// A cursor can be thought of as a boundary between two files in a version
// (ordered by level, then by Smallest.UserKey, then by SeqNums.High). A file
// is either "before" or "after" the cursor.
type lsmScanCursor struct {
	// level is the LSM level (0 to NumLevels). When level=NumLevels, the cursor
	// is at the end and the scan is complete.
	level int
	// key is an inclusive lower bound for Smallest.UserKey for tables on level.
	key []byte
	// seqNum is an inclusive lower bound for SeqNums.High for tables on level
	// with Smallest.UserKey equaling key. Used to break ties within L0, and also
	// used to position a cursor immediately after a given file.
	seqNum base.SeqNum
}

// endLSMCursor is a cursor that is past all files in the LSM.
var endLSMCursor = lsmScanCursor{level: manifest.NumLevels}

// AtEnd returns true if the cursor is after all files in the LSM.
func (c lsmScanCursor) AtEnd() bool {
	return c.level >= manifest.NumLevels
}

// String implements fmt.Stringer.
func (c lsmScanCursor) String() string {
	return fmt.Sprintf("level=%d key=%q seqNum=%d", c.level, c.key, c.seqNum)
}

// Compare compares two cursors. Returns -1 if c < other, 0 if c == other,
// 1 if c > other.
func (c lsmScanCursor) Compare(keyCmp base.Compare, other lsmScanCursor) int {
	if v := cmp.Compare(c.level, other.level); v != 0 {
		return v
	}
	if v := keyCmp(c.key, other.key); v != 0 {
		return v
	}
	return cmp.Compare(c.seqNum, other.seqNum)
}

// makeLSMCursorAtFile returns a cursor that points exactly at the given file.
// This is used when comparing whether a candidate file is at or after a
// boundary position in the scan.
func makeLSMCursorAtFile(f *manifest.TableMetadata, level int) lsmScanCursor {
	return lsmScanCursor{
		level:  level,
		key:    f.Smallest().UserKey,
		seqNum: f.SeqNums.High,
	}
}

// makeLSMCursorAfterFile returns a cursor that is immediately after the given
// file. This is used to advance the scan position past a file that has just
// been processed.
//
// The cursor is positioned such that the file would be considered "before" the
// cursor (i.e., cursor.Compare(makeLSMCursorAtFile(f)) > 0).
func makeLSMCursorAfterFile(f *manifest.TableMetadata, level int) lsmScanCursor {
	seqNum := f.SeqNums.High
	// Increment seqNum to position past this file. Guard against overflow.
	if seqNum < base.SeqNumMax {
		seqNum++
	}
	return lsmScanCursor{
		level:  level,
		key:    f.Smallest().UserKey,
		seqNum: seqNum,
	}
}

// FileIsAfterCursor returns true if the given file is strictly after the cursor
// position. This is useful for skipping files that have already been processed.
func (c lsmScanCursor) FileIsAfterCursor(
	cmp base.Compare, f *manifest.TableMetadata, level int,
) bool {
	return c.Compare(cmp, makeLSMCursorAfterFile(f, level)) < 0
}
