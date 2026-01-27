// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	stdcmp "cmp"
	"fmt"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/objstorage"
)

// ScanCursor represents a position in an LSM scan. The scan proceeds
// level-by-level, and within each level files are ordered by their smallest
// user key. For L0, we break ties using SeqNums.High since files can overlap.
//
// A cursor can be thought of as a boundary between two files in a version
// (ordered by level, then by Smallest.UserKey, then by SeqNums.High). A file
// is either "before" or "after" the cursor.
type ScanCursor struct {
	// Level is the LSM level (0 to NumLevels). When Level=NumLevels, the cursor
	// is at the end and the scan is complete.
	Level int
	// Key is an inclusive lower bound for Smallest.UserKey for tables on Level.
	Key []byte
	// SeqNum is an inclusive lower bound for SeqNums.High for tables on Level
	// with Smallest.UserKey equaling Key. Used to break ties within L0, and also
	// used to position a cursor immediately after a given file.
	SeqNum base.SeqNum
}

// EndScanCursor is a cursor that is past all files in the LSM.
var EndScanCursor = ScanCursor{Level: NumLevels}

// AtEnd returns true if the cursor is after all files in the LSM.
func (c *ScanCursor) AtEnd() bool {
	return c.Level >= NumLevels
}

// String implements fmt.Stringer.
func (c *ScanCursor) String() string {
	return fmt.Sprintf("level=%d key=%q seqNum=%d", c.Level, c.Key, c.SeqNum)
}

// Compare compares two cursors. Returns -1 if c < other, 0 if c == other,
// 1 if c > other.
func (c ScanCursor) Compare(keyCmp base.Compare, other ScanCursor) int {
	if v := stdcmp.Compare(c.Level, other.Level); v != 0 {
		return v
	}
	if v := keyCmp(c.Key, other.Key); v != 0 {
		return v
	}
	return stdcmp.Compare(c.SeqNum, other.SeqNum)
}

// MakeScanCursor returns a cursor that points exactly at the given file.
// This is used when comparing whether a candidate file is at or after a
// boundary position in the scan.
func MakeScanCursor(f *TableMetadata, level int) ScanCursor {
	return ScanCursor{
		Level:  level,
		Key:    f.Smallest().UserKey,
		SeqNum: f.SeqNums.High,
	}
}

// MakeScanCursorAfterFile returns a cursor that is immediately after the given
// file. This is used to advance the scan position past a file that has just
// been processed.
//
// The cursor is positioned such that the file would be considered "before" the
// cursor (i.e., cursor.Compare(MakeScanCursorAtFile(f)) > 0).
func MakeScanCursorAfterFile(f *TableMetadata, level int) ScanCursor {
	return ScanCursor{
		Level:  level,
		Key:    f.Smallest().UserKey,
		SeqNum: f.SeqNums.High + 1,
	}
}

// FileIsAfterCursor returns true if the given file is strictly after the cursor
// position. This is useful for skipping files that have already been processed.
func (c *ScanCursor) FileIsAfterCursor(cmp base.Compare, f *TableMetadata, level int) bool {
	return c.Compare(cmp, MakeScanCursorAfterFile(f, level)) < 0
}

// NextExternalFile returns the first external file after the cursor, returning
// the file and the level. If no such file exists, returns nil fileMetadata.
func (c *ScanCursor) NextExternalFile(
	cmp base.Compare, objProvider objstorage.Provider, bounds base.UserKeyBounds, v *Version,
) (_ *TableMetadata, level int) {
	for !c.AtEnd() {
		if f := c.NextExternalFileOnLevel(cmp, objProvider, bounds.End, v); f != nil {
			return f, c.Level
		}
		// Go to the next level.
		c.Key = bounds.Start
		c.SeqNum = 0
		c.Level++
	}
	return nil, NumLevels
}

// NextExternalFileOnLevel returns the first external file on c.Level which is
// after c and with Smallest.UserKey within the end bound.
func (c *ScanCursor) NextExternalFileOnLevel(
	cmp base.Compare, objProvider objstorage.Provider, endBound base.UserKeyBoundary, v *Version,
) *TableMetadata {
	if c.Level > 0 {
		it := v.Levels[c.Level].Iter()
		return c.FirstExternalFileInLevelIter(cmp, objProvider, it, endBound)
	}
	// For L0, we look at all sublevel iterators and take the first file.
	var first *TableMetadata
	var firstCursor ScanCursor
	for _, sublevel := range v.L0SublevelFiles {
		f := c.FirstExternalFileInLevelIter(cmp, objProvider, sublevel.Iter(), endBound)
		if f != nil {
			fc := MakeScanCursor(f, c.Level)
			if first == nil || fc.Compare(cmp, firstCursor) < 0 {
				first = f
				firstCursor = fc
			}
			// Trim the end bound as an optimization.
			endBound = base.UserKeyInclusive(f.Smallest().UserKey)
		}
	}
	return first
}

// FirstExternalFileInLevelIter finds the first external file after the cursor
// but which starts before the endBound. It is assumed that the iterator
// corresponds to cursor.Level.
func (c *ScanCursor) FirstExternalFileInLevelIter(
	cmp base.Compare,
	objProvider objstorage.Provider,
	it LevelIterator,
	endBound base.UserKeyBoundary,
) *TableMetadata {
	f := it.SeekGE(cmp, c.Key)
	// Skip the file if it starts before cursor.Key or is at that same key with lower
	// sequence number.
	for f != nil && !c.FileIsAfterCursor(cmp, f, c.Level) {
		f = it.Next()
	}
	for ; f != nil && endBound.IsUpperBoundFor(cmp, f.Smallest().UserKey); f = it.Next() {
		if f.Virtual && objstorage.IsExternalTable(objProvider, f.TableBacking.DiskFileNum) {
			return f
		}
	}
	return nil
}

// NextFile returns the first file after the cursor, returning the file and the
// level. If no such file exists, returns nil.
func (c *ScanCursor) NextFile(cmp base.Compare, v *Version) (_ *TableMetadata, level int) {
	for !c.AtEnd() {
		if f := c.nextFileOnLevel(cmp, v); f != nil {
			return f, c.Level
		}
		// Go to the next level.
		c.Key = nil
		c.SeqNum = 0
		c.Level++
	}
	return nil, NumLevels
}

// nextFileOnLevel returns the first file on c.Level which is at or after the
// cursor position.
func (c *ScanCursor) nextFileOnLevel(cmp base.Compare, v *Version) *TableMetadata {
	if c.Level == 0 {
		return c.nextFileOnL0(v)
	}
	it := v.Levels[c.Level].Iter()
	if len(c.Key) == 0 {
		return it.First()
	}
	f := it.SeekGE(cmp, c.Key)
	// Find the first file at or after the cursor position.
	for f != nil && !c.FileIsAfterCursor(cmp, f, c.Level) {
		f = it.Next()
	}
	return f
}

// nextFileOnL0 returns the next L0 file at or after the cursor's sequence
// number. L0 files are sorted by increasing SeqNums.High.
func (c *ScanCursor) nextFileOnL0(v *Version) *TableMetadata {
	iter := v.Levels[0].Iter()
	if c.SeqNum == 0 {
		return iter.First()
	}
	// Find the first file with SeqNums.High >= c.SeqNum.
	for f := iter.First(); f != nil; f = iter.Next() {
		if f.SeqNums.High >= c.SeqNum {
			return f
		}
	}
	return nil
}
