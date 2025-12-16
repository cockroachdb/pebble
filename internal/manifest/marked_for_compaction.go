// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"iter"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/invariants"
	googlebtree "github.com/google/btree"
)

// MarkedForCompactionSet implements an ordered set of tables that are marked
// for compaction. They are ordered by decreasing LSM level, and within each
// level by increasing LargestSeqNum.
//
// Files can be marked for compaction when upgrading a DB to a specific version.
type MarkedForCompactionSet struct {
	tree *googlebtree.BTreeG[tableAndLevel]
}

type tableAndLevel struct {
	meta  *TableMetadata
	level int
}

func markedForCompactionLessFn(a, b tableAndLevel) bool {
	if a.level != b.level {
		return a.level > b.level
	}
	if a.meta.SeqNums.High != b.meta.SeqNums.High {
		return a.meta.SeqNums.High < b.meta.SeqNums.High
	}
	return a.meta.TableNum < b.meta.TableNum
}

// Count returns the number of tables in the set.
func (s *MarkedForCompactionSet) Count() int {
	if s.tree == nil {
		return 0
	}
	return s.tree.Len()
}

// Insert adds a table to the set. The table must not be in the set already.
func (s *MarkedForCompactionSet) Insert(meta *TableMetadata, level int) {
	if s.tree == nil {
		s.tree = googlebtree.NewG[tableAndLevel](8, markedForCompactionLessFn)
	}
	_, existed := s.tree.ReplaceOrInsert(tableAndLevel{meta: meta, level: level})
	if invariants.Enabled && existed {
		panic(errors.AssertionFailedf("table %s already in MarkedForCompaction", meta.TableNum))
	}
}

// Delete removes a table from the set (if it is in the set).
func (s *MarkedForCompactionSet) Delete(meta *TableMetadata, level int) {
	if s.tree != nil {
		s.tree.Delete(tableAndLevel{meta: meta, level: level})
	}
}

// Contains returns true if the set contains the given table/level pair.
func (s *MarkedForCompactionSet) Contains(meta *TableMetadata, level int) bool {
	if s.tree == nil {
		return false
	}
	_, ok := s.tree.Get(tableAndLevel{meta: meta, level: level})
	return ok
}

// Clone the set. The resulting set can be independently modified.
func (s *MarkedForCompactionSet) Clone() MarkedForCompactionSet {
	if s.tree == nil {
		return MarkedForCompactionSet{}
	}
	return MarkedForCompactionSet{tree: s.tree.Clone()}
}

// Ascending returns an iterator which produces the tables marked for compaction
// (along with the level), ordered by decreasing level and within each level by
// increasing LargestSeqNum.
func (s *MarkedForCompactionSet) Ascending() iter.Seq2[*TableMetadata, int] {
	return func(yield func(*TableMetadata, int) bool) {
		if s.tree == nil {
			return
		}
		s.tree.Ascend(func(t tableAndLevel) bool {
			return yield(t.meta, t.level)
		})
	}
}
