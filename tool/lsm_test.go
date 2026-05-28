// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tool

import (
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/stretchr/testify/require"
)

// TestBuildEditsL0OutOfSeqNumOrder exercises buildEdits with a sequence of
// version edits that add L0 files out of largest-sequence-number order. The
// L0Organizer requires successive overlapping L0 files to have non-decreasing
// largest sequence numbers, so buildEdits must construct the per-edit Version
// via a path that orders L0 by seqnum. NewVersionWithFiles satisfies that by
// building the L0 B-Tree with btreeCmpSeqNum; previously buildEdits used
// NewVersionForTesting, which preserved slice order and would panic in
// addFileToSublevels on the second edit.
func TestBuildEditsL0OutOfSeqNumOrder(t *testing.T) {
	cmp := base.DefaultComparer

	newL0Table := func(num base.FileNum, smallestSeq, largestSeq base.SeqNum) *manifest.TableMetadata {
		m := (&manifest.TableMetadata{TableNum: num}).ExtendPointKeyBounds(
			cmp.Compare,
			base.MakeInternalKey([]byte("a"), largestSeq, base.InternalKeyKindSet),
			base.MakeInternalKey([]byte("z"), smallestSeq, base.InternalKeyKindSet),
		)
		m.SeqNums.Low = smallestSeq
		m.SeqNums.High = largestSeq
		m.LargestSeqNumAbsolute = largestSeq
		m.Size = 1
		m.InitPhysicalBacking()
		return m
	}

	// VE 0 introduces an L0 file with a larger SeqNums.High than VE 1's L0
	// file. They share the same user-key interval, so the L0Organizer will see
	// both in the same fileInterval. If buildEdits handed them to a Version
	// constructor that preserved slice order, the second file's smaller
	// SeqNums.High would trip the addFileToSublevels precondition.
	high := newL0Table(1, 200, 200)
	low := newL0Table(2, 100, 100)

	edits := []*manifest.VersionEdit{
		{NewTables: []manifest.NewTableEntry{{Level: 0, Meta: high}}},
		{NewTables: []manifest.NewTableEntry{{Level: 0, Meta: low}}},
	}

	l := newLSM(&pebble.Options{}, sstable.Comparers{cmp.Name: cmp})
	l.cmp = cmp
	require.NotPanics(t, func() {
		require.NoError(t, l.buildEdits(edits))
	})
	require.Len(t, l.state.Edits, 2)
}
