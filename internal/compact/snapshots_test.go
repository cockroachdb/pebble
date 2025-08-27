// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package compact

import (
	"testing"

	"github.com/cockroachdb/pebble/v2/internal/base"
)

func TestSnapshotIndex(t *testing.T) {
	testCases := []struct {
		snapshots      []base.SeqNum
		seq            base.SeqNum
		expectedIndex  int
		expectedSeqNum base.SeqNum
	}{
		{snapshots: []base.SeqNum{}, seq: 1, expectedIndex: 0, expectedSeqNum: base.SeqNumMax},
		{snapshots: []base.SeqNum{1}, seq: 0, expectedIndex: 0, expectedSeqNum: 1},
		{snapshots: []base.SeqNum{1}, seq: 1, expectedIndex: 1, expectedSeqNum: base.SeqNumMax},
		{snapshots: []base.SeqNum{1}, seq: 2, expectedIndex: 1, expectedSeqNum: base.SeqNumMax},
		{snapshots: []base.SeqNum{1, 3}, seq: 1, expectedIndex: 1, expectedSeqNum: 3},
		{snapshots: []base.SeqNum{1, 3}, seq: 2, expectedIndex: 1, expectedSeqNum: 3},
		{snapshots: []base.SeqNum{1, 3}, seq: 3, expectedIndex: 2, expectedSeqNum: base.SeqNumMax},
		{snapshots: []base.SeqNum{1, 3}, seq: 4, expectedIndex: 2, expectedSeqNum: base.SeqNumMax},
		{snapshots: []base.SeqNum{1, 3, 3}, seq: 2, expectedIndex: 1, expectedSeqNum: 3},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			s := Snapshots(c.snapshots)
			idx, seqNum := s.IndexAndSeqNum(c.seq)
			if c.expectedIndex != idx {
				t.Fatalf("expected %d, but got %d", c.expectedIndex, idx)
			}
			if c.expectedSeqNum != seqNum {
				t.Fatalf("expected %d, but got %d", c.expectedSeqNum, seqNum)
			}
		})
	}
}
