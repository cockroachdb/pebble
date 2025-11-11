// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package treesteps

import (
	"math/bits"
	"testing"

	"github.com/cockroachdb/datadriven"
)

// SegmentTree implements a segment tree:
// https://en.wikipedia.org/wiki/Segment_tree
//
// It stores values for points in the range [1, Range]. It can efficiently Add
// to the value of a single point, and query the Sum of any range.
type SegmentTree struct {
	Range int
	Nodes []SegmentNode
}

// SegmentNode is a node in a segment tree.
type SegmentNode struct {
	// Note: in a normal segment tree implementation, we would only need the sum
	// (the other information is implicitly available as we go down the tree). But
	// we store the other fields for the benefit of displaying them.
	Start, End int
	// Index of the node in Tree.Nodes.
	Index int
	// Sum of the values for the range [Start, End].
	Sum  int
	Tree *SegmentTree
}

// NewSegmentTree creates a SegmentTree with the given range.
func NewSegmentTree(xRange int) *SegmentTree {
	size := 1 << (bits.Len32(uint32(xRange-1)) + 1)
	t := &SegmentTree{
		Range: xRange,
		Nodes: make([]SegmentNode, size),
	}
	t.Nodes[0] = SegmentNode{
		Start: 1,
		End:   xRange,
		Tree:  t,
		Index: 0,
	}
	for i := range t.Nodes {
		if n := &t.Nodes[i]; n.End > n.Start {
			t.Nodes[2*i+1] = SegmentNode{
				Start: n.Start,
				End:   (n.Start + n.End) / 2,
				Tree:  t,
				Index: 2*i + 1,
			}
			t.Nodes[2*i+2] = SegmentNode{
				Start: (n.Start+n.End)/2 + 1,
				End:   n.End,
				Tree:  t,
				Index: 2*i + 2,
			}
		}
	}
	return t
}

var _ Node = (*SegmentNode)(nil)

// TreeStepsNode implements the Node interface.
func (n *SegmentNode) TreeStepsNode() NodeInfo {
	info := NodeInfof("n%d", n.Index)
	info.AddPropf("range", "[%d, %d]", n.Start, n.End)
	info.AddPropf("sum", "%d", n.Sum)
	if n.Start < n.End {
		info.AddChildren(
			&n.Tree.Nodes[2*n.Index+1],
			&n.Tree.Nodes[2*n.Index+2],
		)
	}
	return info
}

// Root returns the root node of the segment tree.
func (t *SegmentTree) Root() *SegmentNode {
	return &t.Nodes[0]
}

// Add delta to the value for x.
func (t *SegmentTree) Add(x int, delta int) {
	t.add(0, x, delta)
}

func (t *SegmentTree) add(nodeIdx int, x int, delta int) {
	n := &t.Nodes[nodeIdx]
	if x < n.Start || n.End < x {
		return
	}
	op := StartOpf(n, "add(%d, %d)", x, delta)
	if n.Start < n.End {
		t.add(2*nodeIdx+1, x, delta)
		t.add(2*nodeIdx+2, x, delta)
	}
	n.Sum += delta
	NodeUpdated(n, "sum updated")
	op.Finishf("done")
}

// Sum returns the sum of the values for x1 through x2.
func (t *SegmentTree) Sum(x1, x2 int) int {
	return t.sum(0, x1, x2)
}

func (t *SegmentTree) sum(nodeIdx int, x1, x2 int) (result int) {
	n := &t.Nodes[nodeIdx]
	if x2 < n.Start || n.End < x1 {
		return
	}
	if IsRecording(n) {
		op := StartOpf(n, "sum(%d, %d)", x1, x2)
		defer func() {
			op.Finishf("= %d", result)
		}()
	}
	if x1 <= n.Start && n.End <= x2 {
		return n.Sum
	}
	return t.sum(2*nodeIdx+1, x1, x2) + t.sum(2*nodeIdx+2, x1, x2)
}

func TestSegmentTree(t *testing.T) {
	if !Enabled {
		t.Skip("treesteps not available in this build")
	}
	var tree *SegmentTree
	datadriven.RunTest(t, "testdata/segment_tree", func(t *testing.T, td *datadriven.TestData) string {
		var r *Recording
		var opts []RecordingOption
		if val := 0; td.MaybeScanArgs(t, "max-tree-depth", &val) {
			opts = append(opts, MaxTreeDepth(val))
		}
		if val := 0; td.MaybeScanArgs(t, "max-op-depth", &val) {
			opts = append(opts, MaxOpDepth(val))
		}
		switch td.Cmd {
		case "init":
			var xRange int
			td.ScanArgs(t, "range", &xRange)
			tree = NewSegmentTree(xRange)
			return TreeToString(tree.Root())

		case "add":
			var x, delta int
			td.ScanArgs(t, "x", &x)
			td.ScanArgs(t, "delta", &delta)
			r = StartRecording(tree.Root(), "Segment Tree add", opts...)
			tree.Add(x, delta)

		case "sum":
			var x1, x2 int
			td.ScanArgs(t, "x1", &x1)
			td.ScanArgs(t, "x2", &x2)
			r = StartRecording(tree.Root(), "Segment Tree sum", opts...)
			tree.Sum(x1, x2)

		default:
			td.Fatalf(t, "unknown command %q", td.Cmd)
		}
		if r != nil {
			steps := r.Finish()
			if td.HasArg("url") {
				url := steps.URL()
				return url.String()
			}
			return steps.String()
		}
		return ""
	})
}
