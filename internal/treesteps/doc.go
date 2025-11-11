// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package treesteps provides a framework for recording and visualizing
// step-by-step operations on hierarchical data structures.
//
// The package captures the propagation of operations through a tree-like
// structure, recording each intermediate step along with any state changes.
// This is particularly useful for understanding and debugging how operations
// traverse and modify complex hierarchies.
//
// # Basic Usage
//
// To use treesteps, your hierarchical structure must implement the Node
// interface. Each node should provide:
//   - A descriptive name
//   - Key properties (as key-value pairs)
//   - References to child nodes
//
// Recording works as follows:
//
//  1. Start a recording with StartRecording() on the root node
//  2. For each operation on a node, call StartOpf() before and Finishf() after
//  3. Call NodeUpdated() whenever a node's state changes significantly
//  4. Call Finish() to obtain the recorded steps
//
// The result can be rendered as text or exported as a URL to an interactive
// visualization.
//
// # Build Tags
//
// The treesteps functionality is only available when building with the
// 'invariants' tag. Without this tag, all recording operations become no-ops.
// This allows instrumentation to remain in production code without performance
// impact.
//
// # Example
//
// This example demonstrates recording operations on a simple binary tree that
// tracks the sum of values at each node:
//
//	type SumTree struct {
//	    Value int
//	    Sum   int
//	    Left  *SumTree
//	    Right *SumTree
//	}
//
//	// TreeStepsNode implements the treesteps.Node interface.
//	func (t *SumTree) TreeStepsNode() treesteps.NodeInfo {
//	    info := treesteps.NodeInfof("node")
//	    info.AddPropf("value", "%d", t.Value)
//	    info.AddPropf("sum", "%d", t.Sum)
//	    info.AddChildren(t.Left, t.Right)
//	    return info
//	}
//
//	// UpdateValue sets a new value and propagates the change up the tree.
//	func (t *SumTree) UpdateValue(newValue int) {
//	    op := treesteps.StartOpf(t, "update(%d)", newValue)
//	    t.Value = newValue
//	    treesteps.NodeUpdated(t, "value changed")
//	    t.recomputeSum()
//	    op.Finishf("done")
//	}
//
//	func (t *SumTree) recomputeSum() {)
//	    sum := t.Value
//	    if t.Left != nil {
//	        sum += t.Left.Sum
//	    }
//	    if t.Right != nil {
//	        sum += t.Right.Sum
//	    }
//	    if sum != t.Sum {
//	        t.Sum = sum
//	        treesteps.NodeUpdated(t, "sum updated")
//	    }
//	}
//
//	func Example() {
//	    root := &SumTree{Value: 5}
//	    root.Left = &SumTree{Value: 3}
//	    root.Right = &SumTree{Value: 7}
//
//	    // Start recording
//	    rec := treesteps.StartRecording(root, "Update value")
//
//	    // Perform the operation
//	    root.Left.UpdateValue(10)
//
//	    // Finish and get the steps
//	    steps := rec.Finish()
//
//	    // Print the recorded steps
//	    fmt.Println(steps.String())
//
//	    // Or get a URL for visualization
//	    url := steps.URL()
//	    fmt.Println("Visualization:", url.String())
//	}
//
// # Recording Options
//
// The MaxTreeDepth and MaxOpDepth options can limit the depth of recording,
// which is useful for large or deeply nested structures:
//
//	rec := treesteps.StartRecording(root, "operation",
//	    treesteps.MaxTreeDepth(5),
//	    treesteps.MaxOpDepth(3),
//	)
//
// # Performance Considerations
//
// Check Enabled && IsRecording() before formatting operations:
//
//	if treesteps.Enabled && treesteps.IsRecording(node) {
//	    op := treesteps.StartOpf(node, "expensive operation: %s", expensiveFormat())
//	    defer op.Finishf("done")
//	}
//
// This avoids allocations when no recording is active. It also allows
// statically eliminating code in non-invariants builds.
package treesteps
