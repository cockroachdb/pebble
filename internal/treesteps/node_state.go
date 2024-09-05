// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package treesteps

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type nodeState struct {
	recording *Recording
	depth     int
	node      Node
	name      string

	// ops currently running for this node.
	ops []*Op
}

var mu struct {
	sync.Mutex

	recordingInProgress atomic.Bool
	nodeMap             map[Node]*nodeState
}

func buildTree(n *nodeState) TreeNode {
	var t TreeNode
	info := n.node.TreeStepsNode()
	n.name = info.name
	t.Name = info.name
	t.Properties = info.properties
	if len(n.ops) > 0 {
		t.Ops = make([]string, len(n.ops))
		for i := range t.Ops {
			t.Ops[i] = n.ops[i].details
			if n.ops[i].state != "" {
				t.Ops[i] += " " + n.ops[i].state
			}
		}
	}
	if n.depth < n.recording.maxTreeDepth {
		for i := range info.children {
			c := nodeStateLocked(n.recording, info.children[i])
			c.depth = n.depth + 1
			t.Children = append(t.Children, buildTree(c))
		}
	} else {
		for range info.children {
			t.Children = append(t.Children, TreeNode{Name: "..."})
		}
	}
	return t
}

func nodeStateLocked(w *Recording, n Node) *nodeState {
	ns, ok := mu.nodeMap[n]
	if !ok {
		ns = &nodeState{recording: w, node: n}
		mu.nodeMap[n] = ns
	} else if w != ns.recording {
		panic(fmt.Sprintf("node %v part of multiple recordings", n))
	}
	return ns
}
