// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build invariants

package treesteps

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"unicode"

	"github.com/cockroachdb/pebble/internal/treeprinter"
)

const Enabled = true

// StartRecording starts a new recording that captures step-by-step propagation
// of operations and changes in a hierarchical structure.
//
// Each node in the structure must implement the TreeStepsNode method, which
// returns information about the current state of the node and a list of its
// children. For every operation or sub-operation on a node, StartOpf and
// FinishOpf must be called. Calling Finish() on the Recording will return all
// the collected information (which can be presented as text or as a link to a
// visualization).
//
// It is not legal to start a recording that involves the same node as another
// in-progress recording (this applies to all nodes in the hierarchy).
//
// See SegmentTree in tree_steps_test.go for an example.
func StartRecording(root Node, name string, opts ...RecordingOption) *Recording {
	mu.Lock()
	defer mu.Unlock()
	mu.recordingInProgress.Store(true)
	w := &Recording{
		name:         name,
		maxTreeDepth: 20,
		maxOpDepth:   10,
	}
	for _, o := range opts {
		o(w)
	}

	if mu.nodeMap == nil {
		mu.nodeMap = make(map[Node]*nodeState)
	}
	w.root = nodeStateLocked(w, root)
	w.stepLockedf("initial")
	return w
}

// RecordingOption is an optional argument to StartRecording.
type RecordingOption func(*Recording)

// MaxTreeDepth configures a recording to only show trees up to a certain depth.
// Operations and node updates below that level are ignored.
func MaxTreeDepth(maxTreeDepth int) RecordingOption {
	return func(r *Recording) {
		r.maxTreeDepth = maxTreeDepth
		if r.maxOpDepth > r.maxTreeDepth {
			r.maxOpDepth = r.maxTreeDepth
		}
	}
}

// MaxOpDepth configures a recording to only show operations for nodes up to a
// certain depth. Operations and node updates below that level are ignored.
func MaxOpDepth(maxOpDepth int) RecordingOption {
	return func(r *Recording) {
		r.maxOpDepth = maxOpDepth
		if r.maxTreeDepth < r.maxOpDepth {
			r.maxTreeDepth = r.maxOpDepth
		}
	}
}

// NodeUpdated can be used to trigger a new step in any recording that involves
// the given node. It can be used when the state of the node changed in a
// significant way. The reason is optional and will show up in the step
// description.
func NodeUpdated(n Node, reason string) {
	if !mu.recordingInProgress.Load() {
		return
	}
	mu.Lock()
	defer mu.Unlock()
	if ns, ok := mu.nodeMap[n]; ok {
		if ns.depth > ns.recording.maxOpDepth {
			// Ignore node updates below the op depth (if we are not stepping through
			// operations on these nodes, we should not step through their updates
			// either).
			return
		}
		if reason == "" {
			reason = " updated"
		} else {
			reason = ": " + reason
		}
		ns.recording.stepLockedf("node %s%s", firstWord(ns.name), reason)
	}
}

// Node must be implemented by every node in the hierarchy.
type Node interface {
	TreeStepsNode() NodeInfo
}

// NodeInfo contains the information that we present for each node.
type NodeInfo struct {
	name       string
	properties [][2]string
	children   []Node
}

// NodeInfof returns a NodeInfo with the name initialized with a formatted
// string.
func NodeInfof(format string, args ...any) NodeInfo {
	return NodeInfo{name: fmt.Sprintf(format, args...)}
}

// AddPropf adds a property to the NodeInfo.
func (ni *NodeInfo) AddPropf(key string, format string, args ...any) {
	ni.properties = append(ni.properties, [2]string{key, fmt.Sprintf(format, args...)})
}

// AddChildren adds one or more children to the NodeInfo.
//
// Any nil children are ignored (this includes nil pointers of any type).
func (ni *NodeInfo) AddChildren(nodes ...Node) {
	for _, n := range nodes {
		if n == nil {
			continue
		}
		if val := reflect.ValueOf(n); val.Kind() == reflect.Ptr && val.IsNil() {
			continue
		}
		ni.children = append(ni.children, n)
	}
}

// Recording captures the step-by-step propagation of operations. See
// StartRecording.
type Recording struct {
	name         string
	maxTreeDepth int
	maxOpDepth   int
	root         *nodeState
	steps        []Step
}

// Finish completes the recording and returns the recorded steps.
func (r *Recording) Finish() Steps {
	mu.Lock()
	defer mu.Unlock()
	for n, ns := range mu.nodeMap {
		if ns.recording == r {
			delete(mu.nodeMap, n)
		}
	}
	if len(mu.nodeMap) == 0 {
		mu.recordingInProgress.Store(false)
	}
	r.root = nil
	return Steps{Name: r.name, Steps: r.steps}
}

// stpLockedf updates the tree (calling TreeStepsNode on all the nodes) and
// saves it as a step. The formatted string is the name/description of the step.
func (r *Recording) stepLockedf(format string, args ...any) {
	r.steps = append(r.steps, Step{
		Name: fmt.Sprintf(format, args...),
		Root: buildTree(r.root),
	})
}

// IsRecording indicates whether a recording involving a given node is currently
// in progress. Can be used as a fast check before calling StartOpf.
func IsRecording(n Node) bool {
	if !mu.recordingInProgress.Load() {
		return false
	}
	mu.Lock()
	defer mu.Unlock()
	_, ok := mu.nodeMap[n]
	return ok
}

// Op represents an operation that is associated with a node.
type Op struct {
	details   string
	state     string
	nodeState *nodeState
}

// StartOpf registers the start of a new operation on a node and emits a step.
// The operation will be displayed for the given node until FinishOpf is called.
//
// If a recording involving the node is not currently in progress, does nothing
// and returns nil. Other methods can be called (and do nothing) when Op is nil.
//
// Note: it is recommended that IsRecording() is checked first to avoid any
// allocations (e.g. due to boxing of args) in the normal, non-recording path.
func StartOpf(node Node, format string, args ...any) *Op {
	mu.Lock()
	defer mu.Unlock()
	ns, ok := mu.nodeMap[node]
	if !ok {
		return nil
	}
	if ns.depth > ns.recording.maxOpDepth {
		// Ignore any ops for nodes below maxOpDepth.
		return nil
	}
	op := &Op{
		details:   fmt.Sprintf(format, args...),
		nodeState: ns,
	}
	ns.ops = append(ns.ops, op)
	ns.recording.stepLockedf("%s on %s started", firstWord(op.details), firstWord(op.nodeState.name))
	return op
}

// Updatef changes the state of the operation and emits a step. The state of the
// // operation shows up after the details that were provided in StartOpf.
func (op *Op) Updatef(format string, args ...any) {
	if op == nil {
		return
	}
	mu.Lock()
	defer mu.Unlock()
	op.state = fmt.Sprintf(format, args...)
	op.nodeState.recording.stepLockedf("%s on %s updated", firstWord(op.details), firstWord(op.nodeState.name))
}

// Finishf changes the state of the operation, emits a step, and cleans up the
// operation.The state of the operation shows up after the details that were
// provided in StartOpf.
func (op *Op) Finishf(format string, args ...any) {
	if op == nil {
		return
	}
	mu.Lock()
	defer mu.Unlock()
	op.state = fmt.Sprintf(format, args...)
	op.nodeState.recording.stepLockedf("%s on %s finished", firstWord(op.details), firstWord(op.nodeState.name))
	op.nodeState.ops = slices.DeleteFunc(op.nodeState.ops, func(o *Op) bool { return o == op })
}

func firstWord(str string) string {
	for i, r := range str {
		if unicode.IsSpace(r) || strings.ContainsRune("()[]{}:", r) {
			return str[:i]
		}
	}
	return str
}

// TreeToString returns a string representation of the current state of a Node
// tree.
func TreeToString(n Node) string {
	tp := treeprinter.New()
	treePrint(n, tp)
	return tp.String()
}

func treePrint(n Node, tp treeprinter.Node) {
	ni := n.TreeStepsNode()
	tpNode := tp.Child(ni.name)
	for _, prop := range ni.properties {
		tpNode.Childf("%s: %s", prop[0], prop[1])
	}
	for _, child := range ni.children {
		treePrint(child, tpNode)
	}
}

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
