// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build !invariants

package treesteps

const Enabled = false

type RecordingOption struct{}

func MaxTreeDepth(maxTreeDepth int) RecordingOption { return RecordingOption{} }
func MaxOpDepth(maxOpDepth int) RecordingOption     { return RecordingOption{} }

func StartRecording(root Node, name string, opts ...RecordingOption) *Recording { return nil }

func NodeUpdated(n Node, reason string) {}

// Node must be implemented by every node in the hierarchy.
type Node interface {
	TreeStepsNode() NodeInfo
}
type NodeInfo struct{}

func NodeInfof(format string, args ...any) NodeInfo                  { return NodeInfo{} }
func (ni *NodeInfo) AddPropf(key string, format string, args ...any) {}
func (ni *NodeInfo) AddChildren(nodes ...Node)                       {}

type Recording struct{}

func (r *Recording) Finish() Steps { return Steps{} }

func IsRecording(n Node) bool { return false }

type Op struct{}

func StartOpf(node Node, format string, args ...any) *Op  { return nil }
func (op *Op) Updatef(format string, args ...any)         {}
func UpdateLastOpf(node Node, format string, args ...any) {}
func (op *Op) Finishf(format string, args ...any)         {}

func TreeToString(n Node) string { return "treesteps not supported in this build" }
