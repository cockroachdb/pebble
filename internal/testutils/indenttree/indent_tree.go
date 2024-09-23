// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package indenttree implements a simple text processor which parses a
// hierarchy defined using indentation; see Parse.
package indenttree

import (
	"slices"
	"strings"

	"github.com/cockroachdb/errors"
)

// Parse a multi-line input string into trees of nodes. For example:
//
//	a
//	 a1
//	  a11
//	 a2
//	b
//	 b1
//
// is parsed into two Nodes (a and b). Node a has two children (a1, a2), and a2
// has one child (a11); node b has one child (b1).
//
// The indentation level is arbitrary but it must be consistent. across nodes. For example, the following is not valid:
//
//	a
//	 a1
//	b
//	  b1
//
// Tabs cannot be used for indentation (they can cause confusion if editor
// settings vary). Nodes cannot be skipped, for example the following is not
// valid:
//
//	a
//	  a1
//	    a11
//	b
//	    b12
func Parse(input string) ([]Node, error) {
	input = strings.TrimSuffix(input, "\n")
	if input == "" {
		return nil, errors.Errorf("empty input")
	}
	lines := strings.Split(input, "\n")
	indentLevel := make([]int, len(lines))
	for i, line := range lines {
		level := 0
		for strings.HasPrefix(line[level:], " ") {
			level++
		}
		if len(line) == level {
			return nil, errors.Errorf("empty line in input:\n%s", input)
		}
		if line[level] == '\t' {
			return nil, errors.Errorf("tab indentation in input:\n%s", input)
		}
		indentLevel[i] = level
	}
	levels := slices.Clone(indentLevel)
	slices.Sort(levels)
	levels = slices.Compact(levels)

	var parse func(levelIdx, startLineIdx, endLineIdx int) ([]Node, error)
	parse = func(levelIdx, startLineIdx, endLineIdx int) ([]Node, error) {
		if startLineIdx > endLineIdx {
			return nil, nil
		}
		level := levels[levelIdx]
		if indentLevel[startLineIdx] != level {
			return nil, errors.Errorf("inconsistent indentation in input:\n%s", input)
		}
		nextNode := startLineIdx + 1
		for ; nextNode <= endLineIdx; nextNode++ {
			if indentLevel[nextNode] <= level {
				break
			}
		}
		node := Node{value: lines[startLineIdx][level:]}
		var err error
		node.children, err = parse(levelIdx+1, startLineIdx+1, nextNode-1)
		if err != nil {
			return nil, err
		}
		otherNodes, err := parse(levelIdx, nextNode, endLineIdx)
		if err != nil {
			return nil, err
		}
		return append([]Node{node}, otherNodes...), nil
	}
	return parse(0, 0, len(lines)-1)
}

// Node in a hierarchy returned by Parse.
type Node struct {
	value    string
	children []Node
}

// Value returns the contents of the line for this node (without the
// indentation).
func (n *Node) Value() string {
	return n.value
}

// Children returns the child nodes, if any.
func (n *Node) Children() []Node {
	return n.children
}
