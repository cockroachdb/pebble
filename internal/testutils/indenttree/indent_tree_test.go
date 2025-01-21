// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package indenttree

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/v2/internal/treeprinter"
)

func TestIndentTree(t *testing.T) {
	datadriven.RunTest(t, "testdata", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "parse":
			nodes, err := Parse(d.Input)
			if err != nil {
				return fmt.Sprintf("error: %s", err)
			}
			tp := treeprinter.New()
			root := tp.Child("<root>")
			var dfs func(n Node, tp treeprinter.Node)
			dfs = func(n Node, parent treeprinter.Node) {
				child := parent.Child(n.Value())
				for _, c := range n.Children() {
					dfs(c, child)
				}
			}
			for _, c := range nodes {
				dfs(c, root)
			}
			return tp.String()

		default:
			t.Fatalf("unknown command: %s", d.Cmd)
			return ""
		}
	})
}
