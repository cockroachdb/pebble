// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package treesteps

import (
	"bytes"
	"compress/zlib"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/cockroachdb/pebble/internal/treeprinter"
)

// Steps contains the result of a recording: the full details of the hierarchy
// for each step.
type Steps struct {
	Name  string
	Steps []Step `json:"steps"`
}

// Step contains the tree for a single step in a recording.
type Step struct {
	Name string   `json:"name"`
	Root TreeNode `json:"tree"`
}

// TreeNode is a node in the tree for a step in a recording.
type TreeNode struct {
	Name              string      `json:"name"`
	Properties        [][2]string `json:"props"`
	Ops               []string    `json:"ops"`
	Children          []TreeNode  `json:"children"`
	HasHiddenChildren bool        `json:"hasHiddenChildren"`
}

// String returns the steps as a string (using treeprinter).
func (s Steps) String() string {
	var buf strings.Builder
	for step := range s.Steps {
		if len(s.Steps) > 1 {
			fmt.Fprintf(&buf, "step %d/%d:\n", step+1, len(s.Steps))
		}
		out := s.Steps[step].Root.String()
		if len(s.Steps) > 1 {
			for _, l := range strings.Split(strings.TrimSpace(out), "\n") {
				fmt.Fprintf(&buf, "  %s\n", l)
			}
		} else {
			buf.WriteString(out)
		}
	}
	return buf.String()
}

func (t *TreeNode) String() string {
	tp := treeprinter.New()
	t.print(tp)
	return tp.String()
}

func (t *TreeNode) print(parent treeprinter.Node) {
	name := t.Name
	for i := range t.Ops {
		name += fmt.Sprintf("  ← %s", t.Ops[i])
	}
	n := parent.Child(name)
	for _, p := range t.Properties {
		if p[1] == "" {
			n.AddLine(fmt.Sprintf("   %s", p[0]))
		} else {
			n.AddLine(fmt.Sprintf("   %s: %s", p[0], p[1]))
		}
	}
	for i := range t.Children {
		t.Children[i].print(n)
	}
	if t.HasHiddenChildren {
		n.DotDotDot()
	}
}

// URL for a visualization of the steps. The URL contains the encoded and
// compressed data as the URL fragment.
func (s Steps) URL() url.URL {
	// TODO(radu): ideally we would encode Steps and have a graphical
	// visualization. For now, we generate and encode the ASCII trees.
	var output struct {
		Name      string
		StepNames []string
		Steps     []string
	}
	output.Name = s.Name
	output.StepNames = make([]string, len(s.Steps))
	output.Steps = make([]string, len(s.Steps))
	for i := range s.Steps {
		output.StepNames[i] = s.Steps[i].Name
		output.Steps[i] = s.Steps[i].Root.String()
	}

	var jsonBuf bytes.Buffer
	if err := json.NewEncoder(&jsonBuf).Encode(&output); err != nil {
		panic(err)
	}
	var compressed bytes.Buffer
	encoder := base64.NewEncoder(base64.URLEncoding, &compressed)
	compressor, err := zlib.NewWriterLevel(encoder, zlib.BestCompression)
	if err != nil {
		panic(err)
	}
	if _, err := jsonBuf.WriteTo(compressor); err != nil {
		panic(err)
	}
	if err := compressor.Close(); err != nil {
		panic(err)
	}
	if err := encoder.Close(); err != nil {
		panic(err)
	}
	return url.URL{
		Scheme:   "https",
		Host:     "raduberinde.github.io",
		Path:     "treesteps/decode.html",
		Fragment: compressed.String(),
	}
}
