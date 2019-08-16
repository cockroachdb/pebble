// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tool

import (
	"fmt"

	"github.com/petermattis/pebble/internal/base"
	"github.com/spf13/cobra"
)

// manifestT implements manifest-level tools, including both configuration
// state and the commands themselves.
type manifestT struct {
	Root *cobra.Command
	Dump *cobra.Command
}

func newManifest(opts *base.Options) *manifestT {
	m := &manifestT{}

	m.Root = &cobra.Command{
		Use:   "manifest",
		Short: "manifest introspection tools",
	}
	m.Dump = &cobra.Command{
		Use:   "dump <manifest-files>",
		Short: "print manifest contents",
		Long: `
Print the contents of the MANIFEST files.
`,
		Args: cobra.MinimumNArgs(1),
		Run:  m.runDump,
	}

	m.Root.AddCommand(m.Dump)
	return m
}

func (m *manifestT) runDump(cmd *cobra.Command, args []string) {
	fmt.Fprintf(stderr, "TODO(peter): \"manifest dump\" unimplemented\n")
	osExit(1)
}
