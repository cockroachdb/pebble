// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tool

import (
	"fmt"

	"github.com/petermattis/pebble/internal/base"
	"github.com/spf13/cobra"
)

// walT implements WAL-level tools, including both configuration state and the
// commands themselves.
type walT struct {
	Root *cobra.Command
	Dump *cobra.Command
}

func newWAL(opts *base.Options) *walT {
	w := &walT{}

	w.Root = &cobra.Command{
		Use:   "wal",
		Short: "WAL introspection tools",
	}
	w.Dump = &cobra.Command{
		Use:   "dump <wal-files>",
		Short: "print WAL contents",
		Long: `
Print the contents of the WAL files.
`,
		Args: cobra.MinimumNArgs(1),
		Run:  w.runDump,
	}

	w.Root.AddCommand(w.Dump)
	return w
}

func (w *walT) runDump(cmd *cobra.Command, args []string) {
	fmt.Fprintf(stderr, "TODO(peter): \"wal dump\" unimplemented\n")
	osExit(1)
}
