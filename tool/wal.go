// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tool

import (
	"fmt"

	"github.com/spf13/cobra"
)

// WALCmd is the root of the WAL commands.
var WALCmd = &cobra.Command{
	Use:   "wal",
	Short: "WAL introspection tools",
}

// WALDumpCmd implements WAL dump.
var WALDumpCmd = &cobra.Command{
	Use:   "dump <wal-files>",
	Short: "print WAL contents",
	Long: `
Print the contents of the WAL files.
`,
	Args: cobra.MinimumNArgs(1),
	Run:  runWALDump,
}

func init() {
	WALCmd.AddCommand(WALDumpCmd)
}

func runWALDump(cmd *cobra.Command, args []string) {
	fmt.Fprintf(stderr, "TODO(peter): \"wal dump\" unimplemented\n")
	osExit(1)
}
