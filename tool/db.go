// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tool

import (
	"fmt"

	"github.com/spf13/cobra"
)

// AllCmds is a list of all of the tool commands.
var AllCmds = []*cobra.Command{
	DBCmd,
	ManifestCmd,
	SSTableCmd,
	WALCmd,
}

// DBCmd is the root of the DB commands.
var DBCmd = &cobra.Command{
	Use:   "db",
	Short: "DB introspection tools",
}

// DBCheckCmd implements db check.
var DBCheckCmd = &cobra.Command{
	Use:   "check <dir>",
	Short: "verify checksums and metadata",
	Long: `
Verify sstable, manifest, and WAL checksums. Requires that the specified
database not be in use by another process.
`,
	Args: cobra.ExactArgs(1),
	Run:  runDBCheck,
}

// DBLSMCmd implements db lsm.
var DBLSMCmd = &cobra.Command{
	Use:   "lsm <dir>",
	Short: "print LSM structure",
	Long: `
Print the structure of the LSM tree. Requires that the specified database not
be in use by another process.
`,
	Args: cobra.ExactArgs(1),
	Run:  runDBLSM,
}

// DBScanCmd implements db scan.
var DBScanCmd = &cobra.Command{
	Use:   "db <dir>",
	Short: "print db records",
	Long: `
Print the records in the DB. Requires that the specified database not be in use
by another process.
`,
	Args: cobra.ExactArgs(1),
	Run:  runDBScan,
}

func init() {
	DBCmd.AddCommand(
		DBCheckCmd,
		DBLSMCmd,
		DBScanCmd,
	)
}

func runDBCheck(cmd *cobra.Command, args []string) {
	fmt.Fprintf(stderr, "TODO(peter): \"db check\" unimplemented\n")
	osExit(1)
}

func runDBLSM(cmd *cobra.Command, args []string) {
	fmt.Fprintf(stderr, "TODO(peter): \"db lsm\" unimplemented\n")
	osExit(1)
}

func runDBScan(cmd *cobra.Command, args []string) {
	fmt.Fprintf(stderr, "TODO(peter): \"db scan\" unimplemented\n")
	osExit(1)
}
