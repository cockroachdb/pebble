// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tool

import (
	"fmt"

	"github.com/petermattis/pebble/internal/base"
	"github.com/spf13/cobra"
)

// dbT implements db-level tools, including both configuration state and the
// commands themselves.
type dbT struct {
	Root  *cobra.Command
	Check *cobra.Command
	LSM   *cobra.Command
	Scan  *cobra.Command
}

func newDB(opts *base.Options) *dbT {
	d := &dbT{}

	d.Root = &cobra.Command{
		Use:   "db",
		Short: "DB introspection tools",
	}
	d.Check = &cobra.Command{
		Use:   "check <dir>",
		Short: "verify checksums and metadata",
		Long: `
Verify sstable, manifest, and WAL checksums. Requires that the specified
database not be in use by another process.
`,
		Args: cobra.ExactArgs(1),
		Run:  d.runCheck,
	}
	d.LSM = &cobra.Command{
		Use:   "lsm <dir>",
		Short: "print LSM structure",
		Long: `
Print the structure of the LSM tree. Requires that the specified database not
be in use by another process.
`,
		Args: cobra.ExactArgs(1),
		Run:  d.runLSM,
	}
	d.Scan = &cobra.Command{
		Use:   "scan <dir>",
		Short: "print db records",
		Long: `
Print the records in the DB. Requires that the specified database not be in use
by another process.
`,
		Args: cobra.ExactArgs(1),
		Run:  d.runScan,
	}

	d.Root.AddCommand(d.Check, d.LSM, d.Scan)
	return d
}

func (d *dbT) runCheck(cmd *cobra.Command, args []string) {
	fmt.Fprintf(stderr, "TODO(peter): \"db check\" unimplemented\n")
	osExit(1)
}

func (d *dbT) runLSM(cmd *cobra.Command, args []string) {
	fmt.Fprintf(stderr, "TODO(peter): \"db lsm\" unimplemented\n")
	osExit(1)
}

func (d *dbT) runScan(cmd *cobra.Command, args []string) {
	fmt.Fprintf(stderr, "TODO(peter): \"db scan\" unimplemented\n")
	osExit(1)
}
