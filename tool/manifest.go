// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tool

import (
	"fmt"

	"github.com/spf13/cobra"
)

// ManifestCmd is the root of the manifest commands.
var ManifestCmd = &cobra.Command{
	Use:   "manifest",
	Short: "manifest introspection tools",
}

// ManifestDumpCmd implements manifest dump.
var ManifestDumpCmd = &cobra.Command{
	Use:   "dump <manifest-files>",
	Short: "print manifest contents",
	Long: `
Print the contents of the MANIFEST files.
`,
	Args: cobra.MinimumNArgs(1),
	Run:  runManifestDump,
}

func init() {
	ManifestCmd.AddCommand(ManifestDumpCmd)
}

func runManifestDump(cmd *cobra.Command, args []string) {
	fmt.Fprintf(stderr, "TODO(peter): \"manifest dump\" unimplemented\n")
	osExit(1)
}
