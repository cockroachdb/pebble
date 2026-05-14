// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/bench"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/spf13/cobra"
)

var listFsBench = &cobra.Command{
	Use:   "list [<name>] [<name>] ...",
	Short: "List the available file system benchmarks.",
	Long: `
List the available file system benchmarks. If no <name> is supplied
as an argument, then all the available benchmark names are printed.
If one or more <name>s are supplied as arguments, then the benchmark
descriptions are printed out for those names.
`,
	RunE: runListFsBench,
}

func runListFsBench(_ *cobra.Command, args []string) error {
	benchmarks := bench.FsBenchmarks(vfs.Default)
	if len(args) == 0 {
		fmt.Println("Available benchmarks:")
		for name := range benchmarks {
			fmt.Println(name)
		}
	} else {
		for _, v := range args {
			b, ok := benchmarks[v]
			if !ok {
				return errors.Errorf("trying to print out the description for unknown benchmark: %s", v)
			}
			fmt.Println("Name:", b.Name)
			fmt.Println("Description:", b.Description)
		}
	}
	return nil
}
