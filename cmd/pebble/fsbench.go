// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"github.com/cockroachdb/pebble/bench"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/spf13/cobra"
)

var fsBenchConfig = bench.FsBenchConfig{
	NumTimes: 1,
	FS:       vfs.Default,
}

var fsBenchCmd = &cobra.Command{
	Use:   "fs <dir>",
	Short: "Run file system benchmarks.",
	Long: `
Run file system benchmarks. Each benchmark is predefined and can be
run using the command "bench fs <dir> --bench-name <benchmark>".
Each possible <benchmark> which can be run is defined in the code.
Benchmarks may require the specification of a --duration or
--max-ops flag, to prevent the benchmark from running forever
or running out of memory.

The --num-times flag can be used to run the entire benchmark, more than
once. If the flag isn't provided, then the benchmark is only run once.
`,
	Args: cobra.ExactArgs(1),
	RunE: runFsBench,
}

func init() {
	fsBenchCmd.Flags().IntVar(
		&fsBenchConfig.MaxOps, "max-ops", 0,
		"Maximum number of times the operation which is being benchmarked should be run.",
	)

	fsBenchCmd.Flags().StringVar(
		&fsBenchConfig.BenchName, "bench-name", "", "The benchmark to run.")
	_ = fsBenchCmd.MarkFlagRequired("bench-name")

	fsBenchCmd.Flags().IntVar(
		&fsBenchConfig.NumTimes, "num-times", 1,
		"Number of times each benchmark should be run.")

	fsBenchCmd.AddCommand(listFsBench)
}

func runFsBench(_ *cobra.Command, args []string) error {
	fsBenchConfig.Verbose = commonCfg.Verbose
	return bench.RunFsBench(args[0], &commonCfg, &fsBenchConfig)
}
