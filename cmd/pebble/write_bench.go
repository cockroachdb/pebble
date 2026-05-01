// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"github.com/cockroachdb/pebble/bench"
	"github.com/spf13/cobra"
)

var writeBenchConfig = bench.DefaultWriteBenchConfig()

var writeBenchCmd = &cobra.Command{
	Use:   "write <dir>",
	Short: "Run YCSB F to find an a sustainable write throughput",
	Long: `
Run YCSB F (100% writes) at varying levels of sustained write load (ops/sec) to
determine an optimal value of write throughput.

The benchmark works by maintaining a fixed amount of write load on the DB for a
fixed amount of time. If the database can handle the sustained load - determined
by a heuristic that takes into account the number of files in L0 sub-levels, the
number of L0 sub-levels, and whether the DB has encountered a write stall (i.e.
measured load on the DB drops to zero) - the load is increased on the DB.

Load increases exponentially from an initial load. If the DB fails the heuristic
at the given write load, the load on the DB is paused for a period of time (the
cool-off period) before returning to the last value at which the DB could handle
the load. The exponent is then reset and the process repeats from this new
initial value. This allows the benchmark to converge on and oscillate around the
optimal write load.

The values of load at which the DB passes and fails the heuristic are maintained
over the duration of the benchmark. On completion of the benchmark, an "optimal"
value is computed. The optimal value is computed as the value that minimizes the
mis-classification of the recorded "passes" and "fails"". This can be visualized
as a point on the x-axis that separates the passes and fails into the left and
right half-planes, minimizing the number of fails that fall to the left of this
point (i.e. mis-classified fails) and the number of passes that fall to the
right (i.e. mis-classified passes).

The resultant "optimal sustained write load" value provides an estimate of the
write load that the DB can sustain without failing the target heuristic.

A typical invocation of the benchmark is as follows:

  pebble bench write [PATH] --wipe -c 1024 -d 8h --rate-start 30000 --debug
`,
	Args: cobra.ExactArgs(1),
	RunE: runWriteBenchmark,
}

func init() {
	initWriteBench(writeBenchCmd)
}

func initWriteBench(cmd *cobra.Command) {
	cfg := &writeBenchConfig
	cmd.Flags().Var(
		cfg.Batch, "batch",
		"batch size distribution [{zipf,uniform}:]min[-max]")
	cmd.Flags().StringVar(
		&cfg.Keys, "keys", cfg.Keys, "latest, uniform, or zipf")
	cmd.Flags().Var(
		cfg.Values, "values",
		"value size distribution [{zipf,uniform}:]min[-max][/<target-compression>]")
	cmd.Flags().IntVarP(
		&cfg.Concurrency, "concurrency", "c", cfg.Concurrency, "number of concurrent workers")
	cmd.Flags().IntVar(
		&cfg.RateStart, "rate-start", cfg.RateStart, "starting write load (ops/sec)")
	cmd.Flags().IntVar(
		&cfg.IncBase, "rate-inc-base", cfg.IncBase, "increment / decrement base")
	cmd.Flags().DurationVar(
		&cfg.TestPeriod, "test-period", cfg.TestPeriod, "time to run at a given write load")
	cmd.Flags().DurationVar(
		&cfg.CooloffPeriod, "cooloff-period", cfg.CooloffPeriod, "time to pause write load after a failure")
	cmd.Flags().IntVar(
		&cfg.TargetL0Files, "l0-files", cfg.TargetL0Files, "target L0 file count")
	cmd.Flags().IntVar(
		&cfg.TargetL0SubLevels, "l0-sublevels", cfg.TargetL0SubLevels, "target L0 sublevel count")
	cmd.Flags().BoolVarP(
		&commonCfg.Wipe, "wipe", "w", false, "wipe the database before starting")
	cmd.Flags().Float64Var(
		&cfg.MaxRateDipFraction, "max-rate-dip-fraction", cfg.MaxRateDipFraction,
		"fraction at which to mark a test-run as failed if the actual rate dips below (relative to the desired rate)")
	cmd.Flags().BoolVar(
		&cfg.Debug, "debug", false, "print benchmark debug information")
}

func runWriteBenchmark(_ *cobra.Command, args []string) error {
	return bench.RunWriteBench(args[0], &commonCfg, &writeBenchConfig)
}
