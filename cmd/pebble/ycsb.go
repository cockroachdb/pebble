// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"github.com/cockroachdb/pebble/bench"
	"github.com/cockroachdb/pebble/internal/randvar"
	"github.com/spf13/cobra"
)

var ycsbConfig = bench.DefaultYCSBConfig()

var ycsbCmd = &cobra.Command{
	Use:   "ycsb <dir>",
	Short: "run customizable YCSB benchmark",
	Long: `
Run a customizable YCSB workload. The workload is specified by the --workload
flag which can take either one of the standard workload mixes (A-F), or
customizable workload fixes specified as a command separated list of op=weight
pairs. For example, --workload=read=50,update=50 performs a workload composed
of 50% reads and 50% updates. This is identical to the standard workload A.

The --batch, --scans, and --values flags take the specification for a random
variable: [<type>:]<min>[-<max>]. The <type> parameter must be one of "uniform"
or "zipf". If <type> is omitted, a uniform distribution is used. If <max> is
omitted it is set to the same value as <min>. The specification "1000" results
in a constant 1000. The specification "10-100" results in a uniformly random
variable in the range [10,100). The specification "zipf(10,100)" results in a
zipf distribution with a minimum value of 10 and a maximum value of 100.

The --batch flag controls the size of batches used for insert and update
operations. The --scans flag controls the number of iterations performed by a
scan operation. Read operations always read a single key.

The --values flag provides for an optional "/<target-compression-ratio>"
suffix. The default target compression ratio is 1.0 (i.e. incompressible random
data). A value of 2 will cause random data to be generated that should compress
to 50% of its uncompressed size.

Standard workloads:

  A:  50% reads   /  50% updates
  B:  95% reads   /   5% updates
  C: 100% reads
  D:  95% reads   /   5% inserts
  E:  95% scans   /   5% inserts
  F: 100% inserts
`,
	Args: cobra.ExactArgs(1),
	RunE: runYcsb,
}

func init() {
	initYCSB(ycsbCmd, &ycsbConfig)
}

func initYCSB(cmd *cobra.Command, cfg *bench.YCSBConfig) {
	cmd.Flags().Var(
		cfg.Batch.(*randvar.Flag), "batch",
		"batch size distribution [{zipf,uniform}:]min[-max]")
	cmd.Flags().StringVar(
		&cfg.Keys, "keys", cfg.Keys, "latest, uniform, or zipf")
	cmd.Flags().IntVar(
		&cfg.InitialKeys, "initial-keys", cfg.InitialKeys,
		"initial number of keys to insert before beginning workload")
	cmd.Flags().IntVar(
		&cfg.PrepopulatedKeys, "prepopulated-keys", 0,
		"number of keys that were previously inserted into the database")
	cmd.Flags().Uint64VarP(
		&cfg.NumOps, "num-ops", "n", 0,
		"maximum number of operations (0 means unlimited)")
	cmd.Flags().Var(
		cfg.Scans.(*randvar.Flag), "scans",
		"scan length distribution [{zipf,uniform}:]min[-max]")
	cmd.Flags().StringVar(
		&cfg.Workload, "workload", cfg.Workload,
		"workload type (A-F) or spec (read=X,update=Y,...)")
	cmd.Flags().Var(
		cfg.Values.(*randvar.BytesFlag), "values",
		"value size distribution [{zipf,uniform}:]min[-max][/<target-compression>]")
}

func runYcsb(cmd *cobra.Command, args []string) error {
	commonCfg.RateLimiter = maxOpsPerSec.newRateLimiter()
	return bench.RunYCSB(args[0], &commonCfg, &ycsbConfig)
}
