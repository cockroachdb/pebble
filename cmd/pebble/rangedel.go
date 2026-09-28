// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"github.com/cockroachdb/pebble/bench"
	"github.com/spf13/cobra"
)

var rangeDelConfig = bench.DefaultRangeDelConfig()

var rangeDelCmd = &cobra.Command{
	Use:   "rangedel <dir>",
	Short: "benchmark reads/writes intermixed with range deletions",
	Long: `
Runs a workload that performs point reads, scanning reads and point writes
concurrently with range deletions. Read and write key ranges do not overlap
deletion key ranges. The workload is intended to evaluate the effectiveness of
memtable range delete optimization that avoids rebuilding fragments when reads
do not overlap with deleted ranges.
	`,
	Args: cobra.ExactArgs(1),
	RunE: runRangeDelCmd,
}

func init() {
	f := rangeDelCmd.Flags()
	f.IntVar(&rangeDelConfig.Readers, "readers", rangeDelConfig.Readers,
		"number of concurrent readers")
	f.DurationVar(&rangeDelConfig.ReaderInterval, "reader-interval", rangeDelConfig.ReaderInterval,
		"duration between reads (0 to unthrottle)")
	f.IntVar(&rangeDelConfig.Writers, "writers", rangeDelConfig.Writers,
		"number of concurrent writers")
	f.DurationVar(&rangeDelConfig.WriterInterval, "writer-interval", rangeDelConfig.WriterInterval,
		"duration between writes (0 to unthrottle)")
}

func runRangeDelCmd(cmd *cobra.Command, args []string) error {
	commonCfg.RateLimiter = maxOpsPerSec.newRateLimiter()
	return bench.RunRangeDel(args[0], &commonCfg, &rangeDelConfig)
}
