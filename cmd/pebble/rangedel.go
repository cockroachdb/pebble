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
deletion key ranges.
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
	f.StringVar(&rangeDelConfig.Shape, "rangedel-shape", rangeDelConfig.Shape,
		"DeleteRange workload: cycle (spans reused, so tombstones overlap) or unique "+
			"(every DeleteRange covers a fresh span, spread across --rangedel-queues queues)")
	f.IntVar(&rangeDelConfig.Queues, "rangedel-queues", rangeDelConfig.Queues,
		"unique shape: number of queues, each with its own key prefix and advancing ack level")
	f.Float64Var(&rangeDelConfig.OverlapFrac, "rangedel-overlap-frac", rangeDelConfig.OverlapFrac,
		"unique shape: fraction of DeleteRanges that re-delete from a queue's start, "+
			"overlapping its earlier tombstones")
	f.IntVar(&rangeDelConfig.RangeDelsPerBatch, "rangedels-per-batch", rangeDelConfig.RangeDelsPerBatch,
		"unique shape: DeleteRanges per batch")
	f.IntVar(&rangeDelConfig.SetsPerBatch, "sets-per-batch", rangeDelConfig.SetsPerBatch,
		"unique shape: point Sets per batch (negative: alternate a DeleteRange-only batch "+
			"with a lone Set, as the cycle shape does)")
	f.Float64Var(&rangeDelConfig.IterFrac, "iter-frac", rangeDelConfig.IterFrac,
		"fraction of reads that open an iterator and scan instead of doing a Get "+
			"(negative: alternate between the two)")
	f.StringVar(&rangeDelConfig.DBOptions, "db-options", rangeDelConfig.DBOptions,
		"DB options: default (the benchmark's usual options) or prod-writeheavy (a write-heavy "+
			"production configuration: 10s range-delete flush delay, 256 MiB memtables, "+
			"32 KiB blocks, 10-bit bloom filters, fastest compression; the comparer and "+
			"key schema stay the benchmark's)")
}

func runRangeDelCmd(cmd *cobra.Command, args []string) error {
	commonCfg.RateLimiter = maxOpsPerSec.newRateLimiter()
	return bench.RunRangeDel(args[0], &commonCfg, &rangeDelConfig)
}
