// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"github.com/cockroachdb/pebble/bench"
	"github.com/spf13/cobra"
)

var tombstoneConfig = bench.DefaultTombstoneConfig()

func init() {
	// NB: the tombstone workload piggybacks off the existing flags and
	// configs for the queue and ycsb workloads.
	initQueue(tombstoneCmd, &tombstoneConfig.Queue)
	initYCSB(tombstoneCmd, &tombstoneConfig.YCSB)
}

var tombstoneCmd = &cobra.Command{
	Use:   "tombstone <dir>",
	Short: "run the mixed-workload point tombstone benchmark",
	Long: `
Run a customizable YCSB workload, alongside a single-writer, fixed-sized queue
workload. This command is intended for evaluating compaction heuristics
surrounding point tombstones.

The queue workload writes a point tombstone with every operation. A compaction
strategy that does not account for point tombstones may accumulate many
uncompacted tombstones, causing steady growth of the disk space consumed by
the queue keyspace.

The --queue-values flag controls the distribution of the queue value sizes.
Larger values are more likely to exhibit problematic point tombstone behavior
on a database using a min-overlapping ratio heuristic because the compact
point tombstones may overlap many tables in the next level.

The --queue-size flag controls the fixed number of live keys in the queue. Low
queue sizes may not exercise problematic tombstone behavior if queue sets and
deletes get written to the same sstable. The large-valued sets can serve as a
counterweight to the point tombstones, narrowing the keyrange of the sstable
inflating its size relative to its overlap with the next level.
	`,
	Args: cobra.ExactArgs(1),
	RunE: runTombstoneCmd,
}

func runTombstoneCmd(cmd *cobra.Command, args []string) error {
	commonCfg.RateLimiter = maxOpsPerSec.newRateLimiter()
	return bench.RunTombstone(args[0], &commonCfg, &tombstoneConfig)
}
