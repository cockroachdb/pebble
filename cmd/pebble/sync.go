// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"github.com/cockroachdb/pebble/bench"
	"github.com/spf13/cobra"
)

var syncConfig = bench.DefaultSyncConfig()

var syncCmd = &cobra.Command{
	Use:   "sync <dir>",
	Short: "run the sync benchmark",
	Long:  ``,
	Args:  cobra.ExactArgs(1),
	Run:   runSync,
}

func init() {
	syncCmd.Flags().Var(
		syncConfig.Batch, "batch",
		"batch size distribution [{zipf,uniform}:]min[-max]")
	syncCmd.Flags().BoolVar(
		&syncConfig.WALOnly, "wal-only", false, "write data only to the WAL")
	syncCmd.Flags().Var(
		syncConfig.Values, "values",
		"value size distribution [{zipf,uniform}:]min[-max][/<target-compression>]")
}

func runSync(cmd *cobra.Command, args []string) {
	commonCfg.RateLimiter = maxOpsPerSec.newRateLimiter()
	bench.RunSync(args[0], &commonCfg, &syncConfig)
}
