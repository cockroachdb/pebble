// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"github.com/cockroachdb/pebble/bench"
	"github.com/spf13/cobra"
)

var scanConfig = bench.DefaultScanConfig()

var scanCmd = &cobra.Command{
	Use:   "scan <dir>",
	Short: "run the scan benchmark",
	Long:  ``,
	Args:  cobra.ExactArgs(1),
	Run:   runScan,
}

func init() {
	scanCmd.Flags().BoolVarP(
		&scanConfig.Reverse, "reverse", "r", false, "reverse scan")
	scanCmd.Flags().Var(
		scanConfig.Rows, "rows", "number of rows to scan in each operation")
	scanCmd.Flags().Var(
		scanConfig.Values, "values",
		"value size distribution [{zipf,uniform}:]min[-max][/<target-compression>]")
}

func runScan(cmd *cobra.Command, args []string) {
	commonCfg.RateLimiter = maxOpsPerSec.newRateLimiter()
	bench.RunScan(args[0], &commonCfg, &scanConfig)
}
