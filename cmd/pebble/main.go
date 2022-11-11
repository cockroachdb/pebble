// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"log"
	"os"
	"time"

	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/tool"
	"github.com/spf13/cobra"
)

var (
	cacheSize       int64
	concurrency     int
	disableWAL      bool
	duration        time.Duration
	maxOpsPerSec    = newRateFlag("")
	verbose         bool
	waitCompactions bool
	wipe            bool
)

func main() {
	log.SetFlags(0)

	cobra.EnableCommandSorting = false

	benchCmd := &cobra.Command{
		Use:   "bench",
		Short: "benchmarks",
	}
	benchCmd.AddCommand(
		scanCmd,
		syncCmd,
		tombstoneCmd,
		ycsbCmd,
		fsBenchCmd,
		writeBenchCmd,
	)

	rootCmd := &cobra.Command{
		Use:   "pebble [command] (flags)",
		Short: "pebble benchmarking/introspection tool",
	}
	rootCmd.AddCommand(benchCmd)

	t := tool.New(tool.Comparers(mvccComparer, testkeys.Comparer), tool.Mergers(fauxMVCCMerger))
	rootCmd.AddCommand(t.Commands...)

	for _, cmd := range []*cobra.Command{scanCmd, syncCmd, tombstoneCmd, writeBenchCmd, ycsbCmd} {
		cmd.Flags().BoolVarP(
			&verbose, "verbose", "v", false, "enable verbose event logging")
	}
	for _, cmd := range []*cobra.Command{scanCmd, syncCmd, tombstoneCmd, ycsbCmd} {
		cmd.Flags().Int64Var(
			&cacheSize, "cache", 1<<30, "cache size")
	}
	for _, cmd := range []*cobra.Command{scanCmd, syncCmd, tombstoneCmd, ycsbCmd, fsBenchCmd, writeBenchCmd} {
		cmd.Flags().DurationVarP(
			&duration, "duration", "d", 10*time.Second, "the duration to run (0, run forever)")
	}
	for _, cmd := range []*cobra.Command{scanCmd, syncCmd, tombstoneCmd, ycsbCmd} {
		cmd.Flags().IntVarP(
			&concurrency, "concurrency", "c", 1, "number of concurrent workers")
		cmd.Flags().BoolVar(
			&disableWAL, "disable-wal", false, "disable the WAL (voiding persistence guarantees)")
		cmd.Flags().VarP(
			maxOpsPerSec, "rate", "m", "max ops per second [{zipf,uniform}:]min[-max][/period (sec)]")
		cmd.Flags().BoolVar(
			&waitCompactions, "wait-compactions", false,
			"wait for background compactions to complete after load stops")
		cmd.Flags().BoolVarP(
			&wipe, "wipe", "w", false, "wipe the database before starting")
	}

	if err := rootCmd.Execute(); err != nil {
		// Cobra has already printed the error message.
		os.Exit(1)
	}
}
