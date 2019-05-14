// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"log"
	"math"
	"os"
	"time"

	"github.com/spf13/cobra"
)

var (
	concurrency     int
	disableWAL      bool
	duration        time.Duration
	verbose         bool
	walOnly         bool
	waitCompactions bool
	wipe            bool
)

var rootCmd = &cobra.Command{
	Use:   "pebble [command] (flags)",
	Short: "pebble benchmarking/introspection tool",
	Long:  ``,
}

func main() {
	log.SetFlags(0)

	cobra.EnableCommandSorting = false
	rootCmd.AddCommand(
		scanCmd,
		syncCmd,
		ycsbCmd,
	)

	for _, cmd := range []*cobra.Command{scanCmd, syncCmd, ycsbCmd} {
		cmd.Flags().IntVarP(
			&concurrency, "concurrency", "c", 1, "number of concurrent workers")
		cmd.Flags().BoolVar(
			&disableWAL, "disable-wal", false, "disable the WAL (voiding persistence guarantees)")
		cmd.Flags().DurationVarP(
			&duration, "duration", "d", 10*time.Second, "the duration to run (0, run forever)")
		cmd.Flags().BoolVarP(
			&verbose, "verbose", "v", false, "enable verbose event logging")
		cmd.Flags().BoolVar(
			&waitCompactions, "wait-compactions", false,
			"wait for background compactions to complete after load stops")
		cmd.Flags().BoolVarP(
			&wipe, "wipe", "w", false, "wipe the database before starting")
		cmd.Flags().BoolVar(
			&walOnly, "wal-only", false, "write data only to the WAL")
	}

	scanCmd.Flags().BoolVarP(
		&scanReverse, "reverse", "r", false, "reverse scan")
	scanCmd.Flags().IntVar(
		&scanRows, "rows", scanRows, "number of rows to scan in each operation")
	scanCmd.Flags().IntVar(
		&scanValueSize, "value", scanValueSize, "size of values to scan")

	ycsbCmd.Flags().IntVar(
		&ycsbConfig.batch, "batch", 1,
		"Number of keys to read/insert in each operation")
	ycsbCmd.Flags().Int64Var(
		&ycsbConfig.cycleLength, "cycle-length", math.MaxInt64,
		"Number of keys repeatedly accessed by each writer")
	ycsbCmd.Flags().IntVar(
		&ycsbConfig.minBlockBytes, "min-block-bytes", 1,
		"Minimum amount of raw data written with each insertion")
	ycsbCmd.Flags().IntVar(
		&ycsbConfig.maxBlockBytes, "max-block-bytes", 1,
		"Maximum amount of raw data written with each insertion")
	ycsbCmd.Flags().Uint64VarP(
		&ycsbConfig.numOps, "num-ops", "n", 0, "maximum number of operations (0 means unlimited)")
	ycsbCmd.Flags().IntVar(
		&ycsbConfig.readPercent, "read-percent", 0,
		"Percent (0-100) of operations that are reads of existing keys")
	ycsbCmd.Flags().Int64Var(
		&ycsbConfig.seed, "seed", 1, "Key hash seed")
	ycsbCmd.Flags().BoolVar(
		&ycsbConfig.sequential, "sequential", false,
		"Pick keys sequentially instead of uniformly at random")
	ycsbCmd.Flags().StringVar(
		&ycsbConfig.writeSeq, "write-seq", "",
		"Initial write sequence value. Can be used to use the data produced by a previous run. "+
			"It has to be of the form (R|S)<number>, where S implies that it was taken from a "+
			"previous --sequential run and R implies a previous random run.")
	ycsbCmd.Flags().Float64Var(
		&ycsbConfig.targetCompressionRatio, "target-compression-ratio", 1.0,
		"Target compression ratio for data blocks. Must be >= 1.0")

	if err := rootCmd.Execute(); err != nil {
		// Cobra has already printed the error message.
		os.Exit(1)
	}
}
