// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"math"
	"os"
	"time"

	"github.com/spf13/cobra"
)

var (
	concurrency int
	duration    time.Duration
	wipe        bool
)

var rootCmd = &cobra.Command{
	Use:   "pebble [command] (flags)",
	Short: "pebble benchmarking/introspection tool",
	Long:  ``,
}

func main() {
	cobra.EnableCommandSorting = false
	rootCmd.AddCommand(
		readWhileWritingCmd,
		scanCmd,
		syncCmd,
	)

	for _, cmd := range []*cobra.Command{readWhileWritingCmd, scanCmd, syncCmd} {
		cmd.Flags().IntVarP(
			&concurrency, "concurrency", "c", 1, "number of concurrent workers")
		cmd.Flags().DurationVarP(
			&duration, "duration", "d", 10*time.Second, "the duration to run (0, run forever)")
		cmd.Flags().BoolVarP(
			&wipe, "wipe", "w", false, "wipe the database before starting")
	}

	scanCmd.Flags().BoolVarP(
		&scanReverse, "reverse", "r", false, "reverse scan")
	scanCmd.Flags().IntVar(
		&scanRows, "rows", scanRows, "number of rows to scan in each operation")
	scanCmd.Flags().IntVar(
		&scanValueSize, "value", scanValueSize, "size of values to scan")

	readWhileWritingCmd.Flags().IntVar(
		&readWhileWritingBench.config.batch, "batch", 1,
		"Number of keys to read/insert in each operation")
	readWhileWritingCmd.Flags().Int64Var(
		&readWhileWritingBench.config.cycleLength, "cycle-length", math.MaxInt64,
		"Number of keys repeatedly accessed by each writer")
	readWhileWritingCmd.Flags().IntVar(
		&readWhileWritingBench.config.minBlockBytes, "min-block-bytes", 1,
		"Minimum amount of raw data written with each insertion")
	readWhileWritingCmd.Flags().IntVar(
		&readWhileWritingBench.config.maxBlockBytes, "max-block-bytes", 1,
		"Maximum amount of raw data written with each insertion")
	readWhileWritingCmd.Flags().IntVar(
		&readWhileWritingBench.config.readPercent, "read-percent", 0,
		"Percent (0-100) of operations that are reads of existing keys")
	readWhileWritingCmd.Flags().Int64Var(
		&readWhileWritingBench.config.seed, "seed", 1, "Key hash seed")
	readWhileWritingCmd.Flags().BoolVar(
		&readWhileWritingBench.config.sequential, "sequential", false,
		"Pick keys sequentially instead of uniformly at random")
	readWhileWritingCmd.Flags().StringVar(
		&readWhileWritingBench.config.writeSeq, "write-seq", "",
		"Initial write sequence value. Can be used to use the data produced by a previous run. "+
			"It has to be of the form (R|S)<number>, where S implies that it was taken from a "+
			"previous --sequential run and R implies a previous random run.")
	readWhileWritingCmd.Flags().Float64Var(
		&readWhileWritingBench.config.targetCompressionRatio, "target-compression-ratio", 1.0,
		"Target compression ratio for data blocks. Must be >= 1.0")

	if err := rootCmd.Execute(); err != nil {
		// Cobra has already printed the error message.
		os.Exit(1)
	}
}
