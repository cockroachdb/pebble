// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"log"
	"os"
	"time"

	"github.com/spf13/cobra"
)

var (
	concurrency     int
	disableWAL      bool
	duration        time.Duration
	rocksdb         bool
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
		cmd.Flags().BoolVar(
			&rocksdb, "rocksdb", false,
			"use rocksdb storage engine instead of pebble")
		cmd.Flags().BoolVarP(
			&verbose, "verbose", "v", false, "enable verbose event logging")
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
