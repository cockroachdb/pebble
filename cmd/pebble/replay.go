// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"github.com/cockroachdb/pebble/bench"
	"github.com/spf13/cobra"
)

func initReplayCmd() *cobra.Command {
	c := bench.DefaultReplayConfig()
	cmd := &cobra.Command{
		Use:   "replay <workload>",
		Short: "run the provided captured write workload",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.RunReplay(cmd.OutOrStdout(), commonCfg.Verbose, args[0])
		},
	}
	cmd.Flags().IntVar(
		&c.Count, "count", c.Count, "the number of times to replay the workload")
	cmd.Flags().StringVar(
		&c.Name, "name", "", "the name of the workload being replayed")
	cmd.Flags().VarPF(
		&c.Pacer, "pacer", "p", "the pacer to use: unpaced, reference-ramp, or fixed-ramp=N")
	cmd.Flags().Uint64Var(
		&c.MaxWritesMB, "max-writes", 0, "the maximum volume of writes (MB) to apply, with 0 denoting unlimited")
	cmd.Flags().StringVar(
		&c.OptionsString, "options", "", "Pebble options to override, in the OPTIONS ini format but with any whitespace as field delimiters instead of newlines")
	cmd.Flags().StringVar(
		&c.RunDir, "run-dir", c.RunDir, "the directory to use for the replay data directory; defaults to a random dir in pwd")
	cmd.Flags().Int64Var(
		&c.MaxCacheSize, "max-cache-size", c.MaxCacheSize, "the max size of the block cache")
	cmd.Flags().BoolVar(
		&c.StreamLogs, "stream-logs", c.StreamLogs, "stream the Pebble logs to stdout during replay")
	cmd.Flags().BoolVar(
		&c.IgnoreCheckpoint, "ignore-checkpoint", c.IgnoreCheckpoint, "ignore the workload's initial checkpoint")
	cmd.Flags().StringVar(
		&c.CheckpointDir, "checkpoint-dir", c.CheckpointDir, "path to the checkpoint to use if not <WORKLOAD_DIR>/checkpoint")
	return cmd
}
