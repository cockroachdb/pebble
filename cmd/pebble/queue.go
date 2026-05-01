// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"github.com/cockroachdb/pebble/bench"
	"github.com/spf13/cobra"
)

func initQueue(cmd *cobra.Command, cfg *bench.QueueConfig) {
	cmd.Flags().IntVar(
		&cfg.Size, "queue-size", cfg.Size,
		"size of the queue to maintain")
	cmd.Flags().Var(
		cfg.Values, "queue-values",
		"queue value size distribution [{zipf,uniform}:]min[-max][/<target-compression>]")
}
