// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/internal/humanize"
	"github.com/spf13/cobra"
)

func init() {
	// NB: the tombstone workload piggybacks off the existing flags and
	// configs for the queue and ycsb workloads.
	initQueue(tombstoneCmd)
	initYCSB(tombstoneCmd)
}

var tombstoneCmd = &cobra.Command{
	Use:   "tombstone <dir>",
	Short: "run the mixed-workload point tombstone benchmark",
	Long: `
Run a customizable YCSB workload, alongside a single-writer, fixed-sized queue
workload. This command is intended for evaluating compaction heuristics
surrounding point tombstones.
	`,
	Args: cobra.ExactArgs(1),
	RunE: runTombstoneCmd,
}

func runTombstoneCmd(cmd *cobra.Command, args []string) error {
	if wipe && ycsbConfig.prepopulatedKeys > 0 {
		return errors.New("--wipe and --prepopulated-keys both specified which is nonsensical")
	}

	weights, err := ycsbParseWorkload(ycsbConfig.workload)
	if err != nil {
		return err
	}

	keyDist, err := ycsbParseKeyDist(ycsbConfig.keys)
	if err != nil {
		return err
	}

	batchDist := ycsbConfig.batch
	scanDist := ycsbConfig.scans
	if err != nil {
		return err
	}

	valueDist := ycsbConfig.values
	y := newYcsb(weights, keyDist, batchDist, scanDist, valueDist)
	q := queueTest()

	queueStart := []byte("queue-")
	queueEnd := append(append([]byte{}, queueStart...), 0xFF)

	var pdb pebbleDB
	runTest(args[0], test{
		init: func(d DB, wg *sync.WaitGroup) {
			pdb = d.(pebbleDB)
			y.init(d, wg)
			q.init(d, wg)
		},
		tick: func(elapsed time.Duration, i int) {
			if i%20 == 0 {
				fmt.Println("________elapsed______queue_size")
			}
			queueSize, err := pdb.d.EstimateDiskUsage(queueStart, queueEnd)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("%15s %15s\n", time.Duration(elapsed.Seconds()+0.5)*time.Second, humanize.Uint64(queueSize))
		},
		done: func(elapsed time.Duration) {
			fmt.Println("________elapsed______queue_size")
			queueSize, err := pdb.d.EstimateDiskUsage(queueStart, queueEnd)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("%15s %15s\n", elapsed.Truncate(time.Second), humanize.Uint64(queueSize))
		},
	})
	return nil
}
