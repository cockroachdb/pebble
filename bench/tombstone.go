// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package bench

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/humanize"
)

// TombstoneConfig configures the mixed-workload point tombstone benchmark.
// It piggybacks on the YCSB and queue configurations.
type TombstoneConfig struct {
	YCSB  YCSBConfig
	Queue QueueConfig
}

// DefaultTombstoneConfig returns a TombstoneConfig populated with the same
// defaults as the cmd/pebble CLI.
func DefaultTombstoneConfig() TombstoneConfig {
	return TombstoneConfig{
		YCSB:  DefaultYCSBConfig(),
		Queue: DefaultQueueConfig(),
	}
}

// RunTombstone runs the tombstone benchmark.
func RunTombstone(dir string, common *CommonConfig, cfg *TombstoneConfig) error {
	if common.Wipe && cfg.YCSB.PrepopulatedKeys > 0 {
		return errors.New("--wipe and --prepopulated-keys both specified which is nonsensical")
	}

	weights, err := ycsbParseWorkload(cfg.YCSB.Workload)
	if err != nil {
		return err
	}

	keyDist, err := ycsbParseKeyDist(cfg.YCSB.Keys, &cfg.YCSB)
	if err != nil {
		return err
	}

	y := newYcsb(common, &cfg.YCSB, weights, keyDist, cfg.YCSB.Batch, cfg.YCSB.Scans, cfg.YCSB.Values)
	q, queueOps := newQueueTest(common, &cfg.Queue)

	queueStart := []byte("queue-")
	queueEnd := append(append([]byte{}, queueStart...), 0xFF)

	var lastElapsed time.Duration
	var lastQueueOps int64

	var pdb pebbleDB
	RunTest(dir, common, Test{
		Init: func(d DB, wg *sync.WaitGroup) {
			pdb = d.(pebbleDB)
			y.init(d, wg)
			q.Init(d, wg)
		},
		Tick: func(elapsed time.Duration, i int) {
			if i%20 == 0 {
				fmt.Println("                                             queue                         ycsb")
				fmt.Println("________elapsed______queue_size__ops/sec(inst)___ops/sec(cum)__ops/sec(inst)___ops/sec(cum)")
			}

			curQueueOps := queueOps.Load()
			dur := elapsed - lastElapsed
			queueOpsPerSec := float64(curQueueOps-lastQueueOps) / dur.Seconds()
			queueCumOpsPerSec := float64(curQueueOps) / elapsed.Seconds()

			lastQueueOps = curQueueOps
			lastElapsed = elapsed

			var ycsbOpsPerSec, ycsbCumOpsPerSec float64
			y.reg.Tick(func(tick histogramTick) {
				h := tick.Hist
				ycsbOpsPerSec = float64(h.TotalCount()) / tick.Elapsed.Seconds()
				ycsbCumOpsPerSec = float64(tick.Cumulative.TotalCount()) / elapsed.Seconds()
			})

			queueSize, err := pdb.d.EstimateDiskUsage(queueStart, queueEnd)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("%15s %15s %14.1f %14.1f %14.1f %14.1f\n",
				time.Duration(elapsed.Seconds()+0.5)*time.Second,
				humanize.Bytes.Uint64(queueSize),
				queueOpsPerSec,
				queueCumOpsPerSec,
				ycsbOpsPerSec,
				ycsbCumOpsPerSec)
		},
		Done: func(elapsed time.Duration) {
			fmt.Println("________elapsed______queue_size")
			queueSize, err := pdb.d.EstimateDiskUsage(queueStart, queueEnd)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("%15s %15s\n", elapsed.Truncate(time.Second), humanize.Bytes.Uint64(queueSize))
		},
	})
	return nil
}
