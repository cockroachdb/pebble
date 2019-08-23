// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/petermattis/pebble"
	"github.com/petermattis/pebble/internal/rate"
	"github.com/spf13/cobra"
	"golang.org/x/exp/rand"
)

var syncConfig struct {
	batch   string
	walOnly bool
	values  string
}

var syncCmd = &cobra.Command{
	Use:   "sync <dir>",
	Short: "run the sync benchmark",
	Long:  ``,
	Args:  cobra.ExactArgs(1),
	Run:   runSync,
}

func init() {
	syncCmd.Flags().StringVar(
		&syncConfig.batch, "batch", "5",
		"batch size distribution [{zipf,uniform}:]min[-max]")
	syncCmd.Flags().BoolVar(
		&syncConfig.walOnly, "wal-only", false, "write data only to the WAL")
	syncCmd.Flags().StringVar(
		&syncConfig.values, "values", "uniform:60-80/1.0",
		"value size distribution [{zipf,uniform}:]min[-max][/<target-compression>]")
}

func runSync(cmd *cobra.Command, args []string) {
	reg := newHistogramRegistry()
	var bytes, lastBytes uint64

	opts := pebble.Sync
	if disableWAL {
		opts = pebble.NoSync
	}

	batchDist, err := parseRandVarSpec(syncConfig.batch)
	if err != nil {
		fmt.Println(err)
		return
	}

	valueDist, targetCompression, err := parseValuesSpec(syncConfig.values)
	if err != nil {
		fmt.Println(err)
		return
	}

	runTest(args[0], test{
		init: func(d DB, limiter *rate.Limiter, wg *sync.WaitGroup) {
			wg.Add(concurrency)
			for i := 0; i < concurrency; i++ {
				latency := reg.Register("ops")
				go func() {
					defer wg.Done()

					rand := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
					var raw []byte
					var buf []byte
					for {
						limiter.Wait(context.Background())
						start := time.Now()
						b := d.NewBatch()
						var n uint64
						count := int(batchDist.Uint64())
						for j := 0; j < count; j++ {
							length := int(valueDist.Uint64())
							block := randomBlock(rand, length, targetCompression)

							if syncConfig.walOnly {
								if err := b.LogData(block, nil); err != nil {
									log.Fatal(err)
								}
							} else {
								raw = encodeUint32Ascending(raw[:0], rand.Uint32())
								key := mvccEncode(buf[:0], raw, 0, 0)
								buf = key[:0]
								if err := b.Set(key, block, nil); err != nil {
									log.Fatal(err)
								}
							}
							n += uint64(len(block))
						}
						if err := b.Commit(opts); err != nil {
							log.Fatal(err)
						}
						latency.Record(time.Since(start))
						atomic.AddUint64(&bytes, n)
					}
				}()
			}
		},

		tick: func(elapsed time.Duration, i int) {
			if i%20 == 0 {
				fmt.Println("_elapsed____ops/sec___mb/sec__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
			}
			reg.Tick(func(tick histogramTick) {
				h := tick.Hist
				n := atomic.LoadUint64(&bytes)
				fmt.Printf("%8s %10.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n",
					time.Duration(elapsed.Seconds()+0.5)*time.Second,
					float64(h.TotalCount())/tick.Elapsed.Seconds(),
					float64(n-lastBytes)/(1024.0*1024.0)/tick.Elapsed.Seconds(),
					time.Duration(h.ValueAtQuantile(50)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(95)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(99)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(100)).Seconds()*1000,
				)
				lastBytes = n
			})
		},

		done: func(elapsed time.Duration) {
			fmt.Println("\n_elapsed___ops(total)_ops/sec(cum)_mb/sec(cum)__avg(ms)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
			reg.Tick(func(tick histogramTick) {
				h := tick.Cumulative
				fmt.Printf("%7.1fs %12d %12.1f %11.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n\n",
					elapsed.Seconds(), h.TotalCount(),
					float64(h.TotalCount())/elapsed.Seconds(),
					float64(atomic.LoadUint64(&bytes)/(1024.0*1024.0))/elapsed.Seconds(),
					time.Duration(h.Mean()).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(50)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(95)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(99)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(100)).Seconds()*1000)
			})
		},
	})
}
