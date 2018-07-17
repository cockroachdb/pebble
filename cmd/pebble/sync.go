package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/petermattis/pebble"
	"github.com/petermattis/pebble/db"
	"github.com/spf13/cobra"
)

var syncCmd = &cobra.Command{
	Use:   "sync <dir>",
	Short: "run the sync benchmark",
	Long:  ``,
	Args:  cobra.ExactArgs(1),
	Run:   runSync,
}

func runSync(cmd *cobra.Command, args []string) {
	reg := newHistogramRegistry()

	runTest(args[0], test{
		init: func(d *pebble.DB, wg *sync.WaitGroup) {
			wg.Add(concurrency)
			for i := 0; i < concurrency; i++ {
				latency := reg.Register("ops")
				go func() {
					defer wg.Done()

					ctx := context.Background()
					rand := rand.New(rand.NewSource(time.Now().UnixNano()))
					var buf []byte

					randBlock := func(min, max int) []byte {
						data := make([]byte, rand.Intn(max-min)+min)
						for i := range data {
							data[i] = byte(rand.Int() & 0xff)
						}
						return data
					}

					for {
						start := time.Now()
						b := d.NewBatch()
						for j := 0; j < 5; j++ {
							block := randBlock(60, 80)
							key := encodeUint32Ascending(buf, rand.Uint32())
							if err := b.Set(key, block, nil); err != nil {
								log.Fatal(ctx, err)
							}
							buf = key[:0]
						}
						if err := b.Commit(db.Sync); err != nil {
							log.Fatal(ctx, err)
						}
						latency.Record(time.Since(start))
					}
				}()
			}
		},

		tick: func(elapsed time.Duration, i int) {
			if i%20 == 0 {
				fmt.Println("_elapsed____ops/sec__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
			}
			reg.Tick(func(tick histogramTick) {
				h := tick.Hist
				fmt.Printf("%8s %10.1f %8.1f %8.1f %8.1f %8.1f\n",
					time.Duration(elapsed.Seconds()+0.5)*time.Second,
					float64(h.TotalCount())/tick.Elapsed.Seconds(),
					time.Duration(h.ValueAtQuantile(50)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(95)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(99)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(100)).Seconds()*1000,
				)
			})
		},

		done: func(elapsed time.Duration) {
			fmt.Println("\n_elapsed_____ops(total)___ops/sec(cum)__avg(ms)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
			reg.Tick(func(tick histogramTick) {
				h := tick.Cumulative
				fmt.Printf("%7.1fs %14d %14.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n\n",
					elapsed.Seconds(), h.TotalCount(),
					float64(h.TotalCount())/elapsed.Seconds(),
					time.Duration(h.Mean()).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(50)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(95)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(99)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(100)).Seconds()*1000)
			})
		},
	})
}
