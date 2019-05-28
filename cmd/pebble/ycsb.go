// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"fmt"
	"log"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/petermattis/pebble"
	"github.com/spf13/cobra"
)

var ycsbConfig struct {
	batch                  int
	cycleLength            int64
	minBlockBytes          int
	maxBlockBytes          int
	numOps                 uint64
	readPercent            int
	seed                   int64
	sequential             bool
	writeSeq               string
	targetCompressionRatio float64
}

var ycsbCmd = &cobra.Command{
	Use:   "ycsb <dir>",
	Short: "Run customizable YCSB-like workload. Does not yet offer the standard workloads.",
	Long:  "",
	Args:  cobra.ExactArgs(1),
	RunE:  runYcsb,
}

func init() {
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
}

func runYcsb(cmd *cobra.Command, args []string) error {
	// benchmark-wide state
	var (
		reg             *histogramRegistry
		seq             sequence
		numSuccess      uint64
		numEmptyResults int64
	)

	if ycsbConfig.maxBlockBytes < ycsbConfig.minBlockBytes {
		return fmt.Errorf("Value of 'max-block-bytes' (%d) must be greater than or equal to value of 'min-block-bytes' (%d)",
			ycsbConfig.maxBlockBytes, ycsbConfig.minBlockBytes)
	}
	if ycsbConfig.readPercent > 100 || ycsbConfig.readPercent < 0 {
		return fmt.Errorf("'read-percent' must be in range [0, 100]")
	}
	if ycsbConfig.targetCompressionRatio < 1.0 || math.IsNaN(ycsbConfig.targetCompressionRatio) {
		return fmt.Errorf("'target-compression-ratio' must be a number >= 1.0")
	}
	if ycsbConfig.writeSeq != "" {
		first := ycsbConfig.writeSeq[0]
		if len(ycsbConfig.writeSeq) < 2 || (first != 'R' && first != 'S') {
			return fmt.Errorf("--write-seq has to be of the form '(R|S)<num>'")
		}
		rest := ycsbConfig.writeSeq[1:]
		var err error
		seq.val, err = strconv.ParseInt(rest, 10, 64)
		if err != nil {
			return fmt.Errorf("--write-seq has to be of the form '(R|S)<num>'")
		}
		if first == 'R' && ycsbConfig.sequential {
			return fmt.Errorf("--sequential incompatible with a Random --write-seq")
		}
		if first == 'S' && !ycsbConfig.sequential {
			return fmt.Errorf("--sequential=false incompatible with a Sequential --write-seq")
		}
	}

	seq.cycleLength = ycsbConfig.cycleLength
	seq.seed = ycsbConfig.seed
	reg = newHistogramRegistry()
	runTest(args[0], test{
		init: func(db *pebble.DB, wg *sync.WaitGroup) {
			wg.Add(concurrency)
			for i := 0; i < concurrency; i++ {
				// per-worker goroutine state
				var gen keyGenerator
				if ycsbConfig.sequential {
					gen = newSequentialGenerator(&seq)
				} else {
					gen = newHashGenerator(&seq)
				}
				readLatency := reg.Register("read")
				writeLatency := reg.Register("write")

				go func() {
					defer wg.Done()
					var raw, buf []byte
					for {
						if gen.rand().Intn(100) < ycsbConfig.readPercent {
							num := gen.readKey()
							raw = encodeUint64Ascending(raw[:0], uint64(num))
							key := mvccEncode(buf[:0], raw, 0, 0)
							start := time.Now()
							iter := db.NewIter(nil)
							found := 0
							for iter.SeekGE(key); iter.Valid(); iter.Next() {
								found++
								if found == ycsbConfig.batch {
									break
								}
							}
							if err := iter.Close(); err != nil {
								log.Fatal(err)
							}
							elapsed := time.Since(start)
							readLatency.Record(elapsed)
							if found == 0 {
								atomic.AddInt64(&numEmptyResults, 1)
							}
						} else {
							start := time.Now()
							b := db.NewBatch()
							for i := 0; i < ycsbConfig.batch; i++ {
								num := gen.writeKey()
								val := randomBlock(
									gen.rand(), ycsbConfig.minBlockBytes,
									ycsbConfig.maxBlockBytes,
									ycsbConfig.targetCompressionRatio,
								)
								raw = encodeUint64Ascending(raw[:0], uint64(num))
								key := mvccEncode(buf[:0], raw, 0, 0)
								b.Set(key, val, nil)
							}
							err := b.Commit(pebble.Sync)
							if err != nil {
								log.Fatal(err)
							}
							elapsed := time.Since(start)
							writeLatency.Record(elapsed)
						}
						if ycsbConfig.numOps > 0 &&
							atomic.AddUint64(&numSuccess, 1) >= ycsbConfig.numOps {
							break
						}
					}
				}()
			}
		},
		tick: func(elapsed time.Duration, i int) {
			if i%20 == 0 {
				fmt.Println("optype__elapsed____ops/sec__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
			}
			reg.Tick(func(tick histogramTick) {
				h := tick.Hist
				fmt.Printf("%6s %8s %10.1f %8.1f %8.1f %8.1f %8.1f\n",
					tick.Name,
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
			fmt.Println("\noptype__elapsed_____ops(total)___ops/sec(cum)__avg(ms)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
			reg.Tick(func(tick histogramTick) {
				h := tick.Cumulative
				fmt.Printf("%6s %7.1fs %14d %14.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n",
					tick.Name, elapsed.Seconds(), h.TotalCount(),
					float64(h.TotalCount())/elapsed.Seconds(),
					time.Duration(h.Mean()).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(50)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(95)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(99)).Seconds()*1000,
					time.Duration(h.ValueAtQuantile(100)).Seconds()*1000)
			})

			if empty := atomic.LoadInt64(&numEmptyResults); empty != 0 {
				fmt.Printf("Number of reads that didn't return any results: %d.\n", empty)
			}
			seq := atomic.LoadInt64(&seq.val)
			var ch string
			if ycsbConfig.sequential {
				ch = "S"
			} else {
				ch = "R"
			}
			fmt.Printf("Highest sequence written: %d. Can be passed as --write-seq=%s%d to the next run.\n",
				seq, ch, seq)
		},
	})
	return nil
}
