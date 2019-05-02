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
	"github.com/petermattis/pebble/db"
	"github.com/spf13/cobra"
)

// benchmark config
type readWhileWritingConfig struct {
	batch                  int
	cycleLength            int64
	minBlockBytes          int
	maxBlockBytes          int
	readPercent            int
	seed                   int64
	sequential             bool
	writeSeq               string
	targetCompressionRatio float64
}

// benchmark-wide state
type readWhileWritingState struct {
	config          readWhileWritingConfig
	reg             *histogramRegistry
	seq             sequence
	numEmptyResults int64
}

// per-worker goroutine state
type readWhileWritingLocalState struct {
	bench        *readWhileWritingState
	readLatency  *namedHistogram
	writeLatency *namedHistogram
	gen          keyGenerator
	pebble       *pebble.DB
}

var readWhileWritingBench readWhileWritingState

var readWhileWritingCmd = &cobra.Command{
	Use:   "readwhilewriting <dir>",
	Short: "run mixed MVCC gets and writes",
	Long:  "",
	Args:  cobra.ExactArgs(1),
	RunE:  readWhileWritingBench.runE,
}

func (s *readWhileWritingState) runE(cmd *cobra.Command, args []string) error {
	if s.config.maxBlockBytes < s.config.minBlockBytes {
		return fmt.Errorf("Value of 'max-block-bytes' (%d) must be greater than or equal to value of 'min-block-bytes' (%d)",
			s.config.maxBlockBytes, s.config.minBlockBytes)
	}
	if s.config.readPercent > 100 || s.config.readPercent < 0 {
		return fmt.Errorf("'read-percent' must be in range [0, 100]")
	}
	if s.config.targetCompressionRatio < 1.0 || math.IsNaN(s.config.targetCompressionRatio) {
		return fmt.Errorf("'target-compression-ratio' must be a number >= 1.0")
	}
	if s.config.writeSeq != "" {
		first := s.config.writeSeq[0]
		if len(s.config.writeSeq) < 2 || (first != 'R' && first != 'S') {
			return fmt.Errorf("--write-seq has to be of the form '(R|S)<num>'")
		}
		rest := s.config.writeSeq[1:]
		var err error
		s.seq.val, err = strconv.ParseInt(rest, 10, 64)
		if err != nil {
			return fmt.Errorf("--write-seq has to be of the form '(R|S)<num>'")
		}
		if first == 'R' && s.config.sequential {
			return fmt.Errorf("--sequential incompatible with a Random --write-seq")
		}
		if first == 'S' && !s.config.sequential {
			return fmt.Errorf("--sequential=false incompatible with a Sequential --write-seq")
		}
	}
	s.seq.cycleLength = s.config.cycleLength
	s.seq.seed = s.config.seed
	s.reg = newHistogramRegistry()
	runTest(args[0], test{
		init: s.launchWorkers,
		tick: s.tick,
		done: s.done,
	})
	return nil
}

// launchWorkers starts the goroutines that run the read/write ops and adds them to the
// wait-group.
func (s *readWhileWritingState) launchWorkers(db *pebble.DB, wg *sync.WaitGroup) {
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		worker := readWhileWritingLocalState{
			bench: s,
		}
		if s.config.sequential {
			worker.gen = newSequentialGenerator(&s.seq)
		} else {
			worker.gen = newHashGenerator(&s.seq)
		}
		worker.pebble = db
		worker.readLatency = s.reg.Register("read")
		worker.writeLatency = s.reg.Register("write")
		go func() {
			defer wg.Done()
			worker.run()
		}()
	}
}

// tick prints lines for the interval read/write stats
func (s *readWhileWritingState) tick(elapsed time.Duration, i int) {
	if i%20 == 0 {
		fmt.Println("optype__elapsed____ops/sec__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
	}
	s.reg.Tick(func(tick histogramTick) {
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
}

// done prints lines for the cumulative read/write stats
func (s *readWhileWritingState) done(elapsed time.Duration) {
	fmt.Println("\noptype__elapsed_____ops(total)___ops/sec(cum)__avg(ms)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
	s.reg.Tick(func(tick histogramTick) {
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

	if empty := atomic.LoadInt64(&s.numEmptyResults); empty != 0 {
		fmt.Printf("Number of reads that didn't return any results: %d.\n", empty)
	}
	seq := atomic.LoadInt64(&s.seq.val)
	var ch string
	if s.config.sequential {
		ch = "S"
	} else {
		ch = "R"
	}
	fmt.Printf("Highest sequence written: %d. Can be passed as --write-seq=%s%d to the next run.\n",
		seq, ch, seq)
}

func (s *readWhileWritingLocalState) run() {
	var raw, buf []byte
	for {
		if s.gen.rand().Intn(100) < s.bench.config.readPercent {
			args := make([]interface{}, s.bench.config.batch)
			for i := 0; i < s.bench.config.batch; i++ {
				args[i] = s.gen.readKey()
			}
			start := time.Now()
			empty := true
			for i := range args {
				num := args[i].(int64)
				raw = encodeUint64Ascending(raw[:0], uint64(num))
				key := mvccEncode(buf[:0], raw, 0, 0)
				_, err := s.pebble.Get(key)
				if err != nil && err != db.ErrNotFound {
					log.Fatal(err)
				} else if err != db.ErrNotFound {
					empty = false
				}
			}
			elapsed := time.Since(start)
			s.readLatency.Record(elapsed)
			if empty {
				atomic.AddInt64(&s.bench.numEmptyResults, 1)
			}
		} else {
			const argCount = 2
			args := make([]interface{}, argCount*s.bench.config.batch)
			for i := 0; i < s.bench.config.batch; i++ {
				j := i * argCount
				args[j+0] = s.gen.writeKey()
				args[j+1] = randomBlock(
					s.gen.rand(), s.bench.config.minBlockBytes,
					s.bench.config.maxBlockBytes,
					s.bench.config.targetCompressionRatio,
				)
			}
			start := time.Now()
			b := s.pebble.NewBatch()
			for i := 0; i < s.bench.config.batch; i++ {
				num := args[2*i].(int64)
				raw = encodeUint64Ascending(raw[:0], uint64(num))
				key := mvccEncode(buf[:0], raw, 0, 0)
				val := args[2*i+1].([]byte)
				b.Set(key, val, nil)
			}
			err := b.Commit(db.Sync)
			if err != nil {
				log.Fatal(err)
			}
			elapsed := time.Since(start)
			s.writeLatency.Record(elapsed)
		}
	}
}
