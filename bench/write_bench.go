// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package bench

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/internal/ackseq"
	"github.com/cockroachdb/pebble/internal/randvar"
	"github.com/cockroachdb/pebble/internal/rate"
)

// The following constants match the values that Cockroach uses in Admission
// Control at the time of writing.
// See: https://github.com/cockroachdb/cockroach/blob/cb5d5108a7705eac7be82bc7f0f8b6f4dc825b96/pkg/util/admission/granter.go#L1212-L1229
const (
	defaultL0FileLimit     = 1000
	defaultL0SubLevelLimit = 20
)

// WriteBenchConfig configures the write throughput benchmark.
type WriteBenchConfig struct {
	Batch              *randvar.Flag
	Keys               string
	Values             *randvar.BytesFlag
	Concurrency        int
	RateStart          int
	IncBase            int
	TestPeriod         time.Duration
	CooloffPeriod      time.Duration
	TargetL0Files      int
	TargetL0SubLevels  int
	MaxRateDipFraction float64
	Debug              bool
}

// DefaultWriteBenchConfig returns a WriteBenchConfig populated with the same
// defaults as the cmd/pebble CLI.
func DefaultWriteBenchConfig() WriteBenchConfig {
	return WriteBenchConfig{
		Batch:              randvar.NewFlag("1"),
		Values:             randvar.NewBytesFlag("1000"),
		Keys:               "zipf",
		Concurrency:        1,
		RateStart:          1000,
		IncBase:            100,
		TestPeriod:         60 * time.Second,
		CooloffPeriod:      30 * time.Second,
		TargetL0Files:      defaultL0FileLimit,
		TargetL0SubLevels:  defaultL0SubLevelLimit,
		MaxRateDipFraction: 0.1,
	}
}

// writeBenchResult contains the results of a test run at a given rate.
type writeBenchResult struct {
	name     string
	rate     int
	passed   bool
	elapsed  time.Duration
	bytes    uint64
	levels   int
	writeAmp float64
}

func (r writeBenchResult) String() string {
	return fmt.Sprintf("BenchmarkRaw%s %d ops/sec %v pass %s elapsed %d bytes %d levels %.2f writeAmp",
		r.name,
		r.rate,
		r.passed,
		r.elapsed,
		r.bytes,
		r.levels,
		r.writeAmp,
	)
}

// RunWriteBench runs the write throughput benchmark.
func RunWriteBench(dir string, common *CommonConfig, cfg *WriteBenchConfig) error {
	const workload = "F" // 100% inserts.
	var (
		writers      []*pauseWriter
		writersWg    *sync.WaitGroup
		cooloff      bool
		streak       int
		clockStart   time.Time
		cooloffStart time.Time
		stack        []int
		pass, fail   []int
		rateAcc      float64
	)

	desiredRate := cfg.RateStart
	incBase := cfg.IncBase
	weights, err := ycsbParseWorkload(workload)
	if err != nil {
		return err
	}

	// Build a YCSBConfig view that newYcsb can read InitialKeys/PrepopulatedKeys
	// from when constructing the key distribution.
	ycfgVal := DefaultYCSBConfig()
	ycfgVal.Batch = cfg.Batch
	ycfgVal.Keys = cfg.Keys
	ycfgVal.Values = cfg.Values
	ycfg := &ycfgVal
	keyDist, err := ycsbParseKeyDist(cfg.Keys, ycfg)
	if err != nil {
		return err
	}

	// Construct a new YCSB F benchmark with the configured values.
	y := newYcsb(common, ycfg, weights, keyDist, cfg.Batch, nil /* scans */, cfg.Values)
	y.keyNum = ackseq.New(0)

	setLimit := func(l int) {
		perWriterRate := float64(l) / float64(len(writers))
		for _, w := range writers {
			w.setRate(perWriterRate)
		}
	}

	debugPrint := func(s string) {
		if !cfg.Debug {
			return
		}
		fmt.Print("DEBUG: " + s)
	}

	// Function closure to run on test-run failure.
	onTestFail := func(r writeBenchResult, cancel func()) {
		fail = append(fail, desiredRate)

		fmt.Println(r)

		// a) No room to backtrack. We're done.
		if len(stack) == 0 {
			debugPrint("no room to backtrack; exiting ...\n")
			cancel()
			writersWg.Wait()
			return
		}

		// b) We still have room to backtrack.
		desiredRate, stack = stack[len(stack)-1], stack[:len(stack)-1]
		setLimit(desiredRate)

		// Enter the cool-off period.
		cooloff = true
		var wg sync.WaitGroup
		for _, w := range writers {
			wg.Go(func() { w.pause() })
		}
		wg.Wait()

		streak = 0
		rateAcc = 0
		cooloffStart = time.Now()
		clockStart = time.Now()
		debugPrint("Fail. Pausing writers for cool-off period.\n")
		debugPrint(fmt.Sprintf("new rate=%d\npasses=%v\nfails=%v\nstack=%v\n",
			desiredRate, pass, fail, stack))
	}

	onTestSuccess := func(r writeBenchResult) {
		streak++
		pass = append(pass, desiredRate)
		stack = append(stack, desiredRate)

		r.passed = true
		fmt.Println(r)

		desiredRate = desiredRate + incBase*(1<<(streak-1))
		setLimit(desiredRate)

		rateAcc = 0
		clockStart = time.Now()

		debugPrint(fmt.Sprintf("Pass.\nnew rate=%d\npasses=%v\nfails=%v\nstreak=%d\nstack=%v\n",
			desiredRate, pass, fail, streak, stack))
	}

	name := fmt.Sprintf("write/values=%s", cfg.Values)
	ctx, cancel := context.WithCancel(context.Background())
	RunTest(dir, common, Test{
		Init: func(db DB, wg *sync.WaitGroup) {
			y.db = db
			writersWg = wg

			for i := 0; i < cfg.Concurrency; i++ {
				writer := newPauseWriter(y, float64(desiredRate))
				writers = append(writers, writer)
				writersWg.Add(1)
				go writer.run(ctx, wg)
			}
			setLimit(desiredRate)

			clockStart = time.Now()
		},
		Tick: func(elapsed time.Duration, i int) {
			m := y.db.Metrics()
			if i%20 == 0 {
				if cfg.Debug && i > 0 {
					fmt.Printf("%s\n", m)
				}
				fmt.Println("___elapsed___clock___rate(desired)___rate(actual)___L0files___L0levels___levels______lsmBytes___writeAmp")
			}

			l0Files := m.Levels[0].Tables.Count
			l0Sublevels := m.Levels[0].Sublevels
			nLevels := 0
			for _, l := range m.Levels {
				if l.TableBytesIn > 0 {
					nLevels++
				}
			}
			lsmBytes := m.DiskSpaceUsage()
			total := m.Total()
			writeAmp := (&total).WriteAmp()

			var currRate float64
			var stalled bool
			y.reg.Tick(func(tick histogramTick) {
				h := tick.Hist
				currRate = float64(h.TotalCount()) / tick.Elapsed.Seconds()
				stalled = !cooloff && currRate == 0
			})
			rateAcc += currRate

			failed := stalled ||
				int(l0Files) > cfg.TargetL0Files ||
				int(l0Sublevels) > cfg.TargetL0SubLevels

			fmt.Printf("%10s %7s %15d %14.1f %9d %10d %8d %13d %10.2f\n",
				time.Duration(elapsed.Seconds()+0.5)*time.Second,
				time.Duration(time.Since(clockStart).Seconds()+0.5)*time.Second,
				desiredRate,
				currRate,
				l0Files,
				l0Sublevels,
				nLevels,
				lsmBytes,
				writeAmp,
			)

			if cooloff {
				if time.Since(cooloffStart) < cfg.CooloffPeriod {
					return
				}
				debugPrint("ending cool-off")

				cooloff = false
				for _, w := range writers {
					w.unpause()
				}
				clockStart = time.Now()

				return
			}

			r := writeBenchResult{
				name:     name,
				rate:     desiredRate,
				elapsed:  time.Duration(elapsed.Seconds()+0.5) * time.Second,
				bytes:    lsmBytes,
				levels:   nLevels,
				writeAmp: writeAmp,
			}

			if failed {
				onTestFail(r, cancel)
				return
			}

			testElapsed := time.Since(clockStart)
			if testElapsed < cfg.TestPeriod {
				return
			}

			diff := 1 - rateAcc/(float64(desiredRate)*testElapsed.Seconds())
			if diff > cfg.MaxRateDipFraction {
				if cfg.Debug {
					debugPrint(fmt.Sprintf(
						"difference in rates (%.2f) exceeded threshold (%.2f); marking test as failed\n",
						diff, cfg.MaxRateDipFraction,
					))
				}
				onTestFail(r, cancel)
				return
			}

			onTestSuccess(r)
		},
		Done: func(elapsed time.Duration) {
			var total int64
			y.reg.Tick(func(tick histogramTick) {
				total = tick.Cumulative.TotalCount()
			})
			fmt.Println("___elapsed___ops(total)")
			fmt.Printf("%10s %12d\n", elapsed.Truncate(time.Second), total)
		},
	})

	return nil
}

// pauseWriter issues load against a pebble instance, and can be paused on
// demand to allow the DB to recover.
type pauseWriter struct {
	y        *ycsb
	limiter  *rate.Limiter
	pauseC   chan struct{}
	unpauseC chan struct{}
}

func newPauseWriter(y *ycsb, initialRate float64) *pauseWriter {
	const burst = 1
	return &pauseWriter{
		y:        y,
		limiter:  rate.NewLimiter(float64(initialRate), burst),
		pauseC:   make(chan struct{}),
		unpauseC: make(chan struct{}),
	}
}

func (w *pauseWriter) run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	buf := &ycsbBuf{rng: randvar.NewRand()}
	hist := w.y.reg.Register("insert")
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.pauseC:
			<-w.unpauseC
		default:
			wait(w.limiter)
			start := time.Now()
			w.y.insert(w.y.db, buf)
			hist.Record(time.Since(start))
		}
	}
}

func (w *pauseWriter) pause()            { w.pauseC <- struct{}{} }
func (w *pauseWriter) unpause()          { w.unpauseC <- struct{}{} }
func (w *pauseWriter) setRate(r float64) { w.limiter.SetRate(r) }
