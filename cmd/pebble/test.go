// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"runtime/pprof"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/codahale/hdrhistogram"
	"github.com/petermattis/pebble"
	"github.com/petermattis/pebble/internal/rate"
)

const (
	minLatency = 10 * time.Microsecond
	maxLatency = 10 * time.Second
)

func startCPUProfile() func() {
	f, err := os.Create("cpu.prof")
	if err != nil {
		log.Fatal(err)
	}
	// runtime.SetBlockProfileRate(1)
	// runtime.SetMutexProfileFraction(1)

	if err := pprof.StartCPUProfile(f); err != nil {
		log.Fatal(err)
	}
	return func() {
		pprof.StopCPUProfile()
		f.Close()

		if p := pprof.Lookup("heap"); p != nil {
			f, err := os.Create("heap.prof")
			if err != nil {
				log.Fatal(err)
			}
			if err := p.WriteTo(f, 0); err != nil {
				log.Fatal(err)
			}
			f.Close()
		}

		// if p := pprof.Lookup("mutex"); p != nil {
		// 	f, err := os.Create("mutex.prof")
		// 	if err != nil {
		// 		log.Fatal(err)
		// 	}
		// 	if err := p.WriteTo(f, 0); err != nil {
		// 		log.Fatal(err)
		// 	}
		// 	f.Close()
		// }

		// if p := pprof.Lookup("block"); p != nil {
		// 	f, err := os.Create("block.prof")
		// 	if err != nil {
		// 		log.Fatal(err)
		// 	}
		// 	if err := p.WriteTo(f, 0); err != nil {
		// 		log.Fatal(err)
		// 	}
		// 	f.Close()
		// }
	}
}

func newHistogram() *hdrhistogram.Histogram {
	return hdrhistogram.New(minLatency.Nanoseconds(), maxLatency.Nanoseconds(), 1)
}

type namedHistogram struct {
	name string
	mu   struct {
		sync.Mutex
		current *hdrhistogram.Histogram
	}
}

func newNamedHistogram(name string) *namedHistogram {
	w := &namedHistogram{name: name}
	w.mu.current = newHistogram()
	return w
}

func (w *namedHistogram) Record(elapsed time.Duration) {
	if elapsed < minLatency {
		elapsed = minLatency
	} else if elapsed > maxLatency {
		elapsed = maxLatency
	}

	w.mu.Lock()
	err := w.mu.current.RecordValue(elapsed.Nanoseconds())
	w.mu.Unlock()

	if err != nil {
		// Note that a histogram only drops recorded values that are out of range,
		// but we clamp the latency value to the configured range to prevent such
		// drops. This code path should never happen.
		panic(fmt.Sprintf(`%s: recording value: %s`, w.name, err))
	}
}

func (w *namedHistogram) tick(fn func(h *hdrhistogram.Histogram)) {
	w.mu.Lock()
	defer w.mu.Unlock()
	h := w.mu.current
	w.mu.current = newHistogram()
	fn(h)
}

type histogramTick struct {
	// Name is the name given to the histograms represented by this tick.
	Name string
	// Hist is the merged result of the represented histograms for this tick.
	// Hist.TotalCount() is the number of operations that occurred for this tick.
	Hist *hdrhistogram.Histogram
	// Cumulative is the merged result of the represented histograms for all
	// time. Cumulative.TotalCount() is the total number of operations that have
	// occurred over all time.
	Cumulative *hdrhistogram.Histogram
	// Elapsed is the amount of time since the last tick.
	Elapsed time.Duration
	// Now is the time at which the tick was gathered. It covers the period
	// [Now-Elapsed,Now).
	Now time.Time
}

type histogramRegistry struct {
	mu struct {
		sync.Mutex
		registered []*namedHistogram
	}

	start      time.Time
	cumulative map[string]*hdrhistogram.Histogram
	prevTick   map[string]time.Time
}

func newHistogramRegistry() *histogramRegistry {
	return &histogramRegistry{
		start:      time.Now(),
		cumulative: make(map[string]*hdrhistogram.Histogram),
		prevTick:   make(map[string]time.Time),
	}
}

func (w *histogramRegistry) Register(name string) *namedHistogram {
	hist := newNamedHistogram(name)

	w.mu.Lock()
	w.mu.registered = append(w.mu.registered, hist)
	w.mu.Unlock()

	return hist
}

func (w *histogramRegistry) Tick(fn func(histogramTick)) {
	w.mu.Lock()
	registered := append([]*namedHistogram(nil), w.mu.registered...)
	w.mu.Unlock()

	merged := make(map[string]*hdrhistogram.Histogram)
	var names []string
	for _, hist := range registered {
		hist.tick(func(h *hdrhistogram.Histogram) {
			if p, ok := merged[hist.name]; ok {
				p.Merge(h)
			} else {
				merged[hist.name] = h
				names = append(names, hist.name)
			}
		})
	}

	now := time.Now()
	sort.Strings(names)
	for _, name := range names {
		mergedHist := merged[name]
		if _, ok := w.cumulative[name]; !ok {
			w.cumulative[name] = newHistogram()
		}
		w.cumulative[name].Merge(mergedHist)

		prevTick, ok := w.prevTick[name]
		if !ok {
			prevTick = w.start
		}
		w.prevTick[name] = now
		fn(histogramTick{
			Name:       name,
			Hist:       merged[name],
			Cumulative: w.cumulative[name],
			Elapsed:    now.Sub(prevTick),
			Now:        now,
		})
	}
}

type test struct {
	init func(db DB, limiter *rate.Limiter, wg *sync.WaitGroup)
	tick func(elapsed time.Duration, i int)
	done func(elapsed time.Duration)
}

func runTest(dir string, t test) {
	if rocksdb && findPeakOpsPerSec {
		log.Fatalf("--rocksdb together with --find-peak-ops-per-sec is unsupported")
	}

	// Check if the directory exists.
	if wipe {
		fmt.Printf("wiping %s\n", dir)
		if err := os.RemoveAll(dir); err != nil {
			log.Fatal(err)
		}
	}

	fmt.Printf("dir %s\nconcurrency %d\n", dir, concurrency)

	var db DB
	if rocksdb {
		db = newRocksDB(dir)
	} else {
		db = newPebbleDB(dir)
	}

	limiter := rate.NewLimiter(rate.Limit(maxOpsPerSec), 1)
	var wg sync.WaitGroup
	t.init(db, limiter, &wg)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	done := make(chan os.Signal, 3)
	workersDone := make(chan struct{})
	signal.Notify(done, os.Interrupt)

	go func() {
		wg.Wait()
		close(workersDone)
	}()

	if duration > 0 {
		go func() {
			time.Sleep(duration)
			done <- syscall.Signal(0)
		}()
	}

	stopProf := startCPUProfile()
	defer stopProf()

	backgroundCompactions := func(p *pebble.VersionMetrics) bool {
		// The last level never gets selected as an input level for compaction,
		// only as an output level, so ignore it for the purposes of determining if
		// background compactions are still needed.
		for i := range p.Levels[:len(p.Levels)-1] {
			if p.Levels[i].Score > 1 {
				return true
			}
		}
		return false
	}

	// findPeakInfo holds variables used only for `--find-peak-ops-per-sec`
	var findPeakInfo struct {
		prevCompactionDebt             uint64
		prevTotalReserved              int64
		prevUpdated                    time.Time
		rateLowerBound, rateUpperBound float64
		stallBegan, stallEnded         chan struct{}
		stalled                        bool
		// increased indicates whether pending compaction bytes is non-trivial and
		// has increased over the last interval. Here, an interval is the time between
		// two calls to `db.Metrics()`, which is currently 10 seconds.
		increased bool
		// numSameIncreased is the number of intervals over which pending compaction
		// bytes has trended in the same direction indicated by increased.
		numSameIncreased int
		// numIntervals is the number of intervals that have passed since the last time
		// we adjusted the workload rate.
		numIntervals int
	}
	const (
		// trendLen is the value of numSameIncreased that triggers a change in rate.
		// TODO(ajkr): using this rate change strategy has several problems:
		// (1) the compaction debt estimator is not smooth, so there is a chance that
		//     we identify the wrong trend. We should change the estimator to account
		//     for partially completed flushes/compactions. Currently it has support for
		//     flushes but `Metrics()` isn't using it.
		// (2) decreasing compaction debt over a few intervals may mislead us into
		//     thinking we should raise the rate, while actually if we had kept the same
		//     rate for longer we'd have hit memtable or L0 file count stalls. Instead
		//     of a fixed number of intervals, we could require a trend lasts for one (or
		//     more) full L0->Lbase compaction cycles.
		// (3) at low compaction debt levels (below compactionSlowdownThreshold), the
		//     rate limiter prevents compaction from happening at full speed. This makes
		//     it more likely we observe increasing debt and thus lower the rate, perhaps
		//     unnecessarily.
		// (4) also at low compaction debt levels, the write-amp is higher due to the
		//     higher fanout. Again, this makes us more likely to see increasing debt
		//     and lower the rate. Ideally we would run the benchmark as close to the
		//     disk space limit as possible in order to minimize write-amp.
		// (5) even if we use a smooth debt estimator, we still cannot be sure that a
		//     sequence of trendLen changes in the same direction indicates the compaction
		//     can or cannot keep up. For example, imagine we are stuck on L5->L6
		//     compactions for trendLen intervals. This would clear less debt than if we
		//     had been stuck on an L0->Lbase compaction over those same intervals, which
		//     makes it more likely we see increasing debt and thus decrease rate.
		trendLen = 6
		// noTrendLen is the limit on intervals as measured by numIntervals that we will
		// run without finding a trend of trendLen. If this limit is reached we assume
		// compaction debt is stable, which triggers an increase in rate.
		// TODO(ajkr): this approach probably biases the metric upwards. We can try removing
		// it once debt estimator is smoothed, or use it as a terminating condition.
		noTrendLen = 18
	)

	start := time.Now()
	if findPeakOpsPerSec {
		findPeakInfo.prevUpdated = start
		findPeakInfo.rateUpperBound = float64(maxOpsPerSec)
		findPeakInfo.stallBegan = db.(pebbleDB).stallBegan
		findPeakInfo.stallEnded = db.(pebbleDB).stallEnded
	}
	adjustRate := func() {
		if findPeakInfo.rateUpperBound-findPeakInfo.rateLowerBound < findPeakInfo.rateUpperBound/100 {
			// the range is narrowed to within 1%, should be good enough
			done <- syscall.Signal(0)
			return
		}
		findPeakInfo.prevTotalReserved = limiter.TotalReserved()
		findPeakInfo.prevUpdated = time.Now()
		findPeakInfo.numSameIncreased = 0
		findPeakInfo.numIntervals = 0
		limiter.SetLimit(rate.Limit(
			(findPeakInfo.rateLowerBound + findPeakInfo.rateUpperBound) / 2))
		fmt.Printf("peak ops/sec range narrowed to [%.f, %.f]; trying %.f\n",
			findPeakInfo.rateLowerBound, findPeakInfo.rateUpperBound, float64(limiter.Limit()))
	}
	for i := 0; ; i++ {
		select {
		case <-ticker.C:
			if workersDone != nil {
				t.tick(time.Since(start), i)
				if (i % 10) == 9 {
					metrics := db.Metrics()
					if verbose {
						fmt.Printf("%s", metrics)
					}
					if findPeakOpsPerSec && !findPeakInfo.stalled {
						// If compaction rate is close to zero (here defined as less than 1GB),
						// count it as non-increasing. That allows us to increase the rate if a
						// trivial compaction debt is seen for trendLen consecutive intervals, even
						// if debt is not strictly decreasing over those intervals. It also mitigates
						// the risk that we raise the rate due to seeing slowly increasing compaction
						// debt while compaction is still being throttled. Although ideally we'd use
						// compactionSlowdownThreshold instead of 1GB.
						increased := (metrics.EstimatedCompactionDebt-findPeakInfo.prevCompactionDebt) > 0 &&
							metrics.EstimatedCompactionDebt >= (1<<30)
						findPeakInfo.prevCompactionDebt = metrics.EstimatedCompactionDebt
						if increased == findPeakInfo.increased {
							findPeakInfo.numSameIncreased++
						} else {
							findPeakInfo.increased = increased
							findPeakInfo.numSameIncreased = 1
						}
						findPeakInfo.numIntervals++
						intervalRate := float64(limiter.TotalReserved()-findPeakInfo.prevTotalReserved) /
							time.Since(findPeakInfo.prevUpdated).Seconds()
						if findPeakInfo.numSameIncreased >= trendLen {
							if findPeakInfo.increased {
								findPeakInfo.rateUpperBound = intervalRate
							} else {
								findPeakInfo.rateLowerBound = intervalRate
							}
							adjustRate()
						} else if findPeakInfo.numIntervals >= noTrendLen {
							findPeakInfo.rateLowerBound = intervalRate
							adjustRate()
						}
					}
				}
			} else if waitCompactions {
				p := db.Metrics()
				fmt.Printf("%s", p)
				if !backgroundCompactions(p) {
					return
				}
			}

		case <-findPeakInfo.stallBegan:
			// The current rate is too fast as it caused a stall. Set it as the
			// upper-bound of the search.
			findPeakInfo.rateUpperBound =
				float64(limiter.TotalReserved()-findPeakInfo.prevTotalReserved) /
					time.Since(findPeakInfo.prevUpdated).Seconds()
			// Temporarily disable writes until stall ends
			limiter.SetLimit(0)
			findPeakInfo.stalled = true

		case <-findPeakInfo.stallEnded:
			// Now that the stall ended, try a new (reduced) rate
			adjustRate()
			findPeakInfo.stalled = false

		case <-workersDone:
			if findPeakOpsPerSec {
				log.Fatalf("workers finished early; peak sustainable throughput not found\n")
			}
			workersDone = nil
			t.done(time.Since(start))
			p := db.Metrics()
			fmt.Printf("%s", p)
			if !waitCompactions || !backgroundCompactions(p) {
				return
			}
			fmt.Printf("waiting for background compactions\n")

		case <-done:
			if workersDone != nil {
				t.done(time.Since(start))
			}
			fmt.Printf("%s", db.Metrics())
			if findPeakOpsPerSec {
				fmt.Printf("peak sustainable throughput: %.f\n",
					math.Round((findPeakInfo.rateUpperBound+findPeakInfo.rateLowerBound)/2))
			}
			return
		}
	}
}
