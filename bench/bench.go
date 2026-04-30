// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package bench contains the implementations of the cmd/pebble benchmark
// workloads. The benchmarks are exposed as Config structs and Run* functions
// so that the cmd/pebble CLI binds command-line flags into those configs and
// invokes the Run* functions.
package bench

import (
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"sync"
	"syscall"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/rate"
)

// CommonConfig holds the configuration knobs shared by every benchmark.
// These values come from package-level CLI flags in cmd/pebble; the bench
// package itself does not own any mutable globals.
type CommonConfig struct {
	CacheSize                int64
	Concurrency              int
	DisableWAL               bool
	Duration                 time.Duration
	MaxSize                  uint64
	Verbose                  bool
	WaitCompactions          bool
	Wipe                     bool
	PathToLocalSharedStorage string
	SecondaryCacheSize       int64

	// Ballast, when non-zero, allocates a byte slice of this size held by the
	// opened Pebble DB to keep some disk free for emergency recovery. The
	// cmd/pebble CLI sets this to 1 GiB; benchmarks typically leave it at 0.
	Ballast int64

	// DisableAutoCompactions disables automatic background compactions.
	// Useful when opening a cached fixture solely to take a checkpoint, so
	// the fixture isn't mutated between runs.
	DisableAutoCompactions bool

	// RateLimiter, when non-nil, is shared by all worker goroutines of the
	// benchmark. cmd/pebble constructs this from its rateFlag and assigns it
	// before invoking the benchmark.
	RateLimiter *rate.Limiter
}

// wait calls Wait(1) on the limiter if non-nil.
func wait(l *rate.Limiter) {
	if l != nil {
		l.Wait(1)
	}
}

// Test describes a benchmark that operates on a Pebble DB.
type Test struct {
	Init func(db DB, wg *sync.WaitGroup)
	Tick func(elapsed time.Duration, i int)
	Done func(elapsed time.Duration)
}

// TestWithoutDB describes a benchmark that does not need a Pebble DB
// (currently only the file system benchmarks).
type TestWithoutDB struct {
	Init func(wg *sync.WaitGroup)
	Tick func(elapsed time.Duration, i int)
	Done func(wg *sync.WaitGroup, elapsed time.Duration)
}

// RunTest runs the given Test against a freshly-opened Pebble DB at dir.
func RunTest(dir string, common *CommonConfig, t Test) {
	if common.Wipe {
		fmt.Printf("wiping %s\n", dir)
		if err := os.RemoveAll(dir); err != nil {
			log.Fatal(err)
		}
	}

	fmt.Printf("dir %s\nconcurrency %d\n", dir, common.Concurrency)

	db := NewPebbleDB(dir, common)
	var wg sync.WaitGroup
	t.Init(db, &wg)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	done := make(chan os.Signal, 3)
	workersDone := make(chan struct{})
	signal.Notify(done, os.Interrupt)

	go func() {
		wg.Wait()
		close(workersDone)
	}()

	if common.MaxSize > 0 {
		go func() {
			for {
				time.Sleep(10 * time.Second)
				if db.Metrics().DiskSpaceUsage() > common.MaxSize*1e6 {
					fmt.Println("max size reached")
					done <- syscall.Signal(0)
				}
			}
		}()
	}
	if common.Duration > 0 {
		go func() {
			time.Sleep(common.Duration)
			done <- syscall.Signal(0)
		}()
	}

	stopProf := startCPUProfile()
	defer stopProf()

	backgroundCompactions := func(p *pebble.Metrics) bool {
		// The last level never gets selected as an input level for compaction,
		// only as an output level, so ignore it for the purposes of determining if
		// background compactions are still needed.
		for i := range p.Levels[:len(p.Levels)-1] {
			if p.Levels[i].Score != 0 {
				return true
			}
		}
		return false
	}

	start := time.Now()
	for i := 0; ; i++ {
		select {
		case <-ticker.C:
			if workersDone != nil {
				t.Tick(time.Since(start), i)
				if common.Verbose && (i%10) == 9 {
					fmt.Printf("%s", db.Metrics())
				}
			} else if common.WaitCompactions {
				p := db.Metrics()
				fmt.Printf("%s", p)
				if !backgroundCompactions(p) {
					return
				}
			}

		case <-workersDone:
			workersDone = nil
			t.Done(time.Since(start))
			p := db.Metrics()
			fmt.Printf("%s", p)
			if !common.WaitCompactions || !backgroundCompactions(p) {
				return
			}
			fmt.Printf("waiting for background compactions\n")

		case <-done:
			if workersDone != nil {
				t.Done(time.Since(start))
			}
			fmt.Printf("%s", db.Metrics())
			return
		}
	}
}

// RunTestWithoutDB runs a benchmark that does not need a Pebble DB.
func RunTestWithoutDB(common *CommonConfig, t TestWithoutDB) {
	var wg sync.WaitGroup
	t.Init(&wg)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	done := make(chan os.Signal, 3)
	workersDone := make(chan struct{})
	signal.Notify(done, os.Interrupt)

	go func() {
		wg.Wait()
		close(workersDone)
	}()

	if common.Duration > 0 {
		go func() {
			time.Sleep(common.Duration)
			done <- syscall.Signal(0)
		}()
	}

	stopProf := startCPUProfile()
	defer stopProf()

	start := time.Now()
	for i := 0; ; i++ {
		select {
		case <-ticker.C:
			if workersDone != nil {
				t.Tick(time.Since(start), i)
			}

		case <-workersDone:
			workersDone = nil
			t.Done(&wg, time.Since(start))
			return

		case sig := <-done:
			fmt.Println("operating system is killing the op.", sig)
			if workersDone != nil {
				t.Done(&wg, time.Since(start))
			}
			return
		}
	}
}

func startCPUProfile() func() {
	runtime.SetMutexProfileFraction(1000)

	done := startRecording("cpu.%04d.prof", pprof.StartCPUProfile, pprof.StopCPUProfile)
	return func() {
		done()
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
		if p := pprof.Lookup("mutex"); p != nil {
			f, err := os.Create("mutex.prof")
			if err != nil {
				log.Fatal(err)
			}
			if err := p.WriteTo(f, 0); err != nil {
				log.Fatal(err)
			}
			f.Close()
		}
	}
}

func startRecording(fmtStr string, startFunc func(io.Writer) error, stopFunc func()) func() {
	doneCh := make(chan struct{})
	var doneWG sync.WaitGroup
	doneWG.Add(1)

	go func() {
		defer doneWG.Done()

		start := time.Now()
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()

		var current *os.File
		defer func() {
			if current != nil {
				stopFunc()
				current.Close()
			}
		}()

		for {
			if current != nil {
				stopFunc()
				current.Close()
				current = nil
			}
			path := fmt.Sprintf(fmtStr, int(time.Since(start).Seconds()+0.5))
			f, err := os.Create(path)
			if err != nil {
				log.Fatalf("unable to create cpu profile: %s", err)
				return
			}
			if err := startFunc(f); err != nil {
				log.Fatalf("unable to start cpu profile: %v", err)
				f.Close()
				return
			}
			current = f

			select {
			case <-doneCh:
				return
			case <-t.C:
			}
		}
	}()

	return func() {
		close(doneCh)
		doneWG.Wait()
	}
}
