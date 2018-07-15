package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/codahale/hdrhistogram"
	"github.com/petermattis/pebble"
	"github.com/petermattis/pebble/db"
)

const (
	dir = "synctest"
)

var concurrency = flag.Int("c", 1, "Number of concurrent writers")
var duration = flag.Duration("d", 10*time.Second,
	"The duration to run. If 0, run forever.")

var numOps uint64
var numBytes uint64

const (
	minLatency = 100 * time.Microsecond
	maxLatency = 10 * time.Second
)

func clampLatency(d, min, max time.Duration) time.Duration {
	if d < min {
		return min
	}
	if d > max {
		return max
	}
	return d
}

func encodeUint32Ascending(b []byte, v uint32) []byte {
	return append(b, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

type worker struct {
	db      *pebble.DB
	latency struct {
		sync.Mutex
		*hdrhistogram.WindowedHistogram
	}
}

func newWorker(db *pebble.DB) *worker {
	w := &worker{db: db}
	w.latency.WindowedHistogram = hdrhistogram.NewWindowed(1,
		minLatency.Nanoseconds(), maxLatency.Nanoseconds(), 1)
	return w
}

func (w *worker) run(wg *sync.WaitGroup) {
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
		b := w.db.NewBatch()
		for j := 0; j < 5; j++ {
			block := randBlock(60, 80)
			key := encodeUint32Ascending(buf, rand.Uint32())
			if err := b.Set(key, block, nil); err != nil {
				log.Fatal(ctx, err)
			}
			buf = key[:0]
		}
		bytes := uint64(len(b.Repr()))
		if err := b.Commit(db.Sync); err != nil {
			log.Fatal(ctx, err)
		}
		atomic.AddUint64(&numOps, 1)
		atomic.AddUint64(&numBytes, bytes)
		elapsed := clampLatency(time.Since(start), minLatency, maxLatency)
		w.latency.Lock()
		if err := w.latency.Current.RecordValue(elapsed.Nanoseconds()); err != nil {
			log.Fatal(ctx, err)
		}
		w.latency.Unlock()
	}
}

func main() {
	flag.Parse()

	// Check if the directory exists.
	if _, err := os.Stat(dir); err == nil {
		log.Fatalf("error: supplied path '%s' must not exist", dir)
	}

	defer func() {
		_ = os.RemoveAll(dir)
	}()

	fmt.Printf("writing to %s, concurrency %d\n", dir, *concurrency)

	db, err := pebble.Open(dir, &db.Options{
		MemTableSize:                64 << 20,
		MemTableStopWritesThreshold: 4,
		L0CompactionThreshold:       2,
		L0SlowdownWritesThreshold:   20,
		L0StopWritesThreshold:       32,
		Levels: []db.LevelOptions{{
			BlockSize: 32 << 10,
		}},
	})
	if err != nil {
		log.Fatal(err)
	}

	workers := make([]*worker, *concurrency)

	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		workers[i] = newWorker(db)
		go workers[i].run(&wg)
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	done := make(chan os.Signal, 3)
	signal.Notify(done, os.Interrupt)

	go func() {
		wg.Wait()
		done <- syscall.Signal(0)
	}()

	if *duration > 0 {
		go func() {
			time.Sleep(*duration)
			done <- syscall.Signal(0)
		}()
	}

	{
		f, err := os.Create("cpu.prof")
		if err != nil {
			log.Fatal(err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal(err)
		}
		defer func() {
			pprof.StopCPUProfile()
			f.Close()
		}()
	}

	start := time.Now()
	lastNow := start
	var lastOps uint64
	var lastBytes uint64

	for i := 0; ; i++ {
		select {
		case <-ticker.C:
			var h *hdrhistogram.Histogram
			for _, w := range workers {
				w.latency.Lock()
				m := w.latency.Merge()
				w.latency.Rotate()
				w.latency.Unlock()
				if h == nil {
					h = m
				} else {
					h.Merge(m)
				}
			}

			p50 := h.ValueAtQuantile(50)
			p95 := h.ValueAtQuantile(95)
			p99 := h.ValueAtQuantile(99)
			pMax := h.ValueAtQuantile(100)

			now := time.Now()
			elapsed := now.Sub(lastNow)
			ops := atomic.LoadUint64(&numOps)
			bytes := atomic.LoadUint64(&numBytes)

			if i%20 == 0 {
				fmt.Println("_elapsed____ops/sec___mb/sec__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
			}
			fmt.Printf("%8s %10.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n",
				time.Duration(time.Since(start).Seconds()+0.5)*time.Second,
				float64(ops-lastOps)/elapsed.Seconds(),
				float64(bytes-lastBytes)/(1024.0*1024.0)/elapsed.Seconds(),
				time.Duration(p50).Seconds()*1000,
				time.Duration(p95).Seconds()*1000,
				time.Duration(p99).Seconds()*1000,
				time.Duration(pMax).Seconds()*1000,
			)
			lastNow = now
			lastOps = ops
			lastBytes = bytes

		case <-done:
			return
		}
	}
}
