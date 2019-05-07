// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

const (
	chunkSize           = 64 << 20 // 64 MB
	fillStopThreshold   = 1.1 * chunkSize
	drainDelayThreshold = 1.05 * chunkSize
)

type pacer struct {
	level           int64
	fillStopLevel   float64
	drainDelayLevel float64
	fillCond        sync.Cond
	drainCond       sync.Cond
	drainer         *rate.Limiter
}

func newPacer(mu *sync.Mutex) *pacer {
	p := &pacer{
		fillStopLevel:   fillStopThreshold,
		drainDelayLevel: drainDelayThreshold,
		drainer:         rate.NewLimiter(4<<20, 4<<20),
	}
	p.fillCond.L = mu
	p.drainCond.L = mu
	return p
}

func (p *pacer) fill(n int64) {
	for float64(atomic.LoadInt64(&p.level)) >= p.fillStopLevel {
		p.fillCond.Wait()
	}
	atomic.AddInt64(&p.level, n)
	p.fillCond.Signal()
}

func (p *pacer) drain(n int64, delay bool) bool {
	if delay {
		p.drainer.WaitN(context.Background(), int(n))
	} else {
		p.drainer.AllowN(time.Now(), int(n))
	}
	level := atomic.AddInt64(&p.level, -n)
	p.drainCond.Signal()
	return float64(level) <= p.drainDelayLevel
}

type bucket struct {
	mu        sync.Mutex
	pacer     *pacer
	flushCond sync.Cond
	chunks    []*int64
	fill      int64
	drain     int64
}

func newBucket() *bucket {
	b := &bucket{}
	b.pacer = newPacer(&b.mu)
	b.flushCond.L = &b.mu
	b.chunks = append(b.chunks, new(int64))
	return b
}

func fill(b *bucket) {
	limiter := rate.NewLimiter(10<<20, 10<<20)
	setRate := func(mb int) {
		fmt.Printf("filling at %d MB/sec\n", mb)
		limiter.SetLimit(rate.Limit(mb << 20))
	}
	setRate(10)

	go func() {
		time.Sleep(7 * time.Second)
		setRate(8)
		time.Sleep(7 * time.Second)
		setRate(4)
		time.Sleep(7 * time.Second)
		setRate(1)

		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		for {
			secs := 5 + rng.Intn(5)
			time.Sleep(time.Duration(secs) * time.Second)
			mb := 1 + rng.Intn(19)
			fmt.Printf("filling at %d MB/sec\n", mb)
			limiter.SetLimit(rate.Limit(mb << 20))
		}
	}()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	b.mu.Lock()
	defer b.mu.Unlock()

	for {
		size := 50 + rng.Int63n(150)
		limiter.WaitN(context.Background(), int(size))

		b.pacer.fill(size)
		atomic.AddInt64(&b.fill, size)

		last := b.chunks[len(b.chunks)-1]
		if *last+size > chunkSize {
			last = new(int64)
			b.chunks = append(b.chunks, last)
			b.flushCond.Signal()
		}
		*last += size

		b.mu.Unlock()
		runtime.Gosched()
		b.mu.Lock()
	}
}

func drain(b *bucket) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for {
		b.mu.Lock()
		for len(b.chunks) <= 1 {
			b.flushCond.Wait()
		}
		chunk := b.chunks[0]
		b.mu.Unlock()

		var delay bool
		for i, size := int64(0), int64(0); i < *chunk; i += size {
			size = 50 + rng.Int63n(150)
			if size > (*chunk - i) {
				size = *chunk - i
			}
			delay = b.pacer.drain(size, delay)
			atomic.AddInt64(&b.drain, size)
		}

		b.mu.Lock()
		b.chunks = b.chunks[1:]
		b.mu.Unlock()
	}
}

func main() {
	b := newBucket()
	go fill(b)
	go drain(b)

	tick := time.NewTicker(time.Second)
	start := time.Now()
	lastNow := start
	var lastFill, lastDrain int64

	for i := 0; ; i++ {
		select {
		case <-tick.C:
			if (i % 20) == 0 {
				fmt.Printf("_elapsed___chunks____dirty_____fill____drain\n")
			}

			b.mu.Lock()
			chunks := len(b.chunks)
			b.mu.Unlock()
			dirty := atomic.LoadInt64(&b.pacer.level)
			fill := atomic.LoadInt64(&b.fill)
			drain := atomic.LoadInt64(&b.drain)

			now := time.Now()
			elapsed := now.Sub(lastNow).Seconds()
			fmt.Printf("%8s %8d %8.1f %8.1f %8.1f\n",
				time.Duration(now.Sub(start).Seconds()+0.5)*time.Second,
				chunks,
				float64(dirty)/(1024.0*1024.0),
				float64(fill-lastFill)/(1024.0*1024.0*elapsed),
				float64(drain-lastDrain)/(1024.0*1024.0*elapsed))

			lastNow = now
			lastFill = fill
			lastDrain = drain
		}
	}
}
