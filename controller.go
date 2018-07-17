package pebble

import (
	"context"
	"sync"
	"time"

	"github.com/petermattis/pebble/rate"
)

type controller struct {
	limiter *rate.Limiter
	sensor  *rateCounter
}

func newController(l *rate.Limiter) *controller {
	return &controller{
		limiter: l,
		sensor:  newRateCounter(5*time.Second, 25),
	}
}

func (c *controller) WaitN(n int) {
	size := n
	if burst := c.limiter.Burst(); size > burst {
		size = burst
	}
	_ = c.limiter.WaitN(context.Background(), size)
	c.sensor.Add(int64(n))
}

// TODO(peter): this is similar to https://github.com/dgryski/go-timewindow
type rateCounter struct {
	now         func() time.Time
	bucketWidth int64
	mu          struct {
		sync.Mutex
		startT  int64
		lastT   int64
		total   int64
		head    int
		buckets []int64
	}
}

func newRateCounter(duration time.Duration, resolution int) *rateCounter {
	r := &rateCounter{
		now:         time.Now,
		bucketWidth: int64(duration / time.Duration(resolution)),
	}
	r.mu.buckets = make([]int64, resolution)
	return r
}

func (r *rateCounter) tickLocked() {
	t := r.now().UnixNano() / int64(r.bucketWidth)
	if t <= r.mu.lastT {
		return
	}
	delta := (t - r.mu.lastT)
	if delta >= int64(len(r.mu.buckets)) {
		delta = int64(len(r.mu.buckets))
	}
	for i := int64(0); i < delta; i++ {
		r.mu.head++
		if r.mu.head >= len(r.mu.buckets) {
			r.mu.head = 0
		}
		r.mu.total -= r.mu.buckets[r.mu.head]
		r.mu.buckets[r.mu.head] = 0
	}
	r.mu.lastT = t
	if r.mu.startT == 0 {
		r.mu.startT = r.mu.lastT
	}
}

func (r *rateCounter) Add(n int64) {
	r.mu.Lock()
	r.tickLocked()
	r.mu.buckets[r.mu.head] += n
	r.mu.total += n
	r.mu.Unlock()
}

func (r *rateCounter) Value() int64 {
	r.mu.Lock()
	r.tickLocked()
	sum := r.mu.total
	r.mu.Unlock()
	return sum
}

func (r *rateCounter) Rate() float64 {
	r.mu.Lock()
	r.tickLocked()
	sum := r.mu.total
	elapsed := r.mu.lastT - r.mu.startT
	r.mu.Unlock()
	if elapsed >= int64(len(r.mu.buckets)) {
		elapsed = int64(len(r.mu.buckets))
	}
	return float64(sum) / (float64(elapsed*r.bucketWidth) / float64(time.Second))
}
