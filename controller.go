package pebble

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"
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

type rateCounter struct {
	now         func() time.Time
	bucketWidth time.Duration
	mu          struct {
		sync.Mutex
		start   int64
		last    int64
		sum     int64
		head    int
		buckets []int64
	}
}

func newRateCounter(duration time.Duration, resolution int) *rateCounter {
	r := &rateCounter{
		now:         time.Now,
		bucketWidth: duration / time.Duration(resolution),
	}
	r.mu.buckets = make([]int64, resolution)
	return r
}

func (r *rateCounter) tickLocked(now time.Time) {
	floor := now.UnixNano() / int64(r.bucketWidth)
	if floor <= r.mu.last {
		return
	}
	delta := (floor - r.mu.last)
	if delta >= int64(len(r.mu.buckets)) {
		delta = int64(len(r.mu.buckets))
	}
	for i := int64(0); i < delta; i++ {
		r.mu.head++
		if r.mu.head >= len(r.mu.buckets) {
			r.mu.head = 0
		}
		r.mu.sum -= r.mu.buckets[r.mu.head]
		r.mu.buckets[r.mu.head] = 0
	}
	r.mu.last = floor
	if r.mu.start == 0 {
		r.mu.start = r.mu.last
	}
}

func (r *rateCounter) Add(n int64) {
	r.mu.Lock()
	r.tickLocked(r.now())
	r.mu.buckets[r.mu.head] += n
	r.mu.sum += n
	r.mu.Unlock()
}

func (r *rateCounter) Value() int64 {
	r.mu.Lock()
	r.tickLocked(r.now())
	sum := r.mu.sum
	r.mu.Unlock()
	return sum
}

func (r *rateCounter) Rate() float64 {
	r.mu.Lock()
	r.tickLocked(r.now())
	sum := r.mu.sum
	elapsed := r.mu.last - r.mu.start
	r.mu.Unlock()
	if elapsed >= int64(len(r.mu.buckets)) {
		elapsed = int64(len(r.mu.buckets))
	}
	return float64(sum) / (float64(time.Duration(elapsed)*r.bucketWidth) / float64(time.Second))
}
