package pebble

import (
	"sync"
	"time"
)

const decay_rate = 0.9

// TODO: How does this work if there is concurrency?
type Smoother struct {
	enabled bool
	mu      struct {
		sync.Mutex
		lastStartTime       time.Time
		estimatedWorkTime   time.Duration
		estimatedIterations int64
	}
}

func (s Smoother) before(startTime time.Time) IterTracker {
	if !s.enabled {
		return IterTracker{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// How long between loops.
	loopTime := s.mu.lastStartTime.Sub(startTime)

	// Try and match the delay to the amount of work we need to do. This can be
	// negative if we aren't able to finish all our work in time.
	estimatedDelay := s.mu.estimatedWorkTime - loopTime

	return IterTracker{
		currentRunStart:     startTime,
		estimatedDelay:      estimatedDelay,
		estimatedIterations: s.mu.estimatedIterations,
	}
}

// After we complete a full loop, update our statistics so we compute the next
// loop correctly.
func (s Smoother) after(ratePacer IterTracker, completionTime time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	totalRunTime := completionTime.Sub(ratePacer.currentRunStart)
	totalWorkTime := totalRunTime - ratePacer.actualDelay

	// Update our estimates for work time and iterations.
	s.mu.estimatedWorkTime = time.Duration(int64(
		float64(s.mu.estimatedWorkTime)*decay_rate +
			float64(totalWorkTime)*(1-decay_rate)))

	s.mu.estimatedIterations = int64(
		float64(s.mu.estimatedIterations)*decay_rate +
			float64(ratePacer.actualIterations)*(1-decay_rate))
}

type IterTracker struct {
	currentRunStart     time.Time
	estimatedDelay      time.Duration
	estimatedIterations int64
	lastTime            time.Time
	actualRuntime       time.Duration
	actualDelay         time.Duration
	actualIterations    int64
}

func (t IterTracker) runIter(now time.Time) {

	// On the first run through the loop, never sleep, just compute the estimates
	// for the future iterations.
	if t.actualIterations == 0 {
		t.lastTime = now
		t.actualIterations += 1
		return
	}

	sleepTime := t.computeSleepTime(now)
	// Sleep for the computed time. If the estimate is 0, this is a no-op.
	time.Sleep(sleepTime)

	// Update our statistics.
	runDuration := t.lastTime.Sub(now)
	t.lastTime = now.Add(sleepTime)
	t.actualRuntime += runDuration
	t.actualDelay += sleepTime
	t.actualIterations += 1
}

// computeSleepTime determines the optimal sleep time before this iteration.
func (t IterTracker) computeSleepTime(now time.Time) time.Duration {
	// We have slept enough already for this IterTracker, let this loop complete.
	if t.estimatedIterations <= 0 || t.actualDelay >= t.estimatedDelay {
		return 0
	}
	// TODO(baptist): Consider sleeping a little less each iteration to allow
	// finishing a little early.
	sleepAmount := t.estimatedDelay.Nanoseconds() / t.estimatedIterations
	return time.Duration(sleepAmount)
}
