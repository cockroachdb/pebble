package pebble

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

const decayRate = 0.99
const sampleRate = 10 * time.Millisecond
const numSamples = 100
const minUtilization = 0.1

// Smoother will attempt to smooth out a process that runs multiple iterations.
// The goal is not to have it run faster or slower, but simply with pace itself
// to run evenly over time rather than being bunchy.
//
// Each "thread" can be in one of three states, Running, Idle, Sleeping.
// * Running means the thread is currently working (and writing to disk).
// * Idle means this thread is not doing anything as there is no work to do.
// * Sleeping means the thread has active work to do, but is sleeping to smooth load.
//
// The goal of the smoother is to turn all Idle work into Sleeping work while
// preserving the amount of Running work that is being done. The sleeping work
// can be somewhat evenly placed between small Running tasks. The perfect
// smoother would result in the flush / compaction write bandwidth to be a
// constant.
//
// Some simplifying assumptions are made which could be improved upon.
// * All tasks runs for a similar amount of time (estimatedIterDurationNs).
// * All tasks create a similar amount of IO.
// * Different types of tasks (compaction vs flush) are similar cost.
//
// After a Smoother is created, it must be started by calling start.
type Smoother struct {
	enabled       bool
	runningCount  int32
	sleepingCount int32
	stopper       chan struct{}

	mu struct {
		sync.Mutex
		estimatedIterDurationNs float64
		estimatedUtilization    float64
	}
}

func (s *Smoother) start() {
	s.mu.estimatedUtilization = 1.0
	s.stopper = make(chan struct{})

	go func() {
		ticker := time.NewTicker(sampleRate)
		defer ticker.Stop()

		var sampleRunning, sampleSleeping int32
		var totalSamples int32

		for {
			select {
			case <-s.stopper:
				return

			case <-ticker.C:
				totalSamples++
				// NB: The number of running and sleeping can be >1. This allows
				// utilization to also be greater than 1. Once util is >1 the smoother
				// is disabled. In a single-threaded system this would never occur,
				// however our flushing and compaction is multi-threaded, however our
				// flushing and compaction is multi-threaded.
				sampleRunning += atomic.LoadInt32(&s.runningCount)
				// We only care if at least 1 job is sleeping.
				if atomic.LoadInt32(&s.sleepingCount) > 1 {
					sampleSleeping++
				}

				// Every 100 iterations, update the estimated utilization under lock.
				if totalSamples == numSamples {
					// utilRunning may be bigger than 1, utilSleeping is always less than 1.
					utilRunning := float64(sampleRunning) / float64(totalSamples)
					utilSleeping := float64(sampleSleeping) / float64(totalSamples)

					s.mu.Lock()
					// The sleep time is multiplied by the estimated prior utilization
					// because sleep work should not change the utilization either up or
					// down.
					util := utilRunning + utilSleeping*s.mu.estimatedUtilization
					updatedEstimate := float64(s.mu.estimatedUtilization)*decayRate + float64(util)*(1-decayRate)
					// Prevent the utilization from getting too low. At 10% utilization
					// there should already be sufficient smoothing. If it gets lower the
					// sleeps can get too long, and it may take too long to recover. On
					// most systems it will stay above this.
					s.mu.estimatedUtilization = math.Max(updatedEstimate, minUtilization)
					s.mu.Unlock()

					sampleRunning = 0
					sampleSleeping = 0
					totalSamples = 0
				}
				time.Sleep(sampleRate)
			}
		}
	}()
}

// stop is called to stop them main thread running.
func (s *Smoother) stop() {
	close(s.stopper)
}

// startWork is called before work is started so the smoother can tell if there
// is active work being done.
func (s *Smoother) startWork() {
	atomic.AddInt32(&s.runningCount, 1)
}

// finishWork records that work has completed and returns the amount of time the
// process should sleep after this work. Typically, shouldSleep should be set to
// true, but in error cases or at the end of a larger iteration loop it can be
// set to false. Setting to false means to use the measurements in calculations,
// but don't actually sleep.
func (s *Smoother) finishWork(workDuration time.Duration, shouldSleep bool) time.Duration {
	atomic.AddInt32(&s.runningCount, -1)
	s.mu.Lock()
	s.mu.estimatedIterDurationNs = float64(s.mu.estimatedIterDurationNs)*decayRate + float64(workDuration)*(1-decayRate)
	sleepTime := time.Duration(0)
	if s.mu.estimatedUtilization < 1 {
		sleepTime = time.Duration(s.mu.estimatedIterDurationNs * ((1 / s.mu.estimatedUtilization) - 1))
	}
	s.mu.Unlock()

	// Duration to sleep if the caller wants to sleep after a call.
	//if !s.enabled {
	//	return time.Duration(0)
	//}
	if shouldSleep && sleepTime > 0 {
		atomic.AddInt32(&s.sleepingCount, 1)
		time.Sleep(sleepTime)
		atomic.AddInt32(&s.sleepingCount, -1)
	}
	// Returned for tests or logging.
	return sleepTime
}
