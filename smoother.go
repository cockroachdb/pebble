package pebble

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

const decayRate = 0.9
const sampleRate = 10 * time.Millisecond
const numSamples = 100
const minUtilization = 0.1

// Smoother will attempt to smooth out a process that runs multiple iterations.
// The goal is not to have it run faster or slower, but simply with pace itself
// to run evenly over time rather than being bunchy.
type Smoother struct {
	enabled       bool
	countRunning  int32
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
				sampleRunning += atomic.LoadInt32(&s.countRunning)
				sampleSleeping += atomic.LoadInt32(&s.sleepingCount)

				// Every 100 iterations, update the estimated utilization under lock
				if totalSamples == numSamples {
					utilRunning := float64(sampleRunning) / float64(totalSamples)
					utilSleeping := float64(sampleSleeping) / float64(totalSamples)
					// Add all the running time and half the sleeping time.
					util := utilRunning + utilSleeping/2
					sampleRunning = 0
					sampleSleeping = 0
					totalSamples = 0

					s.mu.Lock()
					updatedEstimate := float64(s.mu.estimatedUtilization)*decayRate + float64(util)*(1-decayRate)
					s.mu.estimatedUtilization = math.Max(updatedEstimate, minUtilization)
					s.mu.Unlock()
				}
				time.Sleep(sampleRate)
			}
		}
	}()
}

func (s *Smoother) stop() {
	close(s.stopper)
}

func (s *Smoother) startWork() {
	atomic.AddInt32(&s.countRunning, 1)
}

// finishWork records that work has completed and returns the amount of time the
// process should sleep after this work.
func (s *Smoother) finishWork(workDuration time.Duration, shouldSleep bool) time.Duration {
	atomic.AddInt32(&s.countRunning, -1)
	s.mu.Lock()
	s.mu.estimatedIterDurationNs = float64(s.mu.estimatedIterDurationNs)*decayRate + float64(workDuration)*(1-decayRate)
	sleepTime := 0.0
	if s.mu.estimatedUtilization < 1 {
		sleepTime = s.mu.estimatedIterDurationNs * ((1 / s.mu.estimatedUtilization) - 1)
	}
	s.mu.Unlock()
	fmt.Println(sleepTime, s.mu.estimatedIterDurationNs, s.mu.estimatedUtilization)

	// Duration to sleep if the caller wants to sleep after a call.
	// FIXME
	//	if !s.enabled {
	//		return time.Duration(0)
	//	}
	if shouldSleep && sleepTime > 0 {
		atomic.AddInt32(&s.sleepingCount, 1)
		time.Sleep(time.Duration(sleepTime))
		atomic.AddInt32(&s.sleepingCount, -1)
	}
	return time.Duration(sleepTime)
}
