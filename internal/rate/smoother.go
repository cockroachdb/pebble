package rate

import (
	"fmt"
	"math"
	"sync/atomic"
	"time"
)

// With a decay rate of 0.01, and a 10ms sample rate, it takes ~10 second to react to a significant change.
const decayRate float64 = 0.01

// minUtilization is the minimum utilization we will allow. This is to prevent
// us from quickly reacting to external changes.
const minUtilization float64 = 0.1

// 500 measurements at 20ms sample rate is 10 seconds.
const sampleRate = 20 * time.Millisecond
const numMeasurements = 500

// The measurementBuffer stores numMeasurements / sample rates in the
// measurement buffer. With these defaults it looks back 10 seconds.
type measurementBuffer struct {
	measurements [numMeasurements]int32
	index        int
}

func newMeasurementBuffer() measurementBuffer {
	b := measurementBuffer{}
	for i := range b.measurements {
		b.measurements[i] = 1
	}
	return b
}

func (b *measurementBuffer) addMeasurement(value int32) {
	b.measurements[b.index] = value
	b.index = (b.index + 1) % numMeasurements
}

func (b *measurementBuffer) getAverage() float64 {
	sum := 0.0
	for _, value := range b.measurements {
		sum += float64(value)
	}
	return sum / float64(numMeasurements)
}

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
	disabled      bool
	runningCount  atomic.Int32
	sleepingCount atomic.Int32

	// estimatedUtilization is the estimated utilization of the disks during
	// compaction. If it is less than 100% then we pace compactions to prevent
	// spike disk behavior.
	estimatedUtilization atomic.Value
	mb                   measurementBuffer

	stopper chan struct{}
}

// NewSmoother creates a new Smoother instance.
func NewSmoother() *Smoother {
	s := Smoother{}
	s.runningCount.Store(0)
	s.sleepingCount.Store(0)
	s.estimatedUtilization.Store(float64(1.0))
	s.stopper = make(chan struct{})
	s.mb = newMeasurementBuffer()
	return &s
}

// Start the smoothing tracking loop. This loop runs in the background and
// checks how many jobs are running and sleeping every 10ms.
func (s *Smoother) Start() {
	go func() {
		ticker := time.NewTicker(sampleRate)
		defer ticker.Stop()
		for {
			select {
			case <-s.stopper:
				return

			case <-ticker.C:
				s.mb.addMeasurement(s.runningCount.Load() - s.sleepingCount.Load())
				s.estimatedUtilization.Store(math.Max(s.mb.getAverage(), minUtilization))
			}
		}
	}()
}

// Stop is called to stop them main thread running.
func (s *Smoother) Stop() {
	close(s.stopper)
}

// Track is called before work is started so the smoother can tell if there
// is active work being done. Close must be called on the smootherInstance when
// the work is complete. The function passed in should return a monotonically
// increasing value that estimates how much data this job has done. This is used
// to determine when to inject sleep intervals.
func (s *Smoother) Track(f func() uint64) Tracked {
	s.runningCount.Add(1)
	now := time.Now()
	return &smootherInstance{s: s, f: f, bytesWritten: f(), startWorkTime: now, startTime: now}
}

type Tracked interface {
	Tick() time.Duration
	Close()
}

const chunkSize = 4 * 1024 * 1024

// We want to record the amount of time we were sleeping vs running over the time window.
func (s *smootherInstance) Close() {
	s.s.runningCount.Add(-1)
}

// SmootherInstance is a handle to a single instance of work being done. It is
// not safe to pass between goroutines.
type smootherInstance struct {
	s             *Smoother
	f             func() uint64
	bytesWritten  uint64
	startWorkTime time.Time
	startTime     time.Time
	chunkEstimate time.Duration
}

// tick is called after work is done to determine how long to sleep.
func (s *smootherInstance) Tick() time.Duration {
	// Duration to sleep if the caller wants to sleep after a call.
	if s.s.disabled {
		return 0
	}
	// If this call is expensive, we could consider calling less frequently.
	curVal := s.f()

	// Most of the time we don't record our work. Wait until we have done enough
	// work to have a meaningful sleep.
	if curVal-s.bytesWritten < chunkSize {
		return 0
	}

	// Only record work if we have written enough bytes.
	sleepTime := s.recordWork(time.Since(s.startWorkTime))
	// TODO: remove
	fmt.Println(sleepTime, s.s.estimatedUtilization.Load(), s.chunkEstimate, curVal, s.bytesWritten)
	time.Sleep(sleepTime)

	s.bytesWritten = s.f()
	s.startWorkTime = time.Now()
	return sleepTime
}

// recordWork should be called periodically while a jobs is running to
// determine how long the process should sleep for.
func (s *smootherInstance) recordWork(duration time.Duration) time.Duration {
	// We just did a chunk of work, add it to the estimated duration of work.
	if s.chunkEstimate == 0 {
		s.chunkEstimate = time.Duration(duration)
	} else {
		// Exponential moving average of the chunk duration estimate.
		s.chunkEstimate = time.Duration(float64(s.chunkEstimate)*(1-decayRate) + float64(duration)*decayRate)
	}

	currentUtil := s.s.estimatedUtilization.Load().(float64) / 3.0

	// If we are over 100% utilization don't sleep.
	if currentUtil > 1.0 {
		return 0
	}
	// Compute the sleep time based on the average rather than the current
	// duration to smooth out the sleep times.
	return time.Duration(float64(s.chunkEstimate)*(1/currentUtil) - 1)
}
