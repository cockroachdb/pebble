package pebble

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSmootherOff(t *testing.T) {
	smoother := Smoother{enabled: false}
	smoother.mu.estimatedUtilization = 0.5
	smoother.startWork()
	sleepTime := smoother.finishWork(time.Duration(100), false)
	require.Equal(t, time.Duration(0), sleepTime)
}

func TestSleepConverge(t *testing.T) {
	smoother := Smoother{enabled: true}
	smoother.mu.estimatedUtilization = .1
	for x := 0; x < 10000; x++ {
		smoother.startWork()
		smoother.finishWork(100*time.Millisecond, false)
	}
	sleepTime := smoother.finishWork(100*time.Millisecond, false)
	// Sleep time is 100ms * 9 since it is 10% utilized (so 90% idle)
	require.InDelta(t, 900*time.Millisecond, sleepTime, float64(10*time.Microsecond))
}

func TestSleep(t *testing.T) {
	smoother := Smoother{enabled: true}
	smoother.mu.estimatedIterDurationNs = float64(10 * time.Millisecond)
	smoother.mu.estimatedUtilization = 0.5

	smoother.startWork()
	smoother.finishWork(1*time.Millisecond, true)

	require.Equal(t, smoother.mu.estimatedUtilization, 0.5)
	require.InDelta(t, smoother.mu.estimatedIterDurationNs, float64(9*time.Millisecond), float64(time.Millisecond))
}

func TestAvgUtil(t *testing.T) {
	smoother := Smoother{enabled: true}
	smoother.start()
	smoother.mu.Lock()
	smoother.mu.estimatedUtilization = 0.5
	smoother.mu.Unlock()
	for i := 0; i < 11; i++ {
		smoother.startWork()
		time.Sleep(102 * time.Millisecond)
		smoother.finishWork(0, false)
		time.Sleep(102 * time.Millisecond)
	}
	smoother.stop()
	smoother.mu.Lock()
	// It may be exactly 0.5, but should be close.
	require.InDelta(t, 0.5, smoother.mu.estimatedUtilization, 0.01)
	smoother.mu.Unlock()
}
