package pebble

import (
	"fmt"
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

func TestMultipleRunners(t *testing.T) {
	smoother := Smoother{enabled: true}
	smoother.start()
	smoother.mu.estimatedUtilization = 1
	for i := 0; i < 11; i++ {
		for j := 0; j < 10; j++ {
			smoother.startWork()
		}
		time.Sleep(102 * time.Millisecond)
		for j := 0; j < 10; j++ {
			smoother.finishWork(0, true)
		}
	}
	smoother.stop()
	fmt.Println(smoother.mu.estimatedUtilization)
	// The estimate should be about 1% closer to 10 starting at 1.0.
	require.InDelta(t, 1.09, smoother.mu.estimatedUtilization, 0.001)
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
	smoother.mu.estimatedUtilization = 0.5
	for i := 0; i < 11; i++ {
		smoother.startWork()
		time.Sleep(102 * time.Millisecond)
		smoother.finishWork(0, false)
		time.Sleep(102 * time.Millisecond)
	}
	smoother.stop()
	// It should never be exactly 0.5, but should be close.  This test has a
	// potential to randomly fail, so think about ways to fix it.
	require.InDelta(t, 0.5, smoother.mu.estimatedUtilization, 0.01)
}
