package rate

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSleepConverge(t *testing.T) {
	smoother := NewSmoother()

	i := smoother.Track(func() uint64 { return 0 })
	defer i.close()

	for x := 0; x < 10000; x++ {
		i.recordWork(100 * time.Millisecond)
	}
	smoother.estimatedUtilization.Store(0.1)
	sleepTime := i.recordWork(1 * time.Millisecond)

	// Sleep time is 900ms since it is 10% utilized and 90% idle. The estimated
	// time is based on the 100ms work time estimate.
	require.InDelta(t, 900*time.Millisecond, sleepTime, float64(time.Microsecond))
}

func TestSleep(t *testing.T) {
	s := NewSmoother()
	s.estimatedUtilization.Store(0.5)

	i := s.Track(func() uint64 { return 0 })
	// The previous estimated time is 1ms, so the sleep time should be 1ms at 50% utilization.
	sleepTime := i.recordWork(time.Millisecond)
	require.InDelta(t, sleepTime, time.Duration(1*float64(time.Millisecond)), float64(time.Microsecond))
}

// TestAvgUtil tests that the smoother converges to the correct average utilization.
func TestAvgUtil(t *testing.T) {
	smoother := NewSmoother()
	smoother.Start()
	defer smoother.Stop()

	tick := smoother.Track(func() uint64 { return 8 * 1024 * 1024 })

	// For the first 100ms we are 100% utilized.
	time.Sleep(100 * time.Millisecond)
	tick.close()

	// Now we are not utilized at all.
	time.Sleep(100 * time.Millisecond)

	// Over the last 10s we should have been utilized for 9.9s/10s = 99% of the time.
	fmt.Println(smoother.estimatedUtilization.Load())
	require.InDelta(t, 0.99, smoother.estimatedUtilization.Load(), 0.01)
}
