package pebble

import (
	"testing"
	"time"
)

func TestRateCounter(t *testing.T) {
	var millis int64
	r := newRateCounter(time.Second, 10)
	r.now = func() time.Time {
		return time.Unix(0, millis*1e6)
	}

	testCases := []struct {
		millis   int64
		val      int64
		expected int64
	}{
		{1, 1, 1},
		{2, 1, 2},
		{100, 1, 3},
		{900, 0, 1},
		{100, 0, 0},
		{200, 5, 5},
		{200, 5, 10},
		{200, 5, 15},
		{200, 5, 20},
		{200, 5, 25},
		{200, 5, 25},
		{200, 5, 25},
		{200, 5, 25},
	}
	for i, c := range testCases {
		millis += c.millis
		r.Add(c.val)
		if v := r.Value(); c.expected != v {
			t.Fatalf("%d: expected %d, but found %d\n%d %d\n", i, c.expected, v, r.mu.head, r.mu.buckets)
		}
	}
}
