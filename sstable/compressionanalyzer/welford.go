// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package compressionanalyzer

import "math"

// Welford maintains running statistics for mean and variance using Welford's
// algorithm.
type Welford struct {
	count int64
	mean  float64
	m2    float64
}

// Add incorporates a new data point x into the running statistics.
func (w *Welford) Add(x float64) {
	w.count++
	delta := x - w.mean
	w.mean += delta / float64(w.count)
	delta2 := x - w.mean
	w.m2 += delta * delta2
}

// Count returns the number of values that have been added.
func (w *Welford) Count() int64 {
	return w.count
}

// Mean returns the current running mean.
// If no values have been added, returns NaN.
func (w *Welford) Mean() float64 {
	if w.count == 0 {
		return math.NaN()
	}
	return w.mean
}

// Variance returns the population variance (M2/n). Returns NaN if no values.
func (w *Welford) Variance() float64 {
	if w.count == 0 {
		return math.NaN()
	}
	return w.m2 / float64(w.count)
}

// SampleVariance returns the sample variance (M2/(n-1)). Returns NaN if fewer than 2 values.
func (w *Welford) SampleVariance() float64 {
	if w.count < 2 {
		return math.NaN()
	}
	return w.m2 / float64(w.count-1)
}

// StandardDeviation returns the population standard deviation.
func (w *Welford) StandardDeviation() float64 {
	return math.Sqrt(w.Variance())
}

// SampleStandardDeviation returns the sample standard deviation.
func (w *Welford) SampleStandardDeviation() float64 {
	return math.Sqrt(w.SampleVariance())
}
