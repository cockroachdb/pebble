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
// If no values have been added, returns 0.
func (w *Welford) Mean() float64 {
	return w.mean
}

// SampleVariance returns the sample variance (M2/(n-1)). Returns 0 if fewer than 2 values.
func (w *Welford) SampleVariance() float64 {
	if w.count < 2 {
		return 0
	}
	return w.m2 / float64(w.count-1)
}

// SampleStandardDeviation returns the sample standard deviation.
func (w *Welford) SampleStandardDeviation() float64 {
	return math.Sqrt(w.SampleVariance())
}

// WeightedWelford maintains running statistics for mean and variance using
// an extension of Welford's algorithm which allows for weighted samples; see
// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Weighted_incremental_algorithm
type WeightedWelford struct {
	wSum  float64
	w2Sum float64
	mean  float64
	s     float64
}

// Add incorporates a new data point x with the given frequency into the running
// statistics.
func (ww *WeightedWelford) Add(x float64, frequency uint64) {
	w := float64(frequency)
	ww.wSum += w
	ww.w2Sum += w * w
	delta := x - ww.mean
	ww.mean += delta * (w / ww.wSum)
	delta2 := x - ww.mean
	ww.s += w * delta * delta2
}

// Mean returns the current running mean.
// If no values have been added, returns 0.
func (ww *WeightedWelford) Mean() float64 {
	return ww.mean
}

// SampleVariance returns the sample variance (M2/(n-1)). Returns 0 if the sum
// of added frequencies is less than 2.
func (ww *WeightedWelford) SampleVariance() float64 {
	if ww.wSum < 2 {
		return 0
	}
	return ww.s / (ww.wSum - 1)
}

// SampleStandardDeviation returns the sample standard deviation.
func (ww *WeightedWelford) SampleStandardDeviation() float64 {
	return math.Sqrt(ww.SampleVariance())
}
