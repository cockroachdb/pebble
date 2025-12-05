// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metricsutil

import (
	"math"
	"testing"
)

// almostEqual checks floats within tiny epsilon
func almostEqual(a, b float64) bool {
	const eps = 1e-5
	return math.Abs(a-b) <= eps
}

func TestWelfordBasic(t *testing.T) {
	tests := []struct {
		name          string
		input         []float64
		wantCount     int64
		wantMean      float64
		wantVarSample float64 // M2/(n-1)
	}{
		{
			name:          "empty",
			input:         nil,
			wantCount:     0,
			wantMean:      0,
			wantVarSample: 0,
		},
		{
			name:          "one value",
			input:         []float64{42},
			wantCount:     1,
			wantMean:      42,
			wantVarSample: 0,
		},
		{
			name:          "1..5",
			input:         []float64{1, 2, 3, 4, 5},
			wantCount:     5,
			wantMean:      3,
			wantVarSample: 2.5, // sample variance
		},
		{
			name:          "constant values",
			input:         []float64{7, 7, 7, 7},
			wantCount:     4,
			wantMean:      7,
			wantVarSample: 0,
		},
		{
			name:          "other",
			input:         []float64{1, 1, 1, 3, 5, 5, 5, 10, 12, 12, 12, 12},
			wantCount:     12,
			wantMean:      6.58 + 0.01/3,
			wantVarSample: 22.08 + 0.01/3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var w Welford
			for _, x := range tc.input {
				w.Add(x)
			}
			if got := w.Count(); got != tc.wantCount {
				t.Errorf("Count = %d; want %d", got, tc.wantCount)
			}
			gotMean := w.Mean()
			if !almostEqual(gotMean, tc.wantMean) {
				t.Errorf("Mean = %v; want %v", gotMean, tc.wantMean)
			}
			gotSample := w.Variance()
			if !almostEqual(gotSample, tc.wantVarSample) {
				t.Errorf("Variance (sample) = %v; want %v", gotSample, tc.wantVarSample)
			}
		})
	}
}

func TestWeightedWelford(t *testing.T) {
	tests := []struct {
		name          string
		input         []float64
		weights       []uint64
		wantMean      float64
		wantVarSample float64 // M2/(n-1)
	}{
		{
			name:          "empty",
			input:         nil,
			wantMean:      0,
			wantVarSample: 0,
		},
		{
			name:          "one value",
			input:         []float64{42},
			weights:       []uint64{1},
			wantMean:      42,
			wantVarSample: 0,
		},
		{
			name:          "constant values",
			input:         []float64{7},
			weights:       []uint64{10},
			wantMean:      7,
			wantVarSample: 0,
		},
		{
			name:          "1..5",
			input:         []float64{1, 2, 3, 4, 5},
			weights:       []uint64{1, 1, 1, 1, 1},
			wantMean:      3,
			wantVarSample: 2.5, // sample variance
		},
		{
			name:          "other",
			input:         []float64{1, 3, 5, 10, 12},
			weights:       []uint64{3, 1, 3, 1, 4},
			wantMean:      6.58 + 0.01/3,
			wantVarSample: 22.08 + 0.01/3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var w WeightedWelford
			for i := range tc.input {
				w.Add(tc.input[i], tc.weights[i])
			}
			gotMean := w.Mean()
			if !almostEqual(gotMean, tc.wantMean) {
				t.Errorf("Mean = %v; want %v", gotMean, tc.wantMean)
			}
			gotSample := w.Variance()
			if !almostEqual(gotSample, tc.wantVarSample) {
				t.Errorf("Variance (sample) = %v; want %v", gotSample, tc.wantVarSample)
			}
		})
	}
}
