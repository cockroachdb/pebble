// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package compressionanalyzer

import (
	"math"
	"testing"
)

// almostEqual checks floats within tiny epsilon
func almostEqual(a, b float64) bool {
	const eps = 1e-9
	return math.Abs(a-b) <= eps
}

func TestWelfordBasic(t *testing.T) {
	tests := []struct {
		name          string
		input         []float64
		wantCount     int64
		wantMean      float64
		wantVarPop    float64 // M2/n
		wantVarSample float64 // M2/(n-1)
	}{
		{
			name:          "empty",
			input:         nil,
			wantCount:     0,
			wantMean:      math.NaN(),
			wantVarPop:    math.NaN(),
			wantVarSample: math.NaN(),
		},
		{
			name:          "one value",
			input:         []float64{42},
			wantCount:     1,
			wantMean:      42,
			wantVarPop:    0,
			wantVarSample: math.NaN(),
		},
		{
			name:          "1..5",
			input:         []float64{1, 2, 3, 4, 5},
			wantCount:     5,
			wantMean:      3,
			wantVarPop:    2,   // population variance of 1,2,3,4,5
			wantVarSample: 2.5, // sample variance
		},
		{
			name:          "constant values",
			input:         []float64{7, 7, 7, 7},
			wantCount:     4,
			wantMean:      7,
			wantVarPop:    0,
			wantVarSample: 0,
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
			if !(math.IsNaN(tc.wantMean) && math.IsNaN(gotMean) || almostEqual(gotMean, tc.wantMean)) {
				t.Errorf("Mean = %v; want %v", gotMean, tc.wantMean)
			}
			gotPop := w.Variance()
			if !(math.IsNaN(tc.wantVarPop) && math.IsNaN(gotPop) || almostEqual(gotPop, tc.wantVarPop)) {
				t.Errorf("Variance (pop) = %v; want %v", gotPop, tc.wantVarPop)
			}
			gotSample := w.SampleVariance()
			if !(math.IsNaN(tc.wantVarSample) && math.IsNaN(gotSample) || almostEqual(gotSample, tc.wantVarSample)) {
				t.Errorf("Variance (sample) = %v; want %v", gotSample, tc.wantVarSample)
			}
		})
	}
}
