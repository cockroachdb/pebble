// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package ewma

import (
	"math"

	"github.com/cockroachdb/pebble/internal/invariants"
)

// Bytes is an estimator for an arbitrary value that is sampled from byte
// blocks.
//
// Consider a stream of data which is divided into blocks of varying size. We
// want to estimate a value (like compression ratio) based on the values from
// recent blocks.
//
// Bytes implements a per-byte exponential moving average (EWMA) estimator: let
// pos_i and val_i be the position and value of each byte for which we have
// data; the estimate at position p is the weighted sum:
//
//		Sum_i val_i*(1-alpha)^(p-pos_i)
//	  -------------------------------
//	     Sum_i (1-alpha)^(p-pos_i)
type Bytes struct {
	alpha       float64
	sum         float64
	totalWeight float64
	gap         int64
}

// Init the estimator such that a block sampled <half-life> bytes ago has half
// the weight compared to a block sampled now.
//
// Intuitively, half of the estimate comes from the values within the half-life
// window; and 75% of the estimate comes from values within 2x half-life.
func (b *Bytes) Init(halfLife int64) {
	*b = Bytes{}
	// Exact value is 1 - 2^(-1/H). The straightforward calculation suffers from
	// precision loss as H grows (we are subtracting two nearly equal numbers). We
	// use a numerically stable alternative:
	//   1 - 2^(-1/H) = 1 - e^(-ln(2)/H) = -expm1(-ln(2)/H)
	b.alpha = -math.Expm1(-math.Ln2 / float64(halfLife))
}

// Estimate returns the current estimate of the value, based on the recent
// SampledBlock() calls. Returns NaN if no blocks have been sampled yet.
func (b *Bytes) Estimate() float64 {
	return b.sum / b.totalWeight
}

// NoSample informs the estimator that a block of the given length was not
// sampled.
func (b *Bytes) NoSample(numBytes int64) {
	if numBytes < 0 {
		if invariants.Enabled {
			panic("invalid numBytes")
		}
		return
	}
	// It would be equivalent (but less efficient) to multiply both sum and
	// totalWeight by (1-alpha)^numBytes instead of keeping track of the gap.
	b.gap += numBytes
}

// SampledBlock informs the estimator that a block of the given length was
// sampled.
func (b *Bytes) SampledBlock(numBytes int64, value float64) {
	if numBytes < 1 {
		if invariants.Enabled {
			panic("invalid numBytes")
		}
		return
	}
	decay := b.decay(b.gap + numBytes)
	b.sum *= decay
	b.totalWeight *= decay
	b.gap = 0

	// The sum of weights for the new bytes is:
	//
	//                                   1 - (1 - alpha)^numBytes
	//       Sum       (1 - alpha)^i  =  ------------------------
	//   0≤i<numBytes                             alpha
	//
	// We can drop the 1/alpha factor from all weights (it cancels out).
	w := 1 - b.decay(numBytes)
	b.sum += value * w
	b.totalWeight += w
}

// decay returns (1 - alpha)^n for the given n.
func (b *Bytes) decay(n int64) float64 {
	// (1 - alpha)^n = e^(n * log(1 - alpha)). Using Exp and Log1p is stable for
	// very small alpha (unlike math.Pow).
	return math.Exp(float64(n) * math.Log1p(-b.alpha))
}
