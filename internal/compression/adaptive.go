// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package compression

import (
	"math"
	"math/rand/v2"
	"sync"

	"github.com/cockroachdb/pebble/v2/internal/ewma"
)

// AdaptiveCompressor is a Compressor that automatically chooses between two
// algorithms: it uses a slower but better algorithm as long as it reduces the
// compressed size (compared to the faster algorithm) by a certain relative
// amount. The decision is probabilistic and based on sampling a subset of
// blocks.
type AdaptiveCompressor struct {
	fast Compressor
	slow Compressor

	reductionCutoff float64
	sampleEvery     int

	// estimator for the relative size reduction when choosing the slow algorithm.
	estimator ewma.Bytes
	rng       rand.PCG

	buf []byte
}

// AdaptiveCompressorParams contains the parameters for an adaptive compressor.
type AdaptiveCompressorParams struct {
	// Fast and Slow are the two compression settings the adaptive compressor
	// chooses between.
	Fast Setting
	Slow Setting
	// ReductionCutoff is the relative size reduction (when using the slow
	// algorithm vs the fast algorithm) below which we use the fast algorithm. For
	// example, if ReductionCutoff is 0.3 then we only use the slow algorithm if
	// it reduces the compressed size (compared to the fast algorithm) by at least
	// 30%.
	ReductionCutoff float64
	// SampleEvery defines the sampling frequency: the probability we sample a
	// block is 1.0/SampleEvery. Sampling means trying both algorithms and
	// recording the compression ratio.
	SampleEvery int
	// SampleHalfLife defines the half-life of the exponentially weighted moving
	// average. It should be a factor larger than the expected average block size.
	SampleHalfLife int64
	SamplingSeed   uint64
}

func NewAdaptiveCompressor(p AdaptiveCompressorParams) *AdaptiveCompressor {
	ac := adaptiveCompressorPool.Get().(*AdaptiveCompressor)
	ac.fast = GetCompressor(p.Fast)
	ac.slow = GetCompressor(p.Slow)
	ac.sampleEvery = p.SampleEvery
	ac.reductionCutoff = p.ReductionCutoff
	ac.estimator.Init(p.SampleHalfLife)
	ac.rng.Seed(p.SamplingSeed, p.SamplingSeed)
	return ac
}

var _ Compressor = (*AdaptiveCompressor)(nil)

var adaptiveCompressorPool = sync.Pool{
	New: func() any { return &AdaptiveCompressor{} },
}

func (ac *AdaptiveCompressor) Compress(dst, src []byte) ([]byte, Setting) {
	estimate := ac.estimator.Estimate()
	// TODO(radu): consider decreasing the sampling frequency if the estimate is
	// far from the cutoff.
	sampleThisBlock := math.IsNaN(estimate) || ac.rng.Uint64()%uint64(ac.sampleEvery) == 0
	if !sampleThisBlock {
		ac.estimator.NoSample(int64(len(src)))
		if estimate < ac.reductionCutoff {
			return ac.fast.Compress(dst, src)
		} else {
			return ac.slow.Compress(dst, src)
		}
	}
	bufFast, fastSetting := ac.fast.Compress(ac.buf[:0], src)
	ac.buf = bufFast[:0]
	dst, slowSetting := ac.slow.Compress(dst, src)
	reduction := 1 - float64(len(dst))/float64(len(bufFast))
	ac.estimator.SampledBlock(int64(len(src)), reduction)
	if reduction < ac.reductionCutoff {
		return append(dst[:0], bufFast...), fastSetting
	}
	return dst, slowSetting
}

func (ac *AdaptiveCompressor) Close() {
	ac.fast.Close()
	ac.slow.Close()
	if cap(ac.buf) > 256*1024 {
		ac.buf = nil // Release large buffers.
	}
	adaptiveCompressorPool.Put(ac)
}
