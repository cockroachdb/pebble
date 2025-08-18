// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tieredmeta

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
)

type SpanIDStats struct {
	SpanID base.TieringSpanID
}

const tempAgeThreshold = base.TieringAttribute(40)

const numBuckets = base.TieringAttribute(10)

// We desire the histogram to be concise, while having enough fidelity to
// inform tiering decisions. Additionally, we need to sum and subtract
// histograms for the same SpanID. We could design something sophisticated,
// but instead of over-engineering here, we assume that the user has provided
// an age threshold (assuming the tiering attribute is some kind of
// timestamp), at which rows become cold. For now, to avoid plumbing, we
// hardcode it to 40, whatever the units (say 40 days), for histogram
// initialization.
//
// We break this up into 20% granularity buckets. So each bucket is 8 wide in
// the above example.
//
// When generating the histogram, we sample the current
// TieringPolicy.ColdTierLTThreshold. Say the value is 500. So the current
// time is ~500+40=540. We find the smallest multiple of the bucket
// granularity that is > current time, which is 544 in this case. Then we keep
// 10 samples in the histogram, each 8 wide, starting from 544+8, so going
// down to 472. That is, we've overlapped with the ColdTierLTThreshold
// sufficiently, if the user changes the policy slightly. Anything older than
// that is fused into one underflow bucket.
//
// Overflows are possible due to application errors (say accidentally writing
// data far in the future). We put these overflows into an unaccounted bucket.
//
// Underflow and overflows are treated differently since we need to add
// histograms that were generated at different times. A prefix of the oldest
// buckets of the older histogram will get merged into the underflow bucket in
// the sum. However, since the overflow will eventually become current, and
// then eventually become underflow, and we can't really track when that
// happens, they are permanently in the unaccounted bucket.
//
// Age plumbing and age changes: TODO(sumeer): we will need to interpolate the
// compute bucket counts, or reread all the old files in the background and
// recompute their histograms. Given that these histograms are stored in the
// files themselves, doing such a recomputation seems wasteful since the
// result will be lost on restart.

type StatsHistogram struct {
	// A BucketStart with a zero value, does not actually start at zero.
	// Instead, it is defined over the interval [1, BucketLength) and is the
	// only bucket which is smaller than BucketLength.
	BucketStart      base.TieringAttribute
	BucketLength     base.TieringAttribute
	Buckets          [numBuckets]Bucket
	UnderflowBytes   uint64
	UnaccountedBytes uint64
	ZeroBytes        uint64
}

type Bucket struct {
	Bytes uint64
}

func (h *StatsHistogram) encode() []byte {
	// TODO(sumeer): serialize the histogram.
	return nil
}

func (h *StatsHistogram) decode(data []byte) error {
	// TODO(sumeer): deserialize the histogram.
	return nil
}

// Add is used to sum histograms stored from different files. The bucket
// boundaries will be aligned, but they will be shifted since the histograms
// are generated at different times.
func (h *StatsHistogram) Add(other *StatsHistogram) {
	if h.BucketLength != other.BucketLength {
		panic(errors.AssertionFailedf("bucket length %d != %d", h.BucketLength, other.BucketLength))
	}
	h.ZeroBytes += other.ZeroBytes
	h.UnaccountedBytes += other.UnaccountedBytes
	h.UnderflowBytes += other.UnderflowBytes
	var buckets [numBuckets]Bucket
	var older *StatsHistogram
	var offset base.TieringAttribute
	if h.BucketStart < other.BucketStart {
		buckets = other.Buckets
		older = h
		offset = other.BucketStart - h.BucketStart
		h.BucketStart = other.BucketStart
	} else {
		buckets = h.Buckets
		older = other
		offset = h.BucketStart - other.BucketStart
	}
	offset /= h.BucketLength
	// Oldest buckets are moved to the underflow.
	for i := base.TieringAttribute(0); i < offset; i++ {
		h.UnderflowBytes += older.Buckets[i].Bytes
	}
	// Newer buckets are shifted to be older.
	for i := offset; i < numBuckets; i++ {
		buckets[i-offset].Bytes += older.Buckets[i].Bytes
	}
	h.Buckets = buckets
}

// Subtract requires that h represents a histogram to which other was
// previously added. It is used when a file corresponding to other has been
// compacted away.
func (h *StatsHistogram) Subtract(other *StatsHistogram) {
	// TODO(sumeer): implement.
}

type histogramWriter struct {
	stats StatsHistogram
}

func newHistogramWriter(curColdTierLTThreshold base.TieringAttribute) *histogramWriter {
	cur := curColdTierLTThreshold + tempAgeThreshold
	bucketLength := tempAgeThreshold / (numBuckets / 2) // 20% granularity
	lastBucketEnd := (cur + 2*bucketLength - 1) / bucketLength
	var bucketStart base.TieringAttribute
	if lastBucketEnd >= (bucketLength * numBuckets) {
		bucketStart = lastBucketEnd - (bucketLength * numBuckets)
	} else {
		bucketStart = 0
	}
	return &histogramWriter{
		stats: StatsHistogram{
			BucketStart:  bucketStart,
			BucketLength: bucketLength,
		},
	}
}

func (w *histogramWriter) record(attr base.TieringAttribute, bytes uint64) {
	if attr == 0 {
		w.stats.ZeroBytes += bytes
		return
	}
	if attr < w.stats.BucketStart {
		w.stats.UnderflowBytes += bytes
		return
	}
	bucketIndex := (attr - w.stats.BucketStart) / w.stats.BucketLength
	if bucketIndex >= numBuckets {
		w.stats.UnaccountedBytes += bytes
		return
	}
	w.stats.Buckets[bucketIndex].Bytes += bytes
}

func (w *histogramWriter) encode() []byte {
	return w.stats.encode()
}
