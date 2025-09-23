// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tieredmeta

import (
	"encoding/binary"
	"math"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
)

type SpanIDStats struct {
	SpanID base.TieringSpanID
}

const tempAgeThreshold = base.TieringAttribute(40)

const numBuckets = 10

// StatsHistogram is a histogram that is bucketed on the TieringAttribute
// axis, and a bucket value records the total size in bytes in that bucket.
//
// We desire the histogram to be concise, while having enough fidelity to
// inform tiering decisions. Additionally, we need to sum histograms for the
// same SpanID. We could design something sophisticated, but instead of
// over-engineering here, we assume that the user has provided an age
// threshold (assuming the tiering attribute is some kind of timestamp), at
// which rows become cold. This is a reasonable assumption since we are
// planning to allow tiering to be specified on a TIMESTAMP/TIMESTAMPTZ column
// with  an expression such as now() + '30 days'. So CockroachDB knows an age
// threshold, and could provide that as an additional field in the
// TieringPolicy.
//
// For now, to avoid plumbing via the TieringPolicy, we hardcode it to 40,
// whatever the units (say 40 days), for histogram initialization.
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
// Age plumbing and age changes: we do linear interpolation to compute the
// bucket counts. Eventually, sstables will get recompacted and new histograms
// will be computed.
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
	// Count is useful to compute the mean size.
	Count uint64
}

type Bucket struct {
	Bytes uint64
}

func (h *StatsHistogram) TotalBytes() uint64 {
	var total uint64
	for i := 0; i < numBuckets; i++ {
		total += h.Buckets[i].Bytes
	}
	total += h.UnderflowBytes
	total += h.UnaccountedBytes
	total += h.ZeroBytes
	return total
}

func (h *StatsHistogram) MeanSize() uint64 {
	if h.Count == 0 {
		return 0
	}
	return h.TotalBytes() / h.Count
}

func (h *StatsHistogram) ApproxColdBytes(coldTierLTThreshold base.TieringAttribute) uint64 {
	var b uint64
	if h.BucketStart <= coldTierLTThreshold {
		b = h.UnderflowBytes
	} else {
		// Else the coldTierLTThreshold is somewhere in the past. This is bad, in
		// that we should have written them to cold storage already, but we
		// haven't. And we have no way of knowing how many of those bytes are
		// actually cold.
		//
		// TODO(sumeer): improve this hack when thinking of a better histogram.
		b = h.UnderflowBytes / 2
	}
	for i := 0; i < numBuckets; i++ {
		bucketEnd := h.BucketStart + (base.TieringAttribute(i)+1)*h.BucketLength
		if bucketEnd <= coldTierLTThreshold {
			b += h.Buckets[i].Bytes
		} else {
			break
		}
	}
	return b
}

const MaxStatsHistogramSizeExcludingBuckets = 6 * binary.MaxVarintLen64

func (h *StatsHistogram) encode() []byte {
	buf := make([]byte, MaxStatsHistogramSizeExcludingBuckets+(numBuckets+1)*binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, uint64(h.BucketStart))
	n += binary.PutUvarint(buf[n:], uint64(h.BucketLength))
	// We encode the number of buckets, to allow for future changes in the struct.
	n += binary.PutUvarint(buf[n:], uint64(numBuckets))
	for i := 0; i < numBuckets; i++ {
		n += binary.PutUvarint(buf[n:], h.Buckets[i].Bytes)
	}
	n += binary.PutUvarint(buf[n:], h.UnderflowBytes)
	n += binary.PutUvarint(buf[n:], h.UnaccountedBytes)
	n += binary.PutUvarint(buf[n:], h.ZeroBytes)
	n += binary.PutUvarint(buf[n:], h.Count)
	return buf[:n]
}

func (h *StatsHistogram) decode(data []byte) error {
	uvarintAndCheck := func() (uint64, error) {
		v, n := binary.Uvarint(data)
		if n <= 0 {
			return 0, errors.AssertionFailedf("could not decode uvarint")
		}
		data = data[n:]
		return v, nil
	}
	if v, err := uvarintAndCheck(); err != nil {
		return err
	} else {
		h.BucketStart = base.TieringAttribute(v)
	}
	if v, err := uvarintAndCheck(); err != nil {
		return err
	} else {
		h.BucketLength = base.TieringAttribute(v)
	}
	if v, err := uvarintAndCheck(); err != nil {
		return err
	} else if v != numBuckets {
		return errors.AssertionFailedf("expected %d buckets, got %d", numBuckets, v)
	}
	for i := 0; i < numBuckets; i++ {
		if v, err := uvarintAndCheck(); err != nil {
			return err
		} else {
			h.Buckets[i].Bytes = v
		}
	}
	if v, err := uvarintAndCheck(); err != nil {
		return err
	} else {
		h.UnderflowBytes = v
	}
	if v, err := uvarintAndCheck(); err != nil {
		return err
	} else {
		h.UnaccountedBytes = v
	}
	if v, err := uvarintAndCheck(); err != nil {
		return err
	} else {
		h.ZeroBytes = v
	}
	if v, err := uvarintAndCheck(); err != nil {
		return err
	} else {
		h.Count = v
	}
	return nil
}

// Merge is used to sum histograms stored in different files. The bucket
// boundaries will be aligned, but they will be shifted since the histograms
// are generated at different times.
func (h *StatsHistogram) Merge(other *StatsHistogram) {
	if h.BucketLength != other.BucketLength {
		panic(errors.AssertionFailedf("bucket length %d != %d", h.BucketLength, other.BucketLength))
	}
	var h2 *StatsHistogram
	if other.BucketStart < h.BucketStart {
		otherCopy := *other
		otherCopy.ageTo(h.BucketStart, h.BucketLength)
		h2 = &otherCopy
	} else {
		h2 = other
		if other.BucketStart > h.BucketStart {
			h.ageTo(other.BucketStart, other.BucketLength)
		}
	}
	for i := 0; i < numBuckets; i++ {
		h.Buckets[i].Bytes += h2.Buckets[i].Bytes
	}
	h.UnderflowBytes += other.UnderflowBytes
	h.UnaccountedBytes += other.UnaccountedBytes
	h.ZeroBytes += other.ZeroBytes
	h.Count += other.Count
}

type interval struct {
	start base.TieringAttribute
	end   base.TieringAttribute
}

func (a interval) intersect(b interval) interval {
	if a.start >= b.end || a.end <= b.start {
		return interval{0, 0}
	}
	return interval{max(a.start, b.start), min(a.end, b.end)}
}

// REQUIRES: a is a subset of b.
func (a interval) fractionOf(b interval) float64 {
	if a.start == a.end {
		return 0
	}
	return float64(a.end-a.start) / float64(b.end-b.start)
}

// INVARIANT: bucketStart > h.BucketStart.
func (h *StatsHistogram) ageTo(
	bucketStart base.TieringAttribute, bucketLength base.TieringAttribute,
) {
	// INVARIANT: ageOffset >= 0.
	ageOffset := bucketStart - h.BucketStart
	numBucketsFullyInUnderflow := min(ageOffset / h.BucketLength)
	offset := min(numBucketsFullyInUnderflow, numBuckets)
	// Oldest buckets are moved to the underflow.
	for i := base.TieringAttribute(0); i < offset; i++ {
		h.UnderflowBytes += h.Buckets[i].Bytes
		h.Buckets[i].Bytes = 0
	}
	if numBucketsFullyInUnderflow > 0 {
		// Newer buckets are shifted to be older.
		for i := offset; i < numBuckets; i++ {
			h.Buckets[i-offset].Bytes = h.Buckets[i].Bytes
		}
		// Remaining buckets are zeroed out.
		for i := numBuckets - offset; i < numBuckets; i++ {
			h.Buckets[i] = Bucket{}
		}
		h.BucketStart += numBucketsFullyInUnderflow * h.BucketLength
		// INVARIANT: h.BucketStart <= bucketStart
	}
	if h.BucketLength != bucketLength {
		// Slow path. The age policy must have changed. Do linear interpolation.
		//
		// The first newInterval is the underflow.
		newInterval := interval{1, bucketStart}
		newIndex := -1
		var newBuckets [numBuckets]Bucket
		for i, oldb := range h.Buckets {
			oldStart := h.BucketStart + base.TieringAttribute(i)*h.BucketLength
			oldInterval := interval{oldStart, oldStart + h.BucketLength}
			for {
				interpolatedBytes :=
					uint64(newInterval.intersect(oldInterval).fractionOf(newInterval) * float64(oldb.Bytes))
				if newIndex < 0 {
					h.UnderflowBytes += interpolatedBytes
				} else if newIndex >= numBuckets {
					h.UnaccountedBytes += interpolatedBytes
				} else {
					newBuckets[newIndex].Bytes += interpolatedBytes
				}
				if newInterval.end > oldInterval.end {
					// Done with oldInterval. Break out of inner loop so that outer loop
					// steps to next oldInterval.
					//
					// NB: When newInterval is the unaccounted "bucket", this condition
					// will always be true.
					break
				}
				// Step new interval.
				newIndex++
				if newIndex < numBuckets {
					newStart := bucketStart + base.TieringAttribute(newIndex)*bucketLength
					newInterval = interval{newStart, newStart + bucketLength}
				} else {
					// The last newInterval is the unaccounted bytes.
					newInterval = interval{bucketStart + numBuckets*bucketLength, math.MaxUint64}
				}
			}
		}
		h.BucketStart = bucketStart
		h.Buckets = newBuckets
		h.BucketLength = bucketLength
	}
	// Else bucket lengths are equal and h.BucketStart == bucketStart.
}

func (h *StatsHistogram) Scale(factor float64) StatsHistogram {
	h2 := *h
	scaleFunc := func(v uint64) uint64 {
		if v == 0 {
			return 0
		}
		return max(1, uint64(float64(v)*factor))
	}
	h2.UnderflowBytes = scaleFunc(h.UnderflowBytes)
	h2.UnaccountedBytes = scaleFunc(h.UnaccountedBytes)
	h2.ZeroBytes = scaleFunc(h.ZeroBytes)
	for i := 0; i < numBuckets; i++ {
		h2.Buckets[i].Bytes = scaleFunc(h.Buckets[i].Bytes)
	}
	h2.Count = scaleFunc(h.Count)
	return h2
}

type histogramWriter struct {
	stats StatsHistogram
}

// TODO(sumeer): Remove tempAgeThreshold and make it a parameter passed by
// TieringHistogramBlockWriter.
func newHistogramWriter(curColdTierLTThreshold base.TieringAttribute) *histogramWriter {
	cur := curColdTierLTThreshold + tempAgeThreshold
	bucketLength := tempAgeThreshold / (numBuckets / 2) // 20% granularity
	lastBucketEnd := ((cur + 2*bucketLength - 1) / bucketLength) * bucketLength
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
	w.stats.Count++
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
